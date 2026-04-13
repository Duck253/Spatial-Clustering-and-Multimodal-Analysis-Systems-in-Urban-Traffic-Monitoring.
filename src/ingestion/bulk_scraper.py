"""
bulk_scraper.py
---------------
Thu thập hàng loạt bài báo giao thông từ Google News RSS + mở rộng RSS nguồn.
Chạy một lần (hoặc định kỳ) để bơm tối đa data vào DB — dùng cùng pipeline
dedup/filter của news_scraper.py.

Sử dụng:
    python src/ingestion/bulk_scraper.py
"""

import sys
import time
import feedparser
from datetime import datetime

sys.stdout.reconfigure(encoding='utf-8')

from src.core.db_manager import get_connection, release_connection
from src.utils.text_helpers import content_hash, get_jaccard_sim, has_traffic_features

# ── Google News RSS — mỗi query trả ~100 bài từ nhiều báo khác nhau ──────────
# Định dạng: ?q=<từ khóa>&hl=vi&gl=VN&ceid=VN:vi
_GN_BASE = "https://news.google.com/rss/search?hl=vi&gl=VN&ceid=VN:vi&q="

GOOGLE_NEWS_QUERIES = [
    # Tắc đường / ùn tắc
    "tắc đường hà nội",
    "ùn tắc giao thông hà nội",
    "kẹt xe hà nội",
    "ùn ứ hà nội",
    # Tai nạn
    "tai nạn giao thông hà nội",
    "tai nạn đường bộ hà nội",
    "va chạm giao thông hà nội",
    # Ngập lụt
    "ngập đường hà nội",
    "ngập lụt giao thông hà nội",
    # Thi công / lô cốt
    "lô cốt hà nội",
    "thi công cầu đường hà nội",
    "cấm đường hà nội",
    # Sự kiện lớn
    "phong tỏa đường hà nội",
    "sự cố giao thông hà nội",
    # Site-specific — Kinh tế Đô thị & Hà Nội Mới (không có RSS công khai)
    "giao thông hà nội site:kinhtedothi.vn",
    "giao thông hà nội site:hanoimoi.vn",
]

# ── RSS mở rộng — các chuyên mục sâu hơn từ báo lớn ─────────────────────────
EXTENDED_RSS = [
    # VnExpress — thêm chuyên mục giao thông riêng
    {"name": "VnExpress Giao thông", "url": "https://vnexpress.net/rss/giao-thong.rss"},
    {"name": "VnExpress Xã hội",     "url": "https://vnexpress.net/rss/xa-hoi.rss"},
    # Tuổi Trẻ
    {"name": "Tuổi Trẻ Hà Nội",     "url": "https://tuoitre.vn/rss/ha-noi.rss"},
    # An ninh Thủ đô — báo chuyên về Hà Nội
    {"name": "An ninh Thủ đô",       "url": "https://anninhthudo.vn/rss/giao-thong.xml"},
    # Pháp luật TP.HCM — có mục giao thông
    {"name": "Pháp Luật Online",     "url": "https://plo.vn/rss/giao-thong-157.rss"},
    # Lao Động
    {"name": "Lao Động",             "url": "https://laodong.vn/rss/giao-thong.rss"},
    # VOV
    {"name": "VOV Giao thông",       "url": "https://vov.vn/rss/giao-thong.rss"},
    # Vietnamnet
    {"name": "VietnamNet GT",        "url": "https://vietnamnet.vn/rss/giao-thong.rss"},
    # Báo Giao thông (chuyên ngành)
    {"name": "Báo Giao thông",       "url": "https://www.baogiaothong.vn/rss/home.rss"},
]

JACCARD_THRESHOLD = 0.6
DEDUP_WINDOW_HOURS = 72   # mở rộng cửa sổ dedup lên 3 ngày cho bulk run
REQUEST_DELAY = 1.0       # giây — tránh bị chặn


# ── Helpers ──────────────────────────────────────────────────────────────────

def _build_google_news_url(query: str) -> str:
    encoded = query.replace(" ", "+")
    return f"{_GN_BASE}{encoded}"


def _get_or_create_source(cursor, name: str, url: str) -> int:
    cursor.execute(
        """INSERT INTO data_source (source_type, source_url, source_name)
           VALUES ('rss', %s, %s) ON CONFLICT (source_url) DO UPDATE SET source_name = EXCLUDED.source_name""",
        (url, name)
    )
    cursor.execute("SELECT source_id FROM data_source WHERE source_url = %s", (url,))
    return cursor.fetchone()[0]


def _load_recent_hashes(cursor) -> set:
    from datetime import timedelta
    cursor.execute(
        "SELECT content_hash FROM raw_feed WHERE fetched_at > NOW() - INTERVAL '%s hours'",
        (DEDUP_WINDOW_HOURS,)
    )
    return {row[0] for row in cursor.fetchall()}


def _load_recent_contents(cursor) -> list:
    cursor.execute(
        "SELECT raw_content FROM raw_feed WHERE fetched_at > NOW() - INTERVAL '%s hours'",
        (DEDUP_WINDOW_HOURS,)
    )
    return [row[0] for row in cursor.fetchall()]


def _is_duplicate(text: str, hashes: set, contents: list) -> bool:
    h = content_hash(text)
    if h in hashes:
        return True
    for old in contents:
        if get_jaccard_sim(text, old) >= JACCARD_THRESHOLD:
            return True
    return False


def _save_article(cursor, source_id: int, text: str, hashes: set, contents: list) -> bool:
    """Lưu 1 bài vào raw_feed. Trả về True nếu lưu thành công."""
    if not has_traffic_features(text):
        return False
    if _is_duplicate(text, hashes, contents):
        return False

    h = content_hash(text)
    cursor.execute(
        """INSERT INTO raw_feed (source_id, content_type, raw_content, content_hash, fetched_at, is_processed)
           VALUES (%s, 'text', %s, %s, NOW(), false)""",
        (source_id, text, h)
    )
    hashes.add(h)
    contents.append(text)
    return True


# ── Scrape một feed URL ───────────────────────────────────────────────────────

def _scrape_feed(cursor, name: str, url: str, hashes: set, contents: list) -> int:
    try:
        feed = feedparser.parse(url)
    except Exception as e:
        print(f"  ⚠️  [{name}] Lỗi fetch: {e}")
        return 0

    if not feed.entries:
        print(f"  ⚠️  [{name}] Feed trống hoặc URL sai")
        return 0

    source_id = _get_or_create_source(cursor, name, url)
    saved = 0
    for entry in feed.entries:
        title = getattr(entry, 'title', '')
        desc  = getattr(entry, 'description', '') or getattr(entry, 'summary', '')
        text  = f"{title}. {desc}".strip()
        if _save_article(cursor, source_id, text, hashes, contents):
            saved += 1

    return saved


# ── Main ─────────────────────────────────────────────────────────────────────

def run_bulk_scraper():
    conn = get_connection()
    if not conn:
        print("❌ Không kết nối được DB.")
        return

    cursor = conn.cursor()
    total_saved = 0
    total_sources = 0
    start_time = datetime.now()

    try:
        # Load cache dedup một lần
        hashes   = _load_recent_hashes(cursor)
        contents = _load_recent_contents(cursor)
        print(f"[BULK] Cache dedup: {len(hashes)} bài trong {DEDUP_WINDOW_HOURS}h gần nhất\n")

        # ── 1. Google News RSS (14 query × ~100 bài) ──────────────────────
        print("━" * 55)
        print(f"[1/2] GOOGLE NEWS RSS — {len(GOOGLE_NEWS_QUERIES)} từ khóa")
        print("━" * 55)

        for query in GOOGLE_NEWS_QUERIES:
            url   = _build_google_news_url(query)
            saved = _scrape_feed(cursor, f"GNews: {query}", url, hashes, contents)
            conn.commit()  # commit từng query để không mất nếu bị lỗi giữa chừng
            total_sources += 1
            total_saved   += saved
            print(f"  ✅ '{query}': +{saved} bài")
            time.sleep(REQUEST_DELAY)

        # ── 2. Extended RSS ───────────────────────────────────────────────
        print(f"\n{'━' * 55}")
        print(f"[2/2] EXTENDED RSS — {len(EXTENDED_RSS)} nguồn báo")
        print("━" * 55)

        for source in EXTENDED_RSS:
            saved = _scrape_feed(cursor, source['name'], source['url'], hashes, contents)
            conn.commit()
            total_sources += 1
            total_saved   += saved
            print(f"  ✅ {source['name']:<25}: +{saved} bài")
            time.sleep(REQUEST_DELAY)

    except Exception as e:
        conn.rollback()
        print(f"\n❌ Lỗi: {e}")
        import traceback; traceback.print_exc()
    finally:
        cursor.close()
        release_connection(conn)

    elapsed = (datetime.now() - start_time).seconds
    print(f"\n{'═' * 55}")
    print(f"✅ BULK SCRAPE HOÀN TẤT")
    print(f"   Nguồn đã quét : {total_sources}")
    print(f"   Bài mới lưu   : {total_saved}")
    print(f"   Thời gian      : {elapsed}s")
    print(f"{'═' * 55}")
    print(f"\n👉 Chạy NLP để xử lý {total_saved} bài mới:")
    print(f"   python src/processing/nlp_engine.py")


if __name__ == "__main__":
    run_bulk_scraper()
