import feedparser
from datetime import datetime, timedelta
from src.core.db_manager import get_connection, release_connection
from src.utils.text_helpers import content_hash, get_jaccard_sim, has_traffic_features

RSS_SOURCES = [
    # ── Báo lớn toàn quốc ──────────────────────────────────────────
    {"name": "VnExpress",      "url": "https://vnexpress.net/rss/thoi-su.rss"},
    {"name": "VnExpress GT",   "url": "https://vnexpress.net/rss/giao-thong.rss"},
    {"name": "Tuổi Trẻ",       "url": "https://tuoitre.vn/rss/giao-thong.rss"},
    {"name": "Tiền Phong",     "url": "https://tienphong.vn/rss/xa-hoi.rss"},
    {"name": "Kênh14",         "url": "https://kenh14.vn/xa-hoi.rss"},
    # ── Google News RSS — tổng hợp đa nguồn theo từ khóa ──────────
    {"name": "GG-KẹtXeHN",    "url": "https://news.google.com/rss/search?q=k%E1%BA%B9t+xe+H%C3%A0+N%E1%BB%99i&hl=vi&gl=VN&ceid=VN:vi"},
    {"name": "GG-TaiNanHN",   "url": "https://news.google.com/rss/search?q=tai+n%E1%BA%A1n+giao+th%C3%B4ng+H%C3%A0+N%E1%BB%99i&hl=vi&gl=VN&ceid=VN:vi"},
    {"name": "GG-NgapHN",     "url": "https://news.google.com/rss/search?q=ng%E1%BA%ADp+%C4%91%C6%B0%E1%BB%9Dng+H%C3%A0+N%E1%BB%99i&hl=vi&gl=VN&ceid=VN:vi"},
]

JACCARD_THRESHOLD = 0.6   # ngưỡng gọi là "trùng gần đúng"
DEDUP_WINDOW_HOURS = 24   # chỉ so sánh tin trong 24h gần nhất


def _ensure_schema(cursor):
    """Đảm bảo cột content_hash và index tồn tại (idempotent)"""
    cursor.execute("ALTER TABLE raw_feed ADD COLUMN IF NOT EXISTS content_hash VARCHAR(32)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_raw_feed_hash ON raw_feed(content_hash)")


def _load_recent_hashes(cursor) -> set:
    """Load toàn bộ hash của tin trong 24h gần nhất để check O(1)"""
    cursor.execute(
        "SELECT content_hash FROM raw_feed WHERE fetched_at > %s",
        (datetime.now() - timedelta(hours=DEDUP_WINDOW_HOURS),)
    )
    return {row[0] for row in cursor.fetchall()}


def _load_recent_contents(cursor) -> list:
    """Load nội dung gần đây để Jaccard check — chỉ dùng khi hash chưa khớp"""
    cursor.execute(
        "SELECT raw_content FROM raw_feed WHERE fetched_at > %s",
        (datetime.now() - timedelta(hours=DEDUP_WINDOW_HOURS),)
    )
    return [row[0] for row in cursor.fetchall()]


def is_duplicate(new_content: str, recent_hashes: set, recent_contents: list) -> bool:
    # ── Bước 1: Hash check O(1) ─────────────────────────────────────
    if content_hash(new_content) in recent_hashes:
        return True

    # ── Bước 2: Jaccard check (near-duplicate) ───────────────────────
    for old_content in recent_contents:
        if get_jaccard_sim(new_content, old_content) >= JACCARD_THRESHOLD:
            return True

    return False


def _get_or_create_source(cursor, source: dict) -> int:
    cursor.execute(
        """INSERT INTO data_source (source_type, source_url, source_name)
           VALUES ('rss', %s, %s) ON CONFLICT (source_url) DO UPDATE SET source_name = EXCLUDED.source_name""",
        (source['url'], source['name'])
    )
    cursor.execute("SELECT source_id FROM data_source WHERE source_url = %s", (source['url'],))
    return cursor.fetchone()[0]


def run_scraper():
    conn = get_connection()
    if not conn:
        return
    cursor = conn.cursor()

    total_saved = 0
    total_skipped_traffic = 0
    total_skipped_dup = 0

    try:
        _ensure_schema(cursor)
        conn.commit()

        # Load dữ liệu gần đây một lần duy nhất (dùng chung cho tất cả nguồn)
        recent_hashes   = _load_recent_hashes(cursor)
        recent_contents = _load_recent_contents(cursor)

        for source in RSS_SOURCES:
            try:
                feed = feedparser.parse(source['url'])
            except Exception as e:
                print(f"  [SCRAPER] ⚠️ Lỗi đọc feed {source['name']}: {e}")
                continue

            if not feed.entries:
                print(f"  [SCRAPER] ⚠️ {source['name']}: không có bài nào (feed trống hoặc URL sai)")
                continue

            source_id = _get_or_create_source(cursor, source)
            saved_from_source = 0

            for entry in feed.entries:
                title = getattr(entry, 'title', '')
                desc  = getattr(entry, 'description', '') or getattr(entry, 'summary', '')
                full_text = f"{title}. {desc}".strip()

                if not has_traffic_features(full_text):
                    total_skipped_traffic += 1
                    continue

                if is_duplicate(full_text, recent_hashes, recent_contents):
                    total_skipped_dup += 1
                    continue

                h = content_hash(full_text)
                cursor.execute(
                    """INSERT INTO raw_feed (source_id, content_type, raw_content, content_hash, fetched_at, is_processed)
                       VALUES (%s, 'text', %s, %s, NOW(), false)""",
                    (source_id, full_text, h)
                )

                # Cập nhật cache trong bộ nhớ để tránh trùng với tin cùng batch
                recent_hashes.add(h)
                recent_contents.append(full_text)

                saved_from_source += 1
                total_saved += 1

            if saved_from_source:
                print(f"  [SCRAPER] {source['name']}: +{saved_from_source} tin mới")

        conn.commit()
        print(f"[SCRAPER] Tổng kết: lưu {total_saved} | bỏ không phải GT: {total_skipped_traffic} | bỏ trùng: {total_skipped_dup}")

    except Exception as e:
        print(f"[SCRAPER] ❌ Lỗi: {e}")
        conn.rollback()
    finally:
        cursor.close()
        release_connection(conn)