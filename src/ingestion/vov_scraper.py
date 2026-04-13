"""
vov_scraper.py
--------------
Thu thập bài báo text từ vovgiaothong.vn — trang web chính thức của
đài VOV Giao thông (radio chuyên về giao thông Hà Nội & toàn quốc).

Vì vovgiaothong.vn không có RSS đầy đủ, scraper này lấy link bài
từ các trang category rồi tải và trích xuất nội dung từng bài.

Category được quét:
  - /check-var-giao-thong/  — kiểm tra vi phạm, camera phạt nguội
  - /thoi-su/               — thời sự giao thông
  - /xe/                    — xe cộ, phương tiện
  - /do-thi/                — đô thị, hạ tầng
  - /                       — trang chủ (bài nổi bật)

Chạy độc lập:
    python src/ingestion/vov_scraper.py
"""

import sys
import re
import time
import urllib.request
import urllib.error
from datetime import datetime

sys.stdout.reconfigure(encoding="utf-8")

from src.core.db_manager import get_connection, release_connection
from src.utils.text_helpers import content_hash, get_jaccard_sim, has_traffic_features

# ── Cấu hình ──────────────────────────────────────────────────────────────────

BASE_URL = "https://vovgiaothong.vn"

CATEGORIES = [
    "/check-var-giao-thong/",
    "/thoi-su/",
    "/xe/",
    "/do-thi/",
    "/",          # trang chủ
]

SOURCE_NAME = "VOV Giao thông"
SOURCE_URL  = BASE_URL

REQUEST_DELAY    = 1.2   # giây giữa mỗi request
REQUEST_TIMEOUT  = 12    # giây timeout mỗi request
JACCARD_THRESHOLD = 0.6
DEDUP_WINDOW_HOURS = 24

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "vi-VN,vi;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


# ── HTTP helpers ───────────────────────────────────────────────────────────────

def _fetch_html(url: str) -> str | None:
    """Tải HTML của một URL. Trả về None nếu lỗi."""
    try:
        req = urllib.request.Request(url, headers=_HEADERS)
        resp = urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT)
        return resp.read().decode("utf-8", errors="ignore")
    except urllib.error.HTTPError as e:
        print(f"  [VOV] HTTP {e.code}: {url}")
        return None
    except Exception as e:
        print(f"  [VOV] Lỗi fetch {url}: {e}")
        return None


# ── Link extraction ────────────────────────────────────────────────────────────

def _extract_article_links(html: str) -> list[str]:
    """Lấy tất cả link bài viết (.html) trong một trang category."""
    pattern = r'href="(https://vovgiaothong\.vn/[a-z0-9\-/]+\.html)"'
    links = re.findall(pattern, html)
    # Lọc bỏ các trang không phải bài viết (quảng cáo, liên hệ...)
    skip = {"lien-he", "gioi-thieu", "quang-cao", "tuyen-dung"}
    filtered = [l for l in links if not any(s in l for s in skip)]
    return list(set(filtered))


# ── Article content extraction ─────────────────────────────────────────────────

def _clean_html(raw: str) -> str:
    """Xóa HTML tags, khoảng trắng thừa."""
    text = re.sub(r"<[^>]+>", " ", raw)
    text = re.sub(r"&nbsp;", " ", text)
    text = re.sub(r"&[a-z]+;", "", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _extract_article_text(html: str) -> str:
    """
    Trích xuất nội dung bài viết từ HTML của vovgiaothong.vn.
    Kết hợp: tiêu đề + mô tả (meta) + đoạn thân bài.
    """
    # ── Tiêu đề ────────────────────────────────────────────────────────────
    title = ""
    m = re.search(r"<h1[^>]*>(.*?)</h1>", html, re.S)
    if m:
        title = _clean_html(m.group(1))

    # ── Mô tả (meta description) ───────────────────────────────────────────
    desc = ""
    m = re.search(r'<meta[^>]*name=["\']description["\'][^>]*content=["\']([^"\']+)', html)
    if m:
        desc = m.group(1).strip()

    # ── Thân bài — tìm trong detail-content ───────────────────────────────
    body_text = ""
    m = re.search(r'class="detail-content[^"]*"[^>]*>(.*?)</section', html, re.S)
    if not m:
        # fallback: tìm div chứa nhiều <p>
        m = re.search(r'class="[^"]*content[^"]*"[^>]*>(.*?)</div>\s*</div>', html, re.S)
    if m:
        body_html = m.group(1)
        paras = re.findall(r"<p[^>]*>(.*?)</p>", body_html, re.S)
        sentences = [_clean_html(p) for p in paras if len(p.strip()) > 30]
        body_text = " ".join(sentences)

    # ── Ghép lại ──────────────────────────────────────────────────────────
    parts = [p for p in [title, desc, body_text] if p]
    return ". ".join(parts).strip()


# ── DB helpers ─────────────────────────────────────────────────────────────────

def _get_or_create_source(cursor) -> int:
    """Tạo data_source cho VOV GT nếu chưa có, trả về source_id."""
    cursor.execute(
        """INSERT INTO data_source (source_type, source_url, source_name)
           VALUES ('radio', %s, %s) ON CONFLICT (source_url) DO UPDATE SET source_name = EXCLUDED.source_name""",
        (SOURCE_URL, SOURCE_NAME)
    )
    cursor.execute("SELECT source_id FROM data_source WHERE source_url = %s", (SOURCE_URL,))
    return cursor.fetchone()[0]


def _load_recent_hashes(cursor) -> set:
    cursor.execute(
        "SELECT content_hash FROM raw_feed WHERE fetched_at > NOW() - INTERVAL %s",
        (f"{DEDUP_WINDOW_HOURS} hours",)
    )
    return {row[0] for row in cursor.fetchall()}


def _load_recent_contents(cursor) -> list:
    cursor.execute(
        "SELECT raw_content FROM raw_feed WHERE fetched_at > NOW() - INTERVAL %s",
        (f"{DEDUP_WINDOW_HOURS} hours",)
    )
    return [row[0] for row in cursor.fetchall()]


def _is_duplicate(text: str, hashes: set, contents: list) -> bool:
    if content_hash(text) in hashes:
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


# ── Main scraper ───────────────────────────────────────────────────────────────

def run_vov_scraper():
    """
    Quét toàn bộ category VOV GT, thu thập bài text liên quan giao thông,
    lưu vào raw_feed (dùng cùng pipeline dedup/filter của news_scraper.py).
    """
    conn = get_connection()
    if not conn:
        print("[VOV] ❌ Không kết nối được DB.")
        return

    cursor = conn.cursor()
    total_saved   = 0
    total_skip_kw = 0
    total_skip_dup = 0
    total_links   = 0
    start_time    = datetime.now()

    try:
        source_id = _get_or_create_source(cursor)
        conn.commit()

        hashes   = _load_recent_hashes(cursor)
        contents = _load_recent_contents(cursor)
        print(f"[VOV] Cache dedup: {len(hashes)} bài trong {DEDUP_WINDOW_HOURS}h gần nhất")

        for cat in CATEGORIES:
            cat_url = BASE_URL + cat
            print(f"\n[VOV] Quét category: {cat_url}")

            cat_html = _fetch_html(cat_url)
            if not cat_html:
                continue

            links = _extract_article_links(cat_html)
            print(f"  → Tìm thấy {len(links)} bài")
            total_links += len(links)

            saved_from_cat = 0
            for link in links:
                time.sleep(REQUEST_DELAY)

                article_html = _fetch_html(link)
                if not article_html:
                    continue

                text = _extract_article_text(article_html)
                if not text or len(text) < 50:
                    continue

                if not has_traffic_features(text):
                    total_skip_kw += 1
                    continue

                if _is_duplicate(text, hashes, contents):
                    total_skip_dup += 1
                    continue

                h = content_hash(text)
                cursor.execute(
                    """INSERT INTO raw_feed
                       (source_id, content_type, raw_content, content_hash, fetched_at, is_processed)
                       VALUES (%s, 'text', %s, %s, NOW(), false)""",
                    (source_id, text, h)
                )
                hashes.add(h)
                contents.append(text)
                saved_from_cat += 1
                total_saved += 1

            conn.commit()
            print(f"  ✅ Lưu: {saved_from_cat} bài mới | bỏ từ khóa: {total_skip_kw} | bỏ trùng: {total_skip_dup}")

    except Exception as e:
        conn.rollback()
        print(f"[VOV] ❌ Lỗi: {e}")
        import traceback; traceback.print_exc()
    finally:
        cursor.close()
        release_connection(conn)

    elapsed = (datetime.now() - start_time).seconds
    print(f"\n{'═' * 55}")
    print(f"✅ VOV GT SCRAPER HOÀN TẤT")
    print(f"   Links đã quét : {total_links}")
    print(f"   Bài mới lưu   : {total_saved}")
    print(f"   Bỏ (từ khóa)  : {total_skip_kw}")
    print(f"   Bỏ (trùng)    : {total_skip_dup}")
    print(f"   Thời gian      : {elapsed}s")
    print(f"{'═' * 55}")


if __name__ == "__main__":
    run_vov_scraper()
