"""
otofun_scraper.py
-----------------
Thu thập bài thảo luận từ diễn đàn otofun.net — chuyên mục An toàn Giao thông.
URL: https://www.otofun.net/forums/an-toan-giao-thong.174/

Cấu trúc trang:
  - Danh sách thread: <a href="/threads/[slug].[id]/" class="structItem-title">
  - Tiêu đề bài: <h1 class="p-title-value">
  - Nội dung bài đầu: <article class="message-body"> / div class="bbWrapper"

Chạy độc lập:
    python -m src.ingestion.otofun_scraper
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

BASE_URL       = "https://www.otofun.net"
FORUM_URL      = f"{BASE_URL}/forums/an-toan-giao-thong.174/"
SOURCE_NAME    = "OtoFun An toàn GT"
SOURCE_TYPE    = "forum"

REQUEST_DELAY     = 1.5    # giây — forum dễ chặn hơn báo
REQUEST_TIMEOUT   = 12
MAX_PAGES         = 5      # lấy 5 trang đầu ~ 100 thread
JACCARD_THRESHOLD = 0.6
DEDUP_WINDOW_HOURS = 48

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "vi-VN,vi;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": "https://www.otofun.net/",
}


# ── HTTP helpers ───────────────────────────────────────────────────────────────

def _fetch_html(url: str) -> str | None:
    try:
        req = urllib.request.Request(url, headers=_HEADERS)
        resp = urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT)
        raw = resp.read()
        return raw.decode("utf-8", errors="ignore")
    except urllib.error.HTTPError as e:
        print(f"  [OTOFUN] HTTP {e.code}: {url}")
        return None
    except Exception as e:
        print(f"  [OTOFUN] Lỗi fetch {url}: {e}")
        return None


# ── Link extraction ────────────────────────────────────────────────────────────

def _extract_thread_links(html: str) -> list[str]:
    """Lấy link các thread trong trang danh sách."""
    # XenForo: <a href="/threads/tieu-de-thread.12345/" ...>
    pattern = r'href="(/threads/[a-z0-9\-]+\.\d+/)"'
    links = re.findall(pattern, html)
    # Bỏ trang phân trang của thread (có ?page=...)
    filtered = [l for l in links if "?" not in l]
    return list(dict.fromkeys(filtered))   # dedup giữ thứ tự


def _get_page_url(page: int) -> str:
    if page == 1:
        return FORUM_URL
    return f"{FORUM_URL}?page={page}"


# ── Article content extraction ─────────────────────────────────────────────────

def _clean_html(raw: str) -> str:
    text = re.sub(r"<[^>]+>", " ", raw)
    text = re.sub(r"&nbsp;", " ", text)
    text = re.sub(r"&[a-z#0-9]+;", "", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _extract_thread_text(html: str) -> str:
    """
    Trích nội dung từ thread XenForo:
      1. Tiêu đề (h1 class="p-title-value")
      2. Nội dung post đầu tiên (div class="bbWrapper")
    """
    # ── Tiêu đề ────────────────────────────────────────────────────────────
    title = ""
    m = re.search(r'class="p-title-value"[^>]*>(.*?)</h1', html, re.S)
    if m:
        title = _clean_html(m.group(1))

    # ── Nội dung post đầu tiên ─────────────────────────────────────────────
    body = ""
    m = re.search(r'class="bbWrapper">(.*?)</div>', html, re.S)
    if m:
        # Lấy text từ các thẻ <p> hoặc toàn bộ text thô
        paras = re.findall(r"<p[^>]*>(.*?)</p>", m.group(1), re.S)
        if paras:
            sentences = [_clean_html(p) for p in paras if len(p.strip()) > 20]
            body = " ".join(sentences)
        else:
            body = _clean_html(m.group(1))

    parts = [p for p in [title, body] if p]
    return ". ".join(parts).strip()


# ── DB helpers ─────────────────────────────────────────────────────────────────

def _get_or_create_source(cursor) -> int:
    cursor.execute(
        """INSERT INTO data_source (source_type, source_url, source_name)
           VALUES (%s, %s, %s) ON CONFLICT (source_url) DO UPDATE SET source_name = EXCLUDED.source_name""",
        (SOURCE_TYPE, FORUM_URL, SOURCE_NAME)
    )
    cursor.execute("SELECT source_id FROM data_source WHERE source_url = %s", (FORUM_URL,))
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


# ── Main scraper ───────────────────────────────────────────────────────────────

def run_otofun_scraper():
    conn = get_connection()
    if not conn:
        print("[OTOFUN] ❌ Không kết nối được DB.")
        return

    cursor = conn.cursor()
    total_saved    = 0
    total_skip_kw  = 0
    total_skip_dup = 0
    total_links    = 0
    start_time     = datetime.now()

    try:
        source_id = _get_or_create_source(cursor)
        conn.commit()

        hashes   = _load_recent_hashes(cursor)
        contents = _load_recent_contents(cursor)
        print(f"[OTOFUN] Cache dedup: {len(hashes)} bài trong {DEDUP_WINDOW_HOURS}h gần nhất")

        for page in range(1, MAX_PAGES + 1):
            page_url = _get_page_url(page)
            print(f"\n[OTOFUN] Trang {page}: {page_url}")

            page_html = _fetch_html(page_url)
            if not page_html:
                print(f"  ⚠️  Không tải được trang {page}, dừng.")
                break

            links = _extract_thread_links(page_html)
            print(f"  → Tìm thấy {len(links)} thread")
            if not links:
                break

            total_links += len(links)
            saved_from_page = 0

            for link in links:
                time.sleep(REQUEST_DELAY)
                thread_url = BASE_URL + link

                thread_html = _fetch_html(thread_url)
                if not thread_html:
                    continue

                text = _extract_thread_text(thread_html)
                if not text or len(text) < 40:
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
                saved_from_page += 1
                total_saved += 1

            conn.commit()
            print(f"  ✅ Lưu: {saved_from_page} | bỏ từ khóa: {total_skip_kw} | bỏ trùng: {total_skip_dup}")
            time.sleep(REQUEST_DELAY)

    except Exception as e:
        conn.rollback()
        print(f"[OTOFUN] ❌ Lỗi: {e}")
        import traceback; traceback.print_exc()
    finally:
        cursor.close()
        release_connection(conn)

    elapsed = (datetime.now() - start_time).seconds
    print(f"\n{'═' * 55}")
    print(f"✅ OTOFUN SCRAPER HOÀN TẤT")
    print(f"   Threads đã quét : {total_links}")
    print(f"   Bài mới lưu     : {total_saved}")
    print(f"   Bỏ (từ khóa)    : {total_skip_kw}")
    print(f"   Bỏ (trùng)      : {total_skip_dup}")
    print(f"   Thời gian        : {elapsed}s")
    print(f"{'═' * 55}")


if __name__ == "__main__":
    run_otofun_scraper()
