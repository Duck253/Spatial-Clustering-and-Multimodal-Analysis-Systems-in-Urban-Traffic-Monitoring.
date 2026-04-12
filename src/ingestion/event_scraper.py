"""
event_scraper.py
----------------
Thu thập sự kiện lớn tại Hà Nội từ Google News RSS → lưu vào bảng planned_event.
EventImpactAnalyzer sẽ dùng data này để cộng điểm nguy cơ tắc đường.

Logic:
  1. Scrape Google News với các từ khóa sự kiện
  2. Nhận diện địa điểm → ánh xạ sang tọa độ GPS (venue map cứng)
  3. Ước tính thời gian + quy mô từ nội dung bài báo
  4. INSERT vào planned_event (bỏ qua nếu đã tồn tại)

Chạy:
    python src/ingestion/event_scraper.py
"""

import sys
import re
import time
import feedparser
from datetime import datetime, timedelta

sys.stdout.reconfigure(encoding='utf-8')

from src.core.db_manager import get_connection, release_connection

# ── Danh sách địa điểm lớn tại Hà Nội gây tắc đường ──────────────────────────
# Format: từ khóa nhận diện → (tên chuẩn, lat, lng, sức chứa, bán kính ảnh hưởng m)
VENUE_MAP = {
    # Sân vận động
    "mỹ đình":          ("Sân vận động Mỹ Đình",            21.0202, 105.7645, 40000, 3000),
    "svđ quốc gia":     ("Sân vận động Mỹ Đình",            21.0202, 105.7645, 40000, 3000),
    "hàng đẫy":         ("Sân vận động Hàng Đẫy",           21.0272, 105.8410, 22000, 2000),
    "quần ngựa":        ("Cung thể thao Quần Ngựa",         21.0356, 105.8347,  5000, 1500),

    # Hội nghị / triển lãm
    "trung tâm hội nghị quốc gia": ("Trung tâm Hội nghị Quốc gia", 21.0156, 105.7789, 3000, 2000),
    "cung hội nghị":    ("Trung tâm Hội nghị Quốc gia",    21.0156, 105.7789,  3000, 2000),
    "triển lãm giảng võ": ("Trung tâm Triển lãm Giảng Võ", 21.0282, 105.8349,  5000, 1500),
    "giảng võ":         ("Trung tâm Triển lãm Giảng Võ",   21.0282, 105.8349,  5000, 1500),

    # Văn hóa / âm nhạc
    "nhà hát lớn":      ("Nhà hát Lớn Hà Nội",             21.0246, 105.8551,   600,  800),
    "cung văn hóa hữu nghị": ("Cung Văn hóa Hữu Nghị",     21.0320, 105.8460,  2000, 1200),
    "hữu nghị":         ("Cung Văn hóa Hữu Nghị",          21.0320, 105.8460,  2000, 1200),
    "nhà hát quân đội": ("Nhà hát Quân đội",               21.0354, 105.8440,   500,  600),

    # Không gian ngoài trời
    "hồ gươm":          ("Phố đi bộ Hồ Gươm",              21.0285, 105.8522, 20000, 1500),
    "phố đi bộ":        ("Phố đi bộ Hồ Gươm",              21.0285, 105.8522, 20000, 1500),
    "công viên thống nhất": ("Công viên Thống Nhất",        21.0167, 105.8456, 10000, 1500),
    "thống nhất":       ("Công viên Thống Nhất",            21.0167, 105.8456, 10000, 1500),
    "vườn thú":         ("Vườn thú Hà Nội",                 21.0375, 105.8311,  8000, 1000),

    # Trung tâm thương mại lớn (event / show)
    "royal city":       ("Vincom Mega Mall Royal City",     20.9986, 105.8126, 15000, 1000),
    "times city":       ("Times City Park Hill",            20.9956, 105.8678, 12000, 1000),
    "lotte":            ("Lotte Center Hà Nội",             21.0191, 105.8099,  5000,  800),
    "aeon mall":        ("AEON Mall Long Biên",             21.0468, 105.8878,  8000, 1000),
}

# ── Từ khóa sự kiện → ước tính lượng người ────────────────────────────────────
EVENT_TYPE_ATTENDANCE = {
    "concert":    15000,
    "chung kết":  40000,
    "bán kết":    30000,
    "v-league":   20000,
    "bóng đá":    20000,
    "trận đấu":   20000,
    "marathon":   10000,
    "lễ hội":     15000,
    "hội chợ":     8000,
    "triển lãm":   5000,
    "hội nghị":    3000,
    "lễ khai mạc":10000,
    "lễ bế mạc":   8000,
    "gameshow":    3000,
    "biểu diễn":   5000,
    "show":        5000,
}

# ── Google News RSS queries ────────────────────────────────────────────────────
EVENT_QUERIES = [
    "sự kiện lớn hà nội",
    "concert hà nội",
    "lễ hội hà nội",
    "bóng đá mỹ đình",
    "v-league hà nội",
    "chung kết hà nội",
    "marathon hà nội",
    "triển lãm hà nội",
    "hội chợ hà nội",
    "biểu diễn hà nội",
]

_GN_BASE = "https://news.google.com/rss/search?hl=vi&gl=VN&ceid=VN:vi&q="


# ── Helpers ───────────────────────────────────────────────────────────────────

def _detect_venue(text: str):
    """Tìm địa điểm trong văn bản, trả về tuple venue hoặc None."""
    text_lower = text.lower()
    for keyword, venue_data in VENUE_MAP.items():
        if keyword in text_lower:
            return venue_data  # (tên, lat, lng, attendance, radius)
    return None


def _estimate_attendance(text: str, venue_capacity: int) -> int:
    """Ước tính lượng người từ loại sự kiện, giới hạn bởi sức chứa venue."""
    text_lower = text.lower()
    for keyword, est in EVENT_TYPE_ATTENDANCE.items():
        if keyword in text_lower:
            return min(est, venue_capacity)
    return min(5000, venue_capacity)  # mặc định


def _extract_event_time(text: str, pub_date=None) -> tuple[datetime, datetime]:
    """
    Cố gắng trích xuất ngày/giờ từ văn bản.
    Fallback: ngày mai 19h–22h nếu không tìm được.
    """
    now = datetime.now()
    text_lower = text.lower()

    # Tìm giờ: "19h", "19:00", "20 giờ"
    hour_match = re.search(r'(\d{1,2})[h:giờ]', text_lower)
    hour = int(hour_match.group(1)) if hour_match and int(hour_match.group(1)) < 24 else 19

    # Tìm ngày: "15/4", "15 tháng 4", "ngày 15"
    date_match = re.search(r'(\d{1,2})[/\-](\d{1,2})', text)
    if date_match:
        day, month = int(date_match.group(1)), int(date_match.group(2))
        year = now.year if month >= now.month else now.year + 1
        try:
            start = datetime(year, month, day, hour, 0)
        except ValueError:
            start = now + timedelta(days=1)
            start = start.replace(hour=hour, minute=0, second=0, microsecond=0)
    elif pub_date:
        # Dùng ngày đăng bài + 1 ngày
        start = pub_date + timedelta(days=1)
        start = start.replace(hour=hour, minute=0, second=0, microsecond=0)
    else:
        start = now + timedelta(days=1)
        start = start.replace(hour=hour, minute=0, second=0, microsecond=0)

    # Nếu start đã qua rồi, bỏ qua (sự kiện cũ)
    end = start + timedelta(hours=3)
    return start, end


def _parse_pub_date(entry) -> datetime | None:
    """Parse published date từ feed entry."""
    try:
        import email.utils
        return datetime(*email.utils.parsedate(entry.published)[:6])
    except Exception:
        return None


def _event_exists(cursor, title: str, start_time: datetime) -> bool:
    """Kiểm tra sự kiện đã tồn tại trong DB chưa (tránh trùng)."""
    cursor.execute(
        """SELECT 1 FROM planned_event
           WHERE title = %s AND ABS(EXTRACT(EPOCH FROM (start_time - %s))) < 3600""",
        (title, start_time)
    )
    return cursor.fetchone() is not None


# ── Main ──────────────────────────────────────────────────────────────────────

def run_event_scraper():
    conn = get_connection()
    if not conn:
        print("❌ Không kết nối được DB.")
        return

    cursor = conn.cursor()
    total_saved = 0
    total_skipped = 0

    try:
        print("━" * 55)
        print(f"[EVENT SCRAPER] Quét {len(EVENT_QUERIES)} từ khóa sự kiện")
        print("━" * 55)

        seen_titles = set()  # dedup trong session

        for query in EVENT_QUERIES:
            url  = _GN_BASE + query.replace(" ", "+")
            try:
                feed = feedparser.parse(url)
            except Exception as e:
                print(f"  ⚠️  '{query}': lỗi fetch — {e}")
                continue

            saved_from_query = 0
            for entry in feed.entries:
                title = getattr(entry, 'title', '').strip()
                desc  = getattr(entry, 'description', '') or ''
                text  = f"{title}. {desc}"

                # Nhận diện địa điểm
                venue = _detect_venue(text)
                if not venue:
                    continue  # Bỏ qua nếu không tìm được địa điểm Hà Nội

                venue_name, lat, lng, capacity, radius = venue
                pub_date   = _parse_pub_date(entry)
                start, end = _extract_event_time(text, pub_date)

                # Bỏ qua sự kiện đã qua
                if end < datetime.now():
                    total_skipped += 1
                    continue

                attendance = _estimate_attendance(text, capacity)

                # Tên sự kiện = tiêu đề bài báo (truncate)
                event_title = title[:200]

                # Dedup trong session
                dedup_key = f"{event_title[:50]}_{start.date()}"
                if dedup_key in seen_titles:
                    total_skipped += 1
                    continue
                seen_titles.add(dedup_key)

                # Dedup trong DB
                if _event_exists(cursor, event_title, start):
                    total_skipped += 1
                    continue

                cursor.execute(
                    """INSERT INTO planned_event
                       (title, latitude, longitude, start_time, end_time, expected_attendance, impact_radius_meters)
                       VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                    (event_title, lat, lng, start, end, attendance, radius)
                )
                saved_from_query += 1
                total_saved += 1
                print(f"  ✅ [{venue_name}] {event_title[:60]}...")
                print(f"       {start.strftime('%d/%m %H:%M')} | ~{attendance:,} người | r={radius}m")

            conn.commit()
            if saved_from_query == 0:
                print(f"  — '{query}': không tìm được sự kiện có địa điểm Hà Nội")
            time.sleep(1.0)

    except Exception as e:
        conn.rollback()
        print(f"\n❌ Lỗi: {e}")
        import traceback; traceback.print_exc()
    finally:
        cursor.close()
        release_connection(conn)

    print(f"\n{'═' * 55}")
    print(f"✅ EVENT SCRAPER HOÀN TẤT")
    print(f"   Sự kiện mới lưu : {total_saved}")
    print(f"   Bỏ qua (trùng/cũ): {total_skipped}")
    print(f"{'═' * 55}")


if __name__ == "__main__":
    run_event_scraper()
