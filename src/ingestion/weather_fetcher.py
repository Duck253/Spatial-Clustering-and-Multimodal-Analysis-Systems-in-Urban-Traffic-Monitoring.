"""
weather_fetcher.py
------------------
Thu thập dữ liệu thời tiết Hà Nội từ Open-Meteo (miễn phí, không cần API key).
Lưu vào bảng weather_snapshot theo từng giờ.

Chế độ:
  - run_weather_fetcher()       : fetch giờ hiện tại (dùng trong scheduler)
  - backfill_weather(days=30)   : nạp lịch sử N ngày qua (chạy 1 lần)

API:
  - Current : api.open-meteo.com/v1/forecast   (15 phút cập nhật 1 lần)
  - History : archive-api.open-meteo.com/v1/archive (hourly, delay ~5 ngày)

Chạy độc lập:
    python src/ingestion/weather_fetcher.py           # fetch hiện tại
    python src/ingestion/weather_fetcher.py backfill  # nạp 30 ngày lịch sử
"""

import sys
import json
import urllib.request
import urllib.error
from datetime import datetime, timezone, timedelta

sys.stdout.reconfigure(encoding='utf-8')

from src.core.db_manager import get_connection, release_connection

# ── Cấu hình ─────────────────────────────────────────────────────────────────
HN_LAT   = 21.0278
HN_LNG   = 105.8342
TIMEOUT  = 12  # giây

# WMO weather code → nhãn dễ đọc (để log)
_WMO_LABELS = {
    0: "Trời quang", 1: "Ít mây", 2: "Nhiều mây", 3: "Âm u",
    45: "Sương mù", 48: "Sương mù đóng băng",
    51: "Mưa phùn nhẹ", 53: "Mưa phùn", 55: "Mưa phùn nặng",
    61: "Mưa nhẹ", 63: "Mưa vừa", 65: "Mưa to",
    80: "Mưa rào nhẹ", 81: "Mưa rào", 82: "Mưa rào nặng",
    95: "Dông", 96: "Dông kèm mưa đá", 99: "Dông mưa đá lớn",
}


def _fetch_json(url: str) -> dict | None:
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "TrafficMonitor/1.0"})
        resp = urllib.request.urlopen(req, timeout=TIMEOUT)
        return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        print(f"  [WEATHER] HTTP {e.code}: {url}")
        return None
    except Exception as e:
        print(f"  [WEATHER] Lỗi fetch: {e}")
        return None


def _save_rows(cur, rows: list[tuple]) -> tuple[int, int]:
    """Lưu danh sách (snapshot_hour, rainfall, is_raining, wind, code). Trả về (inserted, skipped)."""
    inserted = skipped = 0
    for row in rows:
        cur.execute("""
            INSERT INTO weather_snapshot
                (snapshot_hour, rainfall_mm, is_raining, wind_speed_kmh, weather_code)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (snapshot_hour) DO NOTHING
        """, row)
        if cur.rowcount > 0:
            inserted += 1
        else:
            skipped += 1
    return inserted, skipped


# ── Fetch hiện tại ─────────────────────────────────────────────────────────────

def run_weather_fetcher():
    """Fetch thời tiết hiện tại (interval 15 phút) và lưu vào DB."""
    url = (
        f"https://api.open-meteo.com/v1/forecast"
        f"?latitude={HN_LAT}&longitude={HN_LNG}"
        f"&current=precipitation,rain,windspeed_10m,weathercode"
        f"&timezone=Asia/Bangkok"
    )
    data = _fetch_json(url)
    if not data:
        return

    cur_data = data.get("current", {})
    rainfall  = float(cur_data.get("precipitation", 0.0))
    wind      = float(cur_data.get("windspeed_10m", 0.0))
    code      = int(cur_data.get("weathercode", 0))
    is_rain   = rainfall > 0.1

    # Làm tròn về đầu giờ để dễ join
    now_hour = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)

    conn = get_connection()
    if not conn:
        return
    cur = conn.cursor()
    try:
        inserted, _ = _save_rows(cur, [(now_hour, rainfall, is_rain, wind, code)])
        conn.commit()
        label = _WMO_LABELS.get(code, f"code={code}")
        status = "🌧️ Mưa" if is_rain else "☀️ Khô"
        print(f"[WEATHER] {now_hour.strftime('%H:%M')} | {label} | mưa={rainfall}mm | gió={wind}km/h | {status}"
              + (" (đã có)" if not inserted else ""))
    except Exception as e:
        conn.rollback()
        print(f"[WEATHER] ❌ Lỗi lưu DB: {e}")
    finally:
        cur.close()
        release_connection(conn)


# ── Backfill lịch sử ──────────────────────────────────────────────────────────

def backfill_weather(days: int = 30):
    """
    Nạp dữ liệu thời tiết lịch sử từ Open-Meteo Archive API.
    Archive API có delay ~5 ngày, nên kéo đến (hôm nay - 5 ngày).
    """
    end_date   = datetime.now(timezone.utc).date() - timedelta(days=5)
    start_date = end_date - timedelta(days=days)

    print(f"[WEATHER BACKFILL] {start_date} → {end_date} ({days} ngày)")

    url = (
        f"https://archive-api.open-meteo.com/v1/archive"
        f"?latitude={HN_LAT}&longitude={HN_LNG}"
        f"&start_date={start_date}&end_date={end_date}"
        f"&hourly=precipitation,windspeed_10m,weathercode"
        f"&timezone=Asia/Bangkok"
    )
    data = _fetch_json(url)
    if not data:
        print("  ❌ Không lấy được dữ liệu lịch sử.")
        return

    hourly    = data.get("hourly", {})
    times     = hourly.get("time", [])
    rainfalls = hourly.get("precipitation", [])
    winds     = hourly.get("windspeed_10m", [])
    codes     = hourly.get("weathercode", [])

    rows = []
    for i, t_str in enumerate(times):
        try:
            # Open-Meteo trả về local time (Asia/Bangkok = UTC+7)
            local_dt = datetime.strptime(t_str, "%Y-%m-%dT%H:%M")
            utc_dt   = local_dt.replace(tzinfo=timezone(timedelta(hours=7)))
            rainfall = float(rainfalls[i]) if i < len(rainfalls) else 0.0
            wind     = float(winds[i])     if i < len(winds)     else 0.0
            code     = int(codes[i])       if i < len(codes)     else 0
            is_rain  = rainfall > 0.1
            rows.append((utc_dt, rainfall, is_rain, wind, code))
        except Exception:
            continue

    conn = get_connection()
    if not conn:
        return
    cur = conn.cursor()
    try:
        inserted, skipped = _save_rows(cur, rows)
        conn.commit()
        print(f"  ✅ Đã lưu {inserted} giờ | bỏ qua (đã có) {skipped} giờ")

        # Thống kê mưa
        rain_hours = sum(1 for r in rows if r[2])
        print(f"  🌧️  Số giờ có mưa: {rain_hours}/{len(rows)} ({rain_hours/len(rows)*100:.1f}%)")

    except Exception as e:
        conn.rollback()
        print(f"  ❌ Lỗi lưu DB: {e}")
    finally:
        cur.close()
        release_connection(conn)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "backfill":
        days = int(sys.argv[2]) if len(sys.argv) > 2 else 30
        backfill_weather(days)
    else:
        run_weather_fetcher()
