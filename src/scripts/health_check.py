"""
health_check.py
---------------
Kiểm tra nhanh toàn bộ pipeline:
  - Kết nối DB
  - Số lượng và tình trạng dữ liệu trong từng bảng
  - Chất lượng geocoding (% tọa độ hợp lệ trong bbox Hà Nội)
  - Tỷ lệ xử lý raw_feed
  - Tình trạng clustering
  - Zone metrics
  - Tình trạng bảng traffic_features (feature engineering)

Chạy thủ công để kiểm tra hệ thống:
  python src/scripts/health_check.py
"""

import sys
sys.stdout.reconfigure(encoding='utf-8')

from datetime import datetime, timedelta
from src.core.db_manager import get_connection, release_connection

# Bounding box Hà Nội
HN_LAT_MIN, HN_LAT_MAX = 20.56, 21.38
HN_LNG_MIN, HN_LNG_MAX = 105.28, 106.02


def _section(title: str):
    print(f"\n{'─' * 60}")
    print(f"  {title}")
    print(f"{'─' * 60}")


def check_db_connection(cursor) -> bool:
    try:
        cursor.execute("SELECT version()")
        ver = cursor.fetchone()[0].split(",")[0]
        print(f"  ✅ Kết nối DB thành công: {ver}")
        return True
    except Exception as e:
        print(f"  ❌ Kết nối DB thất bại: {e}")
        return False


def check_raw_feed(cursor):
    _section("📥 raw_feed — Dữ liệu thô từ RSS")

    cursor.execute("SELECT COUNT(*) FROM raw_feed")
    total = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM raw_feed WHERE is_processed = FALSE")
    unprocessed = cursor.fetchone()[0]

    cursor.execute("SELECT MAX(fetched_at) FROM raw_feed")
    latest = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM raw_feed WHERE fetched_at >= NOW() - INTERVAL '24 hours'")
    last_24h = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM raw_feed WHERE fetched_at >= NOW() - INTERVAL '1 hour'")
    last_1h = cursor.fetchone()[0]

    processed_pct = round((total - unprocessed) / total * 100, 1) if total > 0 else 0.0

    print(f"  Tổng số bài:       {total:>8,}")
    print(f"  Chưa xử lý:        {unprocessed:>8,}  ({100 - processed_pct:.1f}%)")
    print(f"  Đã xử lý:          {total - unprocessed:>8,}  ({processed_pct:.1f}%)")
    print(f"  Trong 24h qua:     {last_24h:>8,}")
    print(f"  Trong 1h qua:      {last_1h:>8,}")
    print(f"  Bài mới nhất:      {latest.strftime('%Y-%m-%d %H:%M:%S') if latest else 'Không có'}")

    if last_1h == 0:
        print("  ⚠️  CẢNH BÁO: Không có bài mới trong 1h — kiểm tra scraper!")
    elif last_24h < 10:
        print("  ⚠️  CẢNH BÁO: Quá ít dữ liệu trong 24h — scraper có thể đang lỗi")
    else:
        print("  ✅ Scraper hoạt động bình thường")


def check_incidents(cursor):
    _section("🚨 incident — Sự cố đã trích xuất")

    cursor.execute("SELECT COUNT(*) FROM incident")
    total = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM incident WHERE detected_at >= NOW() - INTERVAL '24 hours'")
    last_24h = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM incident WHERE detected_at >= NOW() - INTERVAL '1 hour'")
    last_1h = cursor.fetchone()[0]

    cursor.execute("SELECT AVG(potential_score), MIN(potential_score), MAX(potential_score) FROM incident")
    row = cursor.fetchone()
    avg_score, min_score, max_score = row if row and row[0] else (0, 0, 0)

    cursor.execute("SELECT incident_type, COUNT(*) FROM incident GROUP BY incident_type ORDER BY COUNT(*) DESC")
    types = cursor.fetchall()

    print(f"  Tổng sự cố:        {total:>8,}")
    print(f"  Trong 24h qua:     {last_24h:>8,}")
    print(f"  Trong 1h qua:      {last_1h:>8,}")
    if avg_score:
        print(f"  potential_score:   avg={avg_score:.3f}  min={min_score:.3f}  max={max_score:.3f}")
    print(f"  Phân loại:")
    for t, cnt in types:
        print(f"    - {(t or 'unknown'):<20} : {cnt:,}")


def check_geocoding(cursor):
    _section("📍 location — Chất lượng geocoding")

    cursor.execute("SELECT COUNT(*) FROM location")
    total = cursor.fetchone()[0]

    cursor.execute("""
        SELECT COUNT(*) FROM location
        WHERE latitude  BETWEEN %s AND %s
          AND longitude BETWEEN %s AND %s
    """, (HN_LAT_MIN, HN_LAT_MAX, HN_LNG_MIN, HN_LNG_MAX))
    valid_hn = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM location WHERE latitude IS NULL OR longitude IS NULL")
    null_coords = cursor.fetchone()[0]

    cursor.execute("""
        SELECT COUNT(*) FROM location
        WHERE latitude IS NOT NULL
          AND (latitude  NOT BETWEEN %s AND %s
           OR  longitude NOT BETWEEN %s AND %s)
    """, (HN_LAT_MIN, HN_LAT_MAX, HN_LNG_MIN, HN_LNG_MAX))
    out_of_bbox = cursor.fetchone()[0]

    valid_pct = round(valid_hn / total * 100, 1) if total > 0 else 0.0

    print(f"  Tổng địa điểm:     {total:>8,}")
    print(f"  Hợp lệ (Hà Nội):   {valid_hn:>8,}  ({valid_pct:.1f}%)")
    print(f"  Thiếu tọa độ:      {null_coords:>8,}")
    print(f"  Ngoài bbox HN:     {out_of_bbox:>8,}")

    if valid_pct < 50:
        print("  ⚠️  CẢNH BÁO: Tỷ lệ geocoding tốt thấp — kiểm tra Nominatim / regex")
    elif valid_pct < 80:
        print("  🟡 Geocoding chấp nhận được nhưng có thể cải thiện")
    else:
        print("  ✅ Geocoding chất lượng tốt")


def check_clusters(cursor):
    _section("🗂️  incident_cluster — Kết quả ST-DBSCAN")

    cursor.execute("SELECT COUNT(*) FROM incident_cluster")
    total = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM incident_cluster WHERE detected_at >= NOW() - INTERVAL '24 hours'")
    last_24h = cursor.fetchone()[0]

    cursor.execute("SELECT MAX(detected_at) FROM incident_cluster")
    latest = cursor.fetchone()[0]

    cursor.execute("SELECT AVG(incident_count), MAX(incident_count), AVG(avg_severity) FROM incident_cluster")
    row = cursor.fetchone()
    avg_cnt, max_cnt, avg_sev = row if row and row[0] else (0, 0, 0)

    cursor.execute("SELECT COUNT(*) FROM incident WHERE cluster_id IS NULL AND detected_at >= NOW() - INTERVAL '48 hours'")
    unclustered = cursor.fetchone()[0]

    print(f"  Tổng cụm:          {total:>8,}")
    print(f"  Trong 24h qua:     {last_24h:>8,}")
    print(f"  Cụm mới nhất:      {latest.strftime('%Y-%m-%d %H:%M:%S') if latest else 'Chưa có'}")
    if avg_cnt:
        print(f"  Sự cố/cụm:         avg={avg_cnt:.1f}  max={int(max_cnt)}")
        print(f"  Severity trung bình:{avg_sev:.3f}")
    print(f"  Sự cố chưa gom:    {unclustered:>8,}  (48h gần nhất, có thể là noise)")


def check_zone_metrics(cursor):
    _section("🏙️  administrative_zone — Zone metrics")

    cursor.execute("SELECT COUNT(*) FROM administrative_zone")
    total = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM administrative_zone WHERE zone_pressure_score > 0")
    computed = cursor.fetchone()[0]

    cursor.execute("SELECT MAX(zone_pressure_score), AVG(zone_pressure_score) FROM administrative_zone")
    row = cursor.fetchone()
    max_p, avg_p = row if row and row[0] else (0, 0)

    cursor.execute("""
        SELECT name, zone_pressure_score FROM administrative_zone
        ORDER BY zone_pressure_score DESC LIMIT 3
    """)
    top3 = cursor.fetchall()

    print(f"  Tổng zones:        {total:>8,}")
    print(f"  Đã tính pressure:  {computed:>8,}")
    if avg_p:
        print(f"  Pressure score:    avg={avg_p:.3f}  max={max_p:.3f}")
    if top3:
        print(f"  Top 3 áp lực cao:")
        for name, score in top3:
            print(f"    - {name:<30} : {score:.4f}")

    if computed == 0:
        print("  ⚠️  CẢNH BÁO: Chưa tính zone metrics — chạy zone_metrics.py")
    elif computed < total:
        print(f"  🟡 Còn {total - computed} zones chưa có pressure score")
    else:
        print("  ✅ Zone metrics đầy đủ")


def check_features(cursor):
    _section("🧮 traffic_features — Feature engineering")

    try:
        cursor.execute("SELECT COUNT(*) FROM traffic_features")
        total = cursor.fetchone()[0]

        cursor.execute("SELECT MAX(snapshot_time) FROM traffic_features")
        latest = cursor.fetchone()[0]

        cursor.execute("""
            SELECT COUNT(*) FROM traffic_features
            WHERE target_t1h IS NOT NULL
        """)
        labeled = cursor.fetchone()[0]

        print(f"  Tổng feature rows: {total:>8,}")
        print(f"  Có label (t1h):    {labeled:>8,}")
        print(f"  Snapshot mới nhất: {latest.strftime('%Y-%m-%d %H:%M:%S') if latest else 'Chưa có'}")

        if total == 0:
            print("  ℹ️  Bảng trống — feature engineering chưa chạy (bình thường ở tuần 1)")
        else:
            print("  ✅ Feature pipeline đã có dữ liệu")
    except Exception:
        print("  ⚠️  Bảng traffic_features chưa tồn tại — chạy init_db.py trước")


def run_health_check():
    print("=" * 60)
    print("  URBAN TRAFFIC MONITORING — HEALTH CHECK")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    conn = get_connection()
    if not conn:
        print("❌ Không kết nối được database. Kiểm tra Docker + .env")
        return

    cursor = conn.cursor()
    try:
        _section("🔌 Kết nối Database")
        if not check_db_connection(cursor):
            return

        check_raw_feed(cursor)
        check_incidents(cursor)
        check_geocoding(cursor)
        check_clusters(cursor)
        check_zone_metrics(cursor)
        check_features(cursor)

        print(f"\n{'=' * 60}")
        print("  Health check hoàn tất.")
        print("=" * 60)

    except Exception as e:
        print(f"\n❌ Lỗi health check: {e}")
        import traceback; traceback.print_exc()
    finally:
        cursor.close()
        release_connection(conn)


if __name__ == "__main__":
    run_health_check()
