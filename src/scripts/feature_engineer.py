"""
feature_engineer.py
-------------------
Tạo feature vectors cho ML model dự đoán tắc đường T+1h/2h/3h.
Kết quả được lưu vào bảng traffic_features.

Chiến lược sinh snapshot:
  Mỗi incident tại thời điểm T và địa điểm L tạo ra 1 snapshot.
  Features nhìn LÙI [T-2h, T] và [T-6h, T].
  Target nhìn TIẾN: có incident mới trong 500m tại [T, T+1h]? → target_t1h

Idempotent: dùng ON CONFLICT để không tạo trùng.

Chạy thủ công:
    python src/scripts/feature_engineer.py

Hoặc tích hợp vào scheduler (chạy mỗi 30 phút):
    scheduler.add_job(run_feature_engineer, 'interval', minutes=30)
"""

import sys
sys.stdout.reconfigure(encoding='utf-8')

from datetime import datetime, timezone, timedelta
from src.core.db_manager import get_connection, release_connection

VN_TZ = timezone(timedelta(hours=7))  # UTC+7

# ── Tham số ──────────────────────────────────────────────────────────────────
NEARBY_METERS      = 500    # Bán kính "lân cận" cho incident/cluster
EVENT_RADIUS_M     = 2000   # Bán kính ảnh hưởng sự kiện
PEAK_HOURS_AM      = (7, 9)     # Giờ cao điểm sáng [7, 9)
PEAK_HOURS_PM      = (17, 19)   # Giờ cao điểm chiều [17, 19)
RAIN_THRESHOLD_MM  = 0.1    # Ngưỡng coi là đang mưa


# ── Helpers ───────────────────────────────────────────────────────────────────

def _is_peak(hour: int, is_weekend: bool) -> bool:
    if is_weekend:
        return False
    return PEAK_HOURS_AM[0] <= hour < PEAK_HOURS_AM[1] or \
           PEAK_HOURS_PM[0] <= hour < PEAK_HOURS_PM[1]


def _compute_incident_features(cur, location_id: int, snap_time: datetime) -> dict:
    """Đếm incidents trong 2h và 6h trước snapshot, trong bán kính NEARBY_METERS."""
    # Query cửa sổ 2h: count, avg_score, has_accident
    cur.execute("""
        SELECT
            COUNT(*)                               AS count_2h,
            COALESCE(AVG(i.potential_score), 0.0)  AS avg_score_2h,
            BOOL_OR(i.incident_type = 'accident')  AS has_accident_2h
        FROM incident i
        JOIN location l ON i.location_id = l.location_id
        JOIN location src ON src.location_id = %s
        WHERE i.detected_at >= %s - INTERVAL '2 hours'
          AND i.detected_at <  %s
          AND ST_DWithin(l.geom::geography, src.geom::geography, %s)
    """, (location_id, snap_time, snap_time, NEARBY_METERS))
    row2 = cur.fetchone()

    # Query riêng cửa sổ 6h — outer WHERE khác nên phải tách query
    cur.execute("""
        SELECT COUNT(*)
        FROM incident i
        JOIN location l ON i.location_id = l.location_id
        JOIN location src ON src.location_id = %s
        WHERE i.detected_at >= %s - INTERVAL '6 hours'
          AND i.detected_at <  %s
          AND ST_DWithin(l.geom::geography, src.geom::geography, %s)
    """, (location_id, snap_time, snap_time, NEARBY_METERS))
    row6 = cur.fetchone()

    return {
        "incident_count_2h":   int(row2[0]),
        "avg_score_2h":        float(row2[1]),
        "has_accident_nearby": bool(row2[2]),
        "incident_count_6h":   int(row6[0]),
    }


def _compute_cluster_features(cur, location_id: int, snap_time: datetime) -> dict:
    """Kiểm tra cluster ST-DBSCAN gần nhất trong 2h qua."""
    cur.execute("""
        SELECT c.avg_severity
        FROM incident_cluster c
        JOIN location src ON src.location_id = %s
        WHERE c.detected_at >= %s - INTERVAL '2 hours'
          AND c.detected_at <  %s
          AND ST_DWithin(
                ST_SetSRID(ST_MakePoint(c.center_lng, c.center_lat), 4326)::geography,
                src.geom::geography,
                %s
              )
        ORDER BY c.avg_severity DESC
        LIMIT 1
    """, (location_id, snap_time, snap_time, NEARBY_METERS))
    row = cur.fetchone()
    return {
        "active_cluster_nearby": row is not None,
        "cluster_intensity":     float(row[0]) if row else 0.0,
    }


def _compute_event_features(cur, location_id: int, snap_time: datetime) -> dict:
    """Kiểm tra planned_event trong 3h tới, trong bán kính EVENT_RADIUS_M."""
    cur.execute("""
        SELECT COALESCE(MAX(e.expected_attendance), 0)
        FROM planned_event e
        JOIN location src ON src.location_id = %s
        WHERE e.start_time >= %s
          AND e.start_time <  %s + INTERVAL '3 hours'
          AND ST_DWithin(
                ST_SetSRID(ST_MakePoint(e.longitude, e.latitude), 4326)::geography,
                src.geom::geography,
                %s
              )
    """, (location_id, snap_time, snap_time, EVENT_RADIUS_M))
    row = cur.fetchone()
    attendance = int(row[0]) if row else 0
    return {
        "event_within_3h":        attendance > 0,
        "event_attendance_nearby": attendance,
    }


def _compute_zone_features(cur, location_id: int) -> dict:
    """Lấy chỉ số hạ tầng và lịch sử từ zone chứa địa điểm này."""
    cur.execute("""
        SELECT
            az.zone_pressure_score,
            az.road_ratio,
            az.name,
            COALESCE(
                (SELECT AVG(i2.potential_score)
                 FROM incident i2
                 JOIN location l2 ON i2.location_id = l2.location_id
                 WHERE ST_Contains(az.geom, l2.geom)),
                0.0
            ) AS historical_risk
        FROM administrative_zone az
        JOIN location src ON src.location_id = %s
        WHERE ST_Contains(az.geom, src.geom)
        LIMIT 1
    """, (location_id,))
    row = cur.fetchone()
    if not row:
        return {
            "zone_pressure_score":  0.0,
            "zone_road_density":    0.0,
            "zone_historical_risk": 0.0,
            "zone_name":            None,
        }
    return {
        "zone_pressure_score":  float(row[0]),
        "zone_road_density":    float(row[1]),
        "zone_historical_risk": float(row[3]),
        "zone_name":            row[2],
    }


def _compute_weather_features(cur, snap_time: datetime) -> dict:
    """Lấy dữ liệu thời tiết gần nhất với snapshot_time từ weather_snapshot."""
    cur.execute("""
        SELECT rainfall_mm, is_raining, wind_speed_kmh, weather_code
        FROM weather_snapshot
        WHERE snapshot_hour <= %s
        ORDER BY snapshot_hour DESC
        LIMIT 1
    """, (snap_time,))
    row = cur.fetchone()
    if not row:
        return {"rainfall_mm": 0.0, "is_raining": False,
                "wind_speed_kmh": 0.0, "weather_code": 0}
    return {
        "rainfall_mm":    float(row[0]),
        "is_raining":     bool(row[1]),
        "wind_speed_kmh": float(row[2]),
        "weather_code":   int(row[3]),
    }


def _compute_recurring_prob(cur, location_id: int, snap_time: datetime) -> float:
    """
    Tra cứu xác suất tắc định kỳ theo giờ và ngày.
    Dùng bidirectional LIKE để khớp tên không hoàn toàn giống nhau:
      "Láng" ↔ "Đường Láng", "Võ Chí Công" ↔ "Nút giao Võ Chí Công - Xuân La"
    """
    snap_vn  = snap_time.astimezone(VN_TZ) if snap_time.tzinfo else snap_time
    hour     = snap_vn.hour
    day_type = 'weekend' if snap_vn.weekday() >= 5 else 'weekday'

    cur.execute("""
        SELECT MAX(rp.congestion_prob)
        FROM recurring_pattern rp
        JOIN location src ON src.location_id = %s
        WHERE rp.is_active = TRUE
          AND rp.day_type IN (%s, 'all')
          AND rp.hour_start <= %s
          AND rp.hour_end   >  %s
          AND LENGTH(src.place_name) >= 5
          AND (
              LOWER(rp.location_name) = LOWER(src.place_name)
              OR LOWER(rp.location_name::text) LIKE CONCAT('%%', LOWER(src.place_name), '%%')
              OR LOWER(src.place_name::text)   LIKE CONCAT('%%', LOWER(rp.location_name), '%%')
          )
    """, (location_id, day_type, hour, hour))
    row = cur.fetchone()
    return float(row[0]) if row and row[0] else 0.0


def _compute_targets(cur, location_id: int, snap_time: datetime) -> dict:
    """
    Nhìn tiến: có incident mới trong 500m tại T+1h, T+2h, T+3h không?
    Trả về NULL nếu khoảng thời gian đó chưa có data (tương lai).
    """
    now = datetime.now(timezone.utc)
    results = {}
    windows = [
        ("target_t1h", 0, 1),
        ("target_t2h", 1, 2),
        ("target_t3h", 2, 3),
    ]
    for col, h_start, h_end in windows:
        t_start = snap_time + timedelta(hours=h_start)
        t_end   = snap_time + timedelta(hours=h_end)

        # Nếu khoảng thời gian chưa xảy ra → nhãn NULL
        if t_end > now:
            results[col] = None
            continue

        cur.execute("""
            SELECT COUNT(*)
            FROM incident i
            JOIN location l ON i.location_id = l.location_id
            JOIN location src ON src.location_id = %s
            WHERE i.detected_at >= %s
              AND i.detected_at <  %s
              AND ST_DWithin(l.geom::geography, src.geom::geography, %s)
        """, (location_id, t_start, t_end, NEARBY_METERS))
        count = cur.fetchone()[0]
        results[col] = 1 if count > 0 else 0

    return results


# ── Main ──────────────────────────────────────────────────────────────────────

def run_feature_engineer():
    conn = get_connection()
    if not conn:
        print("❌ Không kết nối được DB.")
        return

    cur = conn.cursor()
    inserted = skipped = errors = 0

    try:
        # Đảm bảo unique index tồn tại để ON CONFLICT hoạt động
        cur.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS uq_features_loc_snap
            ON traffic_features(location_id, snapshot_time)
        """)
        conn.commit()

        # Lấy tất cả incidents cần tạo feature
        cur.execute("""
            SELECT i.incident_id, i.location_id, i.detected_at
            FROM incident i
            JOIN location l ON i.location_id = l.location_id
            WHERE l.geom IS NOT NULL
            ORDER BY i.detected_at
        """)
        incidents = cur.fetchall()
        total = len(incidents)
        print(f"📊 Tìm thấy {total} incidents để tạo features")

        for idx, (incident_id, location_id, snap_time) in enumerate(incidents, 1):
            try:
                # Đảm bảo snap_time có timezone
                if snap_time.tzinfo is None:
                    snap_time = snap_time.replace(tzinfo=timezone.utc)

                # Dùng giờ địa phương Việt Nam (UTC+7) cho tất cả tính toán
                snap_vn    = snap_time.astimezone(VN_TZ)
                hour       = snap_vn.hour
                dow        = snap_vn.weekday()  # 0=T2, 6=CN
                is_weekend = dow >= 5
                is_peak    = _is_peak(hour, is_weekend)

                inc_feat  = _compute_incident_features(cur, location_id, snap_time)
                cls_feat  = _compute_cluster_features(cur, location_id, snap_time)
                evt_feat  = _compute_event_features(cur, location_id, snap_time)
                zone_feat = _compute_zone_features(cur, location_id)
                rec_prob  = _compute_recurring_prob(cur, location_id, snap_time)
                wthr_feat = _compute_weather_features(cur, snap_time)
                targets   = _compute_targets(cur, location_id, snap_time)

                cur.execute("""
                    INSERT INTO traffic_features (
                        location_id, zone_name, snapshot_time,
                        hour_of_day, day_of_week, is_peak_hour, is_weekend,
                        incident_count_2h, incident_count_6h,
                        has_accident_nearby, avg_score_2h,
                        active_cluster_nearby, cluster_intensity,
                        event_within_3h, event_attendance_nearby,
                        zone_road_density, zone_pressure_score, zone_historical_risk,
                        recurring_prob,
                        rainfall_mm, is_raining, wind_speed_kmh,
                        target_t1h, target_t2h, target_t3h
                    ) VALUES (
                        %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s,
                        %s, %s,
                        %s, %s,
                        %s, %s,
                        %s, %s, %s,
                        %s,
                        %s, %s, %s,
                        %s, %s, %s
                    )
                    ON CONFLICT (location_id, snapshot_time) DO UPDATE SET
                        hour_of_day            = EXCLUDED.hour_of_day,
                        day_of_week            = EXCLUDED.day_of_week,
                        is_peak_hour           = EXCLUDED.is_peak_hour,
                        is_weekend             = EXCLUDED.is_weekend,
                        recurring_prob         = EXCLUDED.recurring_prob,
                        rainfall_mm            = EXCLUDED.rainfall_mm,
                        is_raining             = EXCLUDED.is_raining,
                        wind_speed_kmh         = EXCLUDED.wind_speed_kmh
                """, (
                    location_id,
                    zone_feat["zone_name"],
                    snap_time,
                    hour, dow, is_peak, is_weekend,
                    inc_feat["incident_count_2h"],
                    inc_feat["incident_count_6h"],
                    inc_feat["has_accident_nearby"],
                    inc_feat["avg_score_2h"],
                    cls_feat["active_cluster_nearby"],
                    cls_feat["cluster_intensity"],
                    evt_feat["event_within_3h"],
                    evt_feat["event_attendance_nearby"],
                    zone_feat["zone_road_density"],
                    zone_feat["zone_pressure_score"],
                    zone_feat["zone_historical_risk"],
                    rec_prob,
                    wthr_feat["rainfall_mm"],
                    wthr_feat["is_raining"],
                    wthr_feat["wind_speed_kmh"],
                    targets["target_t1h"],
                    targets["target_t2h"],
                    targets["target_t3h"],
                ))

                if cur.rowcount > 0:
                    inserted += 1
                else:
                    skipped += 1

                if idx % 50 == 0 or idx == total:
                    conn.commit()
                    print(f"  [{idx}/{total}] inserted={inserted} skipped={skipped} errors={errors}")

            except Exception as e:
                errors += 1
                conn.rollback()
                print(f"  ⚠️  Incident #{incident_id} lỗi: {e}")

        conn.commit()

        # Thống kê kết quả
        cur.execute("SELECT COUNT(*) FROM traffic_features")
        total_rows = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM traffic_features WHERE target_t1h IS NOT NULL")
        labeled = cur.fetchone()[0]

        cur.execute("""
            SELECT
                AVG(incident_count_2h),
                AVG(zone_pressure_score),
                SUM(CASE WHEN target_t1h = 1 THEN 1 ELSE 0 END)::float
                    / NULLIF(SUM(CASE WHEN target_t1h IS NOT NULL THEN 1 ELSE 0 END), 0)
            FROM traffic_features
        """)
        row = cur.fetchone()

        print(f"\n{'═' * 55}")
        print(f"✅ FEATURE ENGINEERING HOÀN TẤT")
        print(f"   Rows trong traffic_features : {total_rows}")
        print(f"   Có nhãn (target_t1h != NULL): {labeled}")
        print(f"   Inserted / Skipped / Errors : {inserted} / {skipped} / {errors}")
        if row[0] is not None:
            print(f"   avg incident_count_2h       : {row[0]:.2f}")
            print(f"   avg zone_pressure_score      : {row[1]:.3f}")
        if row[2] is not None:
            print(f"   Tỷ lệ tắc T+1h (positive)  : {row[2]*100:.1f}%")
        print(f"{'═' * 55}")

    except Exception as e:
        conn.rollback()
        print(f"❌ Lỗi: {e}")
        import traceback; traceback.print_exc()
    finally:
        cur.close()
        release_connection(conn)


if __name__ == "__main__":
    run_feature_engineer()
