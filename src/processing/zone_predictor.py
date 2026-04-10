"""
zone_predictor.py
-----------------
Dự báo khả năng tắc đường tại một vùng trong N giờ tiếp theo.

Công thức tổng hợp:
  risk = 0.4 * zone_pressure_score
       + 0.4 * incident_factor      (số sự cố gần đây, chuẩn hóa)
       + 0.2 * event_factor         (sự kiện sắp diễn ra trong bán kính)

Ngưỡng:
  risk >= 0.7  → Cao
  risk >= 0.4  → Trung bình
  risk <  0.4  → Thấp
"""

import sys
sys.stdout.reconfigure(encoding='utf-8')

from datetime import datetime, timedelta
from src.core.db_manager import get_connection, release_connection


def predict_congestion(zone_name: str, hours_ahead: int = 1):
    conn = get_connection()
    if not conn:
        print("❌ Không kết nối được database.")
        return

    cursor = conn.cursor()
    now = datetime.now()
    future = now + timedelta(hours=hours_ahead)

    print(f"\n{'='*60}")
    print(f"  DỰ BÁO GIAO THÔNG: {zone_name.upper()}")
    print(f"  Thời điểm dự báo : {now.strftime('%H:%M %d/%m/%Y')}")
    print(f"  Khung thời gian  : {hours_ahead} giờ tiếp theo")
    print(f"{'='*60}")

    try:
        # ── 1. Lấy dữ liệu hạ tầng vùng ────────────────────────────
        cursor.execute("""
            SELECT zone_id, name, population, total_area_km2,
                   pop_density, road_ratio, zone_pressure_score,
                   ST_Centroid(geom) AS centroid
            FROM administrative_zone
            WHERE name ILIKE %s
            LIMIT 1
        """, (f"%{zone_name}%",))
        zone = cursor.fetchone()

        if not zone:
            print(f"⚠️  Không tìm thấy vùng '{zone_name}' trong database.")
            return

        zone_id, name, population, area, pop_density, road_ratio, pressure_score, centroid = zone
        pressure_score = float(pressure_score or 0)
        pop_density    = float(pop_density or 0)
        road_ratio     = float(road_ratio or 0)

        print(f"\n📍 Vùng tìm thấy : {name}")
        print(f"   Dân số         : {int(population):,} người")
        print(f"   Mật độ         : {pop_density:,.0f} người/km²")
        print(f"   Tỷ lệ đường    : {road_ratio:.2%}")
        print(f"   Áp lực hạ tầng : {pressure_score:.2f}/1.0")

        # ── 2. Đếm sự cố gần đây (24h qua) trong vùng ──────────────
        cursor.execute("""
            SELECT COUNT(*), AVG(i.potential_score)
            FROM incident i
            JOIN location l ON i.location_id = l.location_id
            WHERE i.detected_at >= NOW() - INTERVAL '24 hours'
              AND ST_Contains(
                    (SELECT geom FROM administrative_zone WHERE zone_id = %s),
                    l.geom
                  )
        """, (zone_id,))
        row = cursor.fetchone()
        incident_count = int(row[0] or 0)
        avg_incident_score = float(row[1] or 0)

        # Chuẩn hóa: 5 sự cố trong 24h → factor = 1.0
        incident_factor = min(incident_count / 5.0, 1.0)

        print(f"\n🔍 Sự cố 24h qua   : {incident_count} vụ", end="")
        if incident_count > 0:
            print(f" (điểm TB: {avg_incident_score:.2f})")
        else:
            print()

        # ── 3. Kiểm tra sự kiện sắp diễn ra ────────────────────────
        cursor.execute("""
            SELECT title, start_time, expected_attendance, impact_radius_meters,
                   ST_Distance(
                       ST_Transform(ST_SetSRID(ST_MakePoint(longitude, latitude), 4326), 3857),
                       ST_Transform(%s::geometry, 3857)
                   ) AS dist_m
            FROM planned_event
            WHERE start_time BETWEEN NOW() AND %s
              AND ST_Distance(
                    ST_Transform(ST_SetSRID(ST_MakePoint(longitude, latitude), 4326), 3857),
                    ST_Transform(%s::geometry, 3857)
                  ) <= impact_radius_meters
            ORDER BY dist_m
            LIMIT 3
        """, (centroid, future, centroid))
        events = cursor.fetchall()

        event_factor = 0.0
        if events:
            print(f"\n⚠️  Sự kiện trong bán kính:")
            for ev_title, ev_start, ev_attend, ev_radius, ev_dist in events:
                dist_score     = 1.0 - (ev_dist / ev_radius)
                attend_score   = min(ev_attend / 50000.0, 1.0)
                ev_impact      = dist_score * 0.7 + attend_score * 0.3
                event_factor   = max(event_factor, ev_impact)
                print(f"   • {ev_title} lúc {ev_start.strftime('%H:%M')} "
                      f"(cách {ev_dist:.0f}m, {ev_attend:,} người) → impact: {ev_impact:.2f}")
        else:
            print(f"\n✅ Không có sự kiện nào trong {hours_ahead}h tới.")

        # ── 4. Tổng hợp điểm rủi ro ─────────────────────────────────
        risk = (0.4 * pressure_score
              + 0.4 * incident_factor
              + 0.2 * event_factor)

        if risk >= 0.7:
            level, icon = "CAO", "🔴"
        elif risk >= 0.4:
            level, icon = "TRUNG BÌNH", "🟡"
        else:
            level, icon = "THẤP", "🟢"

        print(f"\n{'─'*60}")
        print(f"  Điểm rủi ro tổng hợp : {risk:.2f}/1.0")
        print(f"  Dự báo tắc đường     : {icon} {level}")
        print(f"{'─'*60}")
        print(f"  Chi tiết:")
        print(f"    Áp lực hạ tầng (40%) : {0.4 * pressure_score:.2f}")
        print(f"    Sự cố gần đây  (40%) : {0.4 * incident_factor:.2f}")
        print(f"    Sự kiện sắp tới(20%) : {0.2 * event_factor:.2f}")
        print(f"{'='*60}\n")

    except Exception as e:
        print(f"❌ Lỗi: {e}")
        import traceback; traceback.print_exc()
    finally:
        cursor.close()
        release_connection(conn)


if __name__ == "__main__":
    import sys
    zone = sys.argv[1] if len(sys.argv) > 1 else "Láng"
    hours = int(sys.argv[2]) if len(sys.argv) > 2 else 1
    predict_congestion(zone, hours)