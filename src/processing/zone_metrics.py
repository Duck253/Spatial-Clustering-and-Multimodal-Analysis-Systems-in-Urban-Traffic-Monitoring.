"""
zone_metrics.py
---------------
Tính toán các chỉ số áp lực hạ tầng cho từng vùng hành chính (phường/quận)
và lưu kết quả vào bảng administrative_zone.

Các chỉ số được tính:
  - pop_density         : Mật độ dân số (người/km²)
  - road_ratio          : Tỷ lệ diện tích đường / tổng diện tích (0–1)
  - zone_pressure_score : Chỉ số áp lực tổng hợp đã chuẩn hóa (0–1)

Công thức zone_pressure_score:
  pop_density_norm = (pop_density - min) / (max - min)   # Chuẩn hóa Min-Max
  road_shortage    = 1 - road_ratio                       # Ít đường → áp lực cao
  zone_pressure    = 0.6 × pop_density_norm + 0.4 × road_shortage
"""

from src.core.db_manager import get_connection, release_connection


def compute_zone_metrics():
    conn = get_connection()
    if not conn:
        print("❌ Không kết nối được database.")
        return

    cursor = conn.cursor()

    try:
        # ── 1. Đọc dữ liệu thô ──────────────────────────────────────────────
        cursor.execute("""
            SELECT zone_id, name, population, total_area_km2, road_area_km2
            FROM administrative_zone
            WHERE total_area_km2 > 0
        """)
        rows = cursor.fetchall()

        if not rows:
            print("⚠️  Bảng administrative_zone trống hoặc chưa import dữ liệu.")
            return

        print(f"📊 Đọc được {len(rows)} vùng hành chính. Đang tính toán...\n")

        # ── 2. Tính chỉ số thô ───────────────────────────────────────────────
        zones = []
        for zone_id, name, population, total_area, road_area in rows:
            pop_density = population / total_area if total_area > 0 else 0.0
            road_ratio  = road_area / total_area  if total_area > 0 else 0.0
            road_ratio  = min(road_ratio, 1.0)    # Khóa trần ở 1.0

            zones.append({
                "zone_id":     zone_id,
                "name":        name,
                "pop_density": pop_density,
                "road_ratio":  road_ratio,
            })

        # ── 3. Chuẩn hóa Min-Max mật độ dân số ─────────────────────────────
        densities   = [z["pop_density"] for z in zones]
        min_density = min(densities)
        max_density = max(densities)
        density_range = max_density - min_density

        print(f"  Mật độ dân số → Min: {min_density:,.0f}  Max: {max_density:,.0f} (người/km²)")

        for z in zones:
            if density_range > 0:
                pop_density_norm = (z["pop_density"] - min_density) / density_range
            else:
                pop_density_norm = 0.0

            road_shortage = 1.0 - z["road_ratio"]

            # Trọng số: 60% mật độ dân số, 40% thiếu hụt đường
            z["zone_pressure_score"] = round(0.6 * pop_density_norm + 0.4 * road_shortage, 4)
            z["pop_density"]         = round(z["pop_density"], 2)
            z["road_ratio"]          = round(z["road_ratio"], 4)

        # ── 4. Ghi kết quả vào DB ────────────────────────────────────────────
        update_sql = """
            UPDATE administrative_zone
            SET pop_density         = %s,
                road_ratio          = %s,
                zone_pressure_score = %s
            WHERE zone_id = %s
        """
        update_data = [
            (z["pop_density"], z["road_ratio"], z["zone_pressure_score"], z["zone_id"])
            for z in zones
        ]
        cursor.executemany(update_sql, update_data)
        conn.commit()

        # ── 5. In kết quả để kiểm tra ────────────────────────────────────────
        print(f"\n{'Vùng':<30} {'Mật độ (ng/km²)':>16} {'Tỷ lệ đường':>12} {'Áp lực (0-1)':>13}")
        print("-" * 75)

        zones_sorted = sorted(zones, key=lambda x: x["zone_pressure_score"], reverse=True)
        for z in zones_sorted:
            print(f"  {z['name']:<28} {z['pop_density']:>16,.1f} {z['road_ratio']:>12.4f} {z['zone_pressure_score']:>13.4f}")

        top = zones_sorted[0]
        bot = zones_sorted[-1]
        print(f"\n🔴 Áp lực cao nhất : {top['name']} ({top['zone_pressure_score']:.4f})")
        print(f"🟢 Áp lực thấp nhất: {bot['name']} ({bot['zone_pressure_score']:.4f})")
        print(f"\n✅ Đã cập nhật {len(zones)} vùng vào database.")

    except Exception as e:
        conn.rollback()
        print(f"❌ Lỗi: {e}")
    finally:
        cursor.close()
        release_connection(conn)


if __name__ == "__main__":
    compute_zone_metrics()