import json
import os
from src.core.db_manager import get_connection, release_connection


def import_roads():
    filepath = "data/hanoi_roads.geojson"
    if not os.path.exists(filepath): return print("❌ Không tìm thấy file!")

    conn = get_connection()
    cursor = conn.cursor()

    try:
        # 1. Tạo bảng chứa mạng lưới đường
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS road_network (
                road_id SERIAL PRIMARY KEY,
                highway_type VARCHAR(50),
                geom GEOMETRY(LineString, 4326) -- Đường thẳng
            );
            TRUNCATE TABLE road_network RESTART IDENTITY;
        """)

        # 2. Đọc file
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)

        features = data.get('features', [])
        print(f"🛣️ Tìm thấy {len(features)} đoạn đường. Đang nạp vào DB (hơi lâu một chút)...")

        # 3. Nạp vào DB
        for feat in features:
            geom = feat.get('geometry')
            # Chỉ lấy dữ liệu dạng LineString (đoạn thẳng)
            if not geom or geom['type'] != 'LineString': continue

            hw_type = feat.get('properties', {}).get('highway', 'unknown')

            cursor.execute("""
                INSERT INTO road_network (highway_type, geom)
                VALUES (%s, ST_GeomFromGeoJSON(%s))
            """, (hw_type, json.dumps(geom)))

        conn.commit()

        # 4. Tạo Index để truy vấn cắt không gian siêu tốc
        cursor.execute("CREATE INDEX IF NOT EXISTS road_geom_idx ON road_network USING GIST (geom);")
        conn.commit()

        print("🎉 XONG! Đã bơm toàn bộ mạng lưới đường vào PostGIS.")

    except Exception as e:
        print(f"❌ Lỗi: {e}")
        conn.rollback()
    finally:
        cursor.close()
        release_connection(conn)


if __name__ == "__main__":
    import_roads()