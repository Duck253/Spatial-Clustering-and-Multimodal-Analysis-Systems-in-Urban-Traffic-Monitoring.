import json
import os
from src.core.db_manager import get_connection, release_connection


def import_districts_from_geojson(filepath):
    # Kiểm tra xem file có nằm đúng ở thư mục data/ không
    if not os.path.exists(filepath):
        print(f"❌ Lỗi: Không tìm thấy file tại '{filepath}'.")
        print("Vui lòng đảm bảo bạn đã đặt file hanoi_districts.geojson vào thư mục data/ ở gốc dự án.")
        return

    conn = get_connection()
    if not conn: return
    cursor = conn.cursor()

    try:
        # 1. Dọn dẹp dữ liệu cũ (Để có thể chạy script nhiều lần không bị lỗi trùng)
        print("🧹 Đang dọn dẹp dữ liệu cũ trong bảng administrative_zone...")
        cursor.execute("TRUNCATE TABLE administrative_zone RESTART IDENTITY;")

        # 2. Đọc file GeoJSON
        with open(filepath, 'r', encoding='utf-8') as f:
            geojson_data = json.load(f)

        features = geojson_data.get('features', [])
        print(f"🗺️ Đọc file thành công. Bắt đầu phân tích dữ liệu...\n")

        success_count = 0
        for feature in features:
            props = feature.get('properties', {})
            geom = feature.get('geometry', None)

            # Bỏ qua nếu không có hình dạng không gian
            if not geom:
                continue

                # Lọc chỉ lấy Quận/Huyện (Cấp hành chính số 6 của Việt Nam)
            if props.get('admin_level') != '6':
                continue

            # Lấy tên Quận/Huyện
            name = props.get('name', 'Không xác định')

            # Giả lập dân số (Tạm để 250.000 người/quận)
            population = 250000

            # Ép kiểu Geometry Dictionary thành chuỗi JSON String
            geom_json = json.dumps(geom)

            # 3. Nạp vào PostGIS (Dùng hàm ST_Multi để chuẩn hóa hình học)
            cursor.execute("""
                INSERT INTO administrative_zone (name, parent_name, population, total_area_km2, road_area_km2, geom)
                VALUES (%s, %s, %s, %s, %s, ST_Multi(ST_GeomFromGeoJSON(%s)))
            """, (name, 'Hà Nội', population, 0, 0, geom_json))

            success_count += 1
            print(f"  ✅ Đã thêm ranh giới: {name}")

        # 4. Kích hoạt PostGIS tự động tính diện tích (km2)
        print("\n📐 Đang yêu cầu PostGIS tự động đo đạc diện tích cho các quận...")
        cursor.execute("""
            UPDATE administrative_zone 
            SET total_area_km2 = ROUND((ST_Area(geom::geography) / 1000000)::numeric, 2);
        """)

        conn.commit()
        print(f"\n🎉 HOÀN TẤT! Đã bơm thành công {success_count} Quận/Huyện và đo diện tích vào Database.")

    except Exception as e:
        print(f"❌ Lỗi import: {e}")
        conn.rollback()
    finally:
        cursor.close()
        release_connection(conn)


if __name__ == "__main__":
    # Đường dẫn sẽ tự động tìm vào thư mục data/ nằm ngang hàng với src/
    import_districts_from_geojson("data/hanoi_districts.geojson")