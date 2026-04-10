import psycopg2
from datetime import datetime, timedelta
from src.core.db_manager import get_connection, release_connection


def setup_mock_events():
    conn = get_connection()
    if not conn: return
    cursor = conn.cursor()

    try:
        # 1. Tạo bảng planned_event (nếu chưa có)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS planned_event (
                event_id SERIAL PRIMARY KEY,
                title TEXT NOT NULL,
                latitude FLOAT8 NOT NULL,
                longitude FLOAT8 NOT NULL,
                start_time TIMESTAMPTZ NOT NULL,
                end_time TIMESTAMPTZ,
                expected_attendance INT,
                impact_radius_meters INT DEFAULT 2000
            );
        """)

        # Xóa dữ liệu cũ để chạy lại không bị trùng
        cursor.execute("TRUNCATE TABLE planned_event RESTART IDENTITY;")

        # 2. Giả lập thời gian (Sử dụng thời gian hiện tại làm gốc)
        now = datetime.now()

        # Sự kiện 1: Trận bóng đá tối nay tại Mỹ Đình (Cực kỳ đông)
        match_start = now.replace(hour=19, minute=0, second=0)
        match_end = now.replace(hour=21, minute=30, second=0)

        # Sự kiện 2: Lễ hội âm nhạc tối mai tại Hồ Gươm
        concert_start = (now + timedelta(days=1)).replace(hour=20, minute=0, second=0)
        concert_end = (now + timedelta(days=1)).replace(hour=23, minute=0, second=0)

        # 3. Chèn dữ liệu giả lập
        events_data = [
            ("Trận Chung kết V-League (SVĐ Mỹ Đình)", 21.0202, 105.7645, match_start, match_end, 40000, 3000),
            ("Lễ hội m nhạc Quốc tế (Phố đi bộ Hồ Gươm)", 21.0285, 105.8522, concert_start, concert_end, 15000, 1500)
        ]

        cursor.executemany("""
            INSERT INTO planned_event (title, latitude, longitude, start_time, end_time, expected_attendance, impact_radius_meters)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, events_data)

        conn.commit()
        print("✅ Đã giả lập thành công 2 sự kiện lớn vào Database!")

    except Exception as e:
        print(f"❌ Lỗi: {e}")
        conn.rollback()
    finally:
        cursor.close()
        release_connection(conn)


if __name__ == "__main__":
    setup_mock_events()