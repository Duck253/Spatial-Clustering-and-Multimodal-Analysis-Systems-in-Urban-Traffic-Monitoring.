import psycopg2
from psycopg2 import pool
from src.core.config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS

try:
    # Tạo một "hồ chứa" kết nối (Pool) từ 1 đến 10 kết nối cùng lúc
    db_pool = psycopg2.pool.SimpleConnectionPool(
        1, 10,
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )
except Exception as e:
    print(f"❌ Lỗi khởi tạo Database Pool: {e}")
    db_pool = None

def get_connection():
    """Lấy một kết nối từ Pool"""
    if db_pool:
        return db_pool.getconn()
    return None

def release_connection(conn):
    """Trả kết nối lại cho Pool sau khi dùng xong"""
    if db_pool and conn:
        db_pool.putconn(conn)