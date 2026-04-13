"""
dashboard/db.py
---------------
Kết nối DB dành riêng cho Streamlit (dùng st.cache_resource).
Tách ra để admin_panel.py gọn hơn.
"""

import os
import psycopg2
import psycopg2.extras
import pandas as pd
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

DB_PARAMS = dict(
    host=os.getenv("DB_HOST", "127.0.0.1"),
    port=int(os.getenv("DB_PORT", 5432)),
    dbname=os.getenv("DB_NAME", "traffic_monitoring"),
    user=os.getenv("DB_USER", "admin"),
    password=os.getenv("DB_PASS", "secretpassword"),
)


@st.cache_resource
def get_conn():
    """Một connection dùng chung cho toàn bộ session Streamlit."""
    conn = psycopg2.connect(**DB_PARAMS)
    conn.autocommit = False
    return conn


def query_df(sql: str, params=None) -> pd.DataFrame:
    """Chạy SELECT, trả về DataFrame. Dùng cursor để tránh warning SQLAlchemy."""
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        cur.execute(sql, params)
        rows = cur.fetchall()
        return pd.DataFrame([dict(r) for r in rows]) if rows else pd.DataFrame()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


def execute(sql: str, params=None):
    """Chạy câu lệnh không trả dữ liệu (INSERT/UPDATE/DELETE)."""
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(sql, params)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
