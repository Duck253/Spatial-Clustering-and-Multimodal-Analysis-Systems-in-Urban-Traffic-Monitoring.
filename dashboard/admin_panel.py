"""
dashboard/admin_panel.py
------------------------
Admin panel cho hệ thống Spatial Clustering & Urban Traffic Monitoring.

Chạy:
    streamlit run dashboard/admin_panel.py
"""

import sys
import os

# Thêm project root vào Python path để import được 'dashboard' và 'src'
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import threading
import time
from datetime import datetime, timedelta

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

sys.stdout.reconfigure(encoding="utf-8")

# ── Cấu hình trang ─────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Traffic Monitor — Admin",
    page_icon="🚦",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Import DB helper ────────────────────────────────────────────────────────────
from dashboard.db import query_df, execute

# ── CSS tùy chỉnh ──────────────────────────────────────────────────────────────
st.markdown("""
<style>
    [data-testid="stMetricValue"] { font-size: 2rem; font-weight: 700; }
    .stAlert { border-radius: 8px; }
    .block-container { padding-top: 1.5rem; }
    .job-card { background: #1e2130; border-radius: 10px; padding: 1rem; margin-bottom: 0.5rem; }
    thead th { background-color: #262730 !important; }
</style>
""", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# SIDEBAR — điều hướng
# ══════════════════════════════════════════════════════════════════════════════

with st.sidebar:
    st.title("🚦 Traffic Monitor")
    st.caption("Hệ thống giám sát giao thông Hà Nội")
    st.divider()

    page = st.radio(
        "Điều hướng",
        options=[
            "📊 Tổng quan",
            "📰 Thu thập dữ liệu",
            "🚨 Sự cố giao thông",
            "🗺️ Phân cụm ST-DBSCAN",
            "📅 Sự kiện",
            "⚙️ Điều khiển hệ thống",
        ],
        label_visibility="collapsed",
    )
    st.divider()

    # Nút làm mới dữ liệu
    if st.button("🔄 Làm mới dữ liệu", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    st.caption(f"Cập nhật: {datetime.now().strftime('%H:%M:%S %d/%m/%Y')}")


# ══════════════════════════════════════════════════════════════════════════════
# HÀM TRUY VẤN — cache 60 giây
# ══════════════════════════════════════════════════════════════════════════════

@st.cache_data(ttl=60)
def get_overview_stats():
    return {
        "raw_feed":   query_df("SELECT COUNT(*) AS n FROM raw_feed").iloc[0, 0],
        "unprocessed": query_df("SELECT COUNT(*) AS n FROM raw_feed WHERE is_processed = false").iloc[0, 0],
        "incident":   query_df("SELECT COUNT(*) AS n FROM incident").iloc[0, 0],
        "cluster":    query_df("SELECT COUNT(DISTINCT cluster_id) AS n FROM incident_cluster").iloc[0, 0],
        "source":     query_df("SELECT COUNT(*) AS n FROM data_source WHERE is_active = true").iloc[0, 0],
        "event":      query_df("SELECT COUNT(*) AS n FROM planned_event WHERE end_time > NOW()").iloc[0, 0],
    }


@st.cache_data(ttl=60)
def get_feed_by_hour():
    return query_df("""
        SELECT DATE_TRUNC('hour', fetched_at) AS hour,
               COUNT(*) AS total,
               SUM(CASE WHEN is_processed THEN 1 ELSE 0 END) AS processed
        FROM raw_feed
        WHERE fetched_at > NOW() - INTERVAL '24 hours'
        GROUP BY 1 ORDER BY 1
    """)


@st.cache_data(ttl=60)
def get_source_stats():
    return query_df("""
        SELECT ds.source_url,
               ds.source_type,
               ds.is_active,
               COUNT(rf.feed_id) AS total_articles,
               MAX(rf.fetched_at) AS last_fetched
        FROM data_source ds
        LEFT JOIN raw_feed rf ON rf.source_id = ds.source_id
        GROUP BY ds.source_id, ds.source_url, ds.source_type, ds.is_active
        ORDER BY total_articles DESC
    """)


@st.cache_data(ttl=60)
def get_recent_feed(limit=50):
    return query_df(f"""
        SELECT rf.feed_id,
               ds.source_url,
               LEFT(rf.raw_content, 120) AS nội_dung,
               rf.fetched_at,
               rf.is_processed
        FROM raw_feed rf
        JOIN data_source ds ON ds.source_id = rf.source_id
        ORDER BY rf.fetched_at DESC
        LIMIT {limit}
    """)


@st.cache_data(ttl=60)
def get_recent_incidents(limit=100):
    return query_df(f"""
        SELECT i.incident_id,
               i.incident_type,
               i.severity,
               ROUND(i.potential_score::numeric, 3)  AS potential_score,
               ROUND(i.confidence_level::numeric, 3) AS confidence_level,
               i.detected_at,
               l.place_name,
               l.district,
               l.latitude,
               l.longitude,
               i.cluster_id
        FROM incident i
        LEFT JOIN location l ON l.location_id = i.location_id
        ORDER BY i.detected_at DESC
        LIMIT {limit}
    """)


@st.cache_data(ttl=60)
def get_severity_dist():
    return query_df("""
        SELECT severity, COUNT(*) AS count
        FROM incident
        GROUP BY severity ORDER BY severity
    """)


@st.cache_data(ttl=60)
def get_score_over_time():
    return query_df("""
        SELECT DATE_TRUNC('hour', detected_at) AS hour,
               AVG(potential_score) AS avg_score,
               COUNT(*) AS count
        FROM incident
        WHERE detected_at > NOW() - INTERVAL '48 hours'
        GROUP BY 1 ORDER BY 1
    """)


@st.cache_data(ttl=60)
def get_incident_type_dist():
    return query_df("""
        SELECT incident_type, COUNT(*) AS count
        FROM incident
        GROUP BY incident_type ORDER BY count DESC
    """)


@st.cache_data(ttl=60)
def get_clusters():
    return query_df("""
        SELECT ic.cluster_id,
               COUNT(*) AS incident_count,
               MIN(i.detected_at) AS first_seen,
               MAX(i.detected_at) AS last_seen,
               AVG(i.potential_score) AS avg_score,
               AVG(l.latitude)  AS lat,
               AVG(l.longitude) AS lng
        FROM incident_cluster ic
        JOIN incident i ON i.incident_id = ic.incident_id
        LEFT JOIN location l ON l.location_id = i.location_id
        GROUP BY ic.cluster_id
        ORDER BY last_seen DESC
    """)


@st.cache_data(ttl=60)
def get_incident_map_data():
    return query_df("""
        SELECT l.latitude  AS lat,
               l.longitude AS lon,
               i.potential_score,
               i.severity,
               i.incident_type,
               l.place_name,
               i.detected_at
        FROM incident i
        JOIN location l ON l.location_id = i.location_id
        WHERE l.latitude IS NOT NULL
          AND l.longitude IS NOT NULL
          AND i.detected_at > NOW() - INTERVAL '48 hours'
    """)


@st.cache_data(ttl=60)
def get_planned_events():
    return query_df("""
        SELECT event_id, title,
               latitude, longitude,
               start_time, end_time,
               expected_attendance,
               impact_radius_meters,
               CASE WHEN end_time < NOW() THEN 'Đã qua'
                    WHEN start_time < NOW() THEN 'Đang diễn ra'
                    ELSE 'Sắp tới' END AS status
        FROM planned_event
        ORDER BY start_time DESC
        LIMIT 50
    """)


# ══════════════════════════════════════════════════════════════════════════════
# TRANG: TỔNG QUAN
# ══════════════════════════════════════════════════════════════════════════════

def page_tong_quan():
    st.header("📊 Tổng quan hệ thống")

    stats = get_overview_stats()

    # ── Metrics row ─────────────────────────────────────────────────────────
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("📰 Bài đã thu thập", f"{stats['raw_feed']:,}")
    c2.metric("⏳ Chưa xử lý NLP", f"{stats['unprocessed']:,}",
              delta=f"-{stats['raw_feed'] - stats['unprocessed']:,} đã xử lý",
              delta_color="off")
    c3.metric("🚨 Sự cố phát hiện", f"{stats['incident']:,}")
    c4.metric("🔵 Cụm hiện tại", f"{stats['cluster']:,}")
    c5.metric("📡 Nguồn dữ liệu", f"{stats['source']:,}")
    c6.metric("📅 Sự kiện sắp tới", f"{stats['event']:,}")

    st.divider()

    # ── Biểu đồ hoạt động 24h ───────────────────────────────────────────────
    col_l, col_r = st.columns([2, 1])

    with col_l:
        st.subheader("Hoạt động thu thập tin tức (24h qua)")
        df_hour = get_feed_by_hour()
        if not df_hour.empty:
            fig = go.Figure()
            fig.add_bar(x=df_hour["hour"], y=df_hour["total"],
                        name="Tổng bài", marker_color="#4C78A8")
            fig.add_bar(x=df_hour["hour"], y=df_hour["processed"],
                        name="Đã xử lý NLP", marker_color="#54A24B")
            fig.update_layout(
                barmode="overlay", height=300,
                legend=dict(orientation="h", y=1.1),
                margin=dict(l=0, r=0, t=30, b=0),
                xaxis_title="Giờ", yaxis_title="Số bài",
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Chưa có dữ liệu trong 24h qua.")

    with col_r:
        st.subheader("Loại sự cố")
        df_type = get_incident_type_dist()
        if not df_type.empty:
            fig = px.pie(df_type, values="count", names="incident_type",
                         hole=0.4, height=300,
                         color_discrete_sequence=px.colors.qualitative.Set3)
            fig.update_layout(margin=dict(l=0, r=0, t=0, b=0),
                               legend=dict(font_size=11))
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Chưa có dữ liệu sự cố.")

    # ── Potential score theo thời gian ──────────────────────────────────────
    st.subheader("Điểm nguy cơ trung bình theo giờ (48h qua)")
    df_score = get_score_over_time()
    if not df_score.empty:
        fig = px.area(df_score, x="hour", y="avg_score",
                      labels={"hour": "Thời gian", "avg_score": "Điểm trung bình"},
                      color_discrete_sequence=["#E45756"], height=220)
        fig.update_layout(margin=dict(l=0, r=0, t=10, b=0))
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Chưa có dữ liệu.")


# ══════════════════════════════════════════════════════════════════════════════
# TRANG: THU THẬP DỮ LIỆU
# ══════════════════════════════════════════════════════════════════════════════

def page_thu_thap():
    st.header("📰 Thu thập dữ liệu")

    # ── Thống kê nguồn ───────────────────────────────────────────────────────
    st.subheader("Các nguồn dữ liệu đang hoạt động")
    df_src = get_source_stats()
    if not df_src.empty:
        # Rút gọn URL cho dễ nhìn
        df_src["nguồn"] = df_src["source_url"].apply(
            lambda u: u.split("/")[2] if u and "/" in u else u
        )
        df_src["last_fetched"] = pd.to_datetime(df_src["last_fetched"]).dt.strftime("%H:%M %d/%m")
        df_src["trạng thái"] = df_src["is_active"].map({True: "✅ Active", False: "❌ Inactive"})

        st.dataframe(
            df_src[["nguồn", "source_type", "trạng thái", "total_articles", "last_fetched"]].rename(
                columns={"source_type": "loại", "total_articles": "bài đã lưu", "last_fetched": "lần cuối"}
            ),
            use_container_width=True, hide_index=True,
        )

        # Chart: bài theo nguồn
        top = df_src.nlargest(10, "total_articles")
        fig = px.bar(top, x="total_articles", y="nguồn", orientation="h",
                     labels={"total_articles": "Số bài", "nguồn": ""},
                     color="total_articles", color_continuous_scale="Blues",
                     height=350)
        fig.update_layout(margin=dict(l=0, r=0, t=10, b=0), coloraxis_showscale=False)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("Chưa có nguồn dữ liệu nào.")

    st.divider()

    # ── Bài gần đây ─────────────────────────────────────────────────────────
    st.subheader("Bài báo gần đây nhất")
    limit = st.slider("Số bài hiển thị", 10, 200, 50, step=10)
    df_feed = get_recent_feed(limit)
    if not df_feed.empty:
        df_feed["nguồn"] = df_feed["source_url"].apply(
            lambda u: u.split("/")[2] if u and "/" in u else u
        )
        df_feed["fetched_at"] = pd.to_datetime(df_feed["fetched_at"]).dt.strftime("%H:%M %d/%m")
        df_feed["is_processed"] = df_feed["is_processed"].map({True: "✅", False: "⏳"})
        st.dataframe(
            df_feed[["feed_id", "nguồn", "nội_dung", "fetched_at", "is_processed"]].rename(
                columns={"feed_id": "ID", "fetched_at": "thời gian", "is_processed": "NLP"}
            ),
            use_container_width=True, hide_index=True,
        )
    else:
        st.info("Chưa có bài nào.")


# ══════════════════════════════════════════════════════════════════════════════
# TRANG: SỰ CỐ GIAO THÔNG
# ══════════════════════════════════════════════════════════════════════════════

def page_su_co():
    st.header("🚨 Sự cố giao thông")

    col_l, col_r = st.columns([1, 1])

    with col_l:
        st.subheader("Phân bố mức độ nghiêm trọng")
        df_sev = get_severity_dist()
        if not df_sev.empty:
            labels = {1: "1-Nhẹ", 2: "2-Trung bình", 3: "3-Nặng", 4: "4-Rất nặng", 5: "5-Nghiêm trọng"}
            df_sev["mức độ"] = df_sev["severity"].map(labels).fillna(df_sev["severity"].astype(str))
            colors = ["#54A24B", "#F4C100", "#F58518", "#E45756", "#9D2B2B"]
            fig = px.bar(df_sev, x="mức độ", y="count",
                         color="mức độ", color_discrete_sequence=colors,
                         labels={"count": "Số sự cố"}, height=280)
            fig.update_layout(showlegend=False, margin=dict(l=0, r=0, t=10, b=0))
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Chưa có dữ liệu.")

    with col_r:
        st.subheader("Điểm potential score")
        df_inc = get_recent_incidents(200)
        if not df_inc.empty:
            fig = px.histogram(df_inc, x="potential_score", nbins=20,
                               labels={"potential_score": "Điểm nguy cơ", "count": "Số sự cố"},
                               color_discrete_sequence=["#4C78A8"], height=280)
            fig.update_layout(margin=dict(l=0, r=0, t=10, b=0))
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Chưa có dữ liệu.")

    st.divider()

    # ── Bảng sự cố ──────────────────────────────────────────────────────────
    st.subheader("Danh sách sự cố gần đây")
    df_inc = get_recent_incidents(100)
    if not df_inc.empty:
        # Filter
        fcol1, fcol2, fcol3 = st.columns(3)
        min_score = fcol1.slider("Điểm tối thiểu", 0.0, 1.0, 0.0, 0.05)
        if df_inc["incident_type"].notna().any():
            types = ["Tất cả"] + sorted(df_inc["incident_type"].dropna().unique().tolist())
            selected_type = fcol2.selectbox("Loại sự cố", types)
        else:
            selected_type = "Tất cả"
        has_cluster = fcol3.checkbox("Chỉ hiện có cluster")

        filtered = df_inc[df_inc["potential_score"] >= min_score]
        if selected_type != "Tất cả":
            filtered = filtered[filtered["incident_type"] == selected_type]
        if has_cluster:
            filtered = filtered[filtered["cluster_id"].notna()]

        df_display = filtered.copy()
        df_display["detected_at"] = pd.to_datetime(df_display["detected_at"]).dt.strftime("%H:%M %d/%m")
        df_display["cluster_id"] = df_display["cluster_id"].fillna("—").astype(str)

        st.dataframe(
            df_display[[
                "incident_id", "incident_type", "severity",
                "potential_score", "confidence_level",
                "place_name", "district", "detected_at", "cluster_id"
            ]].rename(columns={
                "incident_id": "ID", "incident_type": "loại",
                "potential_score": "nguy cơ", "confidence_level": "độ tin",
                "place_name": "địa điểm", "detected_at": "thời gian",
                "cluster_id": "cluster"
            }),
            use_container_width=True, hide_index=True,
        )
        st.caption(f"Hiển thị {len(filtered)} / {len(df_inc)} sự cố")
    else:
        st.info("Chưa có sự cố nào được phát hiện.")


# ══════════════════════════════════════════════════════════════════════════════
# TRANG: PHÂN CỤM ST-DBSCAN
# ══════════════════════════════════════════════════════════════════════════════

def page_phan_cum():
    st.header("🗺️ Phân cụm ST-DBSCAN")

    df_clusters = get_clusters()
    df_map = get_incident_map_data()

    # ── Thống kê cluster ─────────────────────────────────────────────────────
    if not df_clusters.empty:
        m1, m2, m3 = st.columns(3)
        m1.metric("Tổng số cụm", len(df_clusters))
        m2.metric("Tổng sự cố đã cluster", int(df_clusters["incident_count"].sum()))
        m3.metric("Điểm nguy cơ TB", f"{df_clusters['avg_score'].mean():.3f}")

        st.divider()
        st.subheader("Danh sách cụm")
        df_show = df_clusters.copy()
        df_show["first_seen"] = pd.to_datetime(df_show["first_seen"]).dt.strftime("%H:%M %d/%m")
        df_show["last_seen"]  = pd.to_datetime(df_show["last_seen"]).dt.strftime("%H:%M %d/%m")
        df_show["avg_score"]  = df_show["avg_score"].round(3)
        df_show["lat"]  = df_show["lat"].round(5)
        df_show["lng"]  = df_show["lng"].round(5)
        st.dataframe(
            df_show.rename(columns={
                "cluster_id": "ID cụm", "incident_count": "số sự cố",
                "first_seen": "bắt đầu", "last_seen": "kết thúc",
                "avg_score": "nguy cơ TB", "lat": "vĩ độ", "lng": "kinh độ",
            }),
            use_container_width=True, hide_index=True,
        )
    else:
        st.info("Chưa có cụm nào. ST-DBSCAN cần tối thiểu 3 sự cố trong bán kính 500m / 2h.")

    st.divider()

    # ── Bản đồ sự cố ────────────────────────────────────────────────────────
    st.subheader("Bản đồ sự cố (48h qua)")
    if not df_map.empty:
        df_map = df_map.dropna(subset=["lat", "lon"]).copy()
        df_map["lat"] = df_map["lat"].astype(float)
        df_map["lon"] = df_map["lon"].astype(float)
        df_map["severity"] = pd.to_numeric(df_map["severity"], errors="coerce").fillna(1).astype(int)
        df_map["potential_score"] = pd.to_numeric(df_map["potential_score"], errors="coerce").fillna(0.0)

        # Scatter map với Plotly
        fig = px.scatter_mapbox(
            df_map,
            lat="lat", lon="lon",
            color="potential_score",
            size="severity",
            size_max=18,
            hover_name="place_name",
            hover_data={"incident_type": True, "potential_score": ":.3f",
                        "detected_at": True, "lat": False, "lon": False},
            color_continuous_scale="YlOrRd",
            range_color=[0, 1],
            zoom=11,
            center={"lat": 21.028, "lon": 105.854},
            mapbox_style="carto-darkmatter",
            height=500,
            labels={"potential_score": "Nguy cơ", "severity": "Mức độ"},
        )
        fig.update_layout(margin=dict(l=0, r=0, t=0, b=0),
                          coloraxis_colorbar=dict(title="Nguy cơ"))
        st.plotly_chart(fig, use_container_width=True)
        st.caption(f"Hiển thị {len(df_map)} điểm sự cố có tọa độ trong 48h qua.")
    else:
        st.info("Chưa có sự cố nào có tọa độ để hiển thị trên bản đồ.")


# ══════════════════════════════════════════════════════════════════════════════
# TRANG: SỰ KIỆN
# ══════════════════════════════════════════════════════════════════════════════

def page_su_kien():
    st.header("📅 Sự kiện theo kế hoạch")

    df_ev = get_planned_events()
    if df_ev.empty:
        st.info("Chưa có sự kiện nào trong DB.")
        return

    # Metrics
    c1, c2, c3 = st.columns(3)
    ongoing = len(df_ev[df_ev["status"] == "Đang diễn ra"])
    upcoming = len(df_ev[df_ev["status"] == "Sắp tới"])
    past     = len(df_ev[df_ev["status"] == "Đã qua"])
    c1.metric("Đang diễn ra", ongoing)
    c2.metric("Sắp tới", upcoming)
    c3.metric("Đã qua", past)

    st.divider()

    # Filter theo status
    status_filter = st.selectbox("Lọc theo trạng thái",
                                  ["Tất cả", "Sắp tới", "Đang diễn ra", "Đã qua"])
    df_show = df_ev if status_filter == "Tất cả" else df_ev[df_ev["status"] == status_filter]

    df_show = df_show.copy()
    df_show["start_time"] = pd.to_datetime(df_show["start_time"]).dt.strftime("%H:%M %d/%m/%Y")
    df_show["end_time"]   = pd.to_datetime(df_show["end_time"]).dt.strftime("%H:%M %d/%m/%Y")
    df_show["expected_attendance"] = df_show["expected_attendance"].apply(lambda x: f"{x:,}")

    # Màu theo status
    def color_status(val):
        colors = {"Đang diễn ra": "background-color: #1a4731",
                  "Sắp tới": "background-color: #1a3a4f",
                  "Đã qua": "background-color: #2d2d2d"}
        return colors.get(val, "")

    styled = df_show[["title", "status", "start_time", "end_time",
                       "expected_attendance", "impact_radius_meters"]].rename(columns={
        "title": "Tên sự kiện", "status": "Trạng thái",
        "start_time": "Bắt đầu", "end_time": "Kết thúc",
        "expected_attendance": "Dự kiến người", "impact_radius_meters": "Bán kính ảnh hưởng (m)"
    })
    st.dataframe(styled, use_container_width=True, hide_index=True)

    # Bản đồ sự kiện
    df_map_ev = df_show[df_ev["latitude"].notna()].copy()
    if not df_map_ev.empty and "latitude" in df_ev.columns:
        st.subheader("Bản đồ địa điểm sự kiện")
        fig = px.scatter_mapbox(
            df_ev.dropna(subset=["latitude", "longitude"]),
            lat="latitude", lon="longitude",
            hover_name="title",
            hover_data={"status": True, "expected_attendance": True,
                        "latitude": False, "longitude": False},
            color="status",
            color_discrete_map={
                "Đang diễn ra": "#00CC44",
                "Sắp tới": "#4488FF",
                "Đã qua": "#888888",
            },
            zoom=11,
            center={"lat": 21.028, "lon": 105.854},
            mapbox_style="carto-darkmatter",
            height=400,
        )
        fig.update_layout(margin=dict(l=0, r=0, t=0, b=0))
        st.plotly_chart(fig, use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# TRANG: ĐIỀU KHIỂN HỆ THỐNG
# ══════════════════════════════════════════════════════════════════════════════

# Lưu trạng thái job trong session
if "job_logs" not in st.session_state:
    st.session_state.job_logs = {}
if "job_running" not in st.session_state:
    st.session_state.job_running = {}


def _run_job_thread(job_name: str, job_fn):
    """Chạy job trong thread riêng, ghi log vào session_state."""
    st.session_state.job_running[job_name] = True
    st.session_state.job_logs[job_name] = f"⏳ {job_name} đang chạy...\n"
    try:
        import io
        from contextlib import redirect_stdout
        buf = io.StringIO()
        with redirect_stdout(buf):
            job_fn()
        output = buf.getvalue() or "(không có output)"
        st.session_state.job_logs[job_name] = f"✅ Hoàn tất lúc {datetime.now().strftime('%H:%M:%S')}\n\n{output}"
    except Exception as e:
        st.session_state.job_logs[job_name] = f"❌ Lỗi: {e}"
    finally:
        st.session_state.job_running[job_name] = False


def _job_card(title: str, desc: str, job_name: str, job_fn, icon: str = "▶️"):
    with st.container(border=True):
        col_info, col_btn = st.columns([3, 1])
        with col_info:
            st.markdown(f"**{icon} {title}**")
            st.caption(desc)
        with col_btn:
            is_running = st.session_state.job_running.get(job_name, False)
            if st.button(
                "Đang chạy..." if is_running else "Chạy ngay",
                key=f"btn_{job_name}",
                disabled=is_running,
                use_container_width=True,
                type="primary" if not is_running else "secondary",
            ):
                t = threading.Thread(target=_run_job_thread, args=(job_name, job_fn), daemon=True)
                t.start()
                st.rerun()

        # Log output
        log = st.session_state.job_logs.get(job_name)
        if log:
            st.code(log, language=None)


def page_dieu_khien():
    st.header("⚙️ Điều khiển hệ thống")
    st.info("Kích hoạt từng module thủ công — tiện dùng khi scheduler chưa chạy hoặc cần debug.")

    st.subheader("Điều khiển module")

    # Import lazily để tránh load PhoBERT khi mở trang khác
    def run_scraper():
        from src.ingestion.news_scraper import run_scraper as _fn; _fn()

    def run_bulk():
        from src.ingestion.bulk_scraper import run_bulk_scraper as _fn; _fn()

    def run_nlp():
        from src.processing.nlp_engine import run_nlp_processor as _fn; _fn()

    def run_clustering():
        from src.processing.st_dbscan import run_clustering as _fn; _fn()

    def run_events():
        from src.ingestion.event_scraper import run_event_scraper as _fn; _fn()

    def run_zone_metrics():
        from src.processing.zone_metrics import compute_zone_metrics as _fn; _fn()

    _job_card(
        "RSS Scraper", "Thu thập tin tức từ 8 nguồn RSS định kỳ (chạy trong ~30s)",
        "scraper", run_scraper, "📰"
    )
    _job_card(
        "Bulk Scraper", "Thu thập hàng loạt từ Google News + RSS mở rộng (chạy ~2-3 phút)",
        "bulk", run_bulk, "📦"
    )
    _job_card(
        "NLP Engine (PhoBERT)", "Phân tích NLP cho các bài chưa xử lý — trích xuất sự cố và địa điểm",
        "nlp", run_nlp, "🧠"
    )
    _job_card(
        "ST-DBSCAN Clustering", "Gom cụm không-thời gian các sự cố trong 24h qua (eps1=500m, eps2=2h)",
        "clustering", run_clustering, "🗺️"
    )
    _job_card(
        "Event Scraper", "Quét Google News tìm sự kiện lớn tại Hà Nội (concert, bóng đá, ...)",
        "events", run_events, "📅"
    )
    _job_card(
        "Zone Metrics", "Tính lại chỉ số áp lực hạ tầng cho từng vùng (zone_pressure_score)",
        "zone", run_zone_metrics, "🏙️"
    )

    st.divider()

    # ── Thông tin DB ─────────────────────────────────────────────────────────
    st.subheader("Thông tin cơ sở dữ liệu")
    try:
        df_tables = query_df("""
            SELECT relname AS table_name,
                   n_live_tup AS row_count,
                   pg_size_pretty(pg_total_relation_size(relid)) AS total_size
            FROM pg_stat_user_tables
            ORDER BY n_live_tup DESC
        """)
        st.dataframe(df_tables.rename(columns={
            "table_name": "Bảng", "row_count": "Số dòng", "total_size": "Kích thước"
        }), use_container_width=True, hide_index=True)
    except Exception as e:
        st.error(f"Không lấy được thông tin DB: {e}")

    # ── Nút dọn dẹp ─────────────────────────────────────────────────────────
    st.divider()
    st.subheader("Bảo trì dữ liệu")
    col1, col2 = st.columns(2)
    with col1:
        if st.button("🗑️ Xóa raw_feed cũ hơn 24h", type="secondary", use_container_width=True):
            try:
                execute("DELETE FROM raw_feed WHERE fetched_at < NOW() - INTERVAL '24 hours'")
                st.success("Đã xóa bài báo cũ. Làm mới trang để cập nhật số liệu.")
                st.cache_data.clear()
            except Exception as e:
                st.error(f"Lỗi: {e}")
    with col2:
        if st.button("🔄 Đánh dấu lại tất cả raw_feed là chưa xử lý", type="secondary", use_container_width=True):
            try:
                execute("UPDATE raw_feed SET is_processed = false")
                st.success("Đã reset trạng thái. NLP sẽ xử lý lại toàn bộ.")
                st.cache_data.clear()
            except Exception as e:
                st.error(f"Lỗi: {e}")


# ══════════════════════════════════════════════════════════════════════════════
# ROUTING
# ══════════════════════════════════════════════════════════════════════════════

if page == "📊 Tổng quan":
    page_tong_quan()
elif page == "📰 Thu thập dữ liệu":
    page_thu_thap()
elif page == "🚨 Sự cố giao thông":
    page_su_co()
elif page == "🗺️ Phân cụm ST-DBSCAN":
    page_phan_cum()
elif page == "📅 Sự kiện":
    page_su_kien()
elif page == "⚙️ Điều khiển hệ thống":
    page_dieu_khien()
