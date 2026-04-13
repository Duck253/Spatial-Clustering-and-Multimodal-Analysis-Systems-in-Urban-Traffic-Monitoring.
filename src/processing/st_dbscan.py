"""
st_dbscan.py
------------
Thuật toán ST-DBSCAN (Spatiotemporal DBSCAN) gom cụm các sự cố giao thông.

Tham số:
  eps1      : bán kính không gian (mét)        — mặc định 500m
  eps2      : cửa sổ thời gian (giờ)           — mặc định 2 giờ
  min_pts   : số điểm tối thiểu để thành cụm   — mặc định 3

Đầu vào : bảng `incident` (join `location`) trong 24h gần nhất
Đầu ra  : bảng `incident_cluster` (tạo nếu chưa có)
           cột `cluster_id` trong `incident` được cập nhật

Kết quả mỗi cụm:
  - Tâm cụm (lat, lng trung bình)
  - Số sự cố
  - Điểm severity trung bình
  - Vùng hành chính chứa tâm cụm
"""

import sys
sys.stdout.reconfigure(encoding='utf-8')

from datetime import datetime, timedelta
from math import radians, cos, sin, asin, sqrt

from src.core.db_manager import get_connection, release_connection

# ── Tham số mặc định ────────────────────────────────────────────────
DEFAULT_EPS1_METERS = 500
DEFAULT_EPS2_HOURS  = 2.0
DEFAULT_MIN_PTS     = 2    # hạ từ 3→2 để tạo cluster trong giai đoạn data còn ít
LOOK_BACK_HOURS     = 48   # mở rộng 24→48h để gom được nhiều điểm hơn

UNVISITED = -2
NOISE     = -1


# ── Haversine distance (mét) ─────────────────────────────────────────
def _haversine(lat1, lng1, lat2, lng2) -> float:
    lat1, lng1, lat2, lng2 = map(radians, [lat1, lng1, lat2, lng2])
    dlat = lat2 - lat1
    dlng = lng2 - lng1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlng / 2) ** 2
    return 2 * 6_371_000 * asin(sqrt(a))


def _get_neighbors(points, idx, eps1, eps2):
    """Trả về danh sách index các điểm trong eps1 (m) VÀ eps2 (h) của points[idx]"""
    p = points[idx]
    neighbors = []
    for i, q in enumerate(points):
        if i == idx:
            continue
        spatial_dist = _haversine(p['lat'], p['lng'], q['lat'], q['lng'])
        temporal_dist = abs((p['time'] - q['time']).total_seconds()) / 3600.0
        if spatial_dist <= eps1 and temporal_dist <= eps2:
            neighbors.append(i)
    return neighbors


def _run_stdbscan(points, eps1, eps2, min_pts):
    """
    Chạy ST-DBSCAN, trả về list cluster_label có cùng độ dài với points.
    -1 = nhiễu (NOISE), 0+ = cluster_id
    """
    n = len(points)
    labels: list[int] = [UNVISITED] * n
    cluster_id = 0

    for i in range(n):
        if labels[i] != UNVISITED:
            continue

        neighbors = _get_neighbors(points, i, eps1, eps2)

        if len(neighbors) < min_pts:
            labels[i] = NOISE
            continue

        # Điểm lõi → tạo cụm mới
        labels[i] = cluster_id
        seed_set = list(neighbors)

        j = 0
        while j < len(seed_set):
            q_idx = seed_set[j]

            if labels[q_idx] == NOISE:
                labels[q_idx] = cluster_id  # Biên cụm

            if labels[q_idx] == UNVISITED:
                labels[q_idx] = cluster_id
                q_neighbors = _get_neighbors(points, q_idx, eps1, eps2)
                if len(q_neighbors) >= min_pts:
                    for nb in q_neighbors:
                        if nb not in seed_set:
                            seed_set.append(nb)

            j += 1

        cluster_id += 1

    return labels


def _ensure_tables(cursor):
    """Tạo bảng incident_cluster và cột cluster_id nếu chưa có"""
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS incident_cluster (
            cluster_id      SERIAL PRIMARY KEY,
            detected_at     TIMESTAMP DEFAULT NOW(),
            center_lat      FLOAT NOT NULL,
            center_lng      FLOAT NOT NULL,
            incident_count  INT NOT NULL,
            avg_severity    FLOAT NOT NULL,
            zone_name       VARCHAR(255),
            eps1_meters     INT,
            eps2_hours      FLOAT
        )
    """)
    cursor.execute("""
        ALTER TABLE incident
        ADD COLUMN IF NOT EXISTS cluster_id INT REFERENCES incident_cluster(cluster_id)
    """)


def run_clustering(eps1=DEFAULT_EPS1_METERS, eps2=DEFAULT_EPS2_HOURS, min_pts=DEFAULT_MIN_PTS):
    conn = get_connection()
    if not conn:
        print("❌ Không kết nối được database.")
        return

    cursor = conn.cursor()
    try:
        _ensure_tables(cursor)
        conn.commit()

        # ── 1. Đọc sự cố gần đây ────────────────────────────────────
        cutoff = datetime.now() - timedelta(hours=LOOK_BACK_HOURS)
        # Bounding box Hà Nội: lat 20.56–21.38, lng 105.28–106.02
        cursor.execute("""
            SELECT i.incident_id, l.latitude, l.longitude, i.detected_at, i.potential_score
            FROM incident i
            JOIN location l ON i.location_id = l.location_id
            WHERE i.detected_at >= %s
              AND l.latitude  BETWEEN 20.56 AND 21.38
              AND l.longitude BETWEEN 105.28 AND 106.02
            ORDER BY i.detected_at
        """, (cutoff,))
        rows = cursor.fetchall()

        if not rows:
            print("⚠️  Không có sự cố nào trong 24h để gom cụm.")
            return

        points = [
            {"id": r[0], "lat": float(r[1]), "lng": float(r[2]),
             "time": r[3], "score": float(r[4])}
            for r in rows
        ]

        print(f"📡 ST-DBSCAN: {len(points)} sự cố | eps1={eps1}m | eps2={eps2}h | min_pts={min_pts}")

        # ── 2. Chạy thuật toán ───────────────────────────────────────
        labels = _run_stdbscan(points, eps1, eps2, min_pts)

        # ── 3. Tổng hợp kết quả ──────────────────────────────────────
        clusters = {}
        noise_count = 0
        for i, label in enumerate(labels):
            if label == NOISE:
                noise_count += 1
                continue
            clusters.setdefault(label, []).append(points[i])

        print(f"   → Tìm thấy {len(clusters)} cụm | {noise_count} điểm nhiễu\n")

        # ── 4. Xóa kết quả cũ và lưu cụm mới vào DB ─────────────────
        cursor.execute("UPDATE incident SET cluster_id = NULL WHERE detected_at >= %s", (cutoff,))
        cursor.execute("DELETE FROM incident_cluster WHERE created_at >= %s", (cutoff,))

        for label, members in sorted(clusters.items()):
            center_lat  = sum(m['lat']   for m in members) / len(members)
            center_lng  = sum(m['lng']   for m in members) / len(members)
            avg_severity = sum(m['score'] for m in members) / len(members)

            # Tìm vùng hành chính chứa tâm cụm
            cursor.execute("""
                SELECT name FROM administrative_zone
                WHERE ST_Contains(geom, ST_SetSRID(ST_MakePoint(%s, %s), 4326))
                LIMIT 1
            """, (center_lng, center_lat))
            zone_row  = cursor.fetchone()
            zone_name = zone_row[0] if zone_row else "Không xác định"

            cursor.execute("""
                INSERT INTO incident_cluster
                    (center_lat, center_lng, incident_count, avg_severity, zone_name, eps1_meters, eps2_hours)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                RETURNING cluster_id
            """, (center_lat, center_lng, len(members), round(avg_severity, 4), zone_name, eps1, eps2))
            db_cluster_id = cursor.fetchone()[0]

            # Gắn cluster_id vào từng sự cố
            incident_ids = [m['id'] for m in members]
            cursor.execute(
                "UPDATE incident SET cluster_id = %s WHERE incident_id = ANY(%s)",
                (db_cluster_id, incident_ids)
            )

            severity_label = "🔴 CAO" if avg_severity >= 0.7 else ("🟡 TB" if avg_severity >= 0.4 else "🟢 THẤP")
            print(f"  Cụm #{db_cluster_id} | {zone_name:<30} | {len(members):>3} sự cố | severity {avg_severity:.2f} {severity_label}")
            print(f"           Tâm: ({center_lat:.5f}, {center_lng:.5f})")

        conn.commit()
        print(f"\n✅ Hoàn tất gom cụm. Đã lưu {len(clusters)} cụm vào DB.")

    except Exception as e:
        conn.rollback()
        print(f"❌ Lỗi ST-DBSCAN: {e}")
        import traceback; traceback.print_exc()
    finally:
        cursor.close()
        release_connection(conn)


if __name__ == "__main__":
    run_clustering()