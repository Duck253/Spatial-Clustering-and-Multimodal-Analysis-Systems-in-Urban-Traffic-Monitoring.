"""
init_db.py
----------
Khởi tạo toàn bộ schema Database cho hệ thống Urban Traffic Monitoring.
Idempotent — an toàn để chạy nhiều lần (dùng IF NOT EXISTS / ON CONFLICT).

Thứ tự tạo bảng (tôn trọng FK):
  1. data_source
  2. incident_category        (+ seed)
  3. traffic_keyword          (+ seed)
  4. raw_feed                 (FK → data_source)
  5. location
  6. administrative_zone
  7. road_network
  8. planned_event
  9. incident_cluster
  10. incident                 (FK → raw_feed, location, incident_cluster)
"""

import sys
sys.stdout.reconfigure(encoding='utf-8')

from src.core.db_manager import get_connection, release_connection

# ── Seed data ───────────────────────────────────────────────────────────────

INCIDENT_CATEGORIES = [
    ("accident",     0.8),
    ("flood",        0.7),
    ("construction", 0.5),
    ("congestion",   0.4),
]

TRAFFIC_KEYWORDS = [
    # Từ khóa mức độ nghiêm trọng cao
    ("tai nạn nghiêm trọng", 1.5),
    ("tai nạn nặng",         1.5),
    ("chết người",           1.8),
    ("tử vong",              1.8),
    ("lật xe",               1.5),
    ("đâm nhau",             1.3),
    # Từ khóa tắc nghẽn
    ("tắc dài",              1.4),
    ("kẹt xe nặng",          1.4),
    ("ùn tắc nghiêm trọng",  1.4),
    ("tê liệt",              1.6),
    ("hàng km",              1.5),
    ("hàng giờ",             1.3),
    # Từ khóa quy mô đông người
    ("nhiều xe",             1.2),
    ("đông xe",              1.2),
    ("ùn ứ kéo dài",         1.3),
    # Từ khóa ngập
    ("ngập sâu",             1.3),
    ("ngập nặng",            1.3),
    # Từ khóa giảm mức độ
    ("đã thông xe",          0.5),
    ("giải tỏa",             0.6),
    ("lưu thông trở lại",    0.5),
]

# ── DDL ─────────────────────────────────────────────────────────────────────

DDL_STATEMENTS = [
    # 1. data_source
    """
    CREATE TABLE IF NOT EXISTS data_source (
        source_id   SERIAL PRIMARY KEY,
        source_type VARCHAR(50)  NOT NULL,
        source_url  TEXT         NOT NULL UNIQUE,
        source_name VARCHAR(100)
    )
    """,

    # 2. incident_category
    """
    CREATE TABLE IF NOT EXISTS incident_category (
        name       VARCHAR(50) PRIMARY KEY,
        base_score FLOAT8      NOT NULL CHECK (base_score BETWEEN 0 AND 1)
    )
    """,

    # 3. traffic_keyword
    """
    CREATE TABLE IF NOT EXISTS traffic_keyword (
        word       TEXT   PRIMARY KEY,
        multiplier FLOAT8 NOT NULL CHECK (multiplier > 0)
    )
    """,

    # 4. raw_feed
    """
    CREATE TABLE IF NOT EXISTS raw_feed (
        feed_id      SERIAL PRIMARY KEY,
        source_id    INT          REFERENCES data_source(source_id),
        content_type VARCHAR(20)  NOT NULL DEFAULT 'text',
        raw_content  TEXT         NOT NULL,
        content_hash VARCHAR(32),
        fetched_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
        is_processed BOOLEAN      NOT NULL DEFAULT FALSE
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_raw_feed_hash       ON raw_feed(content_hash)",
    "CREATE INDEX IF NOT EXISTS idx_raw_feed_processed  ON raw_feed(is_processed) WHERE is_processed = FALSE",
    "CREATE INDEX IF NOT EXISTS idx_raw_feed_fetched    ON raw_feed(fetched_at DESC)",

    # 5. location
    """
    CREATE TABLE IF NOT EXISTS location (
        location_id SERIAL PRIMARY KEY,
        place_name  TEXT   NOT NULL,
        latitude    FLOAT8,
        longitude   FLOAT8,
        geom        GEOMETRY(POINT, 4326),
        CONSTRAINT uq_location_place_name UNIQUE (place_name)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_location_geom ON location USING GIST(geom)",

    # 6. administrative_zone
    """
    CREATE TABLE IF NOT EXISTS administrative_zone (
        zone_id             SERIAL PRIMARY KEY,
        name                VARCHAR(255) NOT NULL,
        parent_name         VARCHAR(255),
        population          INT          DEFAULT 0,
        total_area_km2      FLOAT8       DEFAULT 0,
        road_area_km2       FLOAT8       DEFAULT 0,
        pop_density         FLOAT8       DEFAULT 0,
        road_ratio          FLOAT8       DEFAULT 0,
        zone_pressure_score FLOAT8       DEFAULT 0,
        geom                GEOMETRY(MULTIPOLYGON, 4326)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_admin_zone_geom ON administrative_zone USING GIST(geom)",

    # 7. road_network
    """
    CREATE TABLE IF NOT EXISTS road_network (
        road_id      SERIAL PRIMARY KEY,
        highway_type VARCHAR(50),
        geom         GEOMETRY(LINESTRING, 4326)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_road_geom ON road_network USING GIST(geom)",

    # 8. planned_event
    """
    CREATE TABLE IF NOT EXISTS planned_event (
        event_id              SERIAL PRIMARY KEY,
        title                 TEXT         NOT NULL,
        latitude              FLOAT8       NOT NULL,
        longitude             FLOAT8       NOT NULL,
        start_time            TIMESTAMPTZ  NOT NULL,
        end_time              TIMESTAMPTZ,
        expected_attendance   INT          DEFAULT 0,
        impact_radius_meters  INT          DEFAULT 2000
    )
    """,

    # 8b. recurring_pattern — điểm tắc định kỳ theo giờ/ngày
    """
    CREATE TABLE IF NOT EXISTS recurring_pattern (
        pattern_id      SERIAL PRIMARY KEY,
        location_name   VARCHAR(200) NOT NULL,
        latitude        FLOAT8,
        longitude       FLOAT8,
        day_type        VARCHAR(10)  NOT NULL CHECK (day_type IN ('weekday','weekend','all')),
        hour_start      SMALLINT     NOT NULL CHECK (hour_start BETWEEN 0 AND 23),
        hour_end        SMALLINT     NOT NULL CHECK (hour_end   BETWEEN 0 AND 23),
        congestion_prob FLOAT4       NOT NULL CHECK (congestion_prob BETWEEN 0 AND 1),
        note            TEXT,
        is_active       BOOLEAN DEFAULT TRUE,
        created_at      TIMESTAMPTZ DEFAULT NOW()
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_pattern_active ON recurring_pattern(is_active, day_type, hour_start, hour_end)",

    # 9. incident_cluster
    """
    CREATE TABLE IF NOT EXISTS incident_cluster (
        cluster_id      SERIAL PRIMARY KEY,
        detected_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
        center_lat      FLOAT8       NOT NULL,
        center_lng      FLOAT8       NOT NULL,
        incident_count  INT          NOT NULL,
        avg_severity    FLOAT8       NOT NULL,
        zone_name       VARCHAR(255),
        eps1_meters     INT,
        eps2_hours      FLOAT8
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_cluster_detected ON incident_cluster(detected_at DESC)",

    # 10. incident
    """
    CREATE TABLE IF NOT EXISTS incident (
        incident_id      SERIAL PRIMARY KEY,
        feed_id          INT          REFERENCES raw_feed(feed_id),
        location_id      INT          REFERENCES location(location_id),
        incident_type    VARCHAR(50)  REFERENCES incident_category(name),
        potential_score  FLOAT8       CHECK (potential_score BETWEEN 0 AND 1),
        confidence_level FLOAT8       CHECK (confidence_level BETWEEN 0 AND 1),
        detected_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
        cluster_id       INT          REFERENCES incident_cluster(cluster_id)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_incident_detected    ON incident(detected_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_incident_cluster     ON incident(cluster_id)",
    "CREATE INDEX IF NOT EXISTS idx_incident_location    ON incident(location_id)",

    # 11. traffic_features — feature vector cho ML model T+1/2/3h
    """
    CREATE TABLE IF NOT EXISTS traffic_features (
        feature_id             SERIAL PRIMARY KEY,
        location_id            INT REFERENCES location(location_id),
        zone_name              VARCHAR(100),
        snapshot_time          TIMESTAMPTZ NOT NULL,
        hour_of_day            SMALLINT NOT NULL,
        day_of_week            SMALLINT NOT NULL,
        is_peak_hour           BOOLEAN  NOT NULL DEFAULT FALSE,
        is_weekend             BOOLEAN  NOT NULL DEFAULT FALSE,
        incident_count_2h      INT     DEFAULT 0,
        incident_count_6h      INT     DEFAULT 0,
        has_accident_nearby    BOOLEAN DEFAULT FALSE,
        avg_score_2h           FLOAT4  DEFAULT 0.0,
        active_cluster_nearby  BOOLEAN DEFAULT FALSE,
        cluster_intensity      FLOAT4  DEFAULT 0.0,
        event_within_3h        BOOLEAN DEFAULT FALSE,
        event_attendance_nearby INT    DEFAULT 0,
        zone_road_density      FLOAT4  DEFAULT 0.0,
        zone_pressure_score    FLOAT4  DEFAULT 0.0,
        zone_historical_risk   FLOAT4  DEFAULT 0.0,
        recurring_prob         FLOAT4  DEFAULT 0.0,
        target_t1h             SMALLINT,
        target_t2h             SMALLINT,
        target_t3h             SMALLINT,
        created_at             TIMESTAMPTZ DEFAULT NOW()
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_features_snapshot ON traffic_features(snapshot_time DESC)",
    "CREATE INDEX IF NOT EXISTS idx_features_location ON traffic_features(location_id, snapshot_time DESC)",
]


def init_db():
    conn = get_connection()
    if not conn:
        print("❌ [INIT_DB] Không kết nối được Database. Kiểm tra .env và docker-compose.")
        return False

    cursor = conn.cursor()
    try:
        # Đảm bảo extension PostGIS tồn tại
        cursor.execute("CREATE EXTENSION IF NOT EXISTS postgis")
        conn.commit()

        # Migration: đổi tên cột created_at → detected_at trong incident_cluster (schema cũ)
        cursor.execute("""
            DO $$
            BEGIN
                IF EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'incident_cluster' AND column_name = 'created_at'
                ) AND NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'incident_cluster' AND column_name = 'detected_at'
                ) THEN
                    ALTER TABLE incident_cluster RENAME COLUMN created_at TO detected_at;
                    RAISE NOTICE 'Migration: đã đổi tên created_at → detected_at trong incident_cluster';
                END IF;
            END$$;
        """)
        conn.commit()

        # Tạo các bảng và index
        for stmt in DDL_STATEMENTS:
            cursor.execute(stmt)
        conn.commit()
        print("✅ [INIT_DB] Tất cả bảng đã được tạo.")

        # Seed incident_category
        cursor.executemany(
            "INSERT INTO incident_category (name, base_score) VALUES (%s, %s) ON CONFLICT DO NOTHING",
            INCIDENT_CATEGORIES,
        )
        print(f"   → incident_category: {len(INCIDENT_CATEGORIES)} loại sự cố")

        # Seed traffic_keyword
        cursor.executemany(
            "INSERT INTO traffic_keyword (word, multiplier) VALUES (%s, %s) ON CONFLICT DO NOTHING",
            TRAFFIC_KEYWORDS,
        )
        print(f"   → traffic_keyword: {len(TRAFFIC_KEYWORDS)} từ khóa")

        conn.commit()
        print("✅ [INIT_DB] Seed data hoàn tất. DB sẵn sàng.")
        return True

    except Exception as e:
        conn.rollback()
        print(f"❌ [INIT_DB] Lỗi khởi tạo schema: {e}")
        import traceback; traceback.print_exc()
        return False
    finally:
        cursor.close()
        release_connection(conn)


if __name__ == "__main__":
    ok = init_db()
    sys.exit(0 if ok else 1)
