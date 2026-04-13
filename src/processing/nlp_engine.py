import re
from transformers import pipeline
from datetime import datetime
from src.core.db_manager import get_connection, release_connection
from src.utils.geo_helpers import get_coordinates
from src.utils.hanoi_locations import lookup_location
from src.processing.event_analyzer import EventImpactAnalyzer

print("[SYSTEM] Đang tải Model PhoBERT NER...")
ner_pipeline = pipeline(
    "ner",
    model="NlpHUST/ner-vietnamese-electra-base",
    aggregation_strategy="simple",
)

NER_MAX_CHARS = 400  # ~512 token ≈ 400 ký tự tiếng Việt

# Cache geocoding — tránh gọi Nominatim nhiều lần cho cùng địa danh
_geocode_cache: dict[str, tuple[float, float]] = {}

# Địa danh quá chung chung hoặc ngoài Hà Nội — bỏ qua, không tạo incident
_GENERIC_LOCATIONS = {
    "hà nội", "ha noi", "thủ đô", "thành phố hà nội",
    "tp", "tphcm", "tp.hcm", "hồ chí minh", "thành phố hồ chí minh",
    "thủ đức", "bình dương", "đồng nai", "lâm đồng", "đà nẵng",
    "việt nam", "cả nước", "toàn quốc", "thành", "phố",
}


def load_lexicon_from_db(cursor):
    """Tải từ điển động từ Cơ sở dữ liệu"""
    cursor.execute("SELECT name, base_score FROM incident_category")
    base_scores = {row[0]: row[1] for row in cursor.fetchall()}

    cursor.execute("SELECT word, multiplier FROM traffic_keyword")
    multipliers = {row[0]: row[1] for row in cursor.fetchall()}

    return base_scores, multipliers


def extract_entities(text):
    """
    Bóc tách địa danh bằng cơ chế 3 tầng:
      1. Knowledge Base trực tiếp trên toàn văn bản (nhanh, chính xác)
      2. NER (PhoBERT/Electra)
      3. Regex fallback
    """
    text_lower = text.lower()

    # ── Tầng 1: KB lookup trực tiếp trên full text ───────────────────────
    kb_result = lookup_location(text)
    kb_name = kb_result[0] if kb_result else None
    # Loại bỏ nếu KB trả về địa danh quá chung chung
    if kb_name and kb_name.lower() not in _GENERIC_LOCATIONS:
        place_name = kb_name
        print(f"    [KB-DIRECT] Tìm thấy địa danh trong text: '{place_name}'")
    else:
        place_name = None

    # ── Tầng 2: NER — chỉ chạy nếu KB không tìm được ────────────────────
    if not place_name:
        try:
            entities = ner_pipeline(text[:NER_MAX_CHARS])
            locations = [
                ent['word'] for ent in entities
                if ent['entity_group'] == 'LOC'
                and ent['word'].lower() not in _GENERIC_LOCATIONS
                and len(ent['word']) > 3  # loại token ngắn như "TP", "HN"
            ]
            if locations:
                place_name = ", ".join(locations)
                print(f"    [NER] Tìm thấy: '{place_name}'")
        except Exception as e:
            print(f"    [NER] Lỗi: {e}")

    # ── Tầng 3: Regex fallback ────────────────────────────────────────────
    if not place_name:
        pattern = r"(?:tại|ở|đường|phố|cầu|hầm|ngã tư|ngã ba|tuyến)\s+([A-ZĐ][\w]+(?:\s+[A-ZĐ][\w]+)*)"
        match = re.search(pattern, text)
        if match:
            place_name = match.group(1)
            print(f"    [REGEX] Tìm thấy: '{place_name}'")

    # ── Phân loại sự cố ──────────────────────────────────────────────────
    if any(kw in text_lower for kw in ["tai nạn", "va chạm", "đâm xe", "lật xe"]):
        incident_type = "accident"
    elif any(kw in text_lower for kw in ["ngập", "mưa lớn", "lũ"]):
        incident_type = "flood"
    elif any(kw in text_lower for kw in ["thi công", "lô cốt", "sửa chữa", "cấm đường"]):
        incident_type = "construction"
    else:
        incident_type = "congestion"

    return place_name, incident_type


def get_recurring_boost(cursor, place_name: str, detected_at) -> float:
    """
    Kiểm tra xem địa điểm này có nằm trong khung giờ tắc định kỳ không.
    Trả về giá trị boost [0.0, 0.3] để cộng vào potential_score.
    """
    hour = detected_at.hour
    day_type = 'weekend' if detected_at.weekday() >= 5 else 'weekday'

    cursor.execute("""
        SELECT congestion_prob, note
        FROM recurring_pattern
        WHERE is_active = TRUE
          AND LOWER(location_name) = LOWER(%s)
          AND day_type IN (%s, 'all')
          AND hour_start <= %s
          AND hour_end   >  %s
        ORDER BY congestion_prob DESC
        LIMIT 1
    """, (place_name, day_type, hour, hour))

    row = cursor.fetchone()
    if row:
        prob, note = row
        boost = round(prob * 0.3, 3)   # tối đa +0.3 điểm
        print(f"    [RECURRING] Khung giờ tắc định kỳ: '{note}' (prob={prob:.2f}, +{boost:.2f})")
        return boost
    return 0.0


def calculate_potential_score(text, incident_type, base_scores, multipliers):
    """Tính điểm tiềm năng từ 0.0 đến 1.0 dựa trên từ điển Database"""
    text_lower = text.lower()
    score = base_scores.get(incident_type, 0.3)  # Lấy điểm gốc

    # Nhân hệ số nếu câu văn chứa từ khóa bổ trợ
    for word, mult in multipliers.items():
        if word in text_lower:
            score *= mult

    return min(1.0, score)  # Khóa trần ở mức 1.0


def fetch_active_events(cursor):
    cursor.execute("""
        SELECT title, latitude, longitude, start_time, end_time, expected_attendance, impact_radius_meters
        FROM planned_event
        WHERE end_time > NOW()
    """)
    events = []
    for row in cursor.fetchall():
        events.append({
            "title": row[0], "lat": row[1], "lng": row[2],
            "start_time": row[3], "end_time": row[4],
            "attendance": row[5], "radius": row[6]
        })
    return events


def get_zone_infrastructure_data(cursor, lat, lng):
    """Dùng PostGIS tìm phường chứa tọa độ và trả về dữ liệu hạ tầng"""
    query = """
        SELECT population, total_area_km2, road_area_km2, name
        FROM administrative_zone
        WHERE ST_Contains(geom, ST_SetSRID(ST_MakePoint(%s, %s), 4326))
        LIMIT 1;
    """
    cursor.execute(query, (lng, lat))  # PostGIS nhận (kinh độ, vĩ độ)
    return cursor.fetchone()


def run_nlp_processor():
    conn = get_connection()
    if not conn: return
    cursor = conn.cursor()

    try:
        # 1. Tải dữ liệu bộ nhớ đệm (Từ điển + Sự kiện)
        base_scores, multipliers = load_lexicon_from_db(cursor)
        active_events = fetch_active_events(cursor)
        event_analyzer = EventImpactAnalyzer(active_events)

        # 2. Lấy các bản tin chưa đọc (batch 200 — tăng từ 50 để bắt kịp backlog)
        cursor.execute("""
            SELECT feed_id, raw_content, fetched_at FROM raw_feed
            WHERE is_processed = FALSE
            ORDER BY fetched_at ASC
            LIMIT 200
        """)
        feeds = cursor.fetchall()

        for feed_id, content, fetched_at in feeds:
            print(f"\n--- 🔍 ĐANG XỬ LÝ TIN ID: {feed_id} ---")
            print(f"Nội dung: '{content}'")

            place_name, incident_type = extract_entities(content)

            if not place_name:
                print("❌ Bỏ qua: Không bóc tách được địa danh.")
                cursor.execute("UPDATE raw_feed SET is_processed = true WHERE feed_id = %s", (feed_id,))
                continue

            # Cache geocoding — dùng lại kết quả cho địa danh đã tra
            if place_name in _geocode_cache:
                lat, lng = _geocode_cache[place_name]
            else:
                lat, lng = get_coordinates(place_name)
                if lat and lng:
                    _geocode_cache[place_name] = (lat, lng)

            if not lat or not lng:
                print(f"❌ Bỏ qua: Không định vị được GPS cho '{place_name}'.")
                cursor.execute("UPDATE raw_feed SET is_processed = true WHERE feed_id = %s", (feed_id,))
                continue

            # --- TÍNH ĐIỂM TIỀM NĂNG TỪ VĂN BẢN ---
            potential_score = calculate_potential_score(content, incident_type, base_scores, multipliers)
            print(f"👉 Điểm văn bản ({incident_type}): {potential_score:.2f}/1.0")

            # --- CỘNG ĐIỂM TẮC ĐỊNH KỲ (recurring pattern) ---
            recurring_boost = get_recurring_boost(cursor, place_name, fetched_at)
            if recurring_boost > 0:
                potential_score = min(1.0, potential_score + recurring_boost)
                print(f"🔁 RECURRING: +{recurring_boost:.2f} → {potential_score:.2f}")

            # --- TÍNH ĐIỂM CỘNG HƯỞNG SỰ KIỆN ---
            impact_score, event_name = event_analyzer.calculate_event_impact(lat, lng, fetched_at)
            if impact_score > 0.3:
                bonus = impact_score * 0.5
                potential_score = min(1.0, potential_score + bonus)
                print(f"⚠️ CỘNG HƯỞNG: Gần '{event_name}' (+{bonus:.2f}) -> Điểm cuối cùng: {potential_score:.2f}")

            # --- ĐIỀU CHỈNH THEO HẠ TẦNG VÙNG ---
            zone_data = get_zone_infrastructure_data(cursor, lat, lng)
            if zone_data:
                pop, total_area, road_area, zone_name = zone_data
                if total_area > 0:
                    pop_density = pop / total_area
                    if pop_density > 20000:
                        potential_score = min(1.0, potential_score * 1.2)
                        print(f"📍 Khu vực {zone_name} mật độ cao ({pop_density:,.0f} ng/km²) → điểm: {potential_score:.2f}")

            # 3. Lưu vào Database (savepoint để lỗi 1 bài không hỏng cả batch)
            cursor.execute("SAVEPOINT sp_incident")
            try:
                # Upsert location — tránh tạo duplicate khi cùng địa danh xuất hiện nhiều lần
                cursor.execute(
                    """INSERT INTO location (place_name, latitude, longitude, geom)
                       VALUES (%s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326))
                       ON CONFLICT (place_name) DO UPDATE
                           SET latitude  = EXCLUDED.latitude,
                               longitude = EXCLUDED.longitude,
                               geom      = EXCLUDED.geom
                       RETURNING location_id""",
                    (place_name, lat, lng, lng, lat)
                )
                location_id = cursor.fetchone()[0]

                cursor.execute(
                    """INSERT INTO incident
                       (feed_id, location_id, incident_type, potential_score, confidence_level, detected_at)
                       VALUES (%s, %s, %s, %s, %s, %s)""",
                    (feed_id, location_id, incident_type, potential_score, 0.8, fetched_at)
                )
                cursor.execute("RELEASE SAVEPOINT sp_incident")
                print("✅ LƯU THÀNH CÔNG VÀO DATABASE!")
            except Exception as db_err:
                print(f"❌ Lỗi khi lưu DB: {db_err}")
                cursor.execute("ROLLBACK TO SAVEPOINT sp_incident")

            # Đánh dấu đã đọc
            cursor.execute("UPDATE raw_feed SET is_processed = true WHERE feed_id = %s", (feed_id,))

        conn.commit()

    except Exception as e:
        print(f"❌ Lỗi NLP Engine: {e}")
        conn.rollback()
    finally:
        cursor.close()
        release_connection(conn)


if __name__ == "__main__":
    run_nlp_processor()