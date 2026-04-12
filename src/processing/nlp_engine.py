import re
from transformers import pipeline
from datetime import datetime
from src.core.db_manager import get_connection, release_connection
from src.utils.geo_helpers import get_coordinates
from src.processing.event_analyzer import EventImpactAnalyzer

print("[SYSTEM] Đang tải Model PhoBERT NER...")
ner_pipeline = pipeline(
    "ner",
    model="NlpHUST/ner-vietnamese-electra-base",
    aggregation_strategy="simple",
)

NER_MAX_CHARS = 400  # ~512 token ≈ 400 ký tự tiếng Việt


def load_lexicon_from_db(cursor):
    """Tải từ điển động từ Cơ sở dữ liệu"""
    cursor.execute("SELECT name, base_score FROM incident_category")
    base_scores = {row[0]: row[1] for row in cursor.fetchall()}

    cursor.execute("SELECT word, multiplier FROM traffic_keyword")
    multipliers = {row[0]: row[1] for row in cursor.fetchall()}

    return base_scores, multipliers


def extract_entities(text):
    """Bóc tách địa danh bằng cơ chế Hybrid và phân loại sự cố"""
    # 1. Thử dùng AI (PhoBERT)
    entities = ner_pipeline(text[:NER_MAX_CHARS])
    locations = [ent['word'] for ent in entities if ent['entity_group'] == 'LOC']
    place_name = ", ".join(locations) if locations else None

    # 2. Cơ chế Fallback: Nếu AI thất bại, dùng Regex
    if not place_name:
        pattern = r"(?:tại|ở|đường|phố|cầu|hầm|ngã tư|ngã ba)\s+([A-ZĐ][\w]+(\s+[A-ZĐ][\w]+)*)"
        match = re.search(pattern, text)
        if match:
            place_name = match.group(1)
            print(f"    [FALLBACK] Đã cứu nguy AI! Tìm thấy địa danh: '{place_name}'")

    # 3. Phân loại sự cố cơ bản
    text_lower = text.lower()
    if any(kw in text_lower for kw in ["tai nạn", "va chạm"]): return place_name, "accident"
    if any(kw in text_lower for kw in ["ngập", "mưa lớn"]): return place_name, "flood"
    if any(kw in text_lower for kw in ["thi công", "lô cốt"]): return place_name, "construction"

    return place_name, "congestion"  # Mặc định


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

        # 2. Lấy các bản tin chưa đọc (batch 50 — NLP chạy mỗi 1 phút, đủ theo kịp scraper)
        cursor.execute("""
            SELECT feed_id, raw_content, fetched_at FROM raw_feed
            WHERE is_processed = FALSE
            ORDER BY fetched_at ASC
            LIMIT 50
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

            lat, lng = get_coordinates(place_name)
            if not lat or not lng:
                print(f"❌ Bỏ qua: Không định vị được GPS cho '{place_name}'.")
                cursor.execute("UPDATE raw_feed SET is_processed = true WHERE feed_id = %s", (feed_id,))
                continue

            # --- TÍNH ĐIỂM TIỀM NĂNG TỪ VĂN BẢN ---
            potential_score = calculate_potential_score(content, incident_type, base_scores, multipliers)
            print(f"👉 Điểm văn bản ({incident_type}): {potential_score:.2f}/1.0")

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

            # 3. Lưu vào Database
            try:
                cursor.execute(
                    "INSERT INTO location (place_name, latitude, longitude, geom) VALUES (%s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326)) RETURNING location_id",
                    (place_name, lat, lng, lng, lat)
                )
                location_id = cursor.fetchone()[0]

                cursor.execute(
                    "INSERT INTO incident (feed_id, location_id, incident_type, potential_score, confidence_level, detected_at) VALUES (%s, %s, %s, %s, %s, %s)",
                    (feed_id, location_id, incident_type, potential_score, 0.8, fetched_at)
                    # Nguồn báo chí set độ tin cậy 0.8
                )
                print("✅ LƯU THÀNH CÔNG VÀO DATABASE!")
            except Exception as db_err:
                print(f"❌ Lỗi khi lưu DB: {db_err}")

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