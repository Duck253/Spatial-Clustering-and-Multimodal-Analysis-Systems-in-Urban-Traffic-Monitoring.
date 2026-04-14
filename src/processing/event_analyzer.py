from datetime import datetime, timedelta, timezone
from geopy.distance import geodesic

VN_TZ = timezone(timedelta(hours=7))


class EventImpactAnalyzer:
    def __init__(self, active_events):
        # Danh sách các sự kiện được load từ database
        self.active_events = active_events

    def calculate_event_impact(self, incident_lat, incident_lng, incident_time):
        """
        Tính toán điểm S_event (0.0 đến 1.0).
        Nếu điểm kẹt xe gần sự kiện và sát giờ diễn ra -> Điểm càng gần 1.0
        """
        max_impact_score = 0.0
        influencing_event = None

        for event in self.active_events:
            # 1. Kiểm tra Thời gian (Time Window)
            # Sự kiện sẽ bắt đầu gây tắc đường từ TRƯỚC 2 tiếng, và kéo dài SAU 1 tiếng
            time_to_start = (event['start_time'].timestamp() - incident_time.timestamp()) / 3600  # Tính bằng giờ
            time_from_end = (incident_time.timestamp() - event['end_time'].timestamp()) / 3600

            is_time_relevant = (-1 <= time_from_end) and (time_to_start <= 2.0)

            if not is_time_relevant:
                continue  # Bỏ qua nếu thời gian không liên quan

            # 2. Kiểm tra Không gian (Spatial Distance)
            incident_coords = (incident_lat, incident_lng)
            event_coords = (event['lat'], event['lng'])

            # Tính khoảng cách đường chim bay (mét)
            dist_meters = geodesic(incident_coords, event_coords).meters

            if dist_meters <= event['radius']:
                # 3. Tính điểm Impact
                # Càng gần tâm sự kiện (SVĐ), điểm càng cao
                distance_score = 1.0 - (dist_meters / event['radius'])

                # Sự kiện càng đông người, tỷ trọng kẹt xe càng khủng khiếp (Chuẩn hóa chia cho 50,000 người)
                attendance_score = min(event['attendance'] / 50000.0, 1.0)

                # Công thức tổng hợp (70% do khoảng cách, 30% do quy mô)
                current_impact = (distance_score * 0.7) + (attendance_score * 0.3)

                if current_impact > max_impact_score:
                    max_impact_score = current_impact
                    influencing_event = event['title']

        return max_impact_score, influencing_event


# --- CHẠY THỬ NGHIỆM MÔ PHỎNG ---
if __name__ == "__main__":
    # 1. Load giả lập sự kiện từ DB (Ở đây mình hardcode array để test nhanh logic)
    now = datetime.now(VN_TZ)
    mock_db_events = [
        {
            "title": "Trận Bóng đá (Mỹ Đình)", "lat": 21.0202, "lng": 105.7645,
            "start_time": now.replace(hour=19, minute=0),
            "end_time": now.replace(hour=21, minute=30),
            "attendance": 40000, "radius": 3000
        }
    ]

    analyzer = EventImpactAnalyzer(mock_db_events)

    # 2. Giả lập một bản tin báo kẹt xe từ PhoBERT
    # Tọa độ này là đường Lê Đức Thọ (rất gần SVĐ Mỹ Đình)
    incident_time = now.replace(hour=18, minute=30)  # 6h30 tối (Trực tiếp đổ về SVĐ)
    incident_lat, incident_lng = 21.0285, 105.7680

    print("Phát hiện một điểm kẹt xe mới! Đang phân tích ảnh hưởng sự kiện...")
    impact_score, event_name = analyzer.calculate_event_impact(incident_lat, incident_lng, incident_time)

    if impact_score > 0:
        print(f"⚠️ CẢNH BÁO: Điểm kẹt xe bị cộng hưởng bởi sự kiện: {event_name}")
        print(f"📈 Chỉ số S_event: {impact_score:.2f}/1.0")
        print("=> GỢI Ý HỆ THỐNG: Tự động nâng mức độ nghiêm trọng (Severity) lên +2 bậc!")
    else:
        print("✅ Điểm kẹt xe này là sự cố thông thường, không liên quan đến sự kiện nào.")