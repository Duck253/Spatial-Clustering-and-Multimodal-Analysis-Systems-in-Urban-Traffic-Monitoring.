from geopy.geocoders import Nominatim
from src.utils.hanoi_locations import lookup_location

geolocator = Nominatim(user_agent="smart_city_monitoring")


def get_coordinates(place_name: str, city: str = "Hà Nội") -> tuple[float | None, float | None]:
    """
    Chuyển tên địa danh sang tọa độ GPS.
    Thứ tự ưu tiên:
      1. Knowledge base nội bộ (Hà Nội KB) — nhanh, chính xác với đường/cầu/vành đai
      2. Nominatim (OpenStreetMap)          — fallback cho địa danh lạ
    """
    # 1. Tra knowledge base trước
    result = lookup_location(place_name)
    if result:
        name, lat, lng = result
        print(f"    [KB] '{place_name}' → {name} ({lat:.4f}, {lng:.4f})")
        return lat, lng

    # 2. Fallback Nominatim
    search_query = f"{place_name}, {city}, Việt Nam"
    try:
        geo_location = geolocator.geocode(search_query, timeout=5)
        if geo_location:
            return geo_location.latitude, geo_location.longitude
    except Exception as e:
        print(f"    [GEO] Lỗi Nominatim: {e}")

    return None, None
