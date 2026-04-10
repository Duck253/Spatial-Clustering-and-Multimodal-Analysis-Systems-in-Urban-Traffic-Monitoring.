from geopy.geocoders import Nominatim

geolocator = Nominatim(user_agent="smart_city_monitoring")

def get_coordinates(place_name, city="Hà Nội"):
    """Biến đổi tên đường thành tọa độ GPS"""
    search_query = f"{place_name}, {city}, Việt Nam"
    try:
        geo_location = geolocator.geocode(search_query, timeout=5)
        if geo_location:
            return geo_location.latitude, geo_location.longitude
    except Exception as e:
        print(f"Lỗi Geocoding: {e}")
    return None, None