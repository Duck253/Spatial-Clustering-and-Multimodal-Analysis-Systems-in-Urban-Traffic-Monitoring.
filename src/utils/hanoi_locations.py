"""
hanoi_locations.py
------------------
Knowledge base địa danh giao thông Hà Nội — tra cứu nội bộ O(1).
Ưu tiên dùng trước Nominatim để tăng tỷ lệ geocoding thành công.

Mỗi entry: "từ khóa nhận diện" → (tên chuẩn, lat, lng)
- lat/lng là điểm đại diện (trung tâm đoạn đường / nút giao)
- Từ khóa viết thường, không dấu cũng được match
"""

# ── Vành đai ─────────────────────────────────────────────────────────────────
RING_ROADS = {
    "vành đai 1":               ("Đường Vành đai 1",            21.0245, 105.8412),
    "vành đai 2":               ("Đường Vành đai 2",            21.0156, 105.8234),
    "vành đai 2.5":             ("Đường Vành đai 2.5",          21.0089, 105.8156),
    "vành đai 3":               ("Đường Vành đai 3",            20.9978, 105.7923),
    "vành đai 4":               ("Đường Vành đai 4",            20.9812, 105.7634),
}

# ── Quốc lộ / Đại lộ ─────────────────────────────────────────────────────────
HIGHWAYS = {
    "quốc lộ 1":                ("Quốc lộ 1",                   20.9812, 105.8423),
    "ql1":                      ("Quốc lộ 1",                   20.9812, 105.8423),
    "quốc lộ 1a":               ("Quốc lộ 1A",                  20.9812, 105.8423),
    "ql1a":                     ("Quốc lộ 1A",                  20.9812, 105.8423),
    "quốc lộ 2":                ("Quốc lộ 2",                   21.1234, 105.7856),
    "ql2":                      ("Quốc lộ 2",                   21.1234, 105.7856),
    "quốc lộ 3":                ("Quốc lộ 3",                   21.1456, 105.8567),
    "ql3":                      ("Quốc lộ 3",                   21.1456, 105.8567),
    "quốc lộ 5":                ("Quốc lộ 5",                   20.9923, 105.9234),
    "ql5":                      ("Quốc lộ 5",                   20.9923, 105.9234),
    "quốc lộ 6":                ("Quốc lộ 6",                   20.9567, 105.7823),
    "ql6":                      ("Quốc lộ 6",                   20.9567, 105.7823),
    "quốc lộ 32":               ("Quốc lộ 32",                  21.0534, 105.7234),
    "ql32":                     ("Quốc lộ 32",                  21.0534, 105.7234),
    "đại lộ thăng long":        ("Đại lộ Thăng Long",           21.0123, 105.7145),
    "thăng long":               ("Đại lộ Thăng Long",           21.0123, 105.7145),
    "đại lộ võ nguyên giáp":    ("Đại lộ Võ Nguyên Giáp",       21.0634, 105.7823),
}

# ── Cầu ──────────────────────────────────────────────────────────────────────
BRIDGES = {
    "cầu thanh trì":            ("Cầu Thanh Trì",               20.9734, 105.8756),
    "thanh trì":                ("Cầu Thanh Trì",               20.9734, 105.8756),
    "cầu vĩnh tuy":             ("Cầu Vĩnh Tuy",                21.0023, 105.8912),
    "vĩnh tuy":                 ("Cầu Vĩnh Tuy",                21.0023, 105.8912),
    "cầu nhật tân":             ("Cầu Nhật Tân",                21.0823, 105.8234),
    "nhật tân":                 ("Cầu Nhật Tân",                21.0823, 105.8234),
    "cầu chương dương":         ("Cầu Chương Dương",            21.0412, 105.8634),
    "chương dương":             ("Cầu Chương Dương",            21.0412, 105.8634),
    "cầu long biên":            ("Cầu Long Biên",               21.0445, 105.8589),
    "long biên":                ("Cầu Long Biên",               21.0445, 105.8589),
    "cầu thăng long":           ("Cầu Thăng Long",              21.0923, 105.7734),
    "cầu đông trù":             ("Cầu Đông Trù",                21.0756, 105.8934),
    "cầu tứ liên":              ("Cầu Tứ Liên",                 21.0634, 105.8512),
    "cầu trần hưng đạo":        ("Cầu Trần Hưng Đạo",          21.0312, 105.8634),
    "cầu giấy":                 ("Cầu Giấy",                   21.0334, 105.7956),
}

# ── Hầm ──────────────────────────────────────────────────────────────────────
TUNNELS = {
    "hầm kim liên":             ("Hầm Kim Liên",                21.0123, 105.8456),
    "kim liên":                 ("Hầm Kim Liên",                21.0123, 105.8456),
    "hầm trung hòa":            ("Hầm Trung Hòa",              21.0034, 105.7923),
    "trung hòa":                ("Hầm Trung Hòa",              21.0034, 105.7923),
    "hầm ngã tư vọng":          ("Hầm Ngã Tư Vọng",            20.9956, 105.8434),
}

# ── Đường lớn nội đô ─────────────────────────────────────────────────────────
MAJOR_STREETS = {
    "nguyễn trãi":              ("Đường Nguyễn Trãi",           20.9934, 105.8123),
    "láng":                     ("Đường Láng",                  21.0212, 105.8034),
    "nguyễn chí thanh":         ("Đường Nguyễn Chí Thanh",      21.0256, 105.8245),
    "hoàng quốc việt":          ("Đường Hoàng Quốc Việt",       21.0423, 105.8012),
    "phạm văn đồng":            ("Đường Phạm Văn Đồng",         21.0634, 105.7834),
    "khuất duy tiến":           ("Đường Khuất Duy Tiến",        21.0034, 105.7956),
    "lê văn lương":             ("Đường Lê Văn Lương",          21.0056, 105.7934),
    "tố hữu":                   ("Đường Tố Hữu",                21.0023, 105.7867),
    "lê trọng tấn":             ("Đường Lê Trọng Tấn",          20.9845, 105.8023),
    "nguyễn xiển":              ("Đường Nguyễn Xiển",           20.9923, 105.8245),
    "giải phóng":               ("Đường Giải Phóng",            20.9978, 105.8456),
    "trường chinh":              ("Đường Trường Chinh",          21.0056, 105.8345),
    "lê duẩn":                  ("Đường Lê Duẩn",              21.0089, 105.8467),
    "đinh tiên hoàng":          ("Đường Đinh Tiên Hoàng",       21.0312, 105.8512),
    "phố huế":                  ("Phố Huế",                    21.0212, 105.8512),
    "bạch mai":                 ("Đường Bạch Mai",              21.0056, 105.8512),
    "kim mã":                   ("Đường Kim Mã",                21.0289, 105.8145),
    "liễu giai":                ("Đường Liễu Giai",             21.0312, 105.8123),
    "đội cấn":                  ("Đường Đội Cấn",              21.0378, 105.8234),
    "xuân thủy":                ("Đường Xuân Thủy",             21.0378, 105.7823),
    "cầu giấy":                 ("Đường Cầu Giấy",             21.0334, 105.7956),
    "lê đức thọ":               ("Đường Lê Đức Thọ",           21.0256, 105.7745),
    "mễ trì":                   ("Đường Mễ Trì",                21.0123, 105.7723),
    "nguyễn hoàng":             ("Đường Nguyễn Hoàng",          21.0178, 105.7812),
    "phạm hùng":                ("Đường Phạm Hùng",             21.0245, 105.7756),
}

# ── Nút giao / Ngã tư nổi tiếng ──────────────────────────────────────────────
INTERSECTIONS = {
    "ngã tư khuất duy tiến":    ("Ngã tư Khuất Duy Tiến",       21.0034, 105.7967),
    "ngã tư trung hòa":         ("Ngã tư Trung Hòa",            21.0034, 105.7923),
    "ngã tư vọng":              ("Ngã tư Vọng",                20.9956, 105.8434),
    "ngã tư sở":                ("Ngã tư Sở",                  21.0023, 105.8289),
    "ngã tư kim liên":          ("Ngã tư Kim Liên",             21.0134, 105.8434),
    "ngã tư hàng xanh":         ("Ngã tư Hàng Xanh",           21.0523, 105.8678),
    "nút giao mỹ đình":         ("Nút giao Mỹ Đình",           21.0178, 105.7712),
    "nút giao thanh xuân":      ("Nút giao Thanh Xuân",         20.9945, 105.8156),
    "nút giao pháp vân":        ("Nút giao Pháp Vân",          20.9712, 105.8434),
    "nút giao linh đàm":        ("Nút giao Linh Đàm",          20.9745, 105.8423),
}

# ── Khu vực / Địa danh ───────────────────────────────────────────────────────
AREAS = {
    "mỹ đình":                  ("Khu vực Mỹ Đình",            21.0202, 105.7645),
    "cầu giấy":                 ("Quận Cầu Giấy",              21.0334, 105.7956),
    "đống đa":                  ("Quận Đống Đa",               21.0245, 105.8412),
    "hoàn kiếm":                ("Quận Hoàn Kiếm",             21.0285, 105.8522),
    "hai bà trưng":             ("Quận Hai Bà Trưng",          21.0089, 105.8567),
    "hoàng mai":                ("Quận Hoàng Mai",             20.9845, 105.8456),
    "thanh xuân":               ("Quận Thanh Xuân",            20.9956, 105.8145),
    "nam từ liêm":              ("Quận Nam Từ Liêm",           21.0023, 105.7634),
    "bắc từ liêm":              ("Quận Bắc Từ Liêm",           21.0523, 105.7634),
    "long biên":                ("Quận Long Biên",             21.0534, 105.8834),
    "gia lâm":                  ("Huyện Gia Lâm",              21.0134, 105.9123),
    "đông anh":                 ("Huyện Đông Anh",             21.1234, 105.8456),
    "hà đông":                  ("Quận Hà Đông",               20.9712, 105.7634),
    "linh đàm":                 ("Khu đô thị Linh Đàm",        20.9745, 105.8423),
    "trung hòa nhân chính":     ("Khu Trung Hòa Nhân Chính",   21.0012, 105.7934),
}

# ── Hợp nhất tất cả ──────────────────────────────────────────────────────────
HANOI_LOCATION_KB: dict[str, tuple[str, float, float]] = {
    **RING_ROADS,
    **HIGHWAYS,
    **BRIDGES,
    **TUNNELS,
    **MAJOR_STREETS,
    **INTERSECTIONS,
    **AREAS,
}


def lookup_location(text: str) -> tuple[str, float, float] | None:
    """
    Tìm địa danh trong text, trả về (tên chuẩn, lat, lng) hoặc None.
    Ưu tiên match dài nhất để tránh nhầm ("cầu long biên" > "long biên").
    """
    text_lower = text.lower()
    best_match = None
    best_len = 0

    for keyword, location in HANOI_LOCATION_KB.items():
        if keyword in text_lower and len(keyword) > best_len:
            best_match = location
            best_len = len(keyword)

    return best_match
