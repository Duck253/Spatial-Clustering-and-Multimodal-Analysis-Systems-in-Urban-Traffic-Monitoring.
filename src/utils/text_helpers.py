import re
import hashlib


def normalize_text(text: str) -> str:
    """Chuẩn hóa văn bản: lowercase, bỏ dấu câu thừa, khoảng trắng"""
    text = text.lower().strip()
    text = re.sub(r'[^\w\sàáâãèéêìíòóôõùúăđĩũơưạảấầẩẫậắằẳẵặẹẻẽếềểễệỉịọỏốồổỗộớờởỡợụủứừửữựỳỵýỷỹ]', ' ', text)
    text = re.sub(r'\s+', ' ', text)
    return text


def content_hash(text: str) -> str:
    """MD5 hash của văn bản đã chuẩn hóa — dùng để check trùng chính xác O(1)"""
    return hashlib.md5(normalize_text(text).encode('utf-8')).hexdigest()


def get_jaccard_sim(str1: str, str2: str) -> float:
    """Tính độ tương đồng Jaccard giữa 2 văn bản"""
    a = set(normalize_text(str1).split())
    b = set(normalize_text(str2).split())
    c = a & b
    union_len = len(a) + len(b) - len(c)
    return float(len(c)) / union_len if union_len > 0 else 0.0


def has_traffic_features(text: str) -> bool:
    """Kiểm tra bản tin có liên quan giao thông không"""
    keywords = [
        "kẹt xe", "ùn tắc", "tắc đường", "ùn ứ",
        "tai nạn", "va chạm", "đâm xe",
        "ngập", "ngập lụt", "ngập úng",
        "giao thông", "ngã tư", "tê liệt",
        "cấm đường", "phong tỏa", "chặn đường",
        "lô cốt", "thi công", "sửa đường",
    ]
    text_lower = text.lower()
    return any(kw in text_lower for kw in keywords)