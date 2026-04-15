"""
audio_stream.py
---------------
Tầng 2 VOV GT: bắt live audio stream → faster-whisper ASR → NLP pipeline.

Flow:
  VOV_STREAM_URL (HLS / MP3 / Icecast)
    → ffmpeg capture CHUNK_DURATION_SEC giây → temp WAV 16kHz mono
    → faster-whisper (model=small, lang=vi, VAD filter)
    → has_traffic_features() → dedup (MD5 + Jaccard)
    → raw_feed (content_type='audio_transcript')

Yêu cầu hệ thống:
    1. ffmpeg phải được cài và có trong PATH
       Windows: https://ffmpeg.org/download.html → thêm vào PATH
    2. pip install faster-whisper

Cấu hình trong .env:
    VOV_STREAM_URL=<URL stream>     ← Tìm bằng cách:
                                       Mở vovgiaothong.vn → F12 → Network tab
                                       → lọc "audio" hoặc ".m3u8" → copy URL
    WHISPER_MODEL=small             ← tiny/small/medium/large-v3 (default: small)
    AUDIO_CHUNK_SEC=60              ← độ dài mỗi chunk (giây, default: 60)
    AUDIO_LOOP_SEC=55               ← khoảng cách giữa 2 lần capture (default: 55)

Chạy thủ công:
    python src/ingestion/audio_stream.py --test         # 1 chu kỳ rồi thoát
    python src/ingestion/audio_stream.py                # vòng lặp liên tục
    python src/ingestion/audio_stream.py --url <URL>    # override URL
    python src/ingestion/audio_stream.py --model medium # override model

Tích hợp vào main_scheduler.py (sau khi test thành công):
    from src.ingestion.audio_stream import run_audio_stream_once
    scheduler.add_job(run_audio_stream_once, 'interval', minutes=2, id='audio_job',
                      max_instances=1, misfire_grace_time=60)
"""

import sys
import os
import time
import subprocess
import tempfile
import argparse
from pathlib import Path
from datetime import datetime

sys.stdout.reconfigure(encoding="utf-8")

from dotenv import load_dotenv
load_dotenv()

from src.core.db_manager import get_connection, release_connection
from src.utils.text_helpers import content_hash, get_jaccard_sim, has_traffic_features

try:
    from faster_whisper import WhisperModel
    _WHISPER_AVAILABLE = True
except ImportError:
    _WHISPER_AVAILABLE = False

# ── Cấu hình (đọc từ .env) ───────────────────────────────────────────────────

# VOV GT Hà Nội — HLS stream (verified 2026-04-15)
_DEFAULT_STREAM = "https://play.vovgiaothong.vn/live/gthn/playlist.m3u8"

VOV_STREAM_URL      = os.getenv("VOV_STREAM_URL", _DEFAULT_STREAM)
WHISPER_MODEL_SIZE  = os.getenv("WHISPER_MODEL", "small")
CHUNK_DURATION_SEC  = int(os.getenv("AUDIO_CHUNK_SEC", "60"))
LOOP_INTERVAL_SEC   = int(os.getenv("AUDIO_LOOP_SEC", "55"))  # ~5s overlap

WHISPER_DEVICE       = "cpu"
WHISPER_COMPUTE_TYPE = "int8"   # nhanh nhất trên CPU

JACCARD_THRESHOLD  = 0.6
DEDUP_WINDOW_HOURS = 6          # audio hay lặp nên cửa sổ ngắn hơn text

SOURCE_NAME = "VOV GT Audio"
SOURCE_URL  = "https://vovgiaothong.vn/live-audio"

_whisper_model: "WhisperModel | None" = None


# ── Whisper (lazy load — nặng, chỉ khởi tạo 1 lần) ──────────────────────────

def _get_model() -> "WhisperModel":
    global _whisper_model
    if _whisper_model is None:
        if not _WHISPER_AVAILABLE:
            raise RuntimeError(
                "faster-whisper chưa được cài.\n"
                "Chạy: pip install faster-whisper"
            )
        print(f"[AUDIO] Nạp Whisper model '{WHISPER_MODEL_SIZE}' "
              f"(lần đầu, có thể mất 30–90s để download)...")
        _whisper_model = WhisperModel(
            WHISPER_MODEL_SIZE,
            device=WHISPER_DEVICE,
            compute_type=WHISPER_COMPUTE_TYPE,
        )
        print("[AUDIO] ✅ Model sẵn sàng.")
    return _whisper_model


# ── Audio capture via ffmpeg ──────────────────────────────────────────────────

def _capture_chunk(stream_url: str, duration_sec: int) -> Path | None:
    """
    Dùng ffmpeg để bắt `duration_sec` giây từ stream_url.
    Output: file WAV 16kHz mono (yêu cầu của Whisper).
    Trả về Path đến file tạm, hoặc None nếu thất bại.
    """
    tmp = tempfile.NamedTemporaryFile(suffix=".wav", delete=False)
    tmp.close()
    out_path = Path(tmp.name)

    cmd = [
        "ffmpeg", "-y",
        "-i", stream_url,
        "-t", str(duration_sec),
        "-ar", "16000",              # 16kHz — Whisper yêu cầu
        "-ac", "1",                  # mono
        "-f", "wav",
        str(out_path),
    ]

    try:
        result = subprocess.run(
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            timeout=duration_sec + 30,
        )
        if result.returncode != 0:
            err = result.stderr.decode("utf-8", errors="ignore")[-400:]
            print(f"[AUDIO] ffmpeg lỗi (code {result.returncode}):\n  ...{err}")
            out_path.unlink(missing_ok=True)
            return None

        size_kb = out_path.stat().st_size // 1024
        if size_kb < 2:
            print(f"[AUDIO] WAV file quá nhỏ ({size_kb}KB) — stream có thể không hoạt động.")
            out_path.unlink(missing_ok=True)
            return None

        print(f"[AUDIO] Capture xong: {size_kb}KB WAV")
        return out_path

    except subprocess.TimeoutExpired:
        print("[AUDIO] ffmpeg timeout — stream bị treo.")
        out_path.unlink(missing_ok=True)
        return None
    except FileNotFoundError:
        print(
            "[AUDIO] ❌ Lệnh 'ffmpeg' không tìm thấy.\n"
            "  → Tải tại: https://ffmpeg.org/download.html\n"
            "  → Thêm thư mục bin/ vào PATH rồi restart terminal."
        )
        out_path.unlink(missing_ok=True)
        return None


# ── Transcription ─────────────────────────────────────────────────────────────

def _transcribe(wav_path: Path) -> str:
    """
    Transcribe file WAV bằng faster-whisper.
    VAD filter bật để bỏ qua nhạc nền / quảng cáo / im lặng.
    Trả về toàn bộ transcript dưới dạng 1 chuỗi.
    """
    model = _get_model()
    segments, _info = model.transcribe(
        str(wav_path),
        language="vi",
        beam_size=5,
        vad_filter=True,
        vad_parameters={"min_silence_duration_ms": 800},
    )
    texts = [seg.text.strip() for seg in segments if seg.text.strip()]
    transcript = " ".join(texts)

    preview = transcript[:120] + ("..." if len(transcript) > 120 else "")
    print(f"[AUDIO] Transcript ({len(transcript)} ký tự): {preview}")
    return transcript


# ── DB helpers ────────────────────────────────────────────────────────────────

def _get_or_create_source(cursor) -> int:
    cursor.execute(
        """INSERT INTO data_source (source_type, source_url, source_name)
           VALUES ('radio', %s, %s)
           ON CONFLICT (source_url) DO UPDATE SET source_name = EXCLUDED.source_name""",
        (SOURCE_URL, SOURCE_NAME)
    )
    cursor.execute(
        "SELECT source_id FROM data_source WHERE source_url = %s", (SOURCE_URL,)
    )
    return cursor.fetchone()[0]


def _load_recent_hashes(cursor) -> set:
    cursor.execute(
        "SELECT content_hash FROM raw_feed WHERE fetched_at > NOW() - INTERVAL %s",
        (f"{DEDUP_WINDOW_HOURS} hours",)
    )
    return {row[0] for row in cursor.fetchall()}


def _load_recent_contents(cursor) -> list:
    cursor.execute(
        "SELECT raw_content FROM raw_feed WHERE fetched_at > NOW() - INTERVAL %s",
        (f"{DEDUP_WINDOW_HOURS} hours",)
    )
    return [row[0] for row in cursor.fetchall()]


def _save_transcript(
    cursor, source_id: int, text: str, hashes: set, contents: list
) -> bool:
    """Lưu transcript vào raw_feed sau khi kiểm tra dedup."""
    h = content_hash(text)
    if h in hashes:
        return False
    for old in contents:
        if get_jaccard_sim(text, old) >= JACCARD_THRESHOLD:
            return False

    cursor.execute(
        """INSERT INTO raw_feed
           (source_id, content_type, raw_content, content_hash, fetched_at, is_processed)
           VALUES (%s, 'audio_transcript', %s, %s, NOW(), false)""",
        (source_id, text, h)
    )
    hashes.add(h)
    contents.append(text)
    return True


# ── One cycle ─────────────────────────────────────────────────────────────────

def run_audio_stream_once(stream_url: str | None = None) -> bool:
    """
    1 chu kỳ đầy đủ: capture → transcribe → filter → lưu DB.
    Trả về True nếu có bản tin mới được lưu.
    Được gọi bởi main_scheduler hoặc chạy độc lập.
    """
    url = stream_url or VOV_STREAM_URL
    if not url:
        print(
            "[AUDIO] ❌ VOV_STREAM_URL chưa được cấu hình.\n"
            "  → Thêm VOV_STREAM_URL=<url> vào file .env"
        )
        return False

    t0 = datetime.now()
    print(f"\n[AUDIO] {'─' * 52}")
    print(f"[AUDIO] Capture lúc {t0.strftime('%H:%M:%S')} | {CHUNK_DURATION_SEC}s | {url[:60]}")

    # 1. Capture audio
    wav_path = _capture_chunk(url, CHUNK_DURATION_SEC)
    if wav_path is None:
        return False

    # 2. Transcribe (cleanup WAV dù có lỗi hay không)
    try:
        transcript = _transcribe(wav_path)
    except Exception as e:
        print(f"[AUDIO] Lỗi transcribe: {e}")
        return False
    finally:
        wav_path.unlink(missing_ok=True)

    if not transcript or len(transcript.strip()) < 20:
        print("[AUDIO] Transcript rỗng / quá ngắn — bỏ qua.")
        return False

    # 3. Lọc từ khóa giao thông
    if not has_traffic_features(transcript):
        print("[AUDIO] Không có từ khóa giao thông — bỏ qua.")
        return False

    # 4. Lưu DB
    conn = get_connection()
    if not conn:
        print("[AUDIO] ❌ Không kết nối được DB.")
        return False

    cursor = conn.cursor()
    saved = False
    try:
        source_id = _get_or_create_source(cursor)
        hashes    = _load_recent_hashes(cursor)
        contents  = _load_recent_contents(cursor)
        saved     = _save_transcript(cursor, source_id, transcript, hashes, contents)
        conn.commit()

        elapsed = (datetime.now() - t0).seconds
        if saved:
            print(f"[AUDIO] ✅ Lưu transcript mới ({len(transcript)} ký tự) — {elapsed}s")
        else:
            print(f"[AUDIO] Trùng lặp — bỏ qua. ({elapsed}s)")

    except Exception as e:
        conn.rollback()
        print(f"[AUDIO] Lỗi lưu DB: {e}")
    finally:
        cursor.close()
        release_connection(conn)

    return saved


# ── Continuous loop ───────────────────────────────────────────────────────────

def run_audio_stream(stream_url: str | None = None):
    """
    Vòng lặp liên tục. Mỗi LOOP_INTERVAL_SEC giây capture 1 chunk.
    Thường gọi từ CLI; khi dùng với scheduler thì dùng run_audio_stream_once.
    """
    url = stream_url or VOV_STREAM_URL
    print(f"[AUDIO] ▶ Bắt đầu vòng lặp audio stream")
    print(f"[AUDIO]   URL   : {url or '(chưa cấu hình)'}")
    print(f"[AUDIO]   Model : {WHISPER_MODEL_SIZE} ({WHISPER_DEVICE}/{WHISPER_COMPUTE_TYPE})")
    print(f"[AUDIO]   Chunk : {CHUNK_DURATION_SEC}s | Loop: {LOOP_INTERVAL_SEC}s")

    # Warm-up model trước khi vào vòng lặp
    try:
        _get_model()
    except RuntimeError as e:
        print(f"[AUDIO] ❌ {e}")
        return

    cycle = 0
    while True:
        cycle += 1
        print(f"\n[AUDIO] ══ Chu kỳ #{cycle} ══")
        try:
            run_audio_stream_once(stream_url=url)
        except KeyboardInterrupt:
            raise
        except Exception as e:
            print(f"[AUDIO] Lỗi chu kỳ #{cycle}: {e}")

        print(f"[AUDIO] Chờ {LOOP_INTERVAL_SEC}s đến chunk tiếp theo...")
        time.sleep(LOOP_INTERVAL_SEC)


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="VOV GT Live Audio → faster-whisper ASR → raw_feed"
    )
    parser.add_argument(
        "--test", action="store_true",
        help="Chạy 1 chu kỳ rồi thoát (dùng để kiểm tra)"
    )
    parser.add_argument(
        "--url", type=str, default=None,
        help="Override stream URL (ưu tiên hơn .env)"
    )
    parser.add_argument(
        "--model",
        choices=["tiny", "small", "medium", "large-v2", "large-v3"],
        default=None,
        help="Override Whisper model size"
    )
    parser.add_argument(
        "--chunk", type=int, default=None,
        help="Override độ dài chunk (giây)"
    )
    args = parser.parse_args()

    if args.model:
        WHISPER_MODEL_SIZE = args.model
    if args.chunk:
        CHUNK_DURATION_SEC = args.chunk

    try:
        if args.test:
            print("[AUDIO] Chế độ TEST — chạy 1 chu kỳ")
            ok = run_audio_stream_once(stream_url=args.url)
            sys.exit(0 if ok else 1)
        else:
            run_audio_stream(stream_url=args.url)
    except KeyboardInterrupt:
        print("\n[AUDIO] Đã dừng.")
