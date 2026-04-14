import sys
import time
import logging
import os
sys.stdout.reconfigure(encoding='utf-8')

from apscheduler.schedulers.background import BackgroundScheduler

from src.scripts.init_db import init_db
from src.ingestion.news_scraper import run_scraper
from src.ingestion.event_scraper import run_event_scraper
from src.ingestion.vov_scraper import run_vov_scraper
from src.ingestion.otofun_scraper import run_otofun_scraper
from src.processing.nlp_engine import run_nlp_processor
from src.processing.zone_metrics import compute_zone_metrics
from src.processing.st_dbscan import run_clustering
from src.core.db_manager import get_connection, release_connection

# ── File logging ─────────────────────────────────────────────────────────────
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("logs/scheduler.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)

# ── Job wrappers (bắt exception để APScheduler không dừng job) ───────────────

def job_scraper():
    try:
        run_scraper()
    except Exception as e:
        logger.error(f"[JOB] Scraper lỗi: {e}", exc_info=True)

def job_nlp():
    try:
        run_nlp_processor()
    except Exception as e:
        logger.error(f"[JOB] NLP lỗi: {e}", exc_info=True)

def job_clustering():
    try:
        run_clustering()
    except Exception as e:
        logger.error(f"[JOB] ST-DBSCAN lỗi: {e}", exc_info=True)

def job_event_scraper():
    try:
        run_event_scraper()
    except Exception as e:
        logger.error(f"[JOB] Event scraper lỗi: {e}", exc_info=True)

def job_vov_scraper():
    try:
        run_vov_scraper()
    except Exception as e:
        logger.error(f"[JOB] VOV scraper lỗi: {e}", exc_info=True)

def job_otofun_scraper():
    try:
        run_otofun_scraper()
    except Exception as e:
        logger.error(f"[JOB] OtoFun scraper lỗi: {e}", exc_info=True)

def job_cleanup():
    """Xóa raw_feed đã xử lý và cũ hơn 24h để tránh DB phình to."""
    conn = get_connection()
    if not conn:
        return
    cursor = conn.cursor()
    try:
        cursor.execute("""
            DELETE FROM raw_feed
            WHERE is_processed = TRUE
              AND fetched_at < NOW() - INTERVAL '24 hours'
        """)
        deleted = cursor.rowcount
        conn.commit()
        if deleted > 0:
            logger.info(f"[CLEANUP] Đã xóa {deleted} bản tin cũ khỏi raw_feed.")
    except Exception as e:
        conn.rollback()
        logger.error(f"[JOB] Cleanup lỗi: {e}", exc_info=True)
    finally:
        cursor.close()
        release_connection(conn)


if __name__ == "__main__":
    logger.info("=" * 60)
    logger.info("🚀 KHỞI ĐỘNG HỆ THỐNG URBAN TRAFFIC MONITORING")
    logger.info("=" * 60)

    # ── Bước 0: Khởi tạo schema DB ───────────────────────────────
    logger.info("\n[0/3] Khởi tạo schema Database...")
    if not init_db():
        logger.error("❌ Không thể khởi tạo DB. Dừng hệ thống.")
        sys.exit(1)

    # ── Bước 1: Tính chỉ số hạ tầng vùng (một lần khi khởi động) ─
    logger.info("\n[1/3] Tính toán chỉ số áp lực hạ tầng vùng...")
    compute_zone_metrics()

    # ── Bước 2–4: Chạy mồi ngay lần đầu ─────────────────────────
    logger.info("\n[2/4] Thu thập tin tức lần đầu...")
    job_scraper()
    logger.info("\n[3/4] Thu thập sự kiện lần đầu...")
    job_event_scraper()
    logger.info("\n[4/4] Phân tích NLP lần đầu...")
    job_nlp()

    # ── Lịch trình định kỳ ───────────────────────────────────────
    # max_instances=1: không cho job mới bắt đầu nếu lần trước vẫn chạy
    # misfire_grace_time=60: nếu chậm <60s thì vẫn chạy bù, không bỏ
    scheduler = BackgroundScheduler()
    scheduler.add_job(
        job_scraper, 'interval', minutes=15,
        id='ingest_job', max_instances=1, misfire_grace_time=60,
    )
    scheduler.add_job(
        job_nlp, 'interval', minutes=1,
        id='analysis_job', max_instances=1, misfire_grace_time=30,
    )
    scheduler.add_job(
        job_clustering, 'interval', minutes=10,
        id='cluster_job', max_instances=1, misfire_grace_time=60,
    )
    scheduler.add_job(
        job_event_scraper, 'interval', hours=6,
        id='event_job', max_instances=1, misfire_grace_time=300,
    )
    scheduler.add_job(
        job_vov_scraper, 'interval', minutes=30,
        id='vov_job', max_instances=1, misfire_grace_time=120,
    )
    scheduler.add_job(
        job_otofun_scraper, 'interval', hours=2,
        id='otofun_job', max_instances=1, misfire_grace_time=300,
    )
    scheduler.add_job(
        job_cleanup, 'interval', hours=1,
        id='cleanup_job', max_instances=1, misfire_grace_time=120,
    )

    scheduler.start()
    logger.info("✅ Lịch trình: Scraper (15p) | VOV (30p) | NLP (1p) | ST-DBSCAN (10p) | OtoFun (2h) | Events (6h) | Cleanup (1h)")
    logger.info(f"   Log file: logs/scheduler.log")

    try:
        while True:
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        logger.info("\n👋 Đã tắt hệ thống an toàn.")
