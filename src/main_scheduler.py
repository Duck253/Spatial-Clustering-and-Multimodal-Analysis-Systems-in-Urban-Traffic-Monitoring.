import sys
import time
sys.stdout.reconfigure(encoding='utf-8')

from apscheduler.schedulers.background import BackgroundScheduler
from src.ingestion.news_scraper import run_scraper
from src.processing.nlp_engine import run_nlp_processor
from src.processing.zone_metrics import compute_zone_metrics
from src.processing.st_dbscan import run_clustering

if __name__ == "__main__":
    print("🚀 KHỞI ĐỘNG HỆ THỐNG URBAN TRAFFIC MONITORING")

    # Tính toán chỉ số hạ tầng vùng một lần lúc khởi động
    print("\n[1/3] Tính toán chỉ số áp lực hạ tầng vùng...")
    compute_zone_metrics()

    # Chạy mồi 1 lần ngay lúc khởi động
    print("\n[2/3] Tiến hành thu thập dữ liệu lần đầu...")
    run_scraper()
    print("\n[3/3] Tiến hành phân tích AI lần đầu...")
    run_nlp_processor()

    scheduler = BackgroundScheduler()
    scheduler.add_job(run_scraper,    'interval', minutes=15, id='ingest_job')
    scheduler.add_job(run_nlp_processor, 'interval', minutes=1,  id='analysis_job')
    scheduler.add_job(run_clustering, 'interval', minutes=10, id='cluster_job')

    print("✅ Đã thiết lập lịch trình: Scraper (15p) | NLP (1p) | ST-DBSCAN (10p)")
    scheduler.start()

    try:
        while True:
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        print("\n👋 Đã tắt hệ thống an toàn.")