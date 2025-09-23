import threading
from prometheus_client import Counter, Histogram, start_http_server
from mand.config.settings import settings

scrape_counter = Counter("mand_scrape_total", "Scrape runs", ["supermarket"])
scrape_errors = Counter("mand_scrape_errors_total", "Scrape errors", ["supermarket"])
scrape_duration = Histogram("mand_scrape_duration_seconds", "Scrape duration", ["supermarket"])

def ensure_metrics_server():
    if settings.ENABLE_PROMETHEUS:
        t = threading.Thread(target=lambda: start_http_server(settings.PROMETHEUS_PORT), daemon=True)
        t.start()
