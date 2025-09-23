import logging
from mand.adapters.ah_nl.scraper import scrape_ah_nl_once
from mand.monitoring.metrics import scrape_counter, scrape_errors, scrape_duration

logger = logging.getLogger(__name__)

def job_scrape_ah_nl():
    supermarket = "ah"
    with scrape_duration.labels(supermarket).time():
        try:
            count = scrape_ah_nl_once()
            scrape_counter.labels(supermarket).inc()
            logger.info("AH-NL scrape finished", extra={"count": count})
        except Exception as e:
            scrape_errors.labels(supermarket).inc()
            logger.exception("AH-NL scrape failed")
            raise
