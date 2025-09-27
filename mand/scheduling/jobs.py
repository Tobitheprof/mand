import logging
from mand.adapters.ah_nl.scraper import scrape_ah_nl_once
from mand.adapters.dirk.scraper import scrape_dirk_once
from mand.adapters.jumbo.scraper import scrape_jumbo_once
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

def job_scrape_dirk():
    supermarket = "dirk"
    with scrape_duration.labels(supermarket).time():
        try:
            count = scrape_dirk_once()
            scrape_counter.labels(supermarket).inc()
            logger.info("Dirk scrape finished", extra={"count": count})
        except Exception as e:
            scrape_errors.labels(supermarket).inc()
            logger.exception("Dirk scrape failed")
            raise

def job_scrape_jumbo():
    supermarket = "jumbo"
    with scrape_duration.labels(supermarket).time():
        try:
            count = scrape_jumbo_once()
            scrape_counter.labels(supermarket).inc()
            logger.info("Jumbo scrape finished", extra={"count": count})
        except Exception as e:
            scrape_errors.labels(supermarket).inc()
            logger.exception("Jumbo scrape failed")
            raise