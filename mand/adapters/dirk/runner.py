from mand.adapters.dirk.scraper import scrape_dirk_once
from mand.config.logging import configure_logging

def run():
    configure_logging()
    count = scrape_dirk_once()
    print(f"Scraped & stored {count} products.")
