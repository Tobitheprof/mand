from mand.adapters.jumbo.scraper import scrape_jumbo_once
from mand.config.logging import configure_logging

def run():
    configure_logging()
    count = scrape_jumbo_once()
    print(f"Scraped & stored {count} products.")
