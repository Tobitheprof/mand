from mand.adapters.ah_nl.scraper import scrape_ah_nl_once
from mand.config.logging import configure_logging

def run():
    configure_logging()
    count = scrape_ah_nl_once()
    print(f"Scraped & stored {count} products.")
