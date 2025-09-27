
# MAND — Supermarket Scrapers

MAND collects product data from multiple NL supermarkets, normalizes it to a standard schema, and stores it in a relational database with proper foreign keys:

-   **Supermarkets → Products**
    
-   **Internal Categories → Products**
    
-   **Store Categories → Products**
    

----------

## 📦 What’s inside

-   **Adapters**: per-supermarket scrapers (e.g. `ah_nl`, `jumbo_nl`)
    
-   **Normalization**: price/promo parsing + data sanitation
    
    -   Removes zero-width chars
        
    -   Normalizes whitespace
        
    -   Validates URLs, colors
        
-   **Storage**: SQLAlchemy models with FKs:
    
    -   `supermarkets` (code/name/etc.)
        
    -   `internal_categories` (global taxonomy)
        
    -   `store_categories` (per-supermarket taxonomy)
        
    -   `products` (current snapshot + promo/pricing)
        
    -   `products_raw` (optional audit trail)
        
-   **Monitoring**: `@timed` decorator hooks for instrumentation
    
-   **Scheduling**: APScheduler ready (optional)
    

----------

## ⚙️ Requirements

-   Python **3.9+**
    
-   PostgreSQL database (`PG_DSN`)
    
-   Network egress to supermarket APIs
    

### Python dependencies

Declared in `pyproject.toml`:

`APScheduler==3.10.4  requests==2.32.3  urllib3==2.2.3  fake-useragent==1.5.1  SQLAlchemy==2.0.36  psycopg2-binary==2.9.9  prometheus-client==0.21.0` 

Optional (for migrations):

`alembic>=1.13.1` 

----------

## 🚀 Quick start

`# 1) create & activate a virtualenv python -m venv .venv
. .venv/bin/activate # Windows: .venv\Scripts\activate  # 2) install the package (editable dev mode) pip install -e . # 3) set your environment (minimal)  export PG_DSN="postgresql+psycopg2://user:pass@localhost:5432/mand"  # scraper tuning (optional)  export AH_WORKERS=12 export AH_FETCH_DETAILS=1 export JUMBO_WORKERS=12 export JUMBO_FETCH_DETAILS=1 # 4) run a scraper once python -c "from mand.adapters.ah_nl import scrape_ah_nl_once; print(scrape_ah_nl_once())" python -c "from mand.adapters.jumbo_nl import scrape_jumbo_once; print(scrape_jumbo_once())"` 

> Adjust imports if your module paths differ. Entrypoints are `scrape_ah_nl_once()` and `scrape_jumbo_once()`.

----------

## ⚙️ Configuration

All runtime config is read from `mand.config.settings.settings`.  
You can source these from environment variables or a settings module.

### Database

-   `PG_DSN` _(required)_  
    Example: `postgresql+psycopg2://user:pass@localhost:5432/mand`
    

### Albert Heijn (ah.nl)

-   `AH_WORKERS` _(int, default sensible)_ — thread pool size
    
-   `AH_FETCH_DETAILS` _(bool/int, default 1)_ — fetch GraphQL detail
    
-   `AH_MAX_PAGES` _(int|None)_ — cap pages during dev/testing
    

### Jumbo (jumbo.com)

-   `JUMBO_WORKERS` _(int, default 12)_ — thread pool size
    
-   `JUMBO_FETCH_DETAILS` _(bool/int, default 1)_ — fetch product detail
    
-   `JUMBO_MAX_PAGES` _(int|None)_ — cap crawl pages
    
-   `JUMBO_PAGE_SIZE` _(int, default 24)_ — search page size
    
-   `JUMBO_DELAY_BETWEEN_PAGES` _(seconds, default 0.25)_
    
-   `JUMBO_DELAY_BETWEEN_DETAILS` _(seconds, default 0.10)_
    
-   `JUMBO_REQUEST_TIMEOUT` _(seconds, default 20)_
    
-   `JUMBO_SEARCH_TERMS` _(string, default: "producten")_
    

----------

## 📝 Logging

Use standard logging config. Example:

`# run_logging.py  import logging, sys
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
) from mand.adapters.jumbo_nl import scrape_jumbo_once
scrape_jumbo_once()` 

You’ll see structured logs (`logger.info(...)`) with `extra={...}`:

-   Page offsets & counts
    
-   Queued/fetched SKUs & titles
    
-   Each product upsert (sku/title/category_slug/discount flags)
    
-   Totals per page and overall
    

----------

## 🗄 Data model (overview)

### Reference tables

-   `supermarkets(id, code, name, logo, abbreviation, brand_color)`
    
-   `internal_categories(id, name)`
    
-   `store_categories(id, supermarket_id(FK), code, name, description, logo)`
    

### Fact table

-   `products` (unique on `(product_id, supermarket_id)`)  
    FKs:
    
    -   `supermarkets.id`
        
    -   `internal_categories.id`
        
    -   `store_categories.id`
        

### Optional

-   `products_raw` for unnormalized JSON captures
    

Tables are created via SQLAlchemy on import.  
For production, use **Alembic** migrations.

----------

## 🔄 How scraping & upserting works

1.  Adapter fetches product cards (and details if enabled)
    
2.  Normalization → standard dict
    
3.  Sanitization (`mand.normalization.sanitize.clean_product_record`):
    
    -   Unicode normalization (NFKC)
        
    -   Remove zero-width/control chars
        
    -   Strip HTML tags
        
    -   Collapse whitespace
        
    -   Validate URLs/colors
        
    -   Deduplicate keywords
        
    -   Clamp numeric ranges (e.g. 0–100% discounts)
        
4.  Repository upsert (`ProductRepository.upsert_flat`):
    
    -   Resolves/creates FK rows
        
    -   Upserts into `products` (immediate write per product)
        

> Scrapers don’t need changes when schema evolves — repository adapts.

----------

## ▶️ Running both scrapers

`python - <<'PY' from mand.adapters.ah_nl import scrape_ah_nl_once
from mand.adapters.jumbo_nl import scrape_jumbo_once print("AH:", scrape_ah_nl_once()) print("Jumbo:", scrape_jumbo_once())
PY` 

----------

## ⏰ Scheduling (optional)

Using **APScheduler** for periodic runs:

`# schedule_scrapes.py  import logging, sys from apscheduler.schedulers.blocking import BlockingScheduler

logging.basicConfig(level=logging.INFO, handlers=[logging.StreamHandler(sys.stdout)]) from mand.adapters.ah_nl import scrape_ah_nl_once from mand.adapters.jumbo_nl import scrape_jumbo_once

sched = BlockingScheduler(timezone="UTC")
sched.add_job(scrape_ah_nl_once, "interval", minutes=30, id="ah")
sched.add_job(scrape_jumbo_once, "interval", minutes=30, id="jumbo")

sched.start()` 

Run:

`python schedule_scrapes.py` 

----------

## 📊 Prometheus (optional)

Expose metrics:

`from prometheus_client import start_http_server
start_http_server(9000) # /metrics` 

…and use the `@timed` instrumentation already present.

----------

## 🛠 Troubleshooting

-   **403 / 429 responses**  
    Retries enabled. AH rotates `User-Agent` via `fake-useragent`.
    
-   **Bad URLs / Images**  
    Sanitizer nulls invalid URLs.
    
-   **Duplicate products**  
    Uniqueness = `(product_id, supermarket_id)`. If category changes, product remains same row.
    
-   **Local dev limiting**  
    Use `*_MAX_PAGES` to reduce load during dev.