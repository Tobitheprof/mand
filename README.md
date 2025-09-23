
# MAND — Supermarket Scrapers

MAND collects product data from multiple NL supermarkets, normalizes it to a standard schema, and stores it in a relational database with proper foreign keys (Supermarkets → Products, Internal Categories → Products, Store Categories → Products).

## What’s inside

-   **Adapters**: per-supermarket scrapers (e.g. `ah_nl`, `jumbo_nl`)
    
-   **Normalization**: price/promo parsing + data sanitation (removes zero-width chars, normalizes whitespace, validates URLs, etc.)
    
-   **Storage**: SQLAlchemy models with **FKs**:
    
    -   `supermarkets` (code/name/etc.)
        
    -   `internal_categories` (global taxonomy)
        
    -   `store_categories` (per-supermarket taxonomy)
        
    -   `products` (current snapshot + promo/pricing)
        
    -   (optional) `products_raw` (audit trail)
        
-   **Monitoring**: `@timed` decorator hooks for instrumentation
    
-   **Scheduling**: APScheduler ready (optional)
    

----------

## Requirements

-   Python **3.9+**
    
-   A PostgreSQL database (set `PG_DSN`)
    
-   Network egress to supermarket APIs
    

### Python deps

They’re declared in `pyproject.toml`:

`APScheduler==3.10.4  requests==2.32.3  urllib3==2.2.3  fake-useragent==1.5.1  SQLAlchemy==2.0.36  psycopg2-binary==2.9.9  prometheus-client==0.21.0` 

Optional (if you plan DB migrations): `alembic>=1.13.1`

----------

## Quick start

`# 1) create & activate a virtualenv python -m venv .venv
. .venv/bin/activate # Windows: .venv\Scripts\activate  # 2) install the package (editable dev mode) pip install -e . # 3) set your environment (minimal)  export PG_DSN="postgresql+psycopg2://user:pass@localhost:5432/mand"  # scraper tuning (optional—see Configuration below)  export AH_WORKERS=12 export AH_FETCH_DETAILS=1 export JUMBO_WORKERS=12 export JUMBO_FETCH_DETAILS=1 # 4) run a scraper once (examples below) python -c "from mand.adapters.ah_nl import scrape_ah_nl_once; print(scrape_ah_nl_once())" python -c "from mand.adapters.jumbo_nl import scrape_jumbo_once; print(scrape_jumbo_once())"` 

> If your module paths differ (e.g. you placed the functions in submodules), adjust the imports accordingly. The entry functions are `scrape_ah_nl_once()` and `scrape_jumbo_once()`.

----------

## Configuration

All runtime config is read from `mand.config.settings.settings`. You can source these from env vars or your own settings module; here are the variables the scrapers and storage expect:

### Database

-   `PG_DSN` _(required)_ — e.g. `postgresql+psycopg2://user:pass@localhost:5432/mand`
    

### Albert Heijn (ah.nl)

-   `AH_WORKERS` _(int, default: sensible)_ — thread pool size for detail fetches
    
-   `AH_FETCH_DETAILS` _(bool/int, default: 1)_ — fetch GraphQL detail per product
    
-   `ah_max_pages` _(int|None)_ — cap pages during development/testing
    

### Jumbo (jumbo.com)

-   `JUMBO_WORKERS` _(int, default: 12)_ — thread pool size per page
    
-   `JUMBO_FETCH_DETAILS` _(bool/int, default: 1)_ — fetch product detail
    
-   `JUMBO_MAX_PAGES` _(int|None)_ — cap crawl pages during dev
    
-   `JUMBO_PAGE_SIZE` _(int, default: 24)_ — search page size
    
-   `JUMBO_DELAY_BETWEEN_PAGES` _(seconds, default: 0.25)_
    
-   `JUMBO_DELAY_BETWEEN_DETAILS` _(seconds, default: 0.10)_
    
-   `JUMBO_REQUEST_TIMEOUT` _(seconds, default: 20)_
    
-   `JUMBO_SEARCH_TERMS` _(string, default: "producten")_
    

### Logging

Use standard logging configuration. Example:

`# run_logging.py  import logging, sys
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
) from mand.adapters.jumbo_nl import scrape_jumbo_once
scrape_jumbo_once()` 

You’ll see structured `logger.info(...)` with `extra={...}` for:

-   page offsets and counts
    
-   queued/fetched detail SKUs and titles
    
-   each product upsert (sku/title/category_slug/discount flags)
    
-   totals per page and overall
    

----------

## Data model (overview)

**Reference tables**

-   `supermarkets(id, code, name, logo, abbreviation, brand_color)`
    
-   `internal_categories(id, name)`
    
-   `store_categories(id, supermarket_id(FK), code, name, description, logo)`
    

**Fact table**

-   `products`  
    Unique on `(product_id, supermarket_id)` and **FKs** to:
    
    -   `supermarkets.id`
        
    -   `internal_categories.id`
        
    -   `store_categories.id`
        

**Optional**

-   `products_raw` for unnormalized JSON captures
    

Tables are created via SQLAlchemy on import; in production use Alembic migrations.

----------

## How scraping & upserting works

1.  **Adapter fetches** product cards (and details if enabled)
    
2.  **Normalization** into a standard dict
    
3.  **Sanitization** (`mand.normalization.sanitize.clean_product_record`)
    
    -   NFKC unicode normalization
        
    -   removes zero-width/control chars
        
    -   strips HTML tags
        
    -   collapses whitespace
        
    -   validates URLs / hex colors
        
    -   dedupes keywords
        
    -   clamps numeric ranges (e.g., 0–100% discounts)
        
4.  **Repository upsert** (`ProductRepository.upsert_flat`)
    
    -   resolves/creates FK rows in `supermarkets`, `internal_categories`, `store_categories`
        
    -   upserts into `products` (immediate write per product)
        

You don’t have to change scrapers when the schema evolves—the repository adapts.

----------

## Running both scrapers

`python - <<'PY' from mand.adapters.ah_nl import scrape_ah_nl_once
from mand.adapters.jumbo_nl import scrape_jumbo_once print("AH:", scrape_ah_nl_once()) print("Jumbo:", scrape_jumbo_once())
PY` 

----------

## Scheduling (optional)

Using APScheduler for periodic runs:

`# schedule_scrapes.py  import logging, sys from apscheduler.schedulers.blocking import BlockingScheduler

logging.basicConfig(level=logging.INFO, handlers=[logging.StreamHandler(sys.stdout)]) from mand.adapters.ah_nl import scrape_ah_nl_once from mand.adapters.jumbo_nl import scrape_jumbo_once

sched = BlockingScheduler(timezone="UTC")
sched.add_job(scrape_ah_nl_once, "interval", minutes=30, id="ah")
sched.add_job(scrape_jumbo_nl_once := scrape_jumbo_nl_once if  'scrape_jumbo_nl_once'  in  globals() else scrape_jumbo_once, "interval", minutes=30, id="jumbo")

sched.start()` 

Run:

`python schedule_scrapes.py` 

----------

## Prometheus (optional)

`prometheus-client` is available. If you expose a web process, you can add:

`from prometheus_client import start_http_server
start_http_server(9000) # /metrics` 

…and use the `@timed` instrumentation already present in scraper entrypoints.

----------

## Troubleshooting

-   **403 / 429 responses**  
    Retries are enabled. For AH, the client rotates `User-Agent` via `fake-useragent`. Make sure the package can initialize (some environments need outbound access once to seed).
    
-   **Bad URLs / Images**  
    Sanitizer will null out invalid URLs. Check adapter mapping if you need strict coverage.
    
-   **Duplicate products**  
    Uniqueness is `(product_id, supermarket_id)`. If a product moves categories, it’s still a single row; only `category_slug`/FKs update.
    
-   **Local dev limiting**  
    Use `*_MAX_PAGES` to limit crawling while iterating.

## TODO: extend base request class to include proxy rotation by default upon blocking/blacklisting    
    

----------

## Development

-   Code style: follow standard Python conventions
    
-   Add new supermarkets under `mand/adapters/<store_code>_xx/`
    
-   Ensure each adapter exposes a `scrape_<store>_once()` entrypoint
    
-   Keep normalization output consistent; the repository/sanitizer will do the rest