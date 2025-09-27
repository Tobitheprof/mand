import logging, time, re
from typing import Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from fake_useragent import UserAgent

from mand.config.settings import settings
from mand.normalization.internal_categories import InternalCategoryMapper
from mand.normalization.cleaners import normalize_price
from mand.storage.repository import ProductRepository
from mand.monitoring.instrumentation import timed

logger = logging.getLogger(__name__)

SUPERMARKET = {
    "id": "ah", "name": "Albert Heijn",
    "logo": "https://static.ah.nl/logo/ah-logo.svg",
    "abbreviation": "AH", "brand_color": "#00A3E0"
}

DEFAULT_CATS = [
    {"id": "20885", "slug": "fruit-verse-sappen", "name": "Fruit, verse sappen"},
    {"id": "6401",  "slug": "groente-aardappelen", "name": "Groente & aardappelen"},
    {"id": "1301",  "slug": "maaltijden-salades",  "name": "Maaltijden & salades"},
    {"id": "9344",  "slug": "vlees",               "name": "Vlees"},
    {"id": "1651",  "slug": "vis",                 "name": "Vis"},
    {"id": "7582",  "slug": "zuivel",              "name": "Zuivel"},
    {"id": "1304",  "slug": "brood-gebak",         "name": "Brood & gebak"},
    {"id": "1270",  "slug": "ontbijt",             "name": "Ontbijt"},
]

BASE = "https://www.ah.nl"
SEARCH = BASE + "/zoeken/api/products/search"
GQL = BASE + "/gql"

GQL_QUERY = {
    "operationName": "product",
    "variables": {"id": None, "date": None},
    "query": """
    query product($id: Int!, $date: String) {
      product(id: $id, date: $date) {
        id title summary additionalInformation webPath
        priceV2(forcePromotionVisibility: true) {
          now { amount } was { amount }
          discount { description promotionType availability { startDate endDate } }
          promotionShields { text }
        }
        taxonomies { id name }
        imagePack { large { url } }
      }
    }"""
}

class AHClient:
    def __init__(self, workers: int):
        self.ua = UserAgent()
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": self.ua.random,
            "Accept": "application/json,text/plain,*/*",
            "Accept-Language": "en-US,en;q=0.9,nl;q=0.8",
            "Origin": BASE,
            "Referer": BASE + "/",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
        })
        retry = Retry(total=3, backoff_factor=0.5,
                      status_forcelist=(403, 429, 500, 502, 503, 504),
                      allowed_methods=frozenset(["GET", "POST"]))
        adapter = HTTPAdapter(max_retries=retry, pool_connections=workers, pool_maxsize=workers)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)
        self.workers = workers

    def get(self, url: str, **params) -> Optional[Dict]:
        try:
            r = self.session.get(url, params=params, timeout=20)
            if r.status_code == 403:
                self.session.headers["User-Agent"] = self.ua.random
            r.raise_for_status()
            return r.json()
        except Exception as e:
            logger.warning("GET failed", extra={"url": url, "err": str(e)})
            return None

    def post(self, payload: Dict) -> Optional[Dict]:
        try:
            r = self.session.post(GQL, json=payload, timeout=25)
            if r.status_code == 403:
                self.session.headers["User-Agent"] = self.ua.random
            r.raise_for_status()
            return r.json()
        except Exception as e:
            logger.warning("POST failed", extra={"err": str(e)})
            return None

# ---------- helpers ----------
def _ts() -> str:
    return datetime.now(timezone.utc).isoformat()

def _safe_int(v) -> Optional[int]:
    try: return int(v)
    except Exception: return None

def _extract_image_url(basic: Dict, dprod: Dict) -> Optional[str]:
    imgs = basic.get("images")
    if isinstance(imgs, list) and imgs:
        for im in reversed(imgs):
            if isinstance(im, dict) and im.get("url"):
                return im["url"]
    ip = dprod.get("imagePack")
    if isinstance(ip, dict):
        large = ip.get("large")
        if isinstance(large, dict) and large.get("url"): return large["url"]
        if isinstance(large, list):
            for item in reversed(large):
                if isinstance(item, dict) and item.get("url"):
                    return item["url"]
    elif isinstance(ip, list):
        for entry in ip:
            if not isinstance(entry, dict): continue
            large = entry.get("large")
            if isinstance(large, dict) and large.get("url"): return large["url"]
            if isinstance(large, list):
                for item in reversed(large):
                    if isinstance(item, dict) and item.get("url"):
                        return item["url"]
    return None

def _promo_data(basic: Dict, dprod: Optional[Dict] = None) -> Dict:
    res = {
        "hasPromotion": False,
        "text": None,
        "type": None,
        "category": None,
        "savingsType": None,
        "quantityRequirements": {
            "requiresMinimumQuantity": False,
            "minimumQuantity": None,
            "targetQuantity": None,
            "userInstruction": None,
            "actionRequired": False
        },
        "isProcessed": True,
        "bonus": None  # new: store raw bonus info
    }

    # old shield/discount parsing
    shield_text = (basic.get("shield") or {}).get("text")
    disc = (basic.get("discount") or {})
    pnow = (basic.get("price") or {}).get("now")
    pwas = (basic.get("price") or {}).get("was")

    if shield_text:
        res.update({"hasPromotion": True, "text": shield_text})
    if disc:
        res["hasPromotion"] = True
        if not res["text"]:
            res["text"] = disc.get("promotionType") or disc.get("description")
    if pwas and pnow and pwas > pnow:
        res["hasPromotion"] = True
        if not res["text"]:
            res["text"] = f"Was €{pwas:.2f}, now €{pnow:.2f}"

    # NEW: bonus info from detailed product
    if dprod:
        p2 = (((dprod or {}).get("data") or {}).get("product") or {}).get("priceV2") or {}
        discount = p2.get("discount") or {}
        shields = p2.get("promotionShields") or []
        if discount or shields:
            res["hasPromotion"] = True
            res["text"] = (res["text"] or discount.get("description") or
                           (shields[0].get("text")[0] if shields and shields[0].get("text") else None))
            res["type"] = "BONUS"
            res["category"] = "discount"
            res["savingsType"] = "price_reduction"
            res["bonus"] = {
                "description": discount.get("description"),
                "promotionType": discount.get("promotionType"),
                "segmentType": discount.get("segmentType"),
                "theme": discount.get("theme"),
                "startDate": (discount.get("availability") or {}).get("startDate"),
                "endDate": (discount.get("availability") or {}).get("endDate"),
                "wasPriceVisible": discount.get("wasPriceVisible"),
            }

    return res


# ---------- core scrape ----------
def _fetch_products_in_taxonomy(client: AHClient, taxonomy_id: str, taxonomy_slug: str, max_pages: Optional[int]) -> List[Dict]:
    page, total_pages, out = 0, None, []
    while True:
        data = client.get(SEARCH, taxonomy=taxonomy_id, taxonomySlug=taxonomy_slug, size=36, page=page)
        if not data: break
        for card in (data.get("cards") or []):
            out.extend(card.get("products") or [])
        if total_pages is None:
            total_pages = (data.get("page") or {}).get("totalPages", 1)
        page += 1
        if (max_pages and page >= max_pages) or page >= total_pages: break
        time.sleep(0.05)
    return out

def _fetch_detail(client: AHClient, pid: int) -> Optional[Dict]:
    payload = dict(GQL_QUERY)
    payload["variables"] = {"id": pid, "date": datetime.now().strftime("%Y-%m-%d")}
    return client.post(payload)

def _keywords(title: str) -> List[str]:
    if not title: return []
    cleaned = re.sub(r"[^\w\s-]", " ", title.lower())
    toks = [t.strip("-") for t in cleaned.split() if len(t) > 2]
    return sorted(set(toks))

def _to_record(basic: Dict, dprod: Optional[Dict], mapper: InternalCategoryMapper) -> Dict:
    ts = _ts()
    d = ((dprod or {}).get("data") or {}).get("product") or {}
    title = basic.get("title") or d.get("title") or ""
    summary = d.get("summary") or d.get("additionalInformation")

    cat0 = (basic.get("taxonomies") or [{}])[0] if basic.get("taxonomies") else {}
    category_id = str(cat0.get("id",""))
    category_name = cat0.get("name", basic.get("category",""))

    pb = basic.get("price") or {}
    price_now = normalize_price(pb.get("now", 0))
    price_was = normalize_price(pb.get("was", price_now))
    if price_now == 0 and d:
        pv2 = d.get("priceV2") or {}
        price_now = normalize_price(((pv2.get("now") or {}).get("amount")))
        price_was = normalize_price(((pv2.get("was") or {}).get("amount", price_now)))
    has_discount = price_was > price_now
    disc_pct = round(float((price_was - price_now) / price_was * 100), 2) if (has_discount and price_was > 0) else None
    ptype = "BONUS" if (basic.get("discount") or basic.get("shield") or has_discount) else "NOT_IN_BONUS"

    img = _extract_image_url(basic, d)
    source_url = (BASE + basic.get("link")) if basic.get("link") else (BASE + (d.get("webPath") or ""))

    supermarket_obj = dict(SUPERMARKET)
    category_obj = {"id": category_id, "name": category_name, "description": None, "logo": None}
    internal_cat = mapper.map(SUPERMARKET["id"], category_name, None)
    pricing_obj = {
        "current": float(price_now),
        "original": float(price_was),
        "has_discount": has_discount,
        "discount_percentage": disc_pct,
        "product_type": ptype
    }

    return {
        "product_id": "AH-"+str(basic.get("id","")),
        "name_full": title,
        "name_display": title,
        "description_full": summary,
        "description_display": summary,
        "image_url": img,
        "source_url": source_url,
        "keywords": _keywords(title),
        "created_at": ts, "updated_at": ts, "last_scraped_at": ts,
        "parent_product_id": None,
        "child_products": [],
        "supermarket": supermarket_obj,
        "category": category_obj,
        "pricing": pricing_obj,
        "promotion_data": _promo_data(basic, d),
        "internal_category": internal_cat
    }

@timed
def scrape_ah_nl_once() -> int:
    client = AHClient(workers=settings.AH_WORKERS)
    mapper = InternalCategoryMapper()

    total = 0

    for c in DEFAULT_CATS:
        category_slug = c["slug"]
        basics = _fetch_products_in_taxonomy(client, c["id"], category_slug, settings.ah_max_pages)
        if not basics:
            continue

        details_map: Dict[int, Optional[Dict]] = {}
        if settings.AH_FETCH_DETAILS:
            ids = {_safe_int(b.get("id")) for b in basics if b.get("id")}
            ids = {i for i in ids if i is not None}
            with ThreadPoolExecutor(max_workers=settings.AH_WORKERS) as ex:
                futs = {ex.submit(_fetch_detail, client, pid): pid for pid in ids}
                for fut in as_completed(futs):
                    pid = futs[fut]
                    try:
                        details_map[pid] = fut.result()
                    except Exception:
                        details_map[pid] = None
                    time.sleep(0.01)

        products = [
            _to_record(b, details_map.get(_safe_int(b.get("id"))) if settings.AH_FETCH_DETAILS else None, mapper)
            for b in basics
        ]
        total += len(products)

        # ProductRepository.save_raw(SUPERMARKET["id"], category_slug, products)
        ProductRepository.upsert_flat(category_slug, products)

    logger.info("AH scrape complete", extra={"products": total})
    return total