import logging, time, re
from typing import Dict, List, Optional, Iterable, Tuple
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
    "id": "dirk", "name": "Dirk",
    "logo": None, "abbreviation": "Dirk",
    "brand_color": None  # set "#E2001A" if you want
}

# GQL endpoints
BASE = "https://web-dirk-gateway.detailresult.nl"
GQL = BASE + "/graphql"

# GQL: list product ids inside a webGroup
LIST_QUERY = """
query GetCategory($webGroupId: Int!, $storeId: Int!) {
  listWebGroupProducts(webGroupId: $webGroupId) {
    productAssortment(storeId: $storeId) {
      productId
      productInformation { webgroup }
    }
  }
}
"""

# GQL: full product detail (dirk)
DETAIL_QUERY = """
query GetProduct($productId: Int!, $storeId: Int!) {
  product(productId: $productId) {
    productId
    department
    headerText
    packaging
    description
    additionalDescription
    images { image rankNumber mainImage }
    logos  { description position link image }
    declarations {
      storageInstructions
      cookingInstructions
      instructionsForUse
      ingredients
      contactInformation { contactName contactAdress }
      nutritionalInformation {
        standardPackagingUnit
        soldOrPrepared
        nutritionalValues { text value nutritionalSubValues { text value } }
      }
      allergiesInformation { text }
    }
    productAssortment(storeId: $storeId) {
      productId
      normalPrice
      offerPrice
      isSingleUsePlastic
      singleUsePlasticValue
      startDate
      endDate
      productOffer {
        textPriceSign
        endDate
        startDate
        disclaimerStartDate
        disclaimerEndDate
      }
      productInformation {
        productId
        headerText
        subText
        packaging
        image
        department
        webgroup
        brand
        logos { description position link image }
      }
    }
  }
}
"""

class DirkClient:
    def __init__(self, workers: int):
        self.ua = UserAgent()
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": self.ua.random,
            "Accept": "application/json,text/plain,*/*",
            "Accept-Language": "en-US,en;q=0.9,nl;q=0.8",
            "Origin": "https://www.dirk.nl",
            "Referer": "https://www.dirk.nl/",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "Content-Type": "application/json",
        })
        retry = Retry(
            total=3, backoff_factor=0.4,
            status_forcelist=(403, 429, 500, 502, 503, 504),
            allowed_methods=frozenset(["GET", "POST"])
        )
        adapter = HTTPAdapter(max_retries=retry, pool_connections=workers, pool_maxsize=workers)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)
        self.workers = workers

    def post(self, payload: Dict) -> Optional[Dict]:
        try:
            r = self.session.post(GQL, json=payload, timeout=25)
            if r.status_code == 403:
                # rotate UA and try to be a bit stealthy
                self.session.headers["User-Agent"] = self.ua.random
            r.raise_for_status()
            return r.json()
        except Exception as e:
            logger.warning("POST failed", extra={"err": str(e)})
            return None

# ---------- helpers ----------
def _ts() -> str:
    return datetime.now(timezone.utc).isoformat()

def _keywords(title: str, brand: Optional[str], webgroup: Optional[str], packaging: Optional[str]) -> List[str]:
    parts = " ".join([t for t in [title, brand, webgroup, packaging] if t])
    cleaned = re.sub(r"[^\w\s-]", " ", parts.lower())
    toks = [t.strip("-") for t in cleaned.split() if len(t) > 1]
    # de-dup but keep order
    seen, out = set(), []
    for t in toks:
        if t not in seen:
            out.append(t); seen.add(t)
    return out[:32]

def _choose_image(product: Dict) -> Optional[str]:
    # Prefer main image under product.images; fallback to productAssortment.productInformation.image
    imgs = product.get("images") or []
    if isinstance(imgs, list) and imgs:
        # mainImage True first, then lowest rankNumber
        imgs_sorted = sorted(imgs, key=lambda im: (not im.get("mainImage", False), im.get("rankNumber") or 9999))
        for im in imgs_sorted:
            path = (im or {}).get("image")
            if path:
                return _abs_media(path)
    pi = ((product.get("productAssortment") or {}).get("productInformation")) or {}
    if pi.get("image"):
        return _abs_media(pi["image"])
    return None

def _abs_media(path: str) -> str:
    # Dirk media paths are typically relative like "artikelen/....png" or "logos/....png"
    if path.startswith("http"):
        return path
    return "https://www.dirk.nl/" + path.lstrip("/")

def _promo_data(pa: Dict) -> Dict:
    promo = (pa or {}).get("productOffer") or {}
    text = promo.get("textPriceSign")
    has = bool(promo) or _has_discount(pa)
    res = {
        "hasPromotion": has,
        "text": text,
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
        "isProcessed": True
    }
    # Simple type inference hooks (extend if needed)
    if has and text:
        low = text.lower()
        if re.search(r"\d+\s*voor\s*\d+(?:[.,]\d{2})?", low):
            res.update({"type": "multi_buy_price", "category": "quantity", "savingsType": "special_price"})
        elif re.search(r"\d+\+\d+", low) or "gratis" in low:
            res.update({"type": "buy_get_free", "category": "quantity", "savingsType": "free_items"})
        else:
            res.update({"type": "discount", "category": "discount", "savingsType": "price_reduction"})
    return res

def _has_discount(pa: Dict) -> bool:
    normal = (pa or {}).get("normalPrice")
    offer  = (pa or {}).get("offerPrice")
    try:
        n = float(normalize_price(normal))
        o = float(normalize_price(offer if offer not in (None, 0) else normal))
        return n > 0 and o < n
    except Exception:
        return False

def _pricing(pa: Dict) -> Dict:
    normal = normalize_price((pa or {}).get("normalPrice"))
    # If offerPrice missing or zero, fall back to normal
    raw_offer = (pa or {}).get("offerPrice")
    effective = normalize_price(raw_offer if raw_offer not in (None, 0) else normal)
    has_discount = (normal > 0) and (effective < normal)
    disc_pct = round(float((normal - effective) / normal * 100), 2) if (has_discount and normal > 0) else None
    ptype = "BONUS" if has_discount or ((pa or {}).get("productOffer")) else "NOT_IN_BONUS"
    return {
        "current": float(effective),
        "original": float(normal if normal not in (0,) else effective),
        "has_discount": has_discount,
        "discount_percentage": disc_pct,
        "product_type": ptype
    }

def _finished_shape(payload: Dict) -> bool:
    # Stop when: {"errors":[...], "data": {"listWebGroupProducts": {"productAssortment": null}}}
    if not isinstance(payload, dict): return False
    if not payload.get("errors"): return False
    data = payload.get("data") or {}
    lwp = data.get("listWebGroupProducts")
    return isinstance(lwp, dict) and lwp.get("productAssortment") is None

def _list_ids_and_slug(client: DirkClient, webgroup_id: int, store_id: int) -> Tuple[List[int], str]:
    payload = {"query": LIST_QUERY, "variables": {"webGroupId": webgroup_id, "storeId": store_id}}
    resp = client.post(payload)
    if not resp: return [], f"webgroup-{webgroup_id}"
    if _finished_shape(resp): return [], f"webgroup-{webgroup_id}"
    pa = ((resp.get("data") or {}).get("listWebGroupProducts") or {}).get("productAssortment") or []
    ids, slug = [], None
    for entry in pa:
        if not isinstance(entry, dict): continue
        pid = entry.get("productId")
        if pid is None: continue
        try:
            ids.append(int(pid))
        except Exception:
            continue
        if slug is None:
            pi = entry.get("productInformation") or {}
            slug = (pi.get("webgroup") or "").strip() or None
    return ids, (slug or f"webgroup-{webgroup_id}")

def _fetch_detail(client: DirkClient, product_id: int, store_id: int) -> Optional[Dict]:
    resp = client.post({"query": DETAIL_QUERY, "variables": {"productId": product_id, "storeId": store_id}})
    if not resp: return None
    return ((resp.get("data") or {}).get("product")) or None

def _to_record(d: Dict, mapper: InternalCategoryMapper) -> Optional[Dict]:
    """
    Standardize one Dirk detail product → MAND flat record.
    """
    if not d: return None
    ts = _ts()

    pa = d.get("productAssortment") or {}
    pi = pa.get("productInformation") or {}

    # Names & description
    title = pi.get("headerText") or d.get("headerText") or ""
    summary = d.get("description") or d.get("additionalDescription") or ""

    img = _choose_image(d)
    # Dirk product page pattern not guaranteed; omit unless you have a canonical path
    source_url = None

    # Supermarket & categories
    supermarket_obj = dict(SUPERMARKET)
    category_name = (pi.get("webgroup") or "").strip() or None
    category_obj = {"id": None, "name": category_name, "description": None, "logo": None}
    internal_cat = mapper.map(SUPERMARKET["id"], (d.get("department") or pi.get("department") or ""), None)

    # Pricing & promo
    pricing_obj = _pricing(pa)
    promo_obj = _promo_data(pa)

    brand = pi.get("brand")
    webgroup = pi.get("webgroup")
    packaging = pi.get("packaging")
    keywords = _keywords(title, brand, webgroup, packaging)

    rec = {
        "product_id": str(d.get("productId") or ""),
        "name_full": title,
        "name_display": title,
        "description_full": summary,
        "description_display": summary,
        "image_url": img,
        "source_url": source_url,
        "keywords": keywords,
        "created_at": ts, "updated_at": ts, "last_scraped_at": ts,
        "parent_product_id": None,
        "child_products": [],
        "supermarket": supermarket_obj,
        "category": category_obj,
        "pricing": pricing_obj,
        "promotion_data": promo_obj,
        "internal_category": internal_cat
    }
    return rec

# ---------- core scrape ----------
@timed
def scrape_dirk_once() -> int:
    """
    Iterates webGroupId from settings.DIRK_WEBGROUP_START to settings.DIRK_WEBGROUP_END (inclusive),
    fetches product ids, pulls full details (parallel), standardizes, and upserts per webGroup slug.
    """
    client = DirkClient(workers=16)
    mapper = InternalCategoryMapper()

    total = 0
    store_id = getattr(settings, "DIRK_STORE_ID", 66)
    start_gid = getattr(settings, "DIRK_WEBGROUP_START", 1)
    end_gid   = getattr(settings, "DIRK_WEBGROUP_END", 300)
    fetch_details = getattr(settings, "DIRK_FETCH_DETAILS", True)
    sleep_s = getattr(settings, "DIRK_SLEEP_S", 0.10)

    for gid in range(start_gid, end_gid + 1):
        ids, slug = _list_ids_and_slug(client, gid, store_id)
        if not ids:
            time.sleep(sleep_s)
            continue

        details_map: Dict[int, Optional[Dict]] = {}
        if fetch_details:
            with ThreadPoolExecutor(max_workers=16) as ex:
                futs = {ex.submit(_fetch_detail, client, pid, store_id): pid for pid in ids}
                for fut in as_completed(futs):
                    pid = futs[fut]
                    try:
                        details_map[pid] = fut.result()
                    except Exception:
                        details_map[pid] = None
                    time.sleep(0.01)

        products = []
        for pid in ids:
            d = details_map.get(pid) if fetch_details else None
            if not d:
                # If details off/unavailable, skip—Dirk needs details to standardize.
                continue
            rec = _to_record(d, mapper)
            if rec:
                products.append(rec)

        if not products:
            time.sleep(sleep_s)
            continue

        # Persist per webGroup (use slug as category key)
        ProductRepository.upsert_flat(slug, products)
        total += len(products)

        logger.info("Dirk batch saved", extra={"webGroup": gid, "slug": slug, "count": len(products)})
        time.sleep(sleep_s)

    logger.info("Dirk scrape complete", extra={"products": total})
    return total
