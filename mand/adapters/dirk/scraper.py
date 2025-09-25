import logging, time, re
from typing import Dict, List, Optional, Iterable, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

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
    "logo": "https://d3r3h30p75xj6a.cloudfront.net/files/9/4/6/9/0/2/WS_1080x1080_dirk-logo.png?width=400", "abbreviation": "Dirk",
    "brand_color": "#FF0000"
}

BASE = "https://web-dirk-gateway.detailresult.nl"
GQL  = BASE + "/graphql"

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

# ---------------- DEDUP: persistent seen-id cache ----------------
class SeenIdCache:
    """
    Simple persistent newline-delimited product_id set.
    Thread-safe enough for our usage: we append after upsert per batch.
    """
    def __init__(self, path: Path):
        self.path = path
        self._seen = set()
        if self.path.exists():
            try:
                with self.path.open("r", encoding="utf-8") as f:
                    for line in f:
                        pid = line.strip()
                        if pid:
                            self._seen.add(pid)
            except Exception as e:
                logger.warning("SeenIdCache load failed", extra={"err": str(e)})

    def has(self, pid: int | str) -> bool:
        return str(pid) in self._seen

    def add_many(self, pids: List[int | str]) -> None:
        new = [str(p) for p in pids if str(p) not in self._seen]
        if not new:
            return
        try:
            # Append atomically-ish
            with self.path.open("a", encoding="utf-8") as f:
                for p in new:
                    f.write(p + "\n")
                    f.flush()
            for p in new:
                self._seen.add(p)
        except Exception as e:
            logger.warning("SeenIdCache append failed", extra={"err": str(e)})

# ---------------- HTTP client ----------------
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
                self.session.headers["User-Agent"] = self.ua.random
            r.raise_for_status()
            return r.json()
        except Exception as e:
            logger.warning("POST failed", extra={"err": str(e)})
            return None

# ---------------- helpers ----------------
def _ts() -> str:
    return datetime.now(timezone.utc).isoformat()

def _keywords(title: str, brand: Optional[str], webgroup: Optional[str], packaging: Optional[str]) -> List[str]:
    import re
    parts = " ".join([t for t in [title, brand, webgroup, packaging] if t])
    cleaned = re.sub(r"[^\w\s-]", " ", parts.lower())
    toks = [t.strip("-") for t in cleaned.split() if len(t) > 1]
    seen, out = set(), []
    for t in toks:
        if t not in seen:
            out.append(t); seen.add(t)
    return out[:32]

def _abs_media(path: str) -> str:
    if not path: return None
    if path.startswith("http"): return path
    return "https://www.dirk.nl/" + path.lstrip("/")

def _choose_image(product: Dict) -> Optional[str]:
    imgs = product.get("images") or []
    if isinstance(imgs, list) and imgs:
        imgs_sorted = sorted(imgs, key=lambda im: (not im.get("mainImage", False), im.get("rankNumber") or 9999))
        for im in imgs_sorted:
            p = (im or {}).get("image")
            if p: return _abs_media(p)
    pi = ((product.get("productAssortment") or {}).get("productInformation")) or {}
    if pi.get("image"): return _abs_media(pi["image"])
    return None

def _has_discount(pa: Dict) -> bool:
    normal = (pa or {}).get("normalPrice")
    offer  = (pa or {}).get("offerPrice")
    try:
        n = float(normalize_price(normal))
        o = float(normalize_price(offer if offer not in (None, 0) else normal))
        return n > 0 and o < n
    except Exception:
        return False

def _promo_data(pa: Dict) -> Dict:
    import re
    promo = (pa or {}).get("productOffer") or {}
    text = promo.get("textPriceSign")
    has = bool(promo) or _has_discount(pa)
    res = {
        "hasPromotion": has,
        "text": text,
        "type": None, "category": None, "savingsType": None,
        "quantityRequirements": {
            "requiresMinimumQuantity": False, "minimumQuantity": None,
            "targetQuantity": None, "userInstruction": None, "actionRequired": False
        },
        "isProcessed": True
    }
    if has and text:
        low = text.lower()
        if re.search(r"\d+\s*voor\s*\d+(?:[.,]\d{2})?", low):
            res.update({"type":"multi_buy_price","category":"quantity","savingsType":"special_price"})
        elif re.search(r"\d+\+\d+", low) or "gratis" in low:
            res.update({"type":"buy_get_free","category":"quantity","savingsType":"free_items"})
        else:
            res.update({"type":"discount","category":"discount","savingsType":"price_reduction"})
    return res

def _pricing(pa: Dict) -> Dict:
    normal = normalize_price((pa or {}).get("normalPrice"))
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
        try: ids.append(int(pid))
        except Exception: continue
        if slug is None:
            pi = entry.get("productInformation") or {}
            slug = (pi.get("webgroup") or "").strip() or None
    # [DEDUP] keep order but unique within this group
    seen = set(); uniq_ids = []
    for x in ids:
        if x not in seen:
            seen.add(x); uniq_ids.append(x)
    return uniq_ids, (slug or f"webgroup-{webgroup_id}")

def _fetch_detail(client: DirkClient, product_id: int, store_id: int) -> Optional[Dict]:
    resp = client.post({"query": DETAIL_QUERY, "variables": {"productId": product_id, "StoreId": store_id}})
    # NOTE: API is case-insensitive for variables, but keep consistent:
    if not resp:  # fallback if a typo in key casing above ever causes issues
        resp = client.post({"query": DETAIL_QUERY, "variables": {"productId": product_id, "storeId": store_id}})
    if not resp: return None
    return ((resp.get("data") or {}).get("product")) or None

def _to_record(d: Dict, mapper: InternalCategoryMapper) -> Optional[Dict]:
    if not d: return None
    ts = _ts()
    pa = d.get("productAssortment") or {}
    pi = pa.get("productInformation") or {}

    title = pi.get("headerText") or d.get("headerText") or ""
    summary = d.get("description") or d.get("additionalDescription") or ""
    img = _choose_image(d)
    source_url = None  # Unknown canonical Dirk PDP pattern

    supermarket_obj = dict(SUPERMARKET)
    category_name = (pi.get("webgroup") or "").strip() or None
    category_obj = {"id": None, "name": category_name, "description": None, "logo": None}
    internal_cat = mapper.map(SUPERMARKET["id"], (d.get("department") or pi.get("department") or ""), None)

    return {
        "product_id": str(d.get("productId") or ""),
        "name_full": title,
        "name_display": title,
        "description_full": summary,
        "description_display": summary,
        "image_url": img,
        "source_url": source_url,
        "keywords": _keywords(title, pi.get("brand"), pi.get("webgroup"), pi.get("packaging")),
        "created_at": ts, "updated_at": ts, "last_scraped_at": ts,
        "parent_product_id": None, "child_products": [],
        "supermarket": supermarket_obj,
        "category": category_obj,
        "pricing": _pricing(pa),
        "promotion_data": _promo_data(pa),
        "internal_category": internal_cat
    }

# ---------------- core scrape with de-dup ----------------
@timed
def scrape_dirk_once() -> int:
    client = DirkClient(workers=16)
    mapper = InternalCategoryMapper()

    total = 0
    store_id = getattr(settings, "DIRK_STORE_ID", 66)
    start_gid = getattr(settings, "DIRK_WEBGROUP_START", 1)
    end_gid   = getattr(settings, "DIRK_WEBGROUP_END", 5000)
    fetch_details = getattr(settings, "DIRK_FETCH_DETAILS", True)
    sleep_s = getattr(settings, "DIRK_SLEEP_S", 0.10)

    # [DEDUP] persistent seen cache across runs
    seen_path = Path(getattr(settings, "DIRK_SEEN_IDS_PATH", "dirk_seen_ids.txt"))
    seen_cache = SeenIdCache(seen_path)

    # [DEDUP] in-memory set for this run to avoid double-writing if PDP repeats in multiple groups
    run_seen: set[str] = set()

    for gid in range(start_gid, end_gid + 1):
        ids, slug = _list_ids_and_slug(client, gid, store_id)
        if not ids:
            time.sleep(sleep_s); continue

        # [DEDUP] filter out IDs already seen in prior runs or earlier in this run
        ids_to_fetch = [pid for pid in ids if (str(pid) not in run_seen and not seen_cache.has(pid))]
        if not ids_to_fetch:
            logger.info("All products already seen, skipping group", extra={"webGroup": gid, "slug": slug})
            time.sleep(sleep_s); continue

        details_map: Dict[int, Optional[Dict]] = {}
        if fetch_details:
            with ThreadPoolExecutor(max_workers=settings.DIRK_WORKERS) as ex:
                futs = {ex.submit(_fetch_detail, client, pid, store_id): pid for pid in ids_to_fetch}
                for fut in as_completed(futs):
                    pid = futs[fut]
                    try:
                        details_map[pid] = fut.result()
                    except Exception:
                        details_map[pid] = None
                    time.sleep(0.01)

        products, newly_seen_ids = [], []
        for pid in ids_to_fetch:
            d = details_map.get(pid) if fetch_details else None
            if not d:
                continue
            rec = _to_record(d, mapper)
            if not rec:
                continue
            products.append(rec)
            # [DEDUP] mark as seen for this run
            run_seen.add(str(pid))
            newly_seen_ids.append(pid)

        if not products:
            time.sleep(sleep_s); continue

        # Persist per webGroup
        ProductRepository.upsert_flat(slug, products)
        total += len(products)

        # [DEDUP] after successful upsert, append to persistent cache so future runs skip them
        seen_cache.add_many(newly_seen_ids)

        logger.info("Dirk batch saved", extra={"webGroup": gid, "slug": slug, "count": len(products)})
        time.sleep(sleep_s)

    logger.info("Dirk scrape complete", extra={"products": total})
    return total
