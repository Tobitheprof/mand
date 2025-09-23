#!/usr/bin/env python3
"""
Jumbo (NL) → search (page-by-page) → detail → normalize → immediate DB upserts

Behavior:
- Fetch one search page at a time
- For each product on that page:
    - fetch detail concurrently
    - standardize to MAND schema
    - upsert immediately to DB (via ProductRepository.upsert_flat with single-item list)
- Rich logging for debugging (page offsets, counts, per-SKU detail start/finish, per-product upserts)

This mirrors the structure and behavior of your AH module.
"""

from __future__ import annotations

import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any, Dict, Iterator, List, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from mand.config.settings import settings
from mand.normalization.internal_categories import InternalCategoryMapper
from mand.normalization.cleaners import normalize_price
from mand.storage.repository import ProductRepository
from mand.monitoring.instrumentation import timed

logger = logging.getLogger(__name__)

# -----------------------------
# Supermarket meta
# -----------------------------
SUPERMARKET = {
    "id": "jumbo",
    "name": "Jumbo",
    "logo": None,
    "abbreviation": "Jumbo",
    "brand_color": "#FFD200",
}

SEARCH_TERMS = getattr(settings, "JUMBO_SEARCH_TERMS", "producten")

# -----------------------------
# Endpoints & headers
# -----------------------------
JUMBO_GQL_URL = "https://www.jumbo.com/api/graphql"
REQUEST_TIMEOUT = getattr(settings, "JUMBO_REQUEST_TIMEOUT", 20)
PAGE_SIZE = getattr(settings, "JUMBO_PAGE_SIZE", 24)
DELAY_BETWEEN_PAGES = getattr(settings, "JUMBO_DELAY_BETWEEN_PAGES", 0.25)
DELAY_BETWEEN_DETAILS = getattr(settings, "JUMBO_DELAY_BETWEEN_DETAILS", 0.10)

HEADERS = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "Origin": "https://www.jumbo.com",
    "Referer": "https://www.jumbo.com/",
    "User-Agent": "Mozilla/5.0",
}

# -----------------------------
# GraphQL queries (trimmed to fields we use)
# -----------------------------
SEARCH_QUERY = """
query SearchProducts($input: ProductSearchInput!, $shelfTextInput: ShelfTextInput!, $withFacetChildren: Boolean!) {
  searchProducts(input: $input) {
    redirectUrl
    removeAllAction { friendlyUrl __typename }
    pageHeader { headerText count __typename }
    start
    count
    sortOptions { text friendlyUrl selected __typename }
    categoryTiles {
      count
      catId
      name
      friendlyUrl
      imageLink
      displayOrder
      subtitle
      __typename
    }
    facets {
      key
      displayName
      multiSelect
      tooltip { linkTarget linkText text __typename }
      values {
        ...FacetDetails
        children @include(if: $withFacetChildren) {
          ...FacetDetails @include(if: $withFacetChildren)
          children {
            ...FacetDetails
            children {
              ...FacetDetails
              children {
                ...FacetDetails
                children { ...FacetDetails __typename }
                __typename
              }
              __typename
            }
            __typename
          }
          __typename
        }
        __typename
      }
      __typename
    }
    products {
      ...SearchProductDetails
      crossSells { sku __typename }
      retailSetProducts { ...SearchProductDetails __typename }
      nutriScore { url __typename }
      __typename
    }
    pathways {
      title
      subTitle
      products {
        ...SearchProductDetails
        retailSetProducts { ...SearchProductDetails __typename }
        __typename
      }
      __typename
    }
    textMessage {
      header
      linkText
      longBody
      messageType
      shortBody
      targetUrl
      __typename
    }
    socialLists {
      author
      authorVerified
      followers
      id
      productImages
      thumbnail
      title
      __typename
    }
    selectedFacets { values { name count friendlyUrl __typename } __typename }
    breadcrumbs { text friendlyUrl __typename }
    seo { title description canonicalLink __typename }
    categoryId
    __typename
  }
  getCategoryShelfText(input: $shelfTextInput) {
    shelfText
    __typename
  }
}

fragment FacetDetails on Facet {
  id
  count
  name
  parent
  friendlyUrl
  selected
  thematicAisle
  __typename
}

fragment SearchProductDetails on Product {
  id: sku
  brand
  category: rootCategory
  subtitle: packSizeDisplay
  title
  image
  inAssortment
  availability {
    availability
    isAvailable
    label
    stockLimit
    reason
    availabilityNote
    __typename
  }
  sponsored
  auctionId
  link
  retailSet
  prices: price {
    price
    promoPrice
    pricePerUnit { price unit __typename }
    __typename
  }
  quantityDetails { maxAmount minAmount stepAmount defaultAmount __typename }
  primaryBadge: primaryProductBadges { alt image __typename }
  secondaryBadges: secondaryProductBadges { alt image __typename }
  customerAllergies { short __typename }
  promotions {
    id
    group
    isKiesAndMix
    image
    tags { text inverse __typename }
    start { dayShort date monthShort __typename }
    end { dayShort date monthShort __typename }
    attachments { type path __typename }
    primaryBadge: primaryBadges { alt image __typename }
    volumeDiscounts { discount volume __typename }
    durationTexts { shortTitle __typename }
    maxPromotionQuantity
    url
    __typename
  }
  surcharges { type value { amount currency __typename } __typename }
  characteristics {
    freshness { name value url __typename }
    logo { name value url __typename }
    tags { url name value __typename }
    __typename
  }
  __typename
}
"""

DETAIL_QUERY = """
query productDetail($sku: String!) {
  product(sku: $sku) {
    id
    brand
    brandURL
    ean
    rootCategory
    categories { name path id __typename }
    subtitle
    title
    image
    canonicalUrl
    description
    storage
    recycling
    ingredients
    retailSet
    isMedicine
    preparationAndUsage
    isExcludedForCustomer
    thumbnails { image type __typename }
    images { image type __typename }
    additionalImages { image type __typename }
    productAllergens { mayContain contains __typename }
    nutritionsTable { columns rows __typename }
    nutriScore { value url __typename }
    availability {
      availabilityNote
      label
      isAvailable
      availability
      stockLimit
      reason
      delistDate { iso __typename }
      availabilityNote
      __typename
    }
    link
    price {
      price
      promoPrice
      pricePerUnit { price unit __typename }
      __typename
    }
    quantityDetails { maxAmount minAmount stepAmount defaultAmount __typename }
    primaryProductBadges { alt image __typename }
    secondaryProductBadges { alt image __typename }
    promotions {
      id
      isKiesAndMix
      tags { text inverse __typename }
      group
      image
      url
      durationTexts { title description shortTitle __typename }
      primaryBadges { alt image __typename }
      start { date dayShort monthShort __typename }
      end { date dayShort monthShort __typename }
      volumeDiscounts { discount volume __typename }
      maxPromotionQuantity
      __typename
    }
    manufacturer { description address phone website __typename }
    alcoholByVolume
    nutritionHealthClaims
    additives
    mandatoryInformation
    regulatedProductName
    safety
    safetyWarning
    origin
    fishCatchArea
    fishOriginFreeText
    fishPlaceOfProvenance
    placeOfRearing
    placeOfSlaughter
    placeOfBirth
    customerAllergies {
      showProductContainsMatchingAllergiesPrompt
      showConfigureDietaryPreferencesPrompt
      long
      short
      prompt { text title action __typename }
      __typename
    }
    sponsored
    drainedWeight
    characteristics {
      freshness { name value url __typename }
      logo { name value url __typename }
      tags { url name value __typename }
      __typename
    }
    retailSetProducts {
      id
      brand
      title
      image
      link
      price { price promoPrice __typename }
      __typename
    }
    alternatives {
      id
      brand
      title
      image
      link
      price { price promoPrice __typename }
      __typename
    }
    crossSells {
      id
      brand
      title
      image
      link
      price { price promoPrice __typename }
      __typename
    }
    __typename
  }
}
"""

# -----------------------------
# HTTP client
# -----------------------------
class JumboClient:
    def __init__(self):
        self.session = requests.Session()
        retries = Retry(
            total=3,
            backoff_factor=0.4,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset(["POST"]),
            raise_on_status=False,
        )
        self.session.mount("https://", HTTPAdapter(max_retries=retries))

    def post(self, json_payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        try:
            r = self.session.post(
                JUMBO_GQL_URL,
                json=json_payload,
                headers=HEADERS,
                timeout=REQUEST_TIMEOUT,
            )
            r.raise_for_status()
            data = r.json()
            if isinstance(data, dict) and data.get("errors"):
                logger.warning("Jumbo GQL errors", extra={"errors": data["errors"]})
                return None
            return data.get("data")
        except Exception as e:
            logger.warning("Jumbo POST failed", extra={"err": str(e)})
            return None

# -----------------------------
# Helpers
# -----------------------------
def _ts() -> str:
    return datetime.now(timezone.utc).isoformat()

def _slugify(s: Optional[str]) -> str:
    if not s:
        return "uncategorized"
    s = re.sub(r"[^\w\s-]", "", s.strip().lower())
    s = re.sub(r"\s+", "-", s)
    s = re.sub(r"-{2,}", "-", s)
    return s.strip("-") or "uncategorized"

def _leaf_category(prod: Dict[str, Any]) -> Dict[str, Any]:
    cats = prod.get("categories") or []
    leaf = cats[-1] if cats else {}
    return {
        "id": (leaf.get("id") if isinstance(leaf, dict) else None),
        "name": (leaf.get("name") if isinstance(leaf, dict) else None),
        "description": None,
        "logo": None,
    }

def _parse_date(s: Optional[str]):
    if not s or not isinstance(s, str):
        return None
    s = s.strip()
    for fmt in ("%d-%m-%Y", "%Y-%m-%d", "%d/%m/%Y", "%d %b %Y", "%d %b"):
        try:
            dt = datetime.strptime(s, fmt)
            if "%Y" not in fmt:
                dt = dt.replace(year=datetime.now(timezone.utc).year)
            return dt.date()
        except Exception:
            continue
    return None

def _first_promo_dates(promos: List[Dict[str, Any]]) -> Tuple[Optional[str], Optional[str]]:
    if not promos:
        return None, None
    p = promos[0]
    s = _parse_date(((p.get("start") or {}).get("date")))
    e = _parse_date(((p.get("end") or {}).get("date")))
    return (s.isoformat() if s else None), (e.isoformat() if e else None)

def _pick_image(prod: Dict[str, Any]) -> Optional[str]:
    imgs = prod.get("images") or []
    if isinstance(imgs, list) and imgs:
        first = imgs[0]
        if isinstance(first, dict) and first.get("image"):
            return first["image"]
    return prod.get("image")

def _build_keywords(prod: Dict[str, Any]) -> List[str]:
    title = (prod.get("title") or "").lower()
    brand = (prod.get("brand") or "").lower()
    cats = [c.get("name", "").lower() for c in (prod.get("categories") or []) if c.get("name")]
    base = set()
    for token in re.split(r"[^\w]+", title):
        if token and len(token) > 1:
            base.add(token)
    if brand:
        base.add(brand)
        for token in title.split():
            if len(token) > 1:
                base.add(f"{brand} {token}")
    base.update([c for c in cats if c])
    return sorted(base)[:25]

def _promotion_data(prod: Dict[str, Any]) -> Dict[str, Any]:
    promos = prod.get("promotions") or []
    has = bool(promos)
    first = promos[0] if has else {}
    text = None
    dt_texts = first.get("durationTexts") or []
    if dt_texts:
        for k in ("shortTitle", "title", "description"):
            t = dt_texts[0].get(k)
            if t:
                text = t
                break
    qty = {
        "requiresMinimumQuantity": False,
        "minimumQuantity": None,
        "targetQuantity": None,
        "userInstruction": None,
        "actionRequired": False,
    }
    vols = first.get("volumeDiscounts") or []
    if vols:
        v0 = vols[0]
        qty.update(
            {
                "requiresMinimumQuantity": True,
                "minimumQuantity": v0.get("volume"),
                "targetQuantity": v0.get("volume"),
                "actionRequired": True,
            }
        )
    return {
        "hasPromotion": has,
        "text": text,
        "type": first.get("group"),
        "category": None,
        "savingsType": None,
        "quantityRequirements": qty,
        "isProcessed": True,
    }

def _calc_pricing_and_type(prod: Dict[str, Any]) -> Tuple[Dict[str, Any], Optional[str], Optional[str]]:
    price = (prod.get("price") or {})
    p = normalize_price(price.get("price", 0))
    pp = normalize_price(price.get("promoPrice", 0))
    has_discount = (pp > 0) and (pp < p)
    current = pp if has_discount else p
    original = p
    discount_pct: Optional[float] = None
    if has_discount and original > 0:
        discount_pct = round((original - current) / original * 100.0, 2)

    promos = prod.get("promotions") or []
    start, end = _first_promo_dates(promos)
    ptype = "NOT_IN_BONUS"
    today = datetime.now(timezone.utc).date()
    s_dt = datetime.fromisoformat(start).date() if start else None
    e_dt = datetime.fromisoformat(end).date() if end else None
    if s_dt and e_dt:
        if s_dt <= today <= e_dt:
            ptype = "BONUS"
        elif e_dt < today:
            ptype = "EXPIRED_BONUS"
    elif has_discount or promos:
        ptype = "BONUS" if has_discount else "NOT_IN_BONUS"

    return (
        {
            "current": float(current),
            "original": float(original),
            "has_discount": bool(has_discount),
            "discount_percentage": discount_pct,
            "product_type": ptype,
        },
        start,
        end,
    )

# -----------------------------
# Search pagination (page-by-page with logging)
# -----------------------------
def _fetch_search_page(client: JumboClient, off_set: int = 0, search_terms: str = SEARCH_TERMS) -> Optional[Dict[str, Any]]:
    variables = {
        "input": {
            "searchType": "category",
            "searchTerms": search_terms,
            "friendlyUrl": f"?offSet={off_set}",
            "offSet": off_set,
            "currentUrl": f"/{search_terms}/?offSet={off_set}",
            "previousUrl": "",
            "bloomreachCookieId": "uid=0000000000000:v=1.0:ts=0:hc=1",
        },
        "shelfTextInput": {"searchType": "category", "friendlyUrl": f"?offSet={off_set}"},
        "withFacetChildren": False,
    }
    payload = {"operationName": "SearchProducts", "variables": variables, "query": SEARCH_QUERY}

    logger.info("Jumbo search: fetching page", extra={"offset": off_set, "terms": search_terms})
    data = client.post(payload)
    if not data:
        logger.warning("Jumbo search: empty data", extra={"offset": off_set})
        return None

    sp = data.get("searchProducts") or {}
    products_len = len(sp.get("products") or [])
    total = (sp.get("pageHeader") or {}).get("count")
    logger.info(
        "Jumbo search: page fetched",
        extra={"offset": off_set, "products_on_page": products_len, "reported_total": total},
    )
    return data

def _iter_pages(client: JumboClient, start_offset: int = 0, max_pages: Optional[int] = None) -> Iterator[Tuple[int, List[Dict[str, Any]], Optional[int]]]:
    pages = 0
    off = start_offset
    total_reported = None
    while True:
        data = _fetch_search_page(client, off_set=off)
        if not data:
            break

        sp = data.get("searchProducts") or {}
        if total_reported is None:
            total_reported = (sp.get("pageHeader") or {}).get("count")
            logger.info("Jumbo search: reported total", extra={"total_reported": total_reported})

        products = sp.get("products") or []
        if not products:
            logger.info("Jumbo search: no products on page", extra={"offset": off})
            break

        yield off, products, total_reported

        pages += 1
        if (sp.get("count") or 0) == 0:
            break
        if isinstance(total_reported, int) and (off + PAGE_SIZE) >= total_reported:
            logger.info("Jumbo search: reached reported total", extra={"offset": off})
            break
        if max_pages is not None and pages >= max_pages:
            logger.info("Jumbo search: hit max_pages", extra={"pages": pages, "max_pages": max_pages})
            break

        off += PAGE_SIZE
        if DELAY_BETWEEN_PAGES:
            time.sleep(DELAY_BETWEEN_PAGES)

# -----------------------------
# Detail fetch
# -----------------------------
def _fetch_detail(client: JumboClient, sku: str) -> Optional[Dict[str, Any]]:
    payload = {"operationName": "productDetail", "variables": {"sku": sku}, "query": DETAIL_QUERY}
    logger.info("Jumbo detail: fetch queued", extra={"sku": sku})
    data = client.post(payload)
    if not data:
        logger.warning("Jumbo detail: empty data", extra={"sku": sku})
        return None
    prod = (
        data.get("product")
        or data.get("data", {}).get("product")
        or (data.get("productDetail") if "productDetail" in data else None)
    )
    if prod:
        logger.info("Jumbo detail: fetched", extra={"sku": sku, "title": prod.get("title")})
    else:
        logger.warning("Jumbo detail: no product in payload", extra={"sku": sku})
    return prod

# -----------------------------
# Normalizer → record
# -----------------------------
def _to_record(stub_or_detail: Dict[str, Any], detail: Optional[Dict[str, Any]], mapper: InternalCategoryMapper) -> Dict[str, Any]:
    ts = _ts()
    prod = detail or stub_or_detail

    title = prod.get("title") or (stub_or_detail.get("title") if stub_or_detail else "")
    desc = prod.get("description")
    image_url = _pick_image(prod)
    source_url = prod.get("canonicalUrl") or stub_or_detail.get("link")

    category_obj = _leaf_category(prod)
    category_name = category_obj.get("name")
    pricing, bonus_start, bonus_end = _calc_pricing_and_type(prod)
    internal_cat = mapper.map(SUPERMARKET["id"], category_name, None)

    return {
        "product_id": str(prod.get("id") or stub_or_detail.get("id") or ""),
        "name_full": title,
        "name_display": title,
        "description_full": desc,
        "description_display": desc,
        "image_url": image_url,
        "source_url": source_url,
        "keywords": _build_keywords(prod),
        "created_at": ts,
        "updated_at": ts,
        "last_scraped_at": ts,
        "parent_product_id": None,
        "child_products": [],
        "supermarket": dict(SUPERMARKET),
        "category": category_obj,
        "pricing": pricing,
        "promotion_data": _promotion_data(prod),
        "internal_category": internal_cat,
        "bonus_period_start": bonus_start,
        "bonus_period_end": bonus_end,
    }

# -----------------------------
# Entrypoint (stream per page)
# -----------------------------
@timed
def scrape_jumbo_once() -> int:
    """
    For each search page:
      - queue detail fetch for its products (ThreadPoolExecutor)
      - as each detail returns, normalize and upsert immediately
    """
    client = JumboClient()
    mapper = InternalCategoryMapper()
    fetch_details = getattr(settings, "JUMBO_FETCH_DETAILS", True)
    workers = max(1, int(getattr(settings, "JUMBO_WORKERS", 12)))
    max_pages = getattr(settings, "JUMBO_MAX_PAGES", None)

    total = 0
    logger.info(
        "Jumbo scrape: start",
        extra={"fetch_details": fetch_details, "workers": workers, "max_pages": max_pages},
    )

    for offset, stubs, total_reported in _iter_pages(client, start_offset=0, max_pages=max_pages):
        logger.info("Jumbo page: begin processing", extra={"offset": offset, "page_size": len(stubs)})

        if fetch_details:
            # Per-page pool so we stream this page fully before moving on
            stubs_by_sku = {s.get("id"): s for s in stubs if s.get("id")}
            with ThreadPoolExecutor(max_workers=workers) as ex:
                futs = {ex.submit(_fetch_detail, client, sku): sku for sku in stubs_by_sku.keys()}
                for fut in as_completed(futs):
                    sku = futs[fut]
                    stub = stubs_by_sku.get(sku, {})
                    try:
                        detail = fut.result()
                        rec = _to_record(stub, detail, mapper)
                        slug = _slugify(rec.get("category", {}).get("name"))
                        ProductRepository.upsert_flat(slug, [rec])  # immediate write
                        total += 1
                        logger.info(
                            "Jumbo upsert: saved",
                            extra={
                                "page_offset": offset,
                                "sku": rec.get("product_id"),
                                "title": rec.get("name_display"),
                                "category_slug": slug,
                                "has_discount": rec.get("pricing", {}).get("has_discount"),
                                "product_type": rec.get("pricing", {}).get("product_type"),
                            },
                        )
                    except Exception as e:
                        logger.exception("Jumbo upsert: failed", extra={"page_offset": offset, "sku": sku, "err": str(e)})
                    if DELAY_BETWEEN_DETAILS:
                        time.sleep(DELAY_BETWEEN_DETAILS)
        else:
            # No details—upsert stubs immediately
            for stub in stubs:
                try:
                    rec = _to_record(stub, None, mapper)
                    slug = _slugify(rec.get("category", {}).get("name"))
                    ProductRepository.upsert_flat(slug, [rec])
                    total += 1
                    logger.info(
                        "Jumbo upsert (no-detail): saved",
                        extra={
                            "page_offset": offset,
                            "sku": rec.get("product_id"),
                            "title": rec.get("name_display"),
                            "category_slug": slug,
                            "has_discount": rec.get("pricing", {}).get("has_discount"),
                            "product_type": rec.get("pricing", {}).get("product_type"),
                        },
                    )
                except Exception as e:
                    logger.exception(
                        "Jumbo upsert (no-detail): failed",
                        extra={"page_offset": offset, "sku": stub.get("id"), "err": str(e)},
                    )

        logger.info("Jumbo page: done", extra={"offset": offset, "saved_so_far": total})

    logger.info("Jumbo scrape complete", extra={"products_saved": total})
    return total
