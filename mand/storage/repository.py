# repository.py
from __future__ import annotations
from typing import List, Dict, Any, Optional
from decimal import Decimal

from datetime import datetime, timezone

from sqlalchemy import select, desc
from sqlalchemy.orm import Session

from mand.storage.db import SessionLocal, engine
from mand.storage.models import Base, ProductRaw, Product, Supermarket, InternalCategory, StoreCategory, ProductPriceHistory
from mand.normalization.sanitize import clean_product_record

# create tables (or run via Alembic in real env)
Base.metadata.create_all(bind=engine)


def _as_dec2(x) -> Decimal:
    # defensively coerce to Decimal(18,2)-compatible values
    if x in (None, "", "null"):
        return Decimal("0.00")
    if isinstance(x, Decimal):
        return x.quantize(Decimal("0.01"))
    try:
        return Decimal(str(x)).quantize(Decimal("0.01"))
    except Exception:
        return Decimal("0.00")


def _price_tuple_from_values(values: dict):
    return (
        _as_dec2(values.get("pricing_current")),
        _as_dec2(values.get("pricing_original")),
        bool(values.get("pricing_has_discount", False)),
        _as_dec2(values.get("pricing_discount_percentage")) if values.get("pricing_discount_percentage") is not None else None,
        (values.get("pricing_product_type") or "NOT_IN_BONUS"),
    )


def _maybe_log_price_history(s: Session, product: Product, values: dict, effective_at: datetime | None):
    """
    Insert a ProductPriceHistory row only if any 'meaningful' price field changed.
    """
    new_tuple = _price_tuple_from_values(values)

    # Fetch last history row (if any)
    last = s.execute(
        select(ProductPriceHistory)
        .where(ProductPriceHistory.product_db_id == product.id)
        .order_by(desc(ProductPriceHistory.effective_at))
        .limit(1)
    ).scalar_one_or_none()

    if last:
        last_tuple = (
            _as_dec2(last.pricing_current),
            _as_dec2(last.pricing_original),
            bool(last.pricing_has_discount),
            _as_dec2(last.pricing_discount_percentage) if last.pricing_discount_percentage is not None else None,
            last.pricing_product_type or "NOT_IN_BONUS",
        )
        # If nothing changed, do nothing
        if last_tuple == new_tuple:
            return

    # Insert snapshot (first sighting OR changed)
    s.add(ProductPriceHistory(
        product_db_id=product.id,
        supermarket_id=product.supermarket_id,
        product_business_id=product.product_id,

        pricing_current=new_tuple[0],
        pricing_original=new_tuple[1],
        pricing_has_discount=new_tuple[2],
        pricing_discount_percentage=new_tuple[3],
        pricing_product_type=new_tuple[4],

        effective_at=effective_at or datetime.now(timezone.utc),
    ))


class _Cache:
    """Simple in-memory cache to reduce DB hits per run."""
    supermarkets: dict[str, int] = {}
    internal_categories: dict[str, int] = {}
    store_categories: dict[tuple[int, str], int] = {}  # (supermarket_id, name) -> id
    store_categories_by_code: dict[tuple[int, str], int] = {}  # (supermarket_id, code) -> id


def _get_or_create_supermarket(s: Session, sup_dict: Dict[str, Any]) -> int:
    code = (sup_dict.get("id") or sup_dict.get("code") or "").strip()
    if not code:
        raise ValueError("supermarket.code missing")

    cached = _Cache.supermarkets.get(code)
    if cached:
        return cached

    row = s.execute(select(Supermarket).where(Supermarket.code == code)).scalar_one_or_none()
    if row:
        _Cache.supermarkets[code] = row.id
        return row.id

    row = Supermarket(
        code=code,
        name=sup_dict.get("name") or code,
        logo=sup_dict.get("logo"),
        abbreviation=sup_dict.get("abbreviation"),
        brand_color=sup_dict.get("brand_color"),
    )
    s.add(row)
    s.flush()
    _Cache.supermarkets[code] = row.id
    return row.id


def _get_or_create_internal_category(s: Session, name: Optional[str]) -> int:
    key = (name or "Overig").strip()
    cached = _Cache.internal_categories.get(key)
    if cached:
        return cached

    row = s.execute(select(InternalCategory).where(InternalCategory.name == key)).scalar_one_or_none()
    if row:
        _Cache.internal_categories[key] = row.id
        return row.id

    row = InternalCategory(name=key)
    s.add(row)
    s.flush()
    _Cache.internal_categories[key] = row.id
    return row.id


def _get_or_create_store_category(s: Session, supermarket_id: int, name: Optional[str], code: Optional[str], description: Optional[str], logo: Optional[str]) -> int:
    nm = (name or "Uncategorized").strip()

    # prefer exact code match when present
    if code:
        ck = (supermarket_id, code)
        cached = _Cache.store_categories_by_code.get(ck)
        if cached:
            return cached

        row = s.execute(
            select(StoreCategory).where(
                StoreCategory.supermarket_id == supermarket_id,
                StoreCategory.code == code
            )
        ).scalar_one_or_none()
        if row:
            _Cache.store_categories_by_code[ck] = row.id
            return row.id

    # fallback to name uniqueness within supermarket
    k = (supermarket_id, nm)
    cached2 = _Cache.store_categories.get(k)
    if cached2:
        return cached2

    row = s.execute(
        select(StoreCategory).where(
            StoreCategory.supermarket_id == supermarket_id,
            StoreCategory.name == nm
        )
    ).scalar_one_or_none()
    if row:
        _Cache.store_categories[k] = row.id
        if code:
            _Cache.store_categories_by_code[(supermarket_id, code)] = row.id
        return row.id

    row = StoreCategory(
        supermarket_id=supermarket_id,
        code=code,
        name=nm,
        description=description,
        logo=logo,
    )
    s.add(row)
    s.flush()
    _Cache.store_categories[k] = row.id
    if code:
        _Cache.store_categories_by_code[(supermarket_id, code)] = row.id
    return row.id


class ProductRepository:
    @staticmethod
    def save_raw(supermarket_id: str, category_slug: str, products: List[Dict[str, Any]]):
        if not products:
            return
        with SessionLocal() as s, s.begin():
            s.add_all([
                ProductRaw(supermarket=supermarket_id, category_slug=category_slug, payload=p)
                for p in products
            ])

    @staticmethod
    def upsert_flat(category_slug: str, products: List[Dict[str, Any]]):
        if not products:
            return

        with SessionLocal() as s, s.begin():
            for p in products:
                # 1) CLEAN
                cp = clean_product_record(p, category_slug=category_slug)
                cat_slug = cp.pop("_clean_category_slug", category_slug)

                # 2) Resolve FKs using cleaned dict
                sup = cp.get("supermarket") or {}
                cat = cp.get("category") or {}
                pricing = cp.get("pricing") or {}
                promo = cp.get("promotion_data") or {}
                qty = (promo.get("quantityRequirements") or {})

                supermarket_db_id = _get_or_create_supermarket(s, sup)
                internal_cat_name = (cp.get("internal_category") or {}).get("name")
                internal_cat_id = _get_or_create_internal_category(s, internal_cat_name)
                store_cat_id = _get_or_create_store_category(
                    s,
                    supermarket_db_id,
                    name=cat.get("name"),
                    code=(cat.get("id") if cat else None),
                    description=cat.get("description"),
                    logo=cat.get("logo"),
                )

                # 3) Upsert product
                existing = s.execute(
                    select(Product).where(
                        Product.product_id == (cp.get("product_id") or ""),
                        Product.supermarket_id == supermarket_db_id
                    )
                ).scalar_one_or_none()

                values = dict(
                    product_id=cp.get("product_id") or "",
                    supermarket_id=supermarket_db_id,
                    category_slug=cat_slug,

                    name_full=cp.get("name_full") or "",
                    name_display=cp.get("name_display") or "",
                    description_full=cp.get("description_full"),
                    description_display=cp.get("description_display"),
                    image_url=cp.get("image_url"),
                    source_url=cp.get("source_url"),
                    keywords=cp.get("keywords") or [],
                    created_at=cp.get("created_at"),
                    updated_at=cp.get("updated_at"),
                    last_scraped_at=cp.get("last_scraped_at"),
                    parent_product_id=cp.get("parent_product_id"),
                    child_products=cp.get("child_products") or [],

                    internal_category_id=internal_cat_id,
                    store_category_id=store_cat_id,

                    pricing_current=pricing.get("current", 0.00),
                    pricing_original=pricing.get("original", 0.00),
                    pricing_has_discount=pricing.get("has_discount", False),
                    pricing_discount_percentage=pricing.get("discount_percentage"),
                    pricing_product_type=pricing.get("product_type", "NOT_IN_BONUS"),
                    
                    
                    promo_has_promotion=promo.get("hasPromotion", False),
                    promo_text=promo.get("text"),
                    promo_type=promo.get("type"),
                    promo_category=promo.get("category"),
                    promo_savings_type=promo.get("savingsType"),
                    promo_qty_requires_min=qty.get("requiresMinimumQuantity", False),
                    promo_qty_min=qty.get("minimumQuantity"),
                    promo_qty_target=qty.get("targetQuantity"),
                    promo_qty_instruction=qty.get("userInstruction"),
                    promo_qty_action_required=qty.get("actionRequired", False),
                    promo_is_processed=promo.get("isProcessed", True),
                )

                if existing:
                    for k, v in values.items():
                        setattr(existing, k, v)
                    
                    # [PRICE TRACKING] compare with last history & log if changed
                    _maybe_log_price_history(
                        s,
                        existing,
                        values,
                        effective_at=cp.get("last_scraped_at") or values.get("last_scraped_at")
                    )
                else:
                    new_product = Product(**values)
                    s.add(new_product)
                    s.flush() 

                    # [PRICE TRACKING] first sighting -> create initial history row
                    _maybe_log_price_history(
                        s,
                        new_product,
                        values,
                        effective_at=cp.get("last_scraped_at") or values.get("last_scraped_at")
                    )