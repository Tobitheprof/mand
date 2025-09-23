from typing import List, Dict, Any
from sqlalchemy import select
from mand.storage.db import SessionLocal, engine
from mand.storage.models import Base, ProductRaw, ProductFlat

# create tables (or do via Alembic)
Base.metadata.create_all(bind=engine)

class ProductRepository:
    # optional raw capture (you can remove if you prefer)
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
        """
        Persist each product dict (already matching your nested JSON spec)
        as a single flat row with explicit columns.
        """
        if not products:
            return

        with SessionLocal() as s, s.begin():
            for p in products:
                # Pull nested sub-objects safely
                sup = p.get("supermarket") or {}
                cat = p.get("category") or {}
                pricing = p.get("pricing") or {}
                promo = p.get("promotion_data") or {}
                qty = (promo.get("quantityRequirements") or {})

                existing = s.execute(
                    select(ProductFlat).where(
                        ProductFlat.product_id == (p.get("product_id") or ""),
                        ProductFlat.supermarket_id == (sup.get("id") or "")
                    )
                ).scalar_one_or_none()

                values = dict(
                    category_slug=category_slug,

                    product_id=p.get("product_id") or "",
                    name_full=p.get("name_full") or "",
                    name_display=p.get("name_display") or "",
                    description_full=p.get("description_full"),
                    description_display=p.get("description_display"),
                    image_url=p.get("image_url"),
                    source_url=p.get("source_url"),
                    keywords=p.get("keywords") or [],
                    created_at=p.get("created_at"),
                    updated_at=p.get("updated_at"),
                    last_scraped_at=p.get("last_scraped_at"),
                    parent_product_id=p.get("parent_product_id"),
                    child_products=p.get("child_products") or [],

                    supermarket_id=sup.get("id") or "",
                    supermarket_name=sup.get("name") or "",
                    supermarket_logo=sup.get("logo"),
                    supermarket_abbreviation=sup.get("abbreviation"),
                    supermarket_brand_color=sup.get("brand_color"),

                    category_id=(cat.get("id") if cat else None),
                    category_name=(cat.get("name") if cat else None),
                    category_description=(cat.get("description") if cat else None),
                    category_logo=(cat.get("logo") if cat else None),

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

                    internal_category_id=(p.get("internal_category") or {}).get("id", 19),
                    internal_category_name=(p.get("internal_category") or {}).get("name", "Overig"),
                )

                if existing:
                    for k, v in values.items():
                        setattr(existing, k, v)
                else:
                    s.add(ProductFlat(**values))
