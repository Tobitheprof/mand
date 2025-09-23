from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy import String, Integer, Boolean, Numeric, JSON, DateTime, UniqueConstraint, Index, Text
from datetime import datetime, timezone

class Base(DeclarativeBase):
    pass

# Optional: keep raw capture if you still want it
class ProductRaw(Base):
    __tablename__ = "products_raw"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    supermarket: Mapped[str] = mapped_column(String(32), index=True)
    category_slug: Mapped[str] = mapped_column(String(128), index=True)
    payload: Mapped[dict] = mapped_column(JSON, nullable=False)
    scraped_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), index=True
    )

# ✅ Flat, column-per-key representation
class ProductFlat(Base):
    __tablename__ = "products_flat"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    # KV envelope info (key from "fruit-verse-sappen": [...])
    category_slug: Mapped[str] = mapped_column(String(128), index=True)

    # Core product
    product_id: Mapped[str] = mapped_column(String(64))
    name_full: Mapped[str] = mapped_column(String(512))
    name_display: Mapped[str] = mapped_column(String(512))
    description_full: Mapped[str | None] = mapped_column(Text, nullable=True)
    description_display: Mapped[str | None] = mapped_column(Text, nullable=True)
    image_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    source_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    keywords: Mapped[list] = mapped_column(JSON)  # list of strings
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    last_scraped_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    parent_product_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    child_products: Mapped[list] = mapped_column(JSON)  # store child product dicts as JSON if any

    # Supermarket
    supermarket_id: Mapped[str] = mapped_column(String(32), index=True)
    supermarket_name: Mapped[str] = mapped_column(String(128))
    supermarket_logo: Mapped[str | None] = mapped_column(Text, nullable=True)
    supermarket_abbreviation: Mapped[str | None] = mapped_column(String(16), nullable=True)
    supermarket_brand_color: Mapped[str | None] = mapped_column(String(16), nullable=True)

    # Category (original from supermarket)
    category_id: Mapped[str | None] = mapped_column(String(64), index=True, nullable=True)
    category_name: Mapped[str | None] = mapped_column(String(128), nullable=True)
    category_description: Mapped[str | None] = mapped_column(Text, nullable=True)
    category_logo: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Pricing
    pricing_current: Mapped[Numeric] = mapped_column(Numeric(18, 2))
    pricing_original: Mapped[Numeric] = mapped_column(Numeric(18, 2))
    pricing_has_discount: Mapped[bool] = mapped_column(Boolean, default=False)
    pricing_discount_percentage: Mapped[Numeric | None] = mapped_column(Numeric(8, 2), nullable=True)
    pricing_product_type: Mapped[str] = mapped_column(String(32))  # BONUS / NOT_IN_BONUS / EXPIRED_BONUS (future)

    # Promotion data (flattened)
    promo_has_promotion: Mapped[bool] = mapped_column(Boolean, default=False)
    promo_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    promo_type: Mapped[str | None] = mapped_column(String(64), nullable=True)
    promo_category: Mapped[str | None] = mapped_column(String(64), nullable=True)
    promo_savings_type: Mapped[str | None] = mapped_column(String(64), nullable=True)
    promo_qty_requires_min: Mapped[bool] = mapped_column(Boolean, default=False)
    promo_qty_min: Mapped[int | None] = mapped_column(Integer, nullable=True)
    promo_qty_target: Mapped[int | None] = mapped_column(Integer, nullable=True)
    promo_qty_instruction: Mapped[str | None] = mapped_column(Text, nullable=True)
    promo_qty_action_required: Mapped[bool] = mapped_column(Boolean, default=False)
    promo_is_processed: Mapped[bool] = mapped_column(Boolean, default=True)

    # Internal category
    internal_category_id: Mapped[int] = mapped_column(Integer, index=True)
    internal_category_name: Mapped[str] = mapped_column(String(64))

    # Uniqueness per supermarket
    __table_args__ = (
        UniqueConstraint("product_id", "supermarket_id", name="uq_flat_product_supermarket"),
        Index("ix_flat_supermarket_category", "supermarket_id", "category_slug"),
    )
