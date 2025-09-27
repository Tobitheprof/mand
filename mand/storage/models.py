# models.py
from __future__ import annotations
from datetime import datetime, timezone
from typing import List, Optional

from sqlalchemy import (
    String, Integer, Boolean, Numeric, JSON, DateTime, Text,
    UniqueConstraint, Index, ForeignKey
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


# ========== Reference Tables ==========

class Supermarket(Base):
    __tablename__ = "supermarkets"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    # business id from scraper (e.g., "ah", "jumbo", "dirk")
    code: Mapped[str] = mapped_column(String(32), unique=True, index=True)  # "ah", "jumbo"
    name: Mapped[str] = mapped_column(String(128))
    logo: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    abbreviation: Mapped[Optional[str]] = mapped_column(String(16), nullable=True)
    brand_color: Mapped[Optional[str]] = mapped_column(String(16), nullable=True)

    products: Mapped[List["Product"]] = relationship(back_populates="supermarket", cascade="all,delete-orphan")


class InternalCategory(Base):
    __tablename__ = "internal_categories"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    # global internal taxonomy (your mapper’s output)
    name: Mapped[str] = mapped_column(String(128), unique=True, index=True)

    products: Mapped[List["Product"]] = relationship(back_populates="internal_category", cascade="all,delete-orphan")


class StoreCategory(Base):
    """
    The supermarket's own native category (taxonomy leaf you store today).
    Names are unique per supermarket, not globally.
    """
    __tablename__ = "store_categories"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    supermarket_id: Mapped[int] = mapped_column(ForeignKey("supermarkets.id", ondelete="CASCADE"), index=True)
    code: Mapped[Optional[str]] = mapped_column(String(64), nullable=True, index=True)  # native id if available
    name: Mapped[str] = mapped_column(String(128), index=True)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    logo: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    supermarket: Mapped["Supermarket"] = relationship()
    products: Mapped[List["Product"]] = relationship(back_populates="store_category")

    __table_args__ = (
        UniqueConstraint("supermarket_id", "name", name="uq_store_category_supermarket_name"),
    )


# ========== Optional raw capture (unchanged) ==========

class ProductRaw(Base):
    __tablename__ = "products_raw"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    supermarket: Mapped[str] = mapped_column(String(32), index=True)
    category_slug: Mapped[str] = mapped_column(String(128), index=True)
    payload: Mapped[dict] = mapped_column(JSON, nullable=False)
    scraped_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), index=True
    )


# ========== Products (replaces ProductFlat) ==========

class Product(Base):
    __tablename__ = "products"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    # business key
    product_id: Mapped[str] = mapped_column(String(64))  # store's id (e.g., SKU)
    supermarket_id: Mapped[int] = mapped_column(ForeignKey("supermarkets.id", ondelete="CASCADE"))
    # still useful for fast filters
    category_slug: Mapped[str] = mapped_column(String(128), index=True)

    # text/core
    name_full: Mapped[str] = mapped_column(String(512))
    name_display: Mapped[str] = mapped_column(String(512))
    description_full: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    description_display: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    image_url: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    source_url: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    keywords: Mapped[list] = mapped_column(JSON)  # list[str]
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    last_scraped_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    parent_product_id: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    child_products: Mapped[list] = mapped_column(JSON)  # JSON array

    # FK relations
    internal_category_id: Mapped[int] = mapped_column(ForeignKey("internal_categories.id", ondelete="SET NULL"), index=True)
    store_category_id: Mapped[int] = mapped_column(ForeignKey("store_categories.id", ondelete="SET NULL"), index=True)

    supermarket: Mapped["Supermarket"] = relationship(back_populates="products")
    internal_category: Mapped["InternalCategory"] = relationship(back_populates="products")
    store_category: Mapped["StoreCategory"] = relationship(back_populates="products")

    # pricing snapshot
    pricing_current: Mapped[Numeric] = mapped_column(Numeric(18, 2))
    pricing_original: Mapped[Numeric] = mapped_column(Numeric(18, 2))
    pricing_has_discount: Mapped[bool] = mapped_column(Boolean, default=False)
    pricing_discount_percentage: Mapped[Optional[Numeric]] = mapped_column(Numeric(8, 2), nullable=True)
    pricing_product_type: Mapped[str] = mapped_column(String(32))  # BONUS / NOT_IN_BONUS / EXPIRED_BONUS

    # promo snapshot (flattened)
    promo_has_promotion: Mapped[bool] = mapped_column(Boolean, default=False)
    promo_text: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    promo_type: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    promo_category: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    promo_savings_type: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    promo_qty_requires_min: Mapped[bool] = mapped_column(Boolean, default=False)
    promo_qty_min: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    promo_qty_target: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    promo_qty_instruction: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    promo_qty_action_required: Mapped[bool] = mapped_column(Boolean, default=False)
    promo_is_processed: Mapped[bool] = mapped_column(Boolean, default=True)

    # helpful uniqueness & indexes
    __table_args__ = (
        UniqueConstraint("product_id", "supermarket_id", name="uq_product_business_key"),
        Index("ix_products_supermarket_category", "supermarket_id", "category_slug"),
    )


class ProductPriceHistory(Base):
    """
    Immutable price history snapshots. One row whenever a product's pricing
    *meaningfully changes* (or on first sighting).
    """
    __tablename__ = "product_price_history"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    # FK to products for easy joins
    product_db_id: Mapped[int] = mapped_column(
        ForeignKey("products.id", ondelete="CASCADE"), index=True
    )
    supermarket_id: Mapped[int] = mapped_column(Integer, index=True)
    # Store the business key redundantly for convenience/fast filters
    product_business_id: Mapped[str] = mapped_column(String(64), index=True)

    pricing_current: Mapped[Numeric] = mapped_column(Numeric(18, 2))
    pricing_original: Mapped[Numeric] = mapped_column(Numeric(18, 2))
    pricing_has_discount: Mapped[bool] = mapped_column(Boolean, default=False)
    pricing_discount_percentage: Mapped[Optional[Numeric]] = mapped_column(Numeric(8, 2), nullable=True)
    pricing_product_type: Mapped[str] = mapped_column(String(32))  # BONUS / NOT_IN_BONUS / EXPIRED_BONUS

    # when this snapshot became effective (use scraper timestamp if available)
    effective_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        index=True
    )

    __table_args__ = (
        Index("ix_pricehist_product_time", "product_db_id", "effective_at"),
        Index("ix_pricehist_supermarket", "supermarket_id", "product_business_id"),
    )
