from __future__ import annotations

import logging
from typing import Any, Dict, Mapping, Optional, Sequence

from mand.config.settings import settings
from mand.normalization.llm_category import LLMCategoryAssigner

logger = logging.getLogger(__name__)


INTERNAL_CATEGORY_NAMES: Sequence[str] = (
    "Diepvries",
    "Baby & Kind",
    "Huisdier",
    "Alcohol",
    "Koffie & Thee",
    "Dranken",
    "Brood & Bakkerij",
    "Zuivel",
    "Vleeswaren & Kaas",
    "Vlees & Vis",
    "Vegan & Vegetarisch",
    "Maaltijden",
    "Wereldkeuken",
    "Snacks & Snoep",
    "Verzorging",
    "Huishoudelijk",
    "Groente & Fruit",
    "Overig",
)


class InternalCategoryMapper:


    def __init__(self, store_rules: Optional[Mapping[str, Mapping[str, str]]] = None) -> None:

        self.store_rules: Mapping[str, Mapping[str, str]] = store_rules or {}
        self._llm = LLMCategoryAssigner()  

        self._validate_rule_targets()


    def map_product(self, supermarket: str, product: Dict[str, Any]) -> Optional[str]:

        cat = self._rule_based(supermarket, product)
        if cat:
            return cat

        if settings.LLM_CATEGORY_ENABLED:
            title, description = _extract_title_and_description(product)
            if title or description:
                llm_cat = self._llm.classify(
                    supermarket=supermarket,
                    title=title,
                    description=description,
                    categories=list(INTERNAL_CATEGORY_NAMES),
                )
                if llm_cat:
                    return llm_cat

        return None

    def _rule_based(self, supermarket: str, product: Dict[str, Any]) -> Optional[str]:
        if not supermarket:
            return None

        store_map = self.store_rules.get(supermarket)
        if not store_map:
            return None

        raw_keys = (
            "raw_category_path",
            "raw_category_id",
            "raw_category_name",
            "category_path",
            "category_id",
            "category_name",
        )

        for k in raw_keys:
            raw_val = product.get(k)
            if not raw_val:
                continue

            cat = store_map.get(str(raw_val))
            if cat:
                if cat in INTERNAL_CATEGORY_NAMES:
                    return cat
                logger.warning("Rule mapped to non-canonical category %r for %s", cat, supermarket)
                return None

            if isinstance(raw_val, (list, tuple)) and raw_val:
                last = str(raw_val[-1])
                cat = store_map.get(last)
                if cat:
                    if cat in INTERNAL_CATEGORY_NAMES:
                        return cat
                    logger.warning("Rule mapped to non-canonical category %r for %s", cat, supermarket)
                    return None

        return None

    def _validate_rule_targets(self) -> None:
        if not self.store_rules:
            return
        ok = set(INTERNAL_CATEGORY_NAMES)
        bad: int = 0
        for sm, mapping in self.store_rules.items():
            for raw, target in mapping.items():
                if target not in ok:
                    bad += 1
                    logger.warning(
                        "Store rule for %s -> %r maps to non-canonical category %r",
                        sm, raw, target
                    )
        if bad:
            logger.info("Found %d non-canonical rule targets; see warnings above.", bad)

    
    def map(self, supermarket: str, raw_category: Optional[str], product: Optional[Dict[str, Any]] = None) -> Optional[str]:
        """
        Compat wrapper so existing scrapers don't break.
        - supermarket: store id like "ah", "jumbo", "dirk"
        - raw_category: the category string the scraper extracted
        - product: optional product dict (if the caller already has one)
        """
        merged: Dict[str, Any] = dict(product or {})
        if raw_category:
            # make it discoverable by the rule-based mapper
            merged.setdefault("raw_category_name", raw_category)
            merged.setdefault("category_name", raw_category)
        return self.map_product(supermarket, merged)


def _extract_title_and_description(product: Dict[str, Any]) -> tuple[str, str]:
    title = (
        product.get("name")
        or product.get("title")
        or product.get("name_display")
        or product.get("name_full")
        or ""
    )

    description = (
        product.get("description")
        or product.get("summary")
        or product.get("description_display")
        or product.get("description_full")
        or ""
    )

    return str(title).strip(), str(description).strip()


