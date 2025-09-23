from __future__ import annotations
import html
import math
import re
import unicodedata
from copy import deepcopy
from typing import Any, Dict, Iterable, List, Optional

# -----------------------------
# Low-level helpers
# -----------------------------
_ZW_CHARS = [
    "\u200b",  # ZERO WIDTH SPACE
    "\u200c",  # ZERO WIDTH NON-JOINER
    "\u200d",  # ZERO WIDTH JOINER
    "\ufeff",  # ZERO WIDTH NO-BREAK SPACE / BOM
]

_ALLOWED_URL_RE = re.compile(r"^(https?://)[^\s]+$", re.IGNORECASE)
_HEX_COLOR_RE = re.compile(r"^#?[0-9a-fA-F]{6}$")
_WS_COLLAPSE_RE = re.compile(r"[ \t\u00A0]{2,}")

_HTML_TAG_RE = re.compile(r"<[^>]+>")


def _nfkc(s: str) -> str:
    return unicodedata.normalize("NFKC", s)


def _strip_control(s: str, keep: Iterable[str] = ("\n", "\t", " ")) -> str:
    keep_set = set(keep)
    return "".join(
        ch for ch in s
        if (ch in keep_set) or (unicodedata.category(ch)[0] != "C")
    )


def _strip_zero_width(s: str) -> str:
    for z in _ZW_CHARS:
        s = s.replace(z, "")
    return s


def _collapse_ws(s: str) -> str:
    s = _WS_COLLAPSE_RE.sub(" ", s)
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s.strip(" \t")


def _truncate(s: Optional[str], max_len: int) -> Optional[str]:
    if s is None:
        return None
    return s[:max_len]


def _safe_text(
    s: Optional[str],
    *,
    max_len: int,
    allow_html: bool = False,
    strip_html: bool = True,
) -> Optional[str]:
    if s is None:
        return None
    s = str(s)
    s = html.unescape(s)
    s = _nfkc(s)
    s = _strip_zero_width(s)
    s = _strip_control(s)
    if strip_html and not allow_html:
        s = _HTML_TAG_RE.sub("", s)
    s = _collapse_ws(s)
    return _truncate(s, max_len)


def _safe_slug(s: Optional[str], *, max_len: int = 128) -> str:
    if not s:
        return "uncategorized"
    s = _nfkc(s)
    s = _strip_zero_width(s)
    s = _strip_control(s)
    s = s.lower()
    s = re.sub(r"[^\w\s-]", "", s)
    s = re.sub(r"\s+", "-", s)
    s = re.sub(r"-{2,}", "-", s).strip("-")
    s = s or "uncategorized"
    return s[:max_len]


def _safe_url(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    s = s.strip()
    if _ALLOWED_URL_RE.match(s):
        return s
    return None


def _safe_hex_color(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    s = s.strip()
    if _HEX_COLOR_RE.match(s):
        return s if s.startswith("#") else f"#{s}"
    return None


def _safe_bool(v: Any, default: bool = False) -> bool:
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return bool(v)
    if isinstance(v, str):
        return v.strip().lower() in {"1", "true", "yes", "y", "t"}
    return default


def _safe_num(v: Any, *, dp: int = 2, min_val: Optional[float] = None, max_val: Optional[float] = None) -> float:
    try:
        f = float(v)
        if not math.isfinite(f):
            return 0.0
    except Exception:
        return 0.0
    if min_val is not None and f < min_val:
        f = min_val
    if max_val is not None and f > max_val:
        f = max_val
    q = 10 ** dp
    return math.floor(f * q + 0.5) / q


def _safe_int(v: Any, *, min_val: Optional[int] = None, max_val: Optional[int] = None) -> Optional[int]:
    try:
        i = int(v)
    except Exception:
        return None
    if min_val is not None and i < min_val:
        i = min_val
    if max_val is not None and i > max_val:
        i = max_val
    return i


def _safe_keywords(xs: Any, *, limit: int = 25) -> List[str]:
    out: List[str] = []
    if isinstance(xs, list):
        for x in xs:
            if x is None:
                continue
            t = str(x)
            t = _nfkc(t)
            t = _strip_zero_width(t)
            t = _strip_control(t)
            t = t.lower().strip()
            if not t:
                continue
            # allow words, spaces, hyphens; drop anything too weird
            t = re.sub(r"[^a-z0-9\s-]", "", t)
            t = re.sub(r"\s{2,}", " ", t).strip()
            if t and t not in out:
                out.append(t)
            if len(out) >= limit:
                break
    return out


# -----------------------------
# Public: clean product record
# -----------------------------
def clean_product_record(rec: Dict[str, Any], *, category_slug: Optional[str] = None) -> Dict[str, Any]:
    """
    Returns a cleaned shallow copy of `rec`.
    Only normalizes/sanitizes; does NOT invent missing business keys.
    """
    r = deepcopy(rec)

    # --- core ids/titles/descriptions ---
    r["product_id"] = _safe_text(str(r.get("product_id", "")), max_len=64) or ""
    r["name_full"] = _safe_text(r.get("name_full"), max_len=512) or ""
    r["name_display"] = _safe_text(r.get("name_display"), max_len=512) or r["name_full"]
    r["description_full"] = _safe_text(r.get("description_full"), max_len=20000, allow_html=False)
    r["description_display"] = _safe_text(r.get("description_display"), max_len=20000, allow_html=False)
    r["image_url"] = _safe_url(r.get("image_url"))
    r["source_url"] = _safe_url(r.get("source_url"))
    r["keywords"] = _safe_keywords(r.get("keywords"))

    # timestamps are assumed ISO strings already by scrapers; leave as-is if present

    # parent/children
    r["parent_product_id"] = _safe_text(r.get("parent_product_id"), max_len=64)
    if not isinstance(r.get("child_products"), list):
        r["child_products"] = []

    # --- supermarket block ---
    sup = dict(r.get("supermarket") or {})
    sup["id"] = _safe_text(sup.get("id"), max_len=32) or ""
    sup["name"] = _safe_text(sup.get("name"), max_len=128) or sup["id"]
    sup["logo"] = _safe_url(sup.get("logo"))
    sup["abbreviation"] = _safe_text(sup.get("abbreviation"), max_len=16)
    sup["brand_color"] = _safe_hex_color(sup.get("brand_color"))
    r["supermarket"] = sup

    # --- category block (store's own) ---
    cat = dict(r.get("category") or {})
    cat["id"] = _safe_text(cat.get("id"), max_len=64)
    cat["name"] = _safe_text(cat.get("name"), max_len=128)
    cat["description"] = _safe_text(cat.get("description"), max_len=10000)
    cat["logo"] = _safe_url(cat.get("logo"))
    r["category"] = cat

    # --- internal category block ---
    ic = dict(r.get("internal_category") or {})
    ic["id"] = ic.get("id")  # keep numeric if already mapped, else None
    ic["name"] = _safe_text(ic.get("name"), max_len=128) or "Overig"
    r["internal_category"] = ic

    # --- pricing ---
    pricing = dict(r.get("pricing") or {})
    pricing["current"] = _safe_num(pricing.get("current"), dp=2, min_val=0)
    pricing["original"] = _safe_num(pricing.get("original"), dp=2, min_val=0)
    pricing["has_discount"] = _safe_bool(pricing.get("has_discount"), False)
    dpct = pricing.get("discount_percentage")
    pricing["discount_percentage"] = None if dpct in (None, "", "null") else _safe_num(dpct, dp=2, min_val=0, max_val=100)
    ppt = str(pricing.get("product_type") or "NOT_IN_BONUS").upper()
    if ppt not in {"BONUS", "NOT_IN_BONUS", "EXPIRED_BONUS"}:
        ppt = "NOT_IN_BONUS"
    pricing["product_type"] = ppt
    r["pricing"] = pricing

    # --- promotion_data ---
    promo = dict(r.get("promotion_data") or {})
    promo["hasPromotion"] = _safe_bool(promo.get("hasPromotion"), False)
    promo["text"] = _safe_text(promo.get("text"), max_len=2000)
    promo["type"] = _safe_text(promo.get("type"), max_len=64)
    promo["category"] = _safe_text(promo.get("category"), max_len=64)
    promo["savingsType"] = _safe_text(promo.get("savingsType"), max_len=64)
    qty = dict(promo.get("quantityRequirements") or {})
    qty["requiresMinimumQuantity"] = _safe_bool(qty.get("requiresMinimumQuantity"), False)
    qty["minimumQuantity"] = _safe_int(qty.get("minimumQuantity"), min_val=0)
    qty["targetQuantity"] = _safe_int(qty.get("targetQuantity"), min_val=0)
    qty["userInstruction"] = _safe_text(qty.get("userInstruction"), max_len=512)
    qty["actionRequired"] = _safe_bool(qty.get("actionRequired"), False)
    promo["quantityRequirements"] = qty
    promo["isProcessed"] = _safe_bool(promo.get("isProcessed"), True)
    r["promotion_data"] = promo

    # bonus period fields (if present)
    if r.get("bonus_period_start"):
        r["bonus_period_start"] = _safe_text(str(r["bonus_period_start"]), max_len=32)
    if r.get("bonus_period_end"):
        r["bonus_period_end"] = _safe_text(str(r["bonus_period_end"]), max_len=32)

    # category slug (repo still requires it)
    if category_slug is not None:
        r["_clean_category_slug"] = _safe_slug(category_slug)
    else:
        # attempt from category name as a fallback (won't override repo arg)
        cname = cat.get("name") or "Uncategorized"
        r["_clean_category_slug"] = _safe_slug(cname)

    return r
