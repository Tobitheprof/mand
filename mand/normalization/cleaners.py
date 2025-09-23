import re
from decimal import Decimal, InvalidOperation
from typing import Any

CURRENCY_RE = re.compile(r"[€$£¥₹₽]")

def normalize_price(value: Any) -> Decimal:
    if value in (None, "", "null"): return Decimal("0.00")
    try:
        if isinstance(value, (int, float, Decimal)):
            return Decimal(str(value)).quantize(Decimal("0.01"))
        s = CURRENCY_RE.sub("", str(value)).replace(",", ".").strip()
        return Decimal(s).quantize(Decimal("0.01")) if s else Decimal("0.00")
    except (InvalidOperation, ValueError, TypeError):
        return Decimal("0.00")
