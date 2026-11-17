"""Golden catalogue: the clean 'truth' every vendor feed is derived from."""
import math
import random
import re
from collections.abc import Iterable
from dataclasses import dataclass

# Placeholder values the source uses instead of a real model number.
_NO_MODEL = {"", "does not apply", "n/a", "na", "none", "unknown"}


@dataclass(frozen=True)
class Product:
    true_product_id: str
    title: str
    brand: str
    category: str
    model: str | None
    rating: float | None
    gtin: str | None  # synthetic, 13 digits with a valid check digit
    base_price: float  # synthetic


def gtin_check_digit(first_twelve: list[int]) -> int:
    """EAN-13 check digit: weights 1,3,1,3,... over the first 12 digits."""
    total = sum(d * (3 if i % 2 else 1) for i, d in enumerate(first_twelve))
    return (10 - total % 10) % 10


def _new_gtin(rng: random.Random, seen: set[str]) -> str:
    while True:
        digits = [rng.randint(0, 9) for _ in range(12)]
        if rng.random() < 1 / 3:
            digits[0] = 0  # UPC-A style: an EAN-13 with a leading zero
        gtin = "".join(map(str, digits + [gtin_check_digit(digits)]))
        if gtin not in seen:
            seen.add(gtin)
            return gtin


def _clean_text(value) -> str | None:
    if not isinstance(value, str):
        return None
    text = " ".join(value.split())
    return text or None


def _clean_brand(value) -> str | None:
    text = _clean_text(value)
    if text is None:
        return None
    for pattern in (r"^Visit the (.+) Store$", r"^Brand:\s*(.+)$"):
        m = re.match(pattern, text)
        if m:
            text = m.group(1).strip()
    return text if len(text) >= 2 else None


def _brand(rec: dict, details: dict) -> str | None:
    for candidate in (details.get("Brand"), rec.get("store"), details.get("Manufacturer")):
        brand = _clean_brand(candidate)
        if brand:
            return brand
    return None


def _model(details: dict) -> str | None:
    for key in ("Item model number", "Model Name", "Part Number"):
        value = _clean_text(details.get(key))
        if value and value.lower() not in _NO_MODEL:
            return value
    return None


def _category(rec: dict) -> str:
    cats = [c for c in (rec.get("categories") or []) if isinstance(c, str) and c.strip()]
    if len(cats) >= 2:
        return cats[1].strip()
    return cats[0].strip() if cats else "Appliances"


def build_catalogue(records: Iterable[dict], config: dict) -> list[Product]:
    """Turn raw source records into golden products, adding seeded synthetic barcodes and prices.

    Records without a usable title or brand, and repeated parent_asin values, are skipped.
    """
    gen = config["generate"]
    rng = random.Random(f"{gen['seed']}:catalogue")
    low, high = gen["price_range"]
    products: list[Product] = []
    seen_asin: set[str] = set()
    seen_gtin: set[str] = set()

    for rec in records:
        asin = rec.get("parent_asin")
        title = _clean_text(rec.get("title"))
        details = rec.get("details") if isinstance(rec.get("details"), dict) else {}
        brand = _brand(rec, details)
        if not asin or asin in seen_asin or not title or not brand:
            continue
        seen_asin.add(asin)

        rating = rec.get("average_rating")
        gtin = _new_gtin(rng, seen_gtin) if rng.random() < gen["gtin_fraction"] else None
        price = round(math.exp(rng.uniform(math.log(low), math.log(high))), 2)
        products.append(
            Product(
                true_product_id=f"P{len(products) + 1:06d}",
                title=title,
                brand=brand,
                category=_category(rec),
                model=_model(details),
                rating=float(rating) if isinstance(rating, (int, float)) else None,
                gtin=gtin,
                base_price=price,
            )
        )
    return products
