"""Vendor feed simulation: 4 vendors x daily batches, each with its own schema and mess.

Vendors A and B ship CSV (A comma-delimited with "$1,299.00" prices, B semicolon-delimited with
"1.299,00" prices); C ships flat JSON lines, D nested JSON lines. Day 1 is a full snapshot; later
days are change feeds (changed + new + a few unchanged re-sent rows).

Injected schema drift (batch_index is the position in `batch_dates`):
  * A adds column `rating` on batch_index 1            -> ADDED
  * B renames `title` to `name` on batch_index 2       -> RENAMED (alias declared in B's contract)
  * C sends `price` as a string on batch_index 2       -> TYPE_CHANGED
"""
import random
from dataclasses import dataclass

from catalog_pipeline.generate import mess as messmod
from catalog_pipeline.generate.catalogue import Product

DRIFT_A_ADDS_RATING_FROM = 1
DRIFT_B_RENAMES_TITLE_FROM = 2
DRIFT_C_PRICE_AS_STRING_FROM = 2

SKU_SPACE = 10_000_000


@dataclass(frozen=True)
class VendorSpec:
    vendor_id: str
    fmt: str  # "csv" | "json"
    filename: str
    delimiter: str | None
    sku_prefix: str
    gtin_style: str  # "plain" | "strip_zero" | "pad14"


SPECS = {
    "A": VendorSpec("A", "csv", "products.csv", ",", "A-", "plain"),
    "B": VendorSpec("B", "csv", "products.csv", ";", "BX", "strip_zero"),
    "C": VendorSpec("C", "json", "products.jsonl", None, "c_", "plain"),
    "D": VendorSpec("D", "json", "products.jsonl", None, "D", "pad14"),
}


@dataclass
class Offer:
    """One vendor's listing of one product. Text fields are fixed per vendor; price/qty evolve."""

    sku: str
    true_product_id: str
    title: str
    brand: str | None
    category: str | None
    model: str | None
    gtin: str | None
    rating: float | None
    price: float
    qty: int


@dataclass(frozen=True)
class Batch:
    vendor_id: str
    batch_date: str
    day_index: int
    fmt: str
    filename: str
    delimiter: str | None
    columns: list[str] | None  # CSV header (None for JSON)
    rows: list[dict]


def _gtin_for_vendor(gtin: str | None, style: str) -> str | None:
    if gtin is None:
        return None
    if style == "strip_zero":
        return gtin[1:] if gtin.startswith("0") else gtin  # UPC-A (12 digits)
    if style == "pad14":
        return gtin.zfill(14)
    return gtin


def _random_qty(rng: random.Random, gen: dict) -> int:
    if rng.random() < gen["out_of_stock_fraction"]:
        return 0
    return rng.randint(1, gen["stock_qty_max"])


def _make_offer(spec: VendorSpec, gen: dict, product: Product, sku: str) -> Offer:
    """Build a vendor's static view of a product (seeded per vendor + product, so re-runs match)."""
    vcfg = gen["vendors"][spec.vendor_id]
    mess = gen["mess"]
    rng = random.Random(f"{gen['seed']}:{spec.vendor_id}:{product.true_product_id}")

    title = messmod.messy_title(rng, product.title, product.brand, mess, vcfg["title_max_len"])
    brand = messmod.messy_brand(rng, product.brand, mess["brand_alias_rate"])
    if rng.random() < mess["missing_brand_rate"]:
        brand = None
    category = None if rng.random() < mess["missing_category_rate"] else product.category
    gtin = product.gtin if product.gtin and rng.random() >= mess["missing_gtin_rate"] else None
    spread = vcfg["price_spread"]
    price = round(product.base_price * (1 + rng.uniform(-spread, spread)), 2)
    return Offer(
        sku=sku,
        true_product_id=product.true_product_id,
        title=title,
        brand=brand,
        category=category,
        model=product.model,
        gtin=_gtin_for_vendor(gtin, spec.gtin_style),
        rating=product.rating,
        price=price,
        qty=_random_qty(rng, gen),
    )


def _csv_columns(vendor_id: str, day: int) -> list[str]:
    if vendor_id == "A":
        cols = ["sku", "title", "brand", "category", "model", "price", "qty", "gtin"]
        return cols + ["rating"] if day >= DRIFT_A_ADDS_RATING_FROM else cols
    title_col = "name" if day >= DRIFT_B_RENAMES_TITLE_FROM else "title"
    return ["item_id", title_col, "manufacturer", "cat", "mpn", "price", "stock", "ean"]


def _render_row(spec: VendorSpec, offer: Offer, day: int, rng: random.Random, mess: dict) -> dict:
    """Render one offer as a row in the vendor's schema (with per-row nulls)."""
    sku = None if rng.random() < mess["null_sku_rate"] else offer.sku
    price = None if rng.random() < mess["missing_price_rate"] else offer.price
    qty = None if rng.random() < mess["missing_qty_rate"] else offer.qty

    if spec.vendor_id == "A":
        row = {
            "sku": sku,
            "title": offer.title,
            "brand": offer.brand,
            "category": offer.category,
            "model": offer.model,
            "price": None if price is None else messmod.format_price_usd(price),
            "qty": qty,
            "gtin": offer.gtin,
        }
        if day >= DRIFT_A_ADDS_RATING_FROM:
            row["rating"] = offer.rating
        return row
    if spec.vendor_id == "B":
        title_col = "name" if day >= DRIFT_B_RENAMES_TITLE_FROM else "title"
        return {
            "item_id": sku,
            title_col: offer.title,
            "manufacturer": offer.brand,
            "cat": offer.category,
            "mpn": offer.model,
            "price": None if price is None else messmod.format_price_eu(price),
            "stock": qty,
            "ean": offer.gtin,
        }
    if spec.vendor_id == "C":
        if price is not None and day >= DRIFT_C_PRICE_AS_STRING_FROM:
            price = f"{price:.2f}"
        return {
            "sku": sku,
            "name": offer.title,
            "brand": offer.brand,
            "category": offer.category,
            "price": price,
            "quantity": qty,
            "upc": offer.gtin,
        }
    return {  # D: nested JSON
        "id": sku,
        "product": {"name": offer.title, "brand": offer.brand, "category": offer.category, "model": offer.model},
        "offer": {"price": price, "qty": qty},
        "barcode": offer.gtin,
    }


def _apply_change(offer: Offer, rng: random.Random, gen: dict) -> None:
    """Change price, stock, or both (at least one value differs)."""
    kind = rng.choice(("price", "qty", "both"))
    if kind in ("price", "both"):
        magnitude = gen["price_change_magnitude"]
        new_price = round(max(0.01, offer.price * (1 + rng.uniform(-magnitude, magnitude))), 2)
        offer.price = new_price if new_price != offer.price else round(offer.price + 0.01, 2)
    if kind in ("qty", "both"):
        new_qty = _random_qty(rng, gen)
        offer.qty = new_qty if new_qty != offer.qty else offer.qty + 1


def build_batches(products: list[Product], config: dict) -> tuple[list[Batch], dict[tuple[str, str], str]]:
    """Simulate every vendor over every batch date.

    Returns (batches, truth) where truth maps (vendor_id, vendor_sku) -> true_product_id for every
    listing that ever appears in a feed.
    """
    gen = config["generate"]
    mess = gen["mess"]
    dates = gen["batch_dates"]
    n = len(products)
    batches: list[Batch] = []
    truth: dict[tuple[str, str], str] = {}

    for vendor_id in sorted(gen["vendors"]):
        spec = SPECS[vendor_id]
        vcfg = gen["vendors"][vendor_id]
        rng = random.Random(f"{gen['seed']}:{vendor_id}")

        sku_numbers = rng.sample(range(max(SKU_SPACE, 100 * n)), n)
        skus = [f"{spec.sku_prefix}{num:07d}" for num in sku_numbers]
        covered = sorted(rng.sample(range(n), int(n * vcfg["coverage"])))
        covered_set = set(covered)
        unlisted = [i for i in range(n) if i not in covered_set]

        def new_offer(i: int) -> Offer:
            truth[(vendor_id, skus[i])] = products[i].true_product_id
            return _make_offer(spec, gen, products[i], skus[i])

        offers = [new_offer(i) for i in covered]

        for day, batch_date in enumerate(dates):
            day_rng = random.Random(f"{gen['seed']}:{vendor_id}:day{day}")
            if day == 0:
                emitted = list(offers)
            else:
                n_changed = int(len(offers) * gen["change_rate"])
                changed_pos = day_rng.sample(range(len(offers)), n_changed)
                for pos in changed_pos:
                    _apply_change(offers[pos], day_rng, gen)

                n_new = int(len(unlisted) * gen["new_item_rate"])
                new_idx = day_rng.sample(unlisted, n_new)
                new_set = set(new_idx)
                unlisted = [i for i in unlisted if i not in new_set]
                added = [new_offer(i) for i in new_idx]

                changed_set = set(changed_pos)
                rest_pos = [p for p in range(len(offers)) if p not in changed_set]
                resent_pos = day_rng.sample(rest_pos, int(len(rest_pos) * gen["resend_rate"]))

                emitted = [offers[p] for p in changed_pos] + added + [offers[p] for p in resent_pos]
                offers.extend(added)
                day_rng.shuffle(emitted)

            rows = [_render_row(spec, o, day, day_rng, mess) for o in emitted]
            rows += [r for r in rows if day_rng.random() < mess["duplicate_row_rate"]]  # in-file duplicates

            batches.append(
                Batch(
                    vendor_id=vendor_id,
                    batch_date=batch_date,
                    day_index=day,
                    fmt=spec.fmt,
                    filename=spec.filename,
                    delimiter=spec.delimiter,
                    columns=_csv_columns(vendor_id, day) if spec.fmt == "csv" else None,
                    rows=rows,
                )
            )
    return batches, truth
