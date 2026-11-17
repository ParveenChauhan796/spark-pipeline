"""Mess injection: typos, case/whitespace, brand aliases, word-order shuffles, locale prices."""
import random


def apply_typo(rng: random.Random, text: str) -> str:
    """Swap two adjacent letters, drop one, or double one inside a random longer word."""
    words = text.split(" ")
    candidates = [i for i, w in enumerate(words) if len(w) >= 4 and w.isalpha()]
    if not candidates:
        return text
    i = rng.choice(candidates)
    w = words[i]
    pos = rng.randrange(1, len(w) - 1)
    op = rng.choice(("swap", "drop", "double"))
    if op == "swap":
        w = w[:pos] + w[pos + 1] + w[pos] + w[pos + 2 :]
    elif op == "drop":
        w = w[:pos] + w[pos + 1 :]
    else:
        w = w[:pos] + w[pos] + w[pos:]
    words[i] = w
    return " ".join(words)


def shuffle_words(rng: random.Random, text: str) -> str:
    words = text.split()
    rng.shuffle(words)
    return " ".join(words)


def add_whitespace_noise(rng: random.Random, text: str) -> str:
    """Leading/trailing spaces and occasional double spaces between words."""
    words = text.split()
    body = "".join(w + rng.choice((" ", " ", "  ", "   ")) for w in words[:-1]) + (words[-1] if words else "")
    return " " * rng.randint(0, 2) + body + " " * rng.randint(0, 2)


def truncate(text: str, max_len: int) -> str:
    """Cut at a word boundary, like vendors whose feed has a title length limit."""
    if len(text) <= max_len:
        return text
    return text[:max_len].rsplit(" ", 1)[0]


def messy_title(rng: random.Random, title: str, brand: str, mess: dict, max_len: int) -> str:
    """Render a vendor's version of a product title."""
    text = title
    if brand.lower() not in text.lower() and rng.random() < mess["brand_in_title_rate"]:
        text = f"{brand} {text}"
    text = truncate(text, max_len)
    if rng.random() < mess["typo_rate"]:
        text = apply_typo(rng, text)
    if rng.random() < mess["word_shuffle_rate"]:
        text = shuffle_words(rng, text)
    if rng.random() < mess["uppercase_or_lowercase_rate"]:
        text = rng.choice((str.upper, str.lower))(text)
    if rng.random() < mess["extra_whitespace_rate"]:
        text = add_whitespace_noise(rng, text)
    return text


def messy_brand(rng: random.Random, brand: str, alias_rate: float) -> str:
    """Sometimes render the brand as an alias (suffix, case, no spaces)."""
    if rng.random() >= alias_rate:
        return brand
    variants = [f"{brand} Inc.", f"{brand} Co.", f"{brand} Corp", brand.upper(), brand.lower(), brand.replace(" ", "")]
    return rng.choice([v for v in variants if v != brand] or [brand])


def format_price_usd(price: float) -> str:
    """'$1,299.00' (US locale)."""
    return f"${price:,.2f}"


def format_price_eu(price: float) -> str:
    """'1.299,00' (European locale: swap thousands and decimal separators). Still USD."""
    return f"{price:,.2f}".replace(",", "_").replace(".", ",").replace("_", ".")
