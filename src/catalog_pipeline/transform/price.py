"""Price parsing: "$1,299.00" (US) and "1.299,00" (European) -> decimal(12,2). Single currency (USD)."""
from pyspark.sql import Column
from pyspark.sql import functions as F

# After stripping whitespace and "$", a price must match exactly one of these shapes (checked in
# this order). The first two cover US grouping and plain numbers; the third covers European grouping;
# the last is a bare decimal comma.
_PLAIN = r"^-?\d+(\.\d+)?$"  # 1299, 1299.00, 4.5
_US_GROUPED = r"^-?\d{1,3}(,\d{3})+(\.\d+)?$"  # 1,299  1,299.00
_EU_GROUPED = r"^-?\d{1,3}(\.\d{3})+(,\d+)?$"  # 1.299.000  1.299,00
_COMMA_DECIMAL = r"^-?\d+,\d{1,2}$"  # 12,57


def parse_price(price: Column) -> Column:
    """Parse a raw price string; null when it is missing or has an unrecognised shape.

    Ambiguity: a lone "1.299" is read as the decimal 1.299 (not European 1,299), and "1,299" as the
    US thousands form 1299. The feeds only emit two-decimal prices, so neither case occurs.
    """
    s = F.regexp_replace(F.trim(price), r"[\s$]", "")
    normalized = (
        F.when(s.rlike(_PLAIN), s)
        .when(s.rlike(_US_GROUPED), F.regexp_replace(s, ",", ""))
        .when(s.rlike(_EU_GROUPED), F.regexp_replace(F.regexp_replace(s, r"\.", ""), ",", "."))
        .when(s.rlike(_COMMA_DECIMAL), F.regexp_replace(s, ",", "."))
    )
    return normalized.cast("decimal(12,2)")  # a cast that cannot represent the value yields null
