"""Text normalization for titles and categories."""
from pyspark.sql import Column
from pyspark.sql import functions as F

_NON_ALNUM = r"[^\p{L}\p{N}]+"  # anything that is not a letter or a digit (Unicode aware)


def title_norm(title: Column) -> Column:
    """Lowercase, punctuation to spaces, single spaces, trimmed. Null when nothing is left."""
    s = F.trim(F.regexp_replace(F.lower(title), _NON_ALNUM, " "))
    return F.when(s == "", None).otherwise(s)


def title_tokens(norm: Column) -> Column:
    """Array of words of a normalized title (empty array for null)."""
    return F.coalesce(F.split(norm, " "), F.array().cast("array<string>"))


def category_norm(category: Column) -> Column:
    """Lowercase, '&' spelled out, punctuation to spaces, single spaces. Null when blank."""
    s = F.trim(F.regexp_replace(F.regexp_replace(F.lower(category), "&", " and "), _NON_ALNUM, " "))
    return F.when(s == "", None).otherwise(s)
