"""Resolve product identifiers without treating an unknown marketplace SKU as an article."""

from __future__ import annotations

import re
from typing import Callable, Mapping, Optional

_SUFFIX = re.compile(r"[-_](?:OZ|WB|YM|YANDEX)$", re.IGNORECASE)


def normalize_article(value: str) -> str:
    return _SUFFIX.sub("", str(value or "").strip()).strip()


def extract_article(marketplace: str, hints: Mapping[str, str],
                    lookup: Optional[Callable[[str], str]] = None) -> str:
    direct_keys = {
        "wildberries": ("sellerArticle", "supplierArticle"),
        "yandex_market": ("offerId",),
        "ozon": ("offerId", "offer_id"),
    }
    for key in direct_keys.get(marketplace, ()):
        if hints.get(key):
            return normalize_article(hints[key])
    external_keys = {"wildberries": ("nmID",), "ozon": ("sku",)}
    for key in external_keys.get(marketplace, ()):
        if hints.get(key) and lookup:
            mapped = lookup(str(hints[key]))
            if mapped:
                return normalize_article(mapped)
    return ""
