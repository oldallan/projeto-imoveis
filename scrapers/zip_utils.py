from __future__ import annotations

import re
from typing import Any, Iterable


ZIP_CODE_PATTERN = re.compile(r"\b(\d{5})-?(\d{3})\b")


def normalize_zip_code(value: Any) -> str | None:
    if value is None:
        return None
    match = ZIP_CODE_PATTERN.search(str(value))
    if not match:
        return None
    return f"{match.group(1)}-{match.group(2)}"


def extract_zip_code(*values: Any) -> str | None:
    for value in values:
        normalized = normalize_zip_code(value)
        if normalized:
            return normalized
    return None


def extract_zip_code_from_mapping(mapping: Any, *keys: str) -> str | None:
    if not isinstance(mapping, dict):
        return None
    for key in keys:
        normalized = normalize_zip_code(mapping.get(key))
        if normalized:
            return normalized
    return None


def extract_zip_code_from_texts(values: Iterable[Any]) -> str | None:
    for value in values:
        normalized = normalize_zip_code(value)
        if normalized:
            return normalized
    return None
