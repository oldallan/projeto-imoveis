from __future__ import annotations

import json
import re
from typing import Any, Iterable, Mapping
from urllib.parse import urljoin


def compact_json(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False)


def merge_record(base: Mapping[str, Any], detail: Mapping[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in detail.items():
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        merged[key] = value
    return merged


def count_filled_fields(record: Mapping[str, Any], keys: Iterable[str]) -> int:
    total = 0
    for key in keys:
        value = record.get(key)
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        total += 1
    return total


def absolute_url(url: str | None, base_url: str) -> str | None:
    if not url:
        return None
    return urljoin(base_url, url)


def extract_json_script(html: str, script_id: str) -> dict[str, Any] | None:
    pattern = rf'<script[^>]*id="{re.escape(script_id)}"[^>]*>(.*?)</script>'
    match = re.search(pattern, html, flags=re.DOTALL)
    if not match:
        return None
    return json.loads(match.group(1))


def first_json_object(data: dict[str, Any]) -> dict[str, Any] | None:
    for value in data.values():
        if isinstance(value, dict):
            return value
    return None
