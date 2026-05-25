from __future__ import annotations

import json
from typing import Any
from urllib.parse import urljoin


def compact_json(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False)


def absolute_url(url: str | None, base_url: str) -> str | None:
    if not url:
        return None
    return urljoin(base_url, url)
