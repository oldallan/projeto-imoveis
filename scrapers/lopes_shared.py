from __future__ import annotations

import csv
import json
import re
from io import StringIO
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode
from xml.etree import ElementTree as ET

from curl_cffi import requests

from scrapers.discovery_incremental import (
    build_incremental_discovery_delta,
    find_previous_output,
    infer_run_date_from_output_path,
    load_previous_lastmod_state,
)
from scrapers.io_utils import save_parquet_records
from scrapers.parsing_utils import absolute_url, compact_json
from scrapers.zip_utils import extract_zip_code, extract_zip_code_from_mapping


BASE_SITE_URL = "https://www.lopes.com.br"
SITEMAP_INDEX_URL = "https://www.lopes.com.br/sitemaps/sitemap-index.xml"
DISCOVERY_FILENAME = "lopes_discovery.csv"
DISCOVERY_FIELDNAMES = ["business_type", "lastmod", "listing_id", "listing_url"]
LISTING_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": "https://www.lopes.com.br/",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/146.0.0.0 Safari/537.36"
    ),
}

def parse_money_to_number(text: Optional[str]) -> Optional[int]:
    if not text:
        return None
    digits = re.sub(r"[^\d]", "", text)
    return int(digits) if digits else None


def extract_attr(attributes: List[Dict[str, Any]], attr_type: str) -> Optional[str]:
    for attr in attributes or []:
        if attr.get("type") == attr_type:
            return attr.get("value")
    return None


def extract_attr_number(attributes: List[Dict[str, Any]], attr_type: str) -> Optional[int]:
    value = extract_attr(attributes, attr_type)
    if not value:
        return None
    digits = re.sub(r"[^\d]", "", value)
    return int(digits) if digits else None


def save_csv(records: List[Dict[str, Any]], filename: str, fieldnames: List[str] | None = None) -> None:
    Path(filename).parent.mkdir(parents=True, exist_ok=True)
    if not records:
        with open(filename, "w", newline="", encoding="utf-8-sig") as file:
            if fieldnames:
                writer = csv.DictWriter(file, fieldnames=fieldnames)
                writer.writeheader()
        return
    resolved_fieldnames = fieldnames or sorted({key for record in records for key in record.keys()})
    with open(filename, "w", newline="", encoding="utf-8-sig") as file:
        writer = csv.DictWriter(file, fieldnames=resolved_fieldnames)
        writer.writeheader()
        writer.writerows(records)


def save_parquet(records: List[Dict[str, Any]], filename: str) -> None:
    save_parquet_records(records, filename)
