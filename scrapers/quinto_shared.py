from __future__ import annotations

import csv
import json
import re
import uuid
from io import StringIO
from pathlib import Path
from typing import Any, Dict, List, Set
from xml.etree import ElementTree as ET

import requests

from scrapers.discovery_incremental import (
    build_incremental_discovery_delta,
    find_previous_output,
    infer_output_root_from_output_path,
    infer_run_date_from_output_path,
    load_previous_lastmod_state,
)
from scrapers.io_utils import save_parquet_records
from scrapers.parsing_utils import compact_json
from scrapers.zip_utils import extract_zip_code, extract_zip_code_from_mapping


SITEMAP_INDEX_URL = "https://www.quintoandar.com.br/sitemap-v2.xml"
DISCOVERY_FILENAME = "quinto_discovery.csv"
DISCOVERY_FIELDNAMES = ["business_type", "lastmod", "listing_id", "listing_url"]
HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "Origin": "https://www.quintoandar.com.br",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/145.0.0.0 Safari/537.36"
    ),
}


def _coerce_coordinate(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, str):
        cleaned = value.strip()
        if not cleaned:
            return None
        value = cleaned
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def normalize_address_value(address_value: Any) -> Dict[str, Any]:
    if isinstance(address_value, dict):
        zip_code = extract_zip_code_from_mapping(address_value, "zipCode", "zipcode", "postalCode")
        return {
            "address": address_value.get("address") or address_value.get("street"),
            "neighbourhood": address_value.get("neighborhood") or address_value.get("neighbourhood"),
            "city": address_value.get("city"),
            "state": address_value.get("stateName") or address_value.get("stateAcronym"),
            "zip_code": zip_code,
            "lat": _coerce_coordinate(address_value.get("lat")),
            "lon": _coerce_coordinate(address_value.get("lng")),
        }
    if isinstance(address_value, str):
        zip_code = extract_zip_code(address_value)
        return {
            "address": address_value,
            "neighbourhood": None,
            "city": None,
            "state": None,
            "zip_code": zip_code,
            "lat": None,
            "lon": None,
        }
    return {
        "address": None,
        "neighbourhood": None,
        "city": None,
        "state": None,
        "zip_code": None,
        "lat": None,
        "lon": None,
    }

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
