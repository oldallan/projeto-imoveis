from __future__ import annotations

import csv
import re
import unicodedata
from dataclasses import dataclass, field
from datetime import date, datetime, time as dt_time, timedelta, timezone
from html import unescape as html_unescape
from pathlib import Path
from typing import Any, Dict, List
from zoneinfo import ZoneInfo

from parsel import Selector

from scrapers.io_utils import save_parquet_records
from scrapers.output_paths import build_dated_output_path
from scrapers.parsing_utils import absolute_url, compact_json
from scrapers.zip_utils import extract_zip_code, extract_zip_code_from_mapping


BASE_SITE_URL = "https://www.olx.com.br"
LISTING_ID_PATTERN = re.compile(r"-(\d{6,})(?:\?.*)?$")
BRAZIL_TZ = ZoneInfo("America/Sao_Paulo")
DATE_FORMAT = "%d-%m-%Y"
DEFAULT_DISCOVERY_FILENAME = "olx_discovery.csv"
DEFAULT_INVALID_DISCOVERY_FILENAME = "olx_discovery_invalid_records.csv"
SALE_BASE_URL = "https://www.olx.com.br/imoveis/venda/estado-sp/sao-paulo-e-regiao"
RENT_BASE_URL = "https://www.olx.com.br/imoveis/aluguel/estado-sp/sao-paulo-e-regiao"
DEFAULT_IMPERSONATE_BROWSER = "chrome110"
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/110.0.0.0 Safari/537.36"
)
HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "User-Agent": DEFAULT_USER_AGENT,
}
MONTHS_PT_BR = {
    "jan": 1,
    "fev": 2,
    "mar": 3,
    "abr": 4,
    "mai": 5,
    "jun": 6,
    "jul": 7,
    "ago": 8,
    "set": 9,
    "out": 10,
    "nov": 11,
    "dez": 12,
}
PRICE_TEXT_PATTERN = re.compile(r"R\$\s*[\d\.\,]+", flags=re.IGNORECASE)
LISTING_URL_PATTERN = re.compile(r"/imoveis/.+-\d{6,}")
DISCOVERY_FIELDNAMES = ["listing_url", "business_type", "price_brl", "listing_posted_at"]


@dataclass(frozen=True)
class FlowConfig:
    name: str
    base_url: str


@dataclass
class PreviousRunState:
    price_by_url: dict[str, int | None]
    oldest_posted_at_by_flow: dict[str, datetime]
    newest_posted_at_by_flow: dict[str, datetime] = field(default_factory=dict)
    source_path: str | None = None


@dataclass
class FlowMetrics:
    flow: str
    pages_scanned: int = 0
    items_seen: int = 0
    items_kept: int = 0
    duplicates_in_run: int = 0
    same_price_ignored: int = 0
    invalid_records: int = 0
    stopped_by_old_date: bool = False
    stop_reason: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "flow": self.flow,
            "pages_scanned": self.pages_scanned,
            "items_seen": self.items_seen,
            "items_kept": self.items_kept,
            "duplicates_in_run": self.duplicates_in_run,
            "same_price_ignored": self.same_price_ignored,
            "invalid_records": self.invalid_records,
            "stopped_by_old_date": self.stopped_by_old_date,
            "stop_reason": self.stop_reason,
        }


@dataclass
class PageProcessResult:
    kept_records: list[dict[str, Any]]
    invalid_samples: list[dict[str, Any]]
    duplicates_in_run: int = 0
    same_price_ignored: int = 0
    invalid_records: int = 0
    stop_due_to_old_date: bool = False
    useful_overlap_records: int = 0
    page_fully_in_overlap: bool = False


def normalize_price_brl(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return int(value)
    digits = re.sub(r"[^\d]", "", str(value).strip())
    return int(digits) if digits else None

def save_csv(
    records: List[Dict[str, Any]],
    filename: str,
    fieldnames: List[str] | None = None,
) -> None:
    Path(filename).parent.mkdir(parents=True, exist_ok=True)
    resolved_fieldnames = fieldnames or sorted({key for record in records for key in record.keys()})
    with open(filename, "w", newline="", encoding="utf-8-sig") as file:
        if not resolved_fieldnames:
            file.write("")
            return
        writer = csv.DictWriter(file, fieldnames=resolved_fieldnames)
        writer.writeheader()
        if records:
            writer.writerows(records)


def save_invalid_records_csv(records: List[Dict[str, Any]], filename: str) -> None:
    save_csv(
        records,
        filename=filename,
        fieldnames=[
            "flow",
            "invalid_reason",
            "listing_url",
            "price_brl",
            "listing_posted_at",
            "raw_card_date_text",
            "title",
        ],
    )
