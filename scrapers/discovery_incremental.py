from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Mapping


DATE_FORMAT = "%d-%m-%Y"


@dataclass
class IncrementalDiscoveryState:
    lastmod_by_url: dict[str, str | None]
    source_path: str | None = None
    watermark_lastmod: date | None = None


def parse_run_date(run_date: str) -> date:
    return datetime.strptime(run_date, DATE_FORMAT).date()


def infer_run_date_from_output_path(output_path: str | Path) -> str | None:
    path = Path(output_path)
    try:
        candidate = path.parent.parent.name
    except IndexError:
        return None
    try:
        parse_run_date(candidate)
    except ValueError:
        return None
    return candidate


def find_previous_output(
    *,
    run_date: str,
    source: str,
    filename: str,
    project_root: Path | None = None,
) -> Path | None:
    root = (project_root or Path.cwd()).resolve()
    raw_dir = root / "raw"
    if not raw_dir.exists():
        return None

    current_date = parse_run_date(run_date)
    candidates: list[tuple[date, Path]] = []
    for candidate in raw_dir.glob(f"*/{source}/{filename}"):
        try:
            candidate_date = parse_run_date(candidate.parent.parent.name)
        except ValueError:
            continue
        if candidate_date >= current_date:
            continue
        candidates.append((candidate_date, candidate))

    candidates.sort(key=lambda item: item[0], reverse=True)
    for _, candidate in candidates:
        if _has_non_empty_csv_rows(candidate):
            return candidate
    return None


def _has_non_empty_csv_rows(path: str | Path) -> bool:
    candidate_path = Path(path)
    if not candidate_path.exists() or not candidate_path.is_file():
        return False

    try:
        with candidate_path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                if any(value not in (None, "") for value in row.values()):
                    return True
    except (OSError, csv.Error):
        return False
    return False


def _normalize_lastmod(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _parse_lastmod_date(value: Any) -> date | None:
    text = _normalize_lastmod(value)
    if not text:
        return None

    normalized = text.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(normalized).date()
    except ValueError:
        pass

    try:
        return datetime.strptime(text[:10], "%Y-%m-%d").date()
    except ValueError:
        return None


def load_previous_lastmod_state(path: str | Path | None) -> IncrementalDiscoveryState:
    if not path:
        return IncrementalDiscoveryState(lastmod_by_url={}, source_path=None)

    previous_path = Path(path)
    if not previous_path.exists():
        return IncrementalDiscoveryState(lastmod_by_url={}, source_path=str(previous_path))

    lastmod_by_url: dict[str, str | None] = {}
    watermark_lastmod: date | None = None
    with previous_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            listing_url = str(row.get("listing_url") or "").strip()
            if not listing_url:
                continue
            normalized_lastmod = _normalize_lastmod(row.get("lastmod"))
            lastmod_by_url[listing_url] = normalized_lastmod
            parsed_lastmod = _parse_lastmod_date(normalized_lastmod)
            if parsed_lastmod and (watermark_lastmod is None or parsed_lastmod > watermark_lastmod):
                watermark_lastmod = parsed_lastmod

    return IncrementalDiscoveryState(
        lastmod_by_url=lastmod_by_url,
        source_path=str(previous_path.resolve()),
        watermark_lastmod=watermark_lastmod,
    )


def sort_discovery_records(records: list[Mapping[str, Any]]) -> list[dict[str, Any]]:
    ordered = [dict(record) for record in records]
    ordered.sort(key=lambda record: str(record.get("listing_url") or ""))
    ordered.sort(key=lambda record: str(record.get("lastmod") or ""), reverse=True)
    return ordered


def _deduplicate_by_listing_url_lastmod(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: dict[tuple[str, str | None], dict[str, Any]] = {}
    for record in records:
        listing_url = str(record.get("listing_url") or "").strip()
        if not listing_url:
            continue
        key = (listing_url, _normalize_lastmod(record.get("lastmod")))
        if key not in deduped:
            deduped[key] = record
    return list(deduped.values())


def build_incremental_discovery_delta(
    records: list[Mapping[str, Any]],
    previous_state: IncrementalDiscoveryState,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    delta_records: list[dict[str, Any]] = []
    new_rows = 0
    updated_rows = 0
    unchanged_rows = 0
    window_filtered_rows = 0
    overlap_start_lastmod = (
        previous_state.watermark_lastmod - timedelta(days=1)
        if previous_state.watermark_lastmod
        else None
    )
    candidate_records = _deduplicate_by_listing_url_lastmod([dict(record) for record in records])

    for record in candidate_records:
        listing_url = str(record.get("listing_url") or "").strip()
        if not listing_url:
            continue

        current_lastmod = _normalize_lastmod(record.get("lastmod"))
        current_lastmod_date = _parse_lastmod_date(current_lastmod)
        if (
            overlap_start_lastmod
            and current_lastmod_date
            and current_lastmod_date < overlap_start_lastmod
        ):
            window_filtered_rows += 1
            continue

        previous_lastmod = previous_state.lastmod_by_url.get(listing_url)
        if listing_url not in previous_state.lastmod_by_url:
            new_rows += 1
            delta_records.append(dict(record))
            continue
        if previous_lastmod != current_lastmod:
            updated_rows += 1
            delta_records.append(dict(record))
            continue
        unchanged_rows += 1

    sorted_delta = sort_discovery_records(delta_records)
    metrics = {
        "previous_output_path": previous_state.source_path,
        "full_rows": len(records),
        "delta_rows": len(sorted_delta),
        "new_rows": new_rows,
        "updated_rows": updated_rows,
        "unchanged_rows": unchanged_rows,
        "watermark_lastmod": (
            previous_state.watermark_lastmod.isoformat()
            if previous_state.watermark_lastmod
            else None
        ),
        "overlap_start_lastmod": overlap_start_lastmod.isoformat() if overlap_start_lastmod else None,
        "window_filtered_rows": window_filtered_rows,
    }
    return sorted_delta, metrics
