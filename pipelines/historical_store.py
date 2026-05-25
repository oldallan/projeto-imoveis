from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from pipelines.daily_snapshot import (
    LINK_FILENAME,
    LISTINGS_FILENAME,
    PROPERTIES_FILENAME,
    PROPERTIES_CSV_FILENAME,
    attach_canonical_id,
    project_listings_output_columns,
)
from pipelines.dedupe import build_unified_tables


UPSERT_KEY_COLUMNS = ["source", "business_type", "property_id"]
HISTORY_COLUMNS = ["first_seen_at", "last_seen_at", "created_at", "updated_at"]


def _load_existing_listings(path: Path, columns: list[str]) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=columns)

    existing_df = pd.read_parquet(path)
    for column in columns:
        if column not in existing_df.columns:
            existing_df[column] = pd.NA
    return existing_df[columns].copy()


def _current_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat()


def _build_upsert_key(df: pd.DataFrame) -> pd.Series:
    return (
        df["source"].fillna("")
        + "|"
        + df["business_type"].fillna("")
        + "|"
        + df["property_id"].fillna("")
    )


def _apply_history_metadata(existing_df: pd.DataFrame, incoming_df: pd.DataFrame) -> tuple[pd.DataFrame, int, int]:
    current_timestamp = _current_timestamp()
    incoming = incoming_df.copy()
    existing = existing_df.copy()

    for column in HISTORY_COLUMNS:
        if column not in existing.columns:
            existing[column] = pd.NA

    existing["_upsert_key"] = _build_upsert_key(existing)
    incoming["_upsert_key"] = _build_upsert_key(incoming)

    existing_history = (
        existing[["_upsert_key", "first_seen_at", "created_at"]]
        .drop_duplicates(subset=["_upsert_key"], keep="first")
    )
    incoming = incoming.merge(existing_history, on="_upsert_key", how="left", suffixes=("", "_existing"))

    existing_keys = set(existing["_upsert_key"].tolist())
    incoming_keys = set(incoming["_upsert_key"].tolist())
    updated_count = len(existing_keys & incoming_keys)
    inserted_count = len(incoming_keys - existing_keys)

    incoming["first_seen_at"] = incoming["first_seen_at"].fillna(current_timestamp)
    incoming["created_at"] = incoming["created_at"].fillna(current_timestamp)
    incoming["last_seen_at"] = current_timestamp
    incoming["updated_at"] = current_timestamp

    return incoming.drop(columns=["_upsert_key"]), inserted_count, updated_count


def _upsert_listings(existing_df: pd.DataFrame, incoming_df: pd.DataFrame) -> tuple[pd.DataFrame, int, int]:
    if incoming_df.empty:
        return existing_df.copy(), 0, 0

    base_columns = list(incoming_df.columns)
    aligned_existing = existing_df.copy()
    for column in base_columns + HISTORY_COLUMNS:
        if column not in aligned_existing.columns:
            aligned_existing[column] = pd.NA
    aligned_existing = aligned_existing[base_columns + HISTORY_COLUMNS].copy()

    incoming_with_history, inserted_count, updated_count = _apply_history_metadata(
        aligned_existing,
        incoming_df[base_columns].copy(),
    )

    combined = pd.concat([aligned_existing, incoming_with_history], ignore_index=True)
    combined["_upsert_key"] = _build_upsert_key(combined)
    combined = combined.sort_values(by=["scraped_at"], ascending=[False], na_position="last")
    combined = combined.drop_duplicates(subset=["_upsert_key"], keep="first")
    combined = combined.drop(columns=["_upsert_key"]).reset_index(drop=True)
    return combined, inserted_count, updated_count


def update_historical_store(snapshot_listings: pd.DataFrame, processed_dir: Path) -> dict[str, Any]:
    processed_dir.mkdir(parents=True, exist_ok=True)

    base_columns = [column for column in snapshot_listings.columns if column != "canonical_property_id"]
    latest_listings_path = processed_dir / LISTINGS_FILENAME
    latest_properties_path = processed_dir / PROPERTIES_FILENAME
    latest_properties_csv_path = processed_dir / PROPERTIES_CSV_FILENAME
    latest_links_path = processed_dir / LINK_FILENAME

    existing_base = _load_existing_listings(latest_listings_path, base_columns + HISTORY_COLUMNS)
    accumulated_base, inserted_count, updated_count = _upsert_listings(
        existing_base,
        snapshot_listings[base_columns].copy(),
    )

    accumulated_properties, accumulated_links = build_unified_tables(accumulated_base)
    accumulated_listings = attach_canonical_id(
        accumulated_base,
        accumulated_links,
        accumulated_properties,
    )
    listings_output = project_listings_output_columns(accumulated_listings)

    listings_output.to_parquet(latest_listings_path, index=False)
    accumulated_properties.to_parquet(latest_properties_path, index=False)
    accumulated_properties.to_csv(latest_properties_csv_path, index=False, encoding="utf-8-sig")
    accumulated_links.to_parquet(latest_links_path, index=False)

    return {
        "listings": listings_output,
        "properties": accumulated_properties,
        "links": accumulated_links,
        "inserted_count": inserted_count,
        "updated_count": updated_count,
        "paths": {
            "listings": latest_listings_path,
            "properties": latest_properties_path,
            "properties_csv": latest_properties_csv_path,
            "links": latest_links_path,
        },
    }
