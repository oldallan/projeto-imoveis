from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from pipelines.dedupe import build_unified_tables, deduplicate_data
from pipelines.normalize import load_and_normalize
from pipelines.zipcode_enrichment import ZipCodeEnricher, default_cache_path_from_output_dir


LISTINGS_FILENAME = "listings_unificados.parquet"
LISTINGS_CSV_FILENAME = "listings_unificados.csv"
PROPERTIES_FILENAME = "properties_unified.parquet"
PROPERTIES_CSV_FILENAME = "properties_unified.csv"
LINK_FILENAME = "property_listing_link.parquet"
LISTINGS_EXCLUDED_OUTPUT_COLUMNS = ["description", "long_description"]


def project_listings_output_columns(listings_df: pd.DataFrame) -> pd.DataFrame:
    if listings_df.empty:
        return listings_df.copy()

    excluded = [column for column in LISTINGS_EXCLUDED_OUTPUT_COLUMNS if column in listings_df.columns]
    if not excluded:
        return listings_df.copy()

    return listings_df.drop(columns=excluded)


def attach_canonical_id(
    listings_df: pd.DataFrame,
    link_df: pd.DataFrame,
    properties_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    safe_listings = listings_df.copy()
    if safe_listings.empty or link_df.empty:
        return safe_listings

    enriched = safe_listings
    if "canonical_property_id" not in enriched.columns:
        preferred_join_keys = ["source", "business_type", "property_id", "listing_url"]
        fallback_join_keys = ["property_id", "listing_url"]
        join_keys = (
            preferred_join_keys
            if all(column in enriched.columns and column in link_df.columns for column in preferred_join_keys)
            else fallback_join_keys
        )
        link_fields = join_keys + ["canonical_property_id"]
        enriched = enriched.merge(
            link_df[link_fields].drop_duplicates(),
            on=join_keys,
            how="left",
        )

    if properties_df is None or properties_df.empty:
        return enriched

    property_fields = [
        "canonical_property_id",
        "sale_price_brl",
        "rent_price_brl",
        "is_for_sale",
        "is_for_rent",
        "listing_mode",
    ]
    available_fields = [column for column in property_fields if column in properties_df.columns]
    if len(available_fields) <= 1:
        return enriched

    existing_property_fields = [
        column for column in available_fields if column != "canonical_property_id" and column in enriched.columns
    ]
    if existing_property_fields:
        enriched = enriched.drop(columns=existing_property_fields)

    enriched = enriched.merge(
        properties_df[available_fields].drop_duplicates(subset=["canonical_property_id"]),
        on="canonical_property_id",
        how="left",
    )
    return enriched


def build_daily_snapshot(files: list[str], output_dir: Path) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)

    zip_enricher = ZipCodeEnricher(cache_path=default_cache_path_from_output_dir(output_dir))
    normalized_df = load_and_normalize(files, zip_enricher=zip_enricher)
    listings_df = deduplicate_data(normalized_df)
    properties_df, link_df = build_unified_tables(listings_df)
    listings_with_canonical = attach_canonical_id(listings_df, link_df, properties_df)
    listings_output = project_listings_output_columns(listings_with_canonical)

    listings_path = output_dir / LISTINGS_FILENAME
    listings_csv_path = output_dir / LISTINGS_CSV_FILENAME
    properties_path = output_dir / PROPERTIES_FILENAME
    properties_csv_path = output_dir / PROPERTIES_CSV_FILENAME
    links_path = output_dir / LINK_FILENAME

    listings_output.to_parquet(listings_path, index=False)
    listings_output.to_csv(listings_csv_path, index=False, encoding="utf-8-sig")
    properties_df.to_parquet(properties_path, index=False)
    properties_df.to_csv(properties_csv_path, index=False, encoding="utf-8-sig")
    link_df.to_parquet(links_path, index=False)

    print(
        "[INFO] zipcode_enrichment_metrics="
        + json.dumps(zip_enricher.last_metrics, ensure_ascii=False)
    )

    return {
        "listings": listings_output,
        "properties": properties_df,
        "links": link_df,
        "metrics": dict(zip_enricher.last_metrics),
        "paths": {
            "listings": listings_path,
            "listings_csv": listings_csv_path,
            "properties": properties_path,
            "properties_csv": properties_csv_path,
            "links": links_path,
        },
    }
