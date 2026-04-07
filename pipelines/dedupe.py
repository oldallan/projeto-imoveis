from __future__ import annotations

import pandas as pd


def _listing_key(df: pd.DataFrame) -> pd.Series:
    return (
        df["source"].fillna("")
        + "|"
        + df["business_type"].fillna("")
        + "|"
        + df["property_id"].fillna("")
        + "|"
        + df["listing_url"].fillna("")
    )


def deduplicate_data(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    deduped = df.copy()
    deduped["_listing_key"] = _listing_key(deduped)
    deduped = deduped.sort_values(
        by=["scraped_at"],
        ascending=[False],
        na_position="last",
    )
    deduped = deduped.drop_duplicates(subset=["_listing_key"], keep="first")
    deduped = deduped.drop(columns=["_listing_key"]).reset_index(drop=True)
    return deduped


def _build_canonical_property_id(df: pd.DataFrame) -> pd.Series:
    city = df["city"].fillna("").astype("string").str.strip().str.lower()
    neighbourhood = df["neighbourhood"].fillna("").astype("string").str.strip().str.lower()
    address = df["address"].fillna("").astype("string").str.strip().str.lower()
    area = df["area_m2"].fillna(0).round(0).astype("Int64").astype("string")
    bedrooms = df["bedrooms"].fillna(0).astype("Int64").astype("string")
    bathrooms = df["bathrooms"].fillna(0).astype("Int64").astype("string")

    return city + "|" + neighbourhood + "|" + address + "|" + area + "|" + bedrooms + "|" + bathrooms


def _listing_mode_from_flags(is_for_sale: bool, is_for_rent: bool) -> str:
    if is_for_sale and is_for_rent:
        return "sale_rent"
    if is_for_sale:
        return "sale"
    return "rent"


def _has_positive_price(series: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    return numeric.fillna(0).gt(0)


def _build_canonical_availability(deduped: pd.DataFrame) -> pd.DataFrame:
    availability = (
        deduped.groupby("canonical_property_id")["business_type"]
        .agg(
            has_sale_by_canonical=lambda values: values.fillna("").eq("sale").any(),
            has_rent_by_canonical=lambda values: values.fillna("").eq("rent").any(),
        )
        .reset_index()
    )
    return availability


def _build_property_id_availability(deduped: pd.DataFrame) -> pd.DataFrame:
    valid_property_id = deduped["property_id"].fillna("").astype("string").str.strip().ne("")
    availability = (
        deduped.loc[valid_property_id]
        .groupby(["source", "property_id"])["business_type"]
        .agg(
            has_sale_by_property_id=lambda values: values.fillna("").eq("sale").any(),
            has_rent_by_property_id=lambda values: values.fillna("").eq("rent").any(),
        )
        .reset_index()
    )
    return availability


def _apply_availability_evidence(deduped: pd.DataFrame) -> pd.DataFrame:
    enriched = deduped.merge(
        _build_canonical_availability(deduped),
        on="canonical_property_id",
        how="left",
    )
    enriched = enriched.merge(
        _build_property_id_availability(deduped),
        on=["source", "property_id"],
        how="left",
    )

    evidence_columns = [
        "has_sale_by_canonical",
        "has_rent_by_canonical",
        "has_sale_by_property_id",
        "has_rent_by_property_id",
    ]
    for column in evidence_columns:
        if column not in enriched.columns:
            enriched[column] = False
        enriched[column] = enriched[column].fillna(False).astype(bool)

    enriched["has_sale_price"] = _has_positive_price(enriched["sale_price_brl"])
    enriched["has_rent_price"] = _has_positive_price(enriched["rent_price_brl"])
    enriched["is_for_sale"] = (
        enriched["has_sale_by_canonical"]
        | enriched["has_sale_by_property_id"]
        | enriched["has_sale_price"]
    )
    enriched["is_for_rent"] = (
        enriched["has_rent_by_canonical"]
        | enriched["has_rent_by_property_id"]
        | enriched["has_rent_price"]
    )
    enriched["listing_mode"] = enriched.apply(
        lambda row: _listing_mode_from_flags(
            is_for_sale=bool(row["is_for_sale"]),
            is_for_rent=bool(row["is_for_rent"]),
        ),
        axis=1,
    )
    return enriched


def build_unified_tables(listings_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    deduped = deduplicate_data(listings_df)
    if deduped.empty:
        return pd.DataFrame(), pd.DataFrame()

    deduped = deduped.copy()
    deduped["canonical_property_id"] = _build_canonical_property_id(deduped)
    deduped = _apply_availability_evidence(deduped)

    properties_df = (
        deduped.sort_values("scraped_at", ascending=False)
        .groupby("canonical_property_id", as_index=False)
        .agg(
            city=("city", "first"),
            neighbourhood=("neighbourhood", "first"),
            address=("address", "first"),
            state=("state", "first"),
            zip_code=("zip_code", "first"),
            lat=("lat", "first"),
            lon=("lon", "first"),
            property_type=("property_type", "first"),
            area_m2=("area_m2", "max"),
            total_area_m2=("total_area_m2", "max"),
            bedrooms=("bedrooms", "max"),
            bathrooms=("bathrooms", "max"),
            parking_spots=("parking_spots", "max"),
            suites=("suites", "max"),
            floor=("floor", "max"),
            furnished=("furnished", "max"),
            accepts_pets=("accepts_pets", "max"),
            condominium_name=("condominium_name", "first"),
            condominium_id=("condominium_id", "first"),
            amenities_json=("amenities_json", "first"),
            installations_json=("installations_json", "first"),
            first_seen_at=("scraped_at", "min"),
            last_seen_at=("scraped_at", "max"),
            listings_count=("property_id", "count"),
            is_for_sale=("is_for_sale", "any"),
            is_for_rent=("is_for_rent", "any"),
        )
    )
    properties_df["listing_mode"] = properties_df.apply(
        lambda row: _listing_mode_from_flags(
            is_for_sale=bool(row["is_for_sale"]),
            is_for_rent=bool(row["is_for_rent"]),
        ),
        axis=1,
    )

    link_df = deduped[
        [
            "canonical_property_id",
            "source",
            "business_type",
            "property_id",
            "listing_url",
            "scraped_at",
        ]
    ].copy()

    return properties_df, link_df
