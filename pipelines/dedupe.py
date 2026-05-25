from __future__ import annotations

import pandas as pd


def _business_type_has_context(values: pd.Series, context: str) -> bool:
    normalized = values.fillna("").astype("string").str.strip().str.lower()
    if context == "sale":
        return normalized.isin(["sale", "rent|sale"]).any()
    if context == "rent":
        return normalized.isin(["rent", "rent|sale"]).any()
    return False


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
    area_numeric = pd.to_numeric(df["area_m2"], errors="coerce")
    bedrooms_numeric = pd.to_numeric(df["bedrooms"], errors="coerce")
    bathrooms_numeric = pd.to_numeric(df["bathrooms"], errors="coerce")
    area = area_numeric.fillna(0).round(0).astype("Int64").astype("string")
    bedrooms = bedrooms_numeric.fillna(0).astype("Int64").astype("string")
    bathrooms = bathrooms_numeric.fillna(0).astype("Int64").astype("string")

    canonical_id = city + "|" + neighbourhood + "|" + address + "|" + area + "|" + bedrooms + "|" + bathrooms
    has_location_identity = city.ne("") | neighbourhood.ne("") | address.ne("")
    has_shape_identity = area_numeric.fillna(0).gt(0) | bedrooms_numeric.fillna(0).gt(0) | bathrooms_numeric.fillna(0).gt(0)

    source = df["source"].fillna("").astype("string").str.strip().str.lower()
    property_id = df["property_id"].fillna("").astype("string").str.strip()
    listing_url = df["listing_url"].fillna("").astype("string").str.strip()
    fallback_identity = (
        (source + "|" + property_id).where(property_id.ne(""), source + "|" + listing_url)
    )

    return canonical_id.where(has_location_identity | has_shape_identity, fallback_identity)


def _listing_mode_from_flags(is_for_sale: bool, is_for_rent: bool) -> str:
    if is_for_sale and is_for_rent:
        return "sale_rent"
    if is_for_sale:
        return "sale"
    return "rent"


def _has_positive_price(series: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    return numeric.fillna(0).gt(0)


def _first_non_null(series: pd.Series):
    non_null = series.dropna()
    if non_null.empty:
        return pd.NA
    return non_null.iloc[0]


def _build_canonical_availability(deduped: pd.DataFrame) -> pd.DataFrame:
    availability = (
        deduped.groupby("canonical_property_id")["business_type"]
        .agg(
            has_sale_by_canonical=lambda values: _business_type_has_context(values, "sale"),
            has_rent_by_canonical=lambda values: _business_type_has_context(values, "rent"),
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
            has_sale_by_property_id=lambda values: _business_type_has_context(values, "sale"),
            has_rent_by_property_id=lambda values: _business_type_has_context(values, "rent"),
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
            amenities_json=("amenities_json", "first"),
            installations_json=("installations_json", "first"),
            sale_price_brl=("sale_price_brl", _first_non_null),
            rent_price_brl=("rent_price_brl", _first_non_null),
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
