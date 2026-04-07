from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
import re
from typing import Any, Dict, Iterable, List, Mapping, Optional

import pandas as pd


CANONICAL_COLUMNS = [
    "source",
    "business_type",
    "property_id",
    "listing_url",
    "display_id",
    "description",
    "long_description",
    "address",
    "neighbourhood",
    "city",
    "state",
    "zip_code",
    "lat",
    "lon",
    "property_type",
    "area_m2",
    "total_area_m2",
    "bedrooms",
    "bathrooms",
    "parking_spots",
    "suites",
    "floor",
    "furnished",
    "accepts_pets",
    "has_furniture",
    "sale_price_brl",
    "rent_price_brl",
    "total_price_brl",
    "condo_fee_brl",
    "iptu_brl",
    "home_protection_brl",
    "tenant_service_fee_brl",
    "rental_guarantee_min_brl",
    "rental_guarantee_max_brl",
    "condo_type",
    "listing_created_at",
    "listing_updated_at",
    "listing_status",
    "seller_name",
    "seller_id",
    "seller_public_account_id",
    "seller_type",
    "seller_professional",
    "advertiser_name",
    "advertiser_id",
    "main_image_url",
    "gallery_urls_json",
    "amenities_json",
    "practicality_commodities_json",
    "comfort_commodities_json",
    "installations_json",
    "condominium_name",
    "condominium_id",
    "condominium_url",
    "condominium_amenities_json",
    "features_json",
    "pois_json",
    "nearby_places_json",
    "house_agents_json",
    "currency",
    "images_count",
    "scraped_at",
]

FLOAT_COLUMNS = [
    "lat",
    "lon",
    "area_m2",
    "total_area_m2",
]

INT_COLUMNS = [
    "bedrooms",
    "bathrooms",
    "parking_spots",
    "suites",
    "floor",
    "sale_price_brl",
    "rent_price_brl",
    "total_price_brl",
    "condo_fee_brl",
    "iptu_brl",
    "home_protection_brl",
    "tenant_service_fee_brl",
    "rental_guarantee_min_brl",
    "rental_guarantee_max_brl",
    "images_count",
]

BOOL_COLUMNS = [
    "furnished",
    "accepts_pets",
    "has_furniture",
    "seller_professional",
]


def _is_missing(value: Any) -> bool:
    return value is None or value == "" or pd.isna(value)


def _to_float(value: Any) -> Optional[float]:
    if _is_missing(value):
        return None

    if isinstance(value, str):
        cleaned = value.strip()
        if not cleaned:
            return None
        cleaned = cleaned.replace(".", "").replace(",", ".")
        cleaned = re.sub(r"[^\d\-.]", "", cleaned)
        if not cleaned:
            return None
        value = cleaned

    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_int(value: Any) -> Optional[int]:
    if _is_missing(value):
        return None

    if isinstance(value, str):
        digits = re.sub(r"[^\d]", "", value)
        if not digits:
            return None
        return int(digits)

    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _to_bool(value: Any) -> Optional[bool]:
    if _is_missing(value):
        return None
    if isinstance(value, bool):
        return value

    normalized = str(value).strip().lower()
    truthy = {"1", "true", "sim", "yes"}
    falsy = {"0", "false", "nao", "não", "no"}

    if normalized in truthy:
        return True
    if normalized in falsy:
        return False
    return None


def _pick(record: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        value = record.get(key)
        if not _is_missing(value):
            return value
    return None


def _build_listing_url(record: Mapping[str, Any], source: str) -> Any:
    property_id = _pick(record, "property_id", "sku", "ad_id")

    if source == "lopes" and not _is_missing(property_id):
        return f"https://www.lopes.com.br/imovel/{property_id}"

    return _pick(record, "listing_url", "url")


def _build_address(record: Mapping[str, Any]) -> Any:
    return _pick(record, "address", "street")


def _build_state(record: Mapping[str, Any]) -> Any:
    state = _pick(record, "state")
    city = _pick(record, "city")

    if state == "SP":
        return "São Paulo"

    if not _is_missing(state):
        return state

    if city == "São Paulo":
        return "São Paulo"

    return state


def _build_description(record: Mapping[str, Any]) -> Any:
    title = _pick(record, "title")
    description = _pick(
        record,
        "description",
        "short_sale_description",
        "short_rent_description",
    )

    if not _is_missing(title) and not _is_missing(description):
        return f"{title}\n\n{description}"

    return title if not _is_missing(title) else description


def _normalize_property_type(value: Any, source: str) -> Any:
    if _is_missing(value):
        return value

    normalized_text = str(value).strip()
    if source == "olx":
        mapping = {
            "Casas": "Casa",
            "Apartamentos": "Apartamento",
        }
        lowered = normalized_text.lower()
        if "apartamento" in lowered:
            return "Apartamento"
        if "casa" in lowered:
            return "Casa"
        return mapping.get(normalized_text, normalized_text)

    return normalized_text


def _extract_labeled_money(text: Any, label: str) -> Optional[int]:
    if _is_missing(text):
        return None

    pattern = rf"{re.escape(label)}[^\r\n:]*:\s*R\$\s*([\d\.]+)"
    match = re.search(pattern, str(text), flags=re.IGNORECASE)
    if not match:
        return None

    return _to_int(match.group(1))


def _load_amenities_items(raw_value: Any) -> list[dict[str, Any]]:
    if _is_missing(raw_value):
        return []
    if isinstance(raw_value, list):
        return [item for item in raw_value if isinstance(item, dict)]
    if not isinstance(raw_value, str):
        return []
    try:
        parsed = json.loads(raw_value)
    except Exception:
        return []
    if isinstance(parsed, list):
        return [item for item in parsed if isinstance(item, dict)]
    return []


def _amenity_name(item: Mapping[str, Any]) -> str:
    return str(item.get("name") or "").strip().lower()


def _amenity_label(item: Mapping[str, Any]) -> str:
    return str(item.get("label") or "").strip().lower()


def _amenity_value(item: Mapping[str, Any]) -> Any:
    return item.get("value")


def _amenity_values_labels(item: Mapping[str, Any]) -> list[str]:
    raw_values = item.get("values")
    if not isinstance(raw_values, list):
        return []
    labels: list[str] = []
    for raw_item in raw_values:
        if isinstance(raw_item, dict):
            label = raw_item.get("label")
            if not _is_missing(label):
                labels.append(str(label).strip().lower())
    return labels


def _find_amenity_item(items: list[dict[str, Any]], *names: str) -> Optional[dict[str, Any]]:
    wanted = {name.strip().lower() for name in names}
    for item in items:
        if _amenity_name(item) in wanted:
            return item
    return None


def _find_amenity_by_label_contains(items: list[dict[str, Any]], needle: str) -> Optional[dict[str, Any]]:
    wanted = needle.strip().lower()
    for item in items:
        if wanted in _amenity_label(item):
            return item
    return None


def _amenities_have_label(items: list[dict[str, Any]], amenity_name: str, label_text: str) -> bool:
    wanted_name = amenity_name.strip().lower()
    wanted_label = label_text.strip().lower()
    for item in items:
        if _amenity_name(item) != wanted_name:
            continue
        if wanted_label in str(_amenity_value(item) or "").strip().lower():
            return True
        if wanted_label in _amenity_values_labels(item):
            return True
    return False


def _value_from_amenities(items: list[dict[str, Any]], *names: str, label_contains: str | None = None) -> Any:
    item = _find_amenity_item(items, *names)
    if item is None and label_contains:
        item = _find_amenity_by_label_contains(items, label_contains)
    if item is None:
        return None
    return _amenity_value(item)


def _finalize_normalized_frame(df: pd.DataFrame) -> pd.DataFrame:
    for column in CANONICAL_COLUMNS:
        if column not in df.columns:
            df[column] = pd.NA

    df = df[CANONICAL_COLUMNS].copy()
    df["property_id"] = df["property_id"].astype("string")
    df["listing_url"] = df["listing_url"].astype("string")
    df["source"] = df["source"].astype("string")
    df["business_type"] = df["business_type"].astype("string")
    for column in FLOAT_COLUMNS:
        df[column] = pd.to_numeric(df[column], errors="coerce").astype("Float64")
    for column in INT_COLUMNS:
        df[column] = pd.to_numeric(df[column], errors="coerce").astype("Int64")
    for column in BOOL_COLUMNS:
        df[column] = df[column].map(_to_bool).astype("boolean")
    return df


def _normalize_record(record: Mapping[str, Any], source: str, business_type: str) -> Dict[str, Any]:
    amenities_items = _load_amenities_items(_pick(record, "amenities_json", "amenities"))
    sale_price = _to_int(_pick(record, "sale_price_brl"))
    rent_price = _to_int(_pick(record, "rent_price_brl"))
    generic_price = _to_int(_pick(record, "price_value", "price"))
    total_cost = _to_int(_pick(record, "total_cost_brl"))
    sub_price = _pick(record, "sub_price")
    condo_fee = _to_int(_pick(record, "condo_fee_brl", "condo_iptu_brl", "condo_fee"))
    iptu = _to_int(_pick(record, "iptu_brl", "iptu"))
    property_type = _pick(record, "property_type", "type")
    real_estate_type = _pick(record, "real_estate_type")
    area_m2 = _to_float(_pick(record, "area_m2", "area"))
    total_area_m2 = _to_float(_pick(record, "total_area_m2"))
    bedrooms = _to_int(_pick(record, "bedrooms"))
    bathrooms = _to_int(_pick(record, "bathrooms"))
    parking_spots = _to_int(_pick(record, "parking_spots", "parking"))
    suites = _to_int(_pick(record, "suites"))
    floor = _to_int(_pick(record, "floor"))
    furnished = _to_bool(_pick(record, "furnished", "is_furnished"))
    accepts_pets = _to_bool(_pick(record, "accepts_pets"))
    has_furniture = _to_bool(_pick(record, "has_furniture"))

    if source == "lopes":
        condo_fee = _extract_labeled_money(sub_price, "Condom") or condo_fee
        iptu = _extract_labeled_money(sub_price, "IPTU") or iptu

    if amenities_items:
        condo_fee = condo_fee if condo_fee is not None else _to_int(_value_from_amenities(amenities_items, "condominium", "condominio"))
        iptu = iptu if iptu is not None else _to_int(_value_from_amenities(amenities_items, "iptu"))
        area_m2 = area_m2 if area_m2 is not None else _to_float(_value_from_amenities(amenities_items, "size"))
        total_area_m2 = total_area_m2 if total_area_m2 is not None else area_m2
        bedrooms = bedrooms if bedrooms is not None else _to_int(_value_from_amenities(amenities_items, "rooms"))
        bathrooms = bathrooms if bathrooms is not None else _to_int(_value_from_amenities(amenities_items, "bathrooms"))
        parking_spots = parking_spots if parking_spots is not None else _to_int(_value_from_amenities(amenities_items, "garage_spaces"))
        suites = suites if suites is not None else _to_int(_value_from_amenities(amenities_items, "suites", label_contains="suíte"))
        floor = floor if floor is not None else _to_int(_value_from_amenities(amenities_items, "floor", label_contains="andar"))
        property_type = property_type or _value_from_amenities(amenities_items, "category", label_contains="categoria")
        real_estate_type = real_estate_type or _value_from_amenities(amenities_items, "real_estate_type", "re_types", label_contains="tipo")
        property_type = property_type or real_estate_type
        if furnished is None and _amenities_have_label(amenities_items, "furnished", "mobiliado"):
            furnished = True
        if has_furniture is None and (furnished is True or _amenities_have_label(amenities_items, "furnished", "mobiliado")):
            has_furniture = True
        if accepts_pets is None and _amenities_have_label(amenities_items, "re_complex_features", "permitido animais"):
            accepts_pets = True

    if business_type == "sale" and sale_price is None:
        sale_price = generic_price
    if business_type == "rent" and rent_price is None:
        rent_price = generic_price

    listing_created_at = _pick(record, "listing_created_at", "created_at")

    return {
        "source": source,
        "business_type": business_type,
        "property_id": str(_pick(record, "property_id", "sku", "ad_id") or ""),
        "listing_url": _build_listing_url(record, source),
        "display_id": _pick(record, "display_id"),
        "description": _build_description(record),
        "long_description": _pick(record, "long_description"),
        "address": _build_address(record),
        "neighbourhood": _pick(record, "neighbourhood"),
        "city": _pick(record, "city"),
        "state": _build_state(record),
        "zip_code": _pick(record, "zip_code", "zipcode"),
        "lat": _to_float(_pick(record, "lat")),
        "lon": _to_float(_pick(record, "lon", "lng")),
        "property_type": _normalize_property_type(property_type, source=source),
        "area_m2": area_m2,
        "total_area_m2": total_area_m2,
        "bedrooms": bedrooms,
        "bathrooms": bathrooms,
        "parking_spots": parking_spots,
        "suites": suites,
        "floor": floor,
        "furnished": furnished,
        "accepts_pets": accepts_pets,
        "has_furniture": has_furniture,
        "sale_price_brl": sale_price,
        "rent_price_brl": rent_price,
        "total_price_brl": total_cost or sale_price or rent_price,
        "condo_fee_brl": condo_fee,
        "iptu_brl": iptu,
        "home_protection_brl": _to_int(_pick(record, "home_protection_brl")),
        "tenant_service_fee_brl": _to_int(_pick(record, "tenant_service_fee_brl")),
        "rental_guarantee_min_brl": _to_int(_pick(record, "rental_guarantee_min_brl")),
        "rental_guarantee_max_brl": _to_int(_pick(record, "rental_guarantee_max_brl")),
        "condo_type": _pick(record, "condo_type"),
        "listing_created_at": listing_created_at,
        "listing_updated_at": _pick(record, "listing_updated_at", "updated_at"),
        "listing_status": _pick(record, "listing_status"),
        "seller_name": _pick(record, "seller_name"),
        "seller_id": _pick(record, "seller_id"),
        "seller_public_account_id": _pick(record, "seller_public_account_id"),
        "seller_type": _pick(record, "seller_type"),
        "seller_professional": _to_bool(_pick(record, "seller_professional", "is_professional")),
        "advertiser_name": _pick(record, "advertiser_name", "company_name"),
        "advertiser_id": _pick(record, "advertiser_id", "company_id", "advertiser_id"),
        "main_image_url": _pick(record, "main_image_url", "thumbnail"),
        "gallery_urls_json": _pick(record, "gallery_urls_json", "images"),
        "amenities_json": _pick(record, "amenities_json", "amenities"),
        "practicality_commodities_json": _pick(record, "practicality_commodities_json"),
        "comfort_commodities_json": _pick(record, "comfort_commodities_json"),
        "installations_json": _pick(record, "installations_json", "installations"),
        "condominium_name": _pick(record, "condominium_name"),
        "condominium_id": _pick(record, "condominium_id"),
        "condominium_url": _pick(record, "condominium_url"),
        "condominium_amenities_json": _pick(record, "condominium_amenities_json"),
        "features_json": _pick(record, "features_json"),
        "pois_json": _pick(record, "pois_json"),
        "nearby_places_json": _pick(record, "nearby_places_json"),
        "house_agents_json": _pick(record, "house_agents_json"),
        "currency": _pick(record, "currency") or "BRL",
        "images_count": _to_int(_pick(record, "images_count")) or 0,
        "scraped_at": datetime.now(timezone.utc).isoformat(),
    }


def _records_to_frame(records: Iterable[Mapping[str, Any]], source: str, business_type: str) -> pd.DataFrame:
    normalized = [_normalize_record(record, source=source, business_type=business_type) for record in records]
    if not normalized:
        return pd.DataFrame(columns=CANONICAL_COLUMNS)
    return pd.DataFrame(normalized)


def normalize_data(scraped_batches: Dict[str, List[Dict[str, Any]]]) -> pd.DataFrame:
    """
    Normaliza os dados vindos dos scrapers em um schema unico.

    Espera um dicionario no formato:
    {
        "olx_venda": [...],
        "quinto_venda": [...],
    }
    """
    frames: List[pd.DataFrame] = []

    for batch_name, records in scraped_batches.items():
        if not records:
            continue

        batch_lower = batch_name.lower()
        source = batch_lower.split("_", 1)[0]
        business_type = "rent" if "aluguel" in batch_lower else "sale"
        frames.append(_records_to_frame(records, source=source, business_type=business_type))

    if not frames:
        return pd.DataFrame(columns=CANONICAL_COLUMNS)

    df = pd.concat(frames, ignore_index=True)

    return _finalize_normalized_frame(df)


def _infer_source_and_business(file_path: Path) -> tuple[str, str]:
    stem = file_path.stem.lower()
    source = stem.split("_", 1)[0]
    business_type = "rent" if "aluguel" in stem else "sale"
    return source, business_type


def load_and_normalize(files: List[str]) -> pd.DataFrame:
    frames: List[pd.DataFrame] = []

    for file_name in files:
        file_path = Path(file_name)
        if not file_path.exists():
            print(f"[WARN] arquivo nao encontrado: {file_path}")
            continue

        try:
            raw_df = pd.read_csv(file_path)
        except Exception as exc:
            print(f"[WARN] falha ao ler {file_path}: {exc}")
            continue

        source, business_type = _infer_source_and_business(file_path)
        records = raw_df.to_dict(orient="records")
        normalized = _records_to_frame(records, source=source, business_type=business_type)

        if normalized.empty:
            print(f"[INFO] arquivo sem registros aproveitaveis: {file_path}")
            continue

        frames.append(normalized)

    if not frames:
        return pd.DataFrame(columns=CANONICAL_COLUMNS)

    df = pd.concat(frames, ignore_index=True)
    return _finalize_normalized_frame(df)
