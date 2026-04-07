from __future__ import annotations

import csv
import json
import re
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Set

import requests

from scrapers.detail_utils import compact_json, count_filled_fields, merge_record
from scrapers.throttle import AdaptiveThrottle, init_metrics, record_request
from scrapers.zip_utils import extract_zip_code, extract_zip_code_from_mapping


API_URL = "https://apigw.prod.quintoandar.com.br/house-listing-search/v2/search/list"
PAGE_SIZE = 12
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


def extract_next_data(html: str) -> Dict[str, Any]:
    match = re.search(
        r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>',
        html,
        flags=re.DOTALL,
    )
    if not match:
        raise ValueError("Nao foi possivel localizar __NEXT_DATA__.")
    return json.loads(match.group(1))


def fetch_html(session: requests.Session, url: str, metrics: Dict[str, Any]) -> str:
    started_at = time.perf_counter()
    response = session.get(url, timeout=30)
    record_request(metrics, success=response.ok, elapsed_seconds=time.perf_counter() - started_at)
    response.raise_for_status()
    return response.text


def normalize_address_value(address_value: Any) -> Dict[str, Any]:
    if isinstance(address_value, dict):
        zip_code = extract_zip_code_from_mapping(address_value, "zipCode", "zipcode", "postalCode")
        return {
            "address": address_value.get("address") or address_value.get("street"),
            "city": address_value.get("city"),
            "state": address_value.get("stateName") or address_value.get("stateAcronym"),
            "zip_code": zip_code,
            "lat": address_value.get("lat"),
            "lon": address_value.get("lng"),
        }
    if isinstance(address_value, str):
        zip_code = extract_zip_code(address_value)
        return {
            "address": address_value,
            "city": None,
            "state": None,
            "zip_code": zip_code,
            "lat": None,
            "lon": None,
        }
    return {
        "address": None,
        "city": None,
        "state": None,
        "zip_code": None,
        "lat": None,
        "lon": None,
    }


def normalize_record_from_house(house: Dict[str, Any], business_type: str, fallback_id: Any = None) -> Dict[str, Any]:
    addr = normalize_address_value(house.get("address"))
    property_id = house.get("id", fallback_id)
    is_rent = business_type == "rent"
    listing_url = (
        f"https://www.quintoandar.com.br/imovel/{property_id}/alugar"
        if is_rent
        else f"https://www.quintoandar.com.br/imovel/{property_id}/comprar"
    )
    return {
        "property_id": str(property_id) if property_id is not None else None,
        "sale_price_brl": house.get("salePrice"),
        "rent_price_brl": house.get("rentPrice"),
        "total_cost_brl": house.get("totalCost"),
        "condo_iptu_brl": house.get("condoIptu"),
        "address": addr["address"],
        "city": addr["city"],
        "state": addr["state"],
        "zip_code": addr["zip_code"],
        "zipcode": addr["zip_code"],
        "lat": addr["lat"],
        "lon": addr["lon"],
        "neighbourhood": house.get("neighbourhood"),
        "region_name": house.get("regionName"),
        "property_type": house.get("type"),
        "area_m2": house.get("area"),
        "bedrooms": house.get("bedrooms"),
        "bathrooms": house.get("bathrooms"),
        "parking_spots": house.get("parkingSpots"),
        "suites": house.get("suites"),
        "is_furnished": house.get("isFurnished"),
        "for_rent": house.get("forRent"),
        "for_sale": house.get("forSale"),
        "is_primary_market": house.get("isPrimaryMarket"),
        "listing_tags": compact_json(house.get("listingTags")),
        "categories": compact_json(house.get("categories")),
        "amenities": compact_json(house.get("amenities")),
        "installations": compact_json(house.get("installations")),
        "short_rent_description": house.get("shortRentDescription"),
        "short_sale_description": house.get("shortSaleDescription"),
        "listing_url": listing_url,
    }


def extract_initial_state(session: requests.Session, metrics: Dict[str, Any], base_page_url: str, business_type: str) -> Dict[str, Any]:
    html = fetch_html(session, base_page_url, metrics)
    next_data = extract_next_data(html)
    state = next_data["props"]["pageProps"]["initialState"]
    visible = state["search"]["visibleHouses"]
    houses = state["houses"]
    visible_ids = visible["pages"]["0"]
    search_id = visible["searchId"]

    records = []
    for hid in visible_ids:
        house = houses.get(hid)
        if house:
            records.append(normalize_record_from_house(house, business_type=business_type, fallback_id=hid))

    return {"search_id": search_id, "visible_ids": [str(x) for x in visible_ids], "records": records}


def build_payload(search_id: str, blocklist: List[str], user_id: str, offset: int, business_context: str) -> Dict[str, Any]:
    return {
        "context": {
            "mapShowing": True,
            "listShowing": True,
            "userId": user_id,
            "deviceId": user_id,
            "searchId": search_id,
            "numPhotos": 12,
            "isSSR": False,
        },
        "filters": {
            "businessContext": business_context,
            "blocklist": blocklist,
            "selectedHouses": [],
            "location": {"coordinate": {}, "viewport": {}, "neighborhoods": [], "countryCode": "BR"},
            "priceRange": [],
            "specialConditions": [],
            "excludedSpecialConditions": [],
            "houseSpecs": {
                "area": {"range": {}},
                "houseTypes": [],
                "amenities": [],
                "installations": [],
                "bathrooms": {"range": {}},
                "bedrooms": {"range": {}},
                "parkingSpace": {"range": {}},
                "suites": {"range": {}},
            },
            "availability": "ANY",
            "occupancy": "ANY",
            "partnerIds": [],
            "categories": [],
            "enableFlexibleSearch": True,
        },
        "sorting": {"criteria": "RELEVANCE", "order": "DESC"},
        "pagination": {"pageSize": PAGE_SIZE, "offset": offset},
        "slug": "sao-paulo-sp-brasil",
        "fields": [
            "id", "coverImage", "rent", "totalCost", "salePrice", "iptuPlusCondominium", "area",
            "imageList", "imageCaptionList", "address", "regionName", "city", "visitStatus",
            "activeSpecialConditions", "type", "forRent", "forSale", "isPrimaryMarket", "bedrooms",
            "parkingSpaces", "suites", "listingTags", "yield", "yieldStrategy", "neighbourhood",
            "categories", "bathrooms", "isFurnished", "installations", "amenities",
            "shortRentDescription", "shortSaleDescription",
        ],
        "locationDescriptions": [{"description": "sao-paulo-sp-brasil"}],
        "topics": [],
    }


def fetch_more(
    session: requests.Session,
    metrics: Dict[str, Any],
    search_id: str,
    blocklist: List[str],
    user_id: str,
    offset: int,
    business_context: str,
) -> Dict[str, Any]:
    payload = build_payload(search_id, blocklist, user_id, offset, business_context=business_context)
    started_at = time.perf_counter()
    response = session.post(API_URL, headers=HEADERS, json=payload, timeout=30)
    record_request(metrics, success=response.ok, elapsed_seconds=time.perf_counter() - started_at)
    response.raise_for_status()
    return response.json()


def parse_api_hits(data: Dict[str, Any], business_type: str) -> List[Dict[str, Any]]:
    out = []
    raw_hits = ((data.get("hits") or {}).get("hits")) or []
    if not isinstance(raw_hits, list):
        return out
    for hit in raw_hits:
        src = hit.get("_source") or {}
        if isinstance(src, dict) and src.get("id") is not None:
            out.append(normalize_record_from_house(src, business_type=business_type))
    return out


def parse_detail_html(html: str, business_type: str) -> dict[str, Any]:
    next_data = extract_next_data(html)
    state = next_data["props"]["pageProps"]["initialState"]
    house_info = (((state.get("house") or {}).get("houseInfo")) or {})
    generated_description = house_info.get("generatedDescription") or {}
    address = normalize_address_value(house_info.get("address"))
    photos = house_info.get("photos") or []
    photo_urls = [
        f"https://images.quintoandar.com.br/{photo.get('url')}"
        for photo in photos
        if isinstance(photo, dict) and photo.get("url")
    ]
    house_agents = ((house_info.get("listings") or [{}])[0] or {}).get("houseAgents") or []

    detail = {
        "display_id": house_info.get("displayId"),
        "zip_code": address["zip_code"],
        "zipcode": address["zip_code"],
        "address": address["address"],
        "city": address["city"],
        "state": address["state"],
        "lat": address["lat"],
        "lon": address["lon"],
        "listing_updated_at": house_info.get("lastPublishedDate"),
        "listing_status": house_info.get("status"),
        "listing_created_at": ((house_info.get("listings") or [{}])[0] or {}).get("firstPublicationDate"),
        "total_area_m2": house_info.get("area"),
        "accepts_pets": house_info.get("acceptsPets"),
        "has_furniture": house_info.get("hasFurniture"),
        "furnished": house_info.get("hasFurniture"),
        "home_protection_brl": house_info.get("homeProtection"),
        "tenant_service_fee_brl": house_info.get("tenantServiceFee"),
        "rental_guarantee_min_brl": ((house_info.get("rentalGuarantee") or {}).get("minValue")),
        "rental_guarantee_max_brl": ((house_info.get("rentalGuarantee") or {}).get("maxValue")),
        "condo_type": house_info.get("condoType"),
        "condo_fee_brl": house_info.get("condoPrice"),
        "iptu_brl": house_info.get("iptu"),
        "main_image_url": photo_urls[0] if photo_urls else None,
        "gallery_urls_json": compact_json(photo_urls) if photo_urls else None,
        "amenities_json": compact_json(house_info.get("amenities")),
        "comfort_commodities_json": compact_json(house_info.get("comfortCommodities")),
        "practicality_commodities_json": compact_json(house_info.get("practicalityCommodities")),
        "installations_json": compact_json(house_info.get("installations")),
        "nearby_places_json": compact_json(house_info.get("placesNearby")),
        "house_agents_json": compact_json(house_agents),
        "long_description": generated_description.get("longDescription"),
        "description": generated_description.get("longDescription")
        or generated_description.get("shortRentDescription")
        or generated_description.get("shortSaleDescription"),
        "short_rent_description": generated_description.get("shortRentDescription"),
        "short_sale_description": generated_description.get("shortSaleDescription"),
    }
    if business_type == "rent":
        detail["rent_price_brl"] = house_info.get("rentPrice")
    else:
        detail["sale_price_brl"] = house_info.get("salePrice")
    return detail


def enrich_records_with_details(
    records: List[Dict[str, Any]],
    session: requests.Session,
    metrics: Dict[str, Any],
    business_type: str,
    min_delay_seconds: float,
    max_delay_seconds: float,
    target_delay_seconds: float | None,
    max_consecutive_failures: int,
) -> List[Dict[str, Any]]:
    if not records:
        return records
    throttle = AdaptiveThrottle(min_delay_seconds, max_delay_seconds, target_delay_seconds)
    consecutive_failures = 0
    keys = (
        "zip_code",
        "lat",
        "lon",
        "listing_updated_at",
        "listing_status",
        "main_image_url",
        "gallery_urls_json",
        "long_description",
        "amenities_json",
        "house_agents_json",
    )
    enriched: List[Dict[str, Any]] = []
    for record in records:
        url = record.get("listing_url")
        if not url:
            enriched.append(record)
            continue
        print(f"[INFO] detalhe_quinto url={url}")
        try:
            metrics["detail_requests"] += 1
            html = fetch_html(session, str(url), metrics)
            detail = parse_detail_html(html, business_type=business_type)
            metrics["detail_successes"] += 1
            metrics["detail_fields_filled"] += count_filled_fields(detail, keys)
            merged = merge_record(record, detail)
            enriched.append(merged)
            consecutive_failures = 0
        except requests.HTTPError as exc:
            metrics["detail_failures"] += 1
            consecutive_failures += 1
            status_code = exc.response.status_code if exc.response is not None else None
            if status_code == 404:
                next_delay = throttle.success()
                print(f"[WARN] detalhe_quinto_not_found status=404 next_delay={next_delay:.2f}s")
            else:
                next_delay = throttle.failure(status_code=status_code)
                metrics["detail_backoffs"] += 1
                print(f"[WARN] detalhe_quinto_falhou error={exc} next_delay={next_delay:.2f}s")
            enriched.append(record)
            if consecutive_failures >= max_consecutive_failures:
                enriched.extend(records[len(enriched):])
                break
        except Exception as exc:
            metrics["detail_failures"] += 1
            consecutive_failures += 1
            next_delay = throttle.failure()
            metrics["detail_backoffs"] += 1
            print(f"[WARN] detalhe_quinto_falhou error={exc} next_delay={next_delay:.2f}s")
            enriched.append(record)
            if consecutive_failures >= max_consecutive_failures:
                enriched.extend(records[len(enriched):])
                break
        slept = throttle.sleep()
        metrics["throttle_sleep_seconds"] += slept
    return enriched


def scrape_all(
    *,
    base_page_url: str,
    business_type: str,
    max_batches: int,
    min_delay_seconds: float,
    max_delay_seconds: float,
    target_delay_seconds: float | None,
    max_consecutive_failures: int,
    early_stop_on_low_yield: int,
    detail_min_delay_seconds: float,
    detail_max_delay_seconds: float,
    detail_target_delay_seconds: float | None,
    detail_max_consecutive_failures: int,
    label: str,
) -> List[Dict[str, Any]]:
    session = requests.Session()
    session.headers.update(HEADERS)
    metrics = init_metrics(label)
    throttle = AdaptiveThrottle(min_delay_seconds, max_delay_seconds, target_delay_seconds)

    initial = extract_initial_state(session, metrics, base_page_url=base_page_url, business_type=business_type)
    search_id = initial["search_id"]
    seen_ids: Set[str] = set()
    all_records: List[Dict[str, Any]] = []
    low_yield_batches = 0
    consecutive_failures = 0

    for rec in initial["records"]:
        pid = rec["property_id"]
        if pid not in seen_ids:
            seen_ids.add(pid)
            all_records.append(rec)
    metrics["items_seen"] += len(initial["records"])
    metrics["items_kept"] += len(initial["records"])

    blocklist = list(initial["visible_ids"])
    user_id = str(uuid.uuid4())
    business_context = "RENT" if business_type == "rent" else "SALE"

    for batch in range(1, max_batches + 1):
        offset = (batch - 1) * PAGE_SIZE
        try:
            data = fetch_more(session, metrics, search_id, blocklist, user_id, offset, business_context)
        except requests.RequestException:
            consecutive_failures += 1
            throttle.failure()
            if consecutive_failures >= max_consecutive_failures:
                metrics["stop_reason"] = f"max_consecutive_failures:{consecutive_failures}"
                break
            slept = throttle.sleep()
            metrics["throttle_sleep_seconds"] += slept
            continue

        recs = parse_api_hits(data, business_type=business_type)
        metrics["pages_processed"] += 1
        metrics["items_seen"] += len(recs)
        if not recs:
            metrics["stop_reason"] = "empty_batch"
            break

        new_count = 0
        for rec in recs:
            pid = rec["property_id"]
            if pid and pid not in seen_ids:
                seen_ids.add(pid)
                all_records.append(rec)
                blocklist.append(pid)
                new_count += 1
        metrics["items_kept"] += new_count
        consecutive_failures = 0
        if new_count == 0:
            metrics["stop_reason"] = "no_new_records"
            break
        if early_stop_on_low_yield and new_count <= early_stop_on_low_yield:
            low_yield_batches += 1
        else:
            low_yield_batches = 0
        if low_yield_batches >= 2:
            metrics["stop_reason"] = f"low_yield:{new_count}"
            break
        returned_search_id = data.get("search_id")
        if returned_search_id:
            search_id = returned_search_id
        slept = throttle.sleep()
        metrics["throttle_sleep_seconds"] += slept

    enriched = enrich_records_with_details(
        all_records,
        session=session,
        metrics=metrics,
        business_type=business_type,
        min_delay_seconds=detail_min_delay_seconds,
        max_delay_seconds=detail_max_delay_seconds,
        target_delay_seconds=detail_target_delay_seconds,
        max_consecutive_failures=detail_max_consecutive_failures,
    )
    if metrics["stop_reason"] is None:
        metrics["stop_reason"] = "max_batches_reached"
    print(f"[INFO] metrics={json.dumps(metrics, ensure_ascii=False)}")
    return enriched


def save_csv(records: List[Dict[str, Any]], filename: str) -> None:
    if not records:
        return
    Path(filename).parent.mkdir(parents=True, exist_ok=True)
    fieldnames = sorted({key for record in records for key in record.keys()})
    with open(filename, "w", newline="", encoding="utf-8-sig") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)
