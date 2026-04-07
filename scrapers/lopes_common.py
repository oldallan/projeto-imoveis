from __future__ import annotations

import csv
import json
import re
import time
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode

from curl_cffi import requests

from scrapers.detail_utils import absolute_url, compact_json, count_filled_fields, merge_record
from scrapers.throttle import AdaptiveThrottle, init_metrics, record_request
from scrapers.zip_utils import extract_zip_code, extract_zip_code_from_mapping


BASE_SITE_URL = "https://www.lopes.com.br"
DETAIL_API_PREFIX = "https://apis.lopes.com.br/portal-product/v2/products-improved/"
DETAIL_PROGRESS_EVERY = 25


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


def fetch_json(
    url: str,
    session: requests.Session,
    metrics: Dict[str, Any],
    retries: int,
    retry_min_delay_seconds: float,
    retry_max_delay_seconds: float,
) -> Optional[Dict[str, Any]]:
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Origin": "https://www.lopes.com.br",
        "Referer": "https://www.lopes.com.br/",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/146.0.0.0 Safari/537.36"
        ),
    }
    for attempt in range(retries):
        started_at = time.perf_counter()
        try:
            response = session.get(url, headers=headers, impersonate="chrome110", timeout=30)
            elapsed_seconds = time.perf_counter() - started_at
            if response.status_code == 200:
                record_request(metrics, success=True, elapsed_seconds=elapsed_seconds, retries=attempt)
                return response.json()
            record_request(metrics, success=False, elapsed_seconds=elapsed_seconds, retries=attempt)
        except Exception:
            record_request(metrics, success=False, elapsed_seconds=time.perf_counter() - started_at, retries=attempt)
        sleep_seconds = min(retry_max_delay_seconds, retry_min_delay_seconds * (attempt + 1))
        time.sleep(sleep_seconds)
        metrics["throttle_sleep_seconds"] += sleep_seconds
    return None


def fetch_html(session: requests.Session, url: str, metrics: Dict[str, Any]) -> str | None:
    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": "https://www.lopes.com.br/",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/146.0.0.0 Safari/537.36"
        ),
    }
    started_at = time.perf_counter()
    try:
        response = session.get(url, headers=headers, impersonate="chrome110", timeout=30)
        record_request(metrics, success=response.status_code == 200, elapsed_seconds=time.perf_counter() - started_at)
        if response.status_code != 200:
            return None
        return response.text
    except Exception:
        record_request(metrics, success=False, elapsed_seconds=time.perf_counter() - started_at)
        return None


def build_api_url(base_api_url: str, page: int, lines_per_page: int = 23) -> str:
    params = {"page": page - 1, "linesPerPage": lines_per_page}
    return f"{base_api_url}?{urlencode(params)}"


def build_listing_url(item: Dict[str, Any], business_type: str) -> Optional[str]:
    pid = item.get("id") or item.get("sku")
    ptype = item.get("type")
    operation = "venda" if business_type == "sale" else "aluguel"
    if pid and ptype:
        return f"{BASE_SITE_URL}/imovel/{str(ptype).strip().lower()}-{operation}-sao-paulo-{pid}"
    return None


def parse_photos(photo_list: List[Dict[str, Any]]) -> Dict[str, Any]:
    urls = []
    for photo in photo_list or []:
        for key in ("url", "href", "src", "link", "imageUrl"):
            value = photo.get(key)
            if value:
                urls.append(absolute_url(value, BASE_SITE_URL))
                break
    return {
        "images_count": len(urls),
        "images": " | ".join(urls) if urls else None,
        "main_image_url": urls[0] if urls else None,
        "gallery_urls_json": compact_json(urls) if urls else None,
    }


def find_nested_product(data: Any) -> Dict[str, Any]:
    if isinstance(data, dict):
        product = data.get("product")
        if isinstance(product, dict):
            return product
        nested = data.get("b")
        if isinstance(nested, dict):
            found = find_nested_product(nested)
            if found:
                return found
        for value in data.values():
            found = find_nested_product(value)
            if found:
                return found
    elif isinstance(data, list):
        for item in data:
            found = find_nested_product(item)
            if found:
                return found
    return {}


def parse_products(data: Dict[str, Any], business_type: str) -> List[Dict[str, Any]]:
    records = []
    items = (data.get("products") or {}).get("content", [])
    for item in items:
        try:
            attributes = item.get("attributes", [])
            location = item.get("locationDTO", {}) or {}
            photos_info = parse_photos(item.get("photo", []))
            price_text = item.get("sellingPriceFormat") if business_type == "sale" else (
                item.get("rentPriceFormat") or item.get("sellingPriceFormat") or item.get("priceFormat")
            )
            record = {
                "property_id": item.get("id"),
                "sku": item.get("sku"),
                "title": item.get("imageAlternateText") or item.get("name") or item.get("label"),
                "type": item.get("type"),
                "property_type": item.get("type"),
                "product_type": item.get("productType"),
                "listing_type": item.get("dealType"),
                "division_unit_type": item.get("divisionUnitType"),
                "label": item.get("label"),
                "price": price_text,
                "price_value": parse_money_to_number(price_text),
                "price_from": item.get("priceFormat"),
                "sub_price": item.get("subPrice"),
                "condo_fee": parse_money_to_number(item.get("subPrice")) if item.get("subPrice") else None,
                "area": extract_attr_number(attributes, "area_attr"),
                "bedrooms": extract_attr_number(attributes, "bedroom_attr"),
                "bathrooms": extract_attr_number(attributes, "bathroom_attr"),
                "parking": extract_attr_number(attributes, "parking_lots_attr"),
                "street": item.get("street") or location.get("address"),
                "number": location.get("number"),
                "city": location.get("city"),
                "state": location.get("state"),
                "neighbourhood": item.get("neighborhood") or location.get("neighborhood"),
                "zipcode": location.get("zipCode"),
                "lat": item.get("lat"),
                "lng": item.get("lng"),
                "geolocation": item.get("geolocation"),
                "url": build_listing_url(item, business_type=business_type),
                "listing_url": build_listing_url(item, business_type=business_type),
                "images_count": photos_info["images_count"],
                "images": photos_info["images"],
                "main_image_url": photos_info["main_image_url"],
                "gallery_urls_json": photos_info["gallery_urls_json"],
                "company_name": (item.get("company") or {}).get("name"),
                "company_type": (item.get("company") or {}).get("type"),
                "company_id": (item.get("company") or {}).get("company_id"),
                "company_short_name": (item.get("company") or {}).get("short_name"),
                "is_bargain": item.get("isBargain"),
                "tag": item.get("tag"),
            }
            records.append(record)
        except Exception:
            continue
    return records


def parse_detail_html(html: str) -> dict[str, Any]:
    match = re.search(r'<script id="ng-state" type="application/json">\s*(\{[\s\S]*?\})\s*</script>', html)
    if not match:
        return {}
    data = json.loads(match.group(1))
    product = find_nested_product(data)
    if not product:
        return {}
    address = product.get("address") or {}
    if isinstance(address, str):
        address = {"formatted": address}
    elif not isinstance(address, dict):
        address = {}
    attributes = product.get("attributes") or []
    condominium = product.get("condominium") or {}
    prices = product.get("prices") or {}
    features = product.get("features") or []
    pois = product.get("pois") or []
    photos = product.get("photos") or []
    photo_urls = []
    for photo in photos:
        if isinstance(photo, dict):
            for key in ("url", "href", "src", "imageUrl", "link"):
                value = photo.get(key)
                if value:
                    photo_urls.append(absolute_url(value, BASE_SITE_URL))
                    break
    advertiser = product.get("advertiser") or {}
    listing_owner = product.get("listingOwner") or {}
    seo = product.get("seo") or {}
    zip_code = (
        extract_zip_code_from_mapping(address, "zipCode", "zipcode", "postalCode")
        or extract_zip_code(address.get("formatted"), address.get("street"), product.get("description"), html)
    )
    detail = {
        "description": product.get("description"),
        "address": address.get("formatted") or address.get("street"),
        "street": address.get("street"),
        "city": address.get("city"),
        "neighbourhood": address.get("neighborhood"),
        "state": address.get("state"),
        "zip_code": zip_code,
        "total_area_m2": extract_attr_number(attributes, "total_area_attr") or extract_attr_number(attributes, "area_attr"),
        "area": extract_attr_number(attributes, "area_attr"),
        "suites": extract_attr_number(attributes, "suite_attr"),
        "bedrooms": extract_attr_number(attributes, "bedroom_attr"),
        "bathrooms": extract_attr_number(attributes, "bathroom_attr"),
        "parking": extract_attr_number(attributes, "parking_lots_attr"),
        "condominium_name": condominium.get("name"),
        "condominium_id": condominium.get("id"),
        "condominium_url": absolute_url(condominium.get("url"), BASE_SITE_URL),
        "condominium_amenities_json": compact_json(condominium.get("amenities")),
        "features_json": compact_json(features),
        "pois_json": compact_json(pois),
        "advertiser_name": advertiser.get("name") or advertiser.get("shortName"),
        "advertiser_id": listing_owner.get("id"),
        "seller_type": listing_owner.get("type"),
        "main_image_url": photo_urls[0] if photo_urls else None,
        "gallery_urls_json": compact_json(photo_urls) if photo_urls else None,
        "condo_fee_brl": prices.get("condominium"),
        "sale_price_brl": prices.get("sale"),
        "rent_price_brl": prices.get("rent"),
        "total_price_brl": prices.get("fullMonthlyPrice"),
        "listing_url": absolute_url(seo.get("url"), BASE_SITE_URL),
    }
    return detail


def detail_has_useful_data(detail: Dict[str, Any]) -> bool:
    useful_keys = (
        "description",
        "address",
        "zip_code",
        "total_area_m2",
        "suites",
        "condominium_name",
        "gallery_urls_json",
        "main_image_url",
    )
    return any(detail.get(key) not in (None, "", "[]") for key in useful_keys)


def parse_detail_payload(payload: Dict[str, Any]) -> dict[str, Any]:
    product = payload.get("product") if isinstance(payload, dict) else {}
    if not isinstance(product, dict):
        return {}

    address = product.get("address")
    if isinstance(address, str):
        address = {"formatted": address}
    elif not isinstance(address, dict):
        address = {}

    attributes = product.get("attributes") or []
    condominium = product.get("condominium") or {}
    prices = product.get("prices") or {}
    features = product.get("features") or []
    pois = product.get("pois") or []
    photos = product.get("photos") or []
    advertiser = product.get("advertiser") or {}
    listing_owner = product.get("listingOwner") or {}
    seo = product.get("seo") or {}
    photo_urls = []
    for photo in photos:
        if isinstance(photo, dict):
            for key in ("url", "href", "src", "imageUrl", "link"):
                value = photo.get(key)
                if value:
                    photo_urls.append(absolute_url(value, BASE_SITE_URL))
                    break

    advertiser_name = None
    advertiser_id = None
    if isinstance(advertiser, dict):
        advertiser_name = advertiser.get("name") or advertiser.get("shortName")
        advertiser_id = advertiser.get("id")
    if isinstance(listing_owner, dict):
        advertiser_id = advertiser_id or listing_owner.get("id")
    zip_code = (
        extract_zip_code_from_mapping(address, "zipCode", "zipcode", "postalCode")
        or extract_zip_code(address.get("formatted"), address.get("street"), product.get("description"))
    )

    return {
        "description": product.get("description"),
        "address": address.get("formatted") or address.get("street"),
        "street": address.get("street"),
        "city": address.get("city"),
        "neighbourhood": address.get("neighborhood") or address.get("district"),
        "state": address.get("state"),
        "zip_code": zip_code,
        "zipcode": zip_code,
        "total_area_m2": extract_attr_number(attributes, "total_area_attr") or extract_attr_number(attributes, "area_attr"),
        "area": extract_attr_number(attributes, "area_attr"),
        "suites": extract_attr_number(attributes, "suite_attr"),
        "bedrooms": extract_attr_number(attributes, "bedroom_attr"),
        "bathrooms": extract_attr_number(attributes, "bathroom_attr"),
        "parking": extract_attr_number(attributes, "parking_lots_attr"),
        "condominium_name": condominium.get("name"),
        "condominium_id": condominium.get("id"),
        "condominium_url": absolute_url(condominium.get("url"), BASE_SITE_URL),
        "condominium_amenities_json": compact_json(condominium.get("amenities")),
        "features_json": compact_json(features),
        "pois_json": compact_json(pois),
        "advertiser_name": advertiser_name,
        "advertiser_id": advertiser_id,
        "seller_type": listing_owner.get("type") if isinstance(listing_owner, dict) else None,
        "main_image_url": photo_urls[0] if photo_urls else None,
        "gallery_urls_json": compact_json(photo_urls) if photo_urls else None,
        "condo_fee_brl": prices.get("condominium"),
        "sale_price_brl": prices.get("sale"),
        "rent_price_brl": prices.get("rent"),
        "total_price_brl": prices.get("fullMonthlyPrice"),
        "listing_url": absolute_url(seo.get("url"), BASE_SITE_URL) if isinstance(seo, dict) else None,
    }


def fetch_detail_payload(
    session: requests.Session,
    record: Dict[str, Any],
    metrics: Dict[str, Any],
) -> Dict[str, Any] | None:
    property_id = record.get("property_id") or record.get("sku")
    if not property_id:
        return None
    url = f"{DETAIL_API_PREFIX}{property_id}"
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Origin": "https://www.lopes.com.br",
        "Referer": record.get("listing_url") or "https://www.lopes.com.br/",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/146.0.0.0 Safari/537.36"
        ),
    }
    started_at = time.perf_counter()
    try:
        response = session.get(url, headers=headers, impersonate="chrome110", timeout=30)
        record_request(metrics, success=response.status_code == 200, elapsed_seconds=time.perf_counter() - started_at)
        if response.status_code != 200:
            return None
        payload = response.json()
        return payload if isinstance(payload, dict) else None
    except Exception:
        record_request(metrics, success=False, elapsed_seconds=time.perf_counter() - started_at)
        return None


def maybe_log_detail_progress(
    *,
    label: str,
    processed: int,
    total: int,
    metrics: Dict[str, Any],
    throttle: AdaptiveThrottle,
    interval: int = DETAIL_PROGRESS_EVERY,
) -> None:
    if total <= 0:
        return
    if processed != 1 and processed != total and processed % interval != 0:
        return
    snapshot = throttle.snapshot()
    print(
        "[INFO] detalhe_lopes_progress "
        f"label={label} "
        f"processed={processed}/{total} "
        f"success={metrics['detail_successes']} "
        f"failures={metrics['detail_failures']} "
        f"api_success={metrics['detail_api_successes']} "
        f"html_success={metrics['detail_html_successes']} "
        f"delay={snapshot['current_delay_seconds']:.2f}s"
    )


def enrich_records_with_details(
    records: List[Dict[str, Any]],
    session: requests.Session,
    metrics: Dict[str, Any],
    detail_min_delay_seconds: float,
    detail_max_delay_seconds: float,
    detail_target_delay_seconds: float | None,
    detail_max_consecutive_failures: int,
    label: str = "lopes",
) -> List[Dict[str, Any]]:
    if not records:
        return records
    throttle = AdaptiveThrottle(detail_min_delay_seconds, detail_max_delay_seconds, detail_target_delay_seconds)
    consecutive_failures = 0
    keys = (
        "description",
        "zip_code",
        "total_area_m2",
        "suites",
        "condominium_name",
        "condominium_amenities_json",
        "features_json",
        "pois_json",
        "gallery_urls_json",
    )
    enriched = []
    total_records = len(records)
    for index, record in enumerate(records, start=1):
        url = record.get("listing_url")
        if not url:
            enriched.append(record)
            maybe_log_detail_progress(
                label=label,
                processed=index,
                total=total_records,
                metrics=metrics,
                throttle=throttle,
            )
            continue
        metrics["detail_requests"] += 1
        detail: Dict[str, Any] = {}
        metrics["detail_api_requests"] += 1
        payload = fetch_detail_payload(session, record, metrics)
        if payload:
            try:
                detail = parse_detail_payload(payload)
            except Exception as exc:
                print(f"[WARN] detalhe_lopes_parse_api_falhou error={exc}")
        if detail_has_useful_data(detail):
            metrics["detail_api_successes"] += 1
        else:
            detail = {}
        if not detail:
            metrics["detail_html_requests"] += 1
            html = fetch_html(session, str(url), metrics)
            if html:
                try:
                    detail = parse_detail_html(html)
                except Exception as exc:
                    print(f"[WARN] detalhe_lopes_parse_html_falhou error={exc}")
            if detail_has_useful_data(detail):
                metrics["detail_html_successes"] += 1
            else:
                detail = {}
        if not detail:
            metrics["detail_failures"] += 1
            consecutive_failures += 1
            throttle.failure()
            metrics["detail_backoffs"] += 1
            enriched.append(record)
            if consecutive_failures >= detail_max_consecutive_failures:
                enriched.extend(records[len(enriched):])
                maybe_log_detail_progress(
                    label=label,
                    processed=index,
                    total=total_records,
                    metrics=metrics,
                    throttle=throttle,
                )
                break
            slept = throttle.sleep()
            metrics["throttle_sleep_seconds"] += slept
            maybe_log_detail_progress(
                label=label,
                processed=index,
                total=total_records,
                metrics=metrics,
                throttle=throttle,
            )
            continue
        merged = merge_record(record, detail)
        metrics["detail_successes"] += 1
        metrics["detail_fields_filled"] += count_filled_fields(detail, keys)
        consecutive_failures = 0
        enriched.append(merged)
        slept = throttle.sleep()
        metrics["throttle_sleep_seconds"] += slept
        maybe_log_detail_progress(
            label=label,
            processed=index,
            total=total_records,
            metrics=metrics,
            throttle=throttle,
        )
    return enriched


def scrape_all(
    *,
    base_api_url: str,
    business_type: str,
    max_pages: int,
    lines_per_page: int,
    min_delay_seconds: float,
    max_delay_seconds: float,
    retry_min_delay_seconds: float,
    retry_max_delay_seconds: float,
    target_delay_seconds: float | None,
    max_consecutive_failures: int,
    early_stop_on_low_yield: int,
    detail_min_delay_seconds: float,
    detail_max_delay_seconds: float,
    detail_target_delay_seconds: float | None,
    detail_max_consecutive_failures: int,
    label: str,
) -> List[Dict[str, Any]]:
    metrics = init_metrics(label)
    throttle = AdaptiveThrottle(min_delay_seconds, max_delay_seconds, target_delay_seconds)
    session = requests.Session()
    all_records: List[Dict[str, Any]] = []
    seen_ids = set()
    consecutive_failures = 0
    low_yield_pages = 0

    for page in range(1, max_pages + 1):
        data = fetch_json(
            build_api_url(base_api_url, page, lines_per_page=lines_per_page),
            session=session,
            metrics=metrics,
            retries=3,
            retry_min_delay_seconds=retry_min_delay_seconds,
            retry_max_delay_seconds=retry_max_delay_seconds,
        )
        if not data:
            consecutive_failures += 1
            if consecutive_failures >= max_consecutive_failures:
                metrics["stop_reason"] = f"max_consecutive_failures:{consecutive_failures}"
                break
            slept = throttle.sleep()
            metrics["throttle_sleep_seconds"] += slept
            continue

        records = parse_products(data, business_type=business_type)
        metrics["pages_processed"] += 1
        metrics["items_seen"] += len(records)
        if not records:
            metrics["stop_reason"] = "empty_page"
            continue

        new_count = 0
        for rec in records:
            pid = rec.get("property_id") or rec.get("sku")
            if not pid or pid in seen_ids:
                continue
            seen_ids.add(pid)
            all_records.append(rec)
            new_count += 1
        metrics["items_kept"] += new_count
        consecutive_failures = 0
        if new_count == 0:
            metrics["stop_reason"] = "no_new_records"
            break
        if early_stop_on_low_yield and new_count <= early_stop_on_low_yield:
            low_yield_pages += 1
        else:
            low_yield_pages = 0
        if low_yield_pages >= 2:
            metrics["stop_reason"] = f"low_yield:{new_count}"
            break
        slept = throttle.sleep()
        metrics["throttle_sleep_seconds"] += slept

    enriched = enrich_records_with_details(
        all_records,
        session=session,
        metrics=metrics,
        detail_min_delay_seconds=detail_min_delay_seconds,
        detail_max_delay_seconds=detail_max_delay_seconds,
        detail_target_delay_seconds=detail_target_delay_seconds,
        detail_max_consecutive_failures=detail_max_consecutive_failures,
        label=label,
    )
    if metrics["stop_reason"] is None:
        metrics["stop_reason"] = "max_pages_reached"
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
