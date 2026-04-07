from __future__ import annotations

import csv
import json
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Set

from curl_cffi import requests

from scrapers.detail_utils import absolute_url, compact_json, count_filled_fields, merge_record
from scrapers.throttle import AdaptiveThrottle, init_metrics, record_request
from scrapers.zip_utils import extract_zip_code, extract_zip_code_from_mapping


BASE_SITE_URL = "https://www.olx.com.br"


def fetch_page(url: str, session: requests.Session, metrics: Dict[str, Any]) -> tuple[str | None, int | None]:
    started_at = time.perf_counter()
    try:
        response = session.get(url, impersonate="chrome110", timeout=30)
        elapsed_seconds = time.perf_counter() - started_at

        if response.status_code != 200:
            record_request(metrics, success=False, elapsed_seconds=elapsed_seconds)
            return None, response.status_code

        record_request(metrics, success=True, elapsed_seconds=elapsed_seconds)
        return response.text, response.status_code
    except Exception:
        record_request(metrics, success=False, elapsed_seconds=time.perf_counter() - started_at)
        return None, None


def extract_all_props(props: List[Dict[str, Any]]) -> Dict[str, Any]:
    return {p.get("name"): p.get("value") for p in props if p.get("name")}


def _extract_balanced_segment(text: str, start_index: int) -> str | None:
    if start_index < 0 or start_index >= len(text):
        return None
    opening = text[start_index]
    closing = "]" if opening == "[" else "}" if opening == "{" else None
    if closing is None:
        return None

    depth = 0
    in_string = False
    escape = False
    for index in range(start_index, len(text)):
        char = text[index]
        if in_string:
            if escape:
                escape = False
            elif char == "\\":
                escape = True
            elif char == '"':
                in_string = False
            continue

        if char == '"':
            in_string = True
            continue
        if char == opening:
            depth += 1
            continue
        if char == closing:
            depth -= 1
            if depth == 0:
                return text[start_index : index + 1]
    return None


def _iter_datalayer_payloads(html: str) -> List[Any]:
    payloads: List[Any] = []
    markers = (
        "window.dataLayer =",
        "window.dataLayer=",
        "dataLayer =",
        "dataLayer=",
        "window.dataLayer.push(",
        "dataLayer.push(",
    )

    for marker in markers:
        search_from = 0
        while True:
            marker_index = html.find(marker, search_from)
            if marker_index == -1:
                break
            opening_index = -1
            for char in ("[", "{"):
                pos = html.find(char, marker_index + len(marker))
                if pos != -1 and (opening_index == -1 or pos < opening_index):
                    opening_index = pos
            if opening_index == -1:
                search_from = marker_index + len(marker)
                continue
            segment = _extract_balanced_segment(html, opening_index)
            if not segment:
                search_from = marker_index + len(marker)
                continue
            try:
                payloads.append(json.loads(segment))
            except json.JSONDecodeError:
                pass
            search_from = opening_index + len(segment)
    return payloads


def _extract_next_data_payload(html: str) -> dict[str, Any] | None:
    match = re.search(
        r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>',
        html,
        flags=re.DOTALL,
    )
    if not match:
        return None
    try:
        data = json.loads(match.group(1))
    except json.JSONDecodeError:
        return None

    candidates = [
        (((data.get("props") or {}).get("pageProps") or {}).get("ad")),
        (((data.get("props") or {}).get("pageProps") or {}).get("adData")),
        (((data.get("props") or {}).get("pageProps") or {}).get("adDetail")),
        (((data.get("props") or {}).get("pageProps") or {}).get("dataLayer")),
    ]
    for candidate in candidates:
        if isinstance(candidate, dict):
            return candidate
        if isinstance(candidate, list):
            for item in candidate:
                if isinstance(item, dict) and ("adDetail" in item or "adProperties" in item):
                    return item
    return None


def _extract_detail_item_from_candidate(candidate: dict[str, Any]) -> dict[str, Any] | None:
    page = candidate.get("page") if isinstance(candidate.get("page"), dict) else {}
    detail = candidate.get("detail") if isinstance(candidate.get("detail"), dict) else {}
    page_detail = page.get("detail") if isinstance(page.get("detail"), dict) else {}

    if page.get("pageType") == "ad_detail":
        return candidate
    if isinstance(page.get("adDetail"), dict):
        return candidate
    if isinstance(page.get("adProperties"), list):
        return candidate
    if page_detail.get("adDate") or page_detail.get("list_id") or page_detail.get("zipcode"):
        return candidate
    if detail.get("adDate") or detail.get("list_id") or detail.get("zipcode"):
        return candidate
    return None


def _extract_picture_urls(pictures: Any) -> List[str]:
    urls: List[str] = []
    if isinstance(pictures, list):
        for picture in pictures:
            if isinstance(picture, dict):
                for key in ("original", "large", "medium", "link", "url", "src"):
                    value = picture.get(key)
                    if value:
                        urls.append(absolute_url(value, BASE_SITE_URL))
                        break
            elif isinstance(picture, str):
                urls.append(absolute_url(picture, BASE_SITE_URL))
    elif isinstance(pictures, int):
        return []
    return [url for url in urls if url]


def detail_has_useful_data(detail: Dict[str, Any]) -> bool:
    useful_keys = (
        "listing_created_at",
        "zip_code",
        "seller_name",
        "seller_public_account_id",
        "gallery_urls_json",
        "main_image_url",
        "description",
        "condo_fee_brl",
        "iptu_brl",
        "lat",
        "lon",
    )
    return any(detail.get(key) not in (None, "", "[]") for key in useful_keys)


def _unix_seconds_to_iso8601(value: Any) -> str | None:
    if value is None:
        return None
    try:
        timestamp = int(str(value).strip())
    except (TypeError, ValueError):
        return None
    if timestamp <= 0:
        return None
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat()


def parse_listing_html(html: str) -> List[Dict[str, Any]]:
    records = []

    match = re.search(
        r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>',
        html,
        re.DOTALL,
    )
    if not match:
        return records

    data = json.loads(match.group(1))
    try:
        ads = data["props"]["pageProps"]["ads"]
    except KeyError:
        return records

    for ad in ads:
        try:
            props = ad.get("properties", [])
            props_dict = extract_all_props(props)
            location_details = ad.get("locationDetails") or {}
            category_name = ad.get("categoryName")
            category = ad.get("category")
            url = ad.get("friendlyUrl")

            record = {
                "property_id": ad.get("listId"),
                "ad_id": ad.get("adId"),
                "title": ad.get("subject"),
                "description": ad.get("body"),
                "price": ad.get("price"),
                "price_value": ad.get("priceValue"),
                "old_price": ad.get("oldPrice"),
                "currency": ad.get("currency"),
                "state": location_details.get("uf") or ad.get("uf"),
                "city": location_details.get("municipality") or ad.get("municipality"),
                "neighbourhood": location_details.get("neighbourhood") or ad.get("neighbourhood"),
                "zipcode": ad.get("zipcode"),
                "zone": ad.get("zone"),
                "lat": ad.get("lat"),
                "lon": ad.get("lon"),
                "url": absolute_url(url, BASE_SITE_URL),
                "listing_url": absolute_url(url, BASE_SITE_URL),
                "thumbnail": ad.get("thumbnail"),
                "main_image_url": ad.get("thumbnail"),
                "images_count": len(ad.get("images", [])) if ad.get("images") else 0,
                "created_at": ad.get("createdAt"),
                "updated_at": ad.get("modified"),
                "list_time": ad.get("listTime"),
                "advertiser_id": ad.get("accountId"),
                "is_professional": ad.get("professionalAd"),
                "category": ad.get("category"),
                "highlighted": ad.get("highlighted"),
                "premium": ad.get("premium"),
            }
            record.update(
                {
                    "area": props_dict.get("size"),
                    "bedrooms": props_dict.get("rooms"),
                    "bathrooms": props_dict.get("bathrooms"),
                    "parking": props_dict.get("garage_spaces"),
                    "suites": props_dict.get("suites"),
                    "floor": props_dict.get("floor"),
                    "furnished": props_dict.get("furnished"),
                    "condo_fee": props_dict.get("condominium"),
                    "iptu": props_dict.get("iptu"),
                    "property_type": props_dict.get("property_type") or category_name or category,
                    "real_estate_type": props_dict.get("real_estate_type"),
                }
            )
            records.append(record)
        except Exception:
            continue

    return records


def _parse_detail_payload(html: str) -> dict[str, Any] | None:
    for payload in _iter_datalayer_payloads(html):
        if isinstance(payload, list):
            for item in payload:
                if isinstance(item, dict):
                    detail_item = _extract_detail_item_from_candidate(item)
                    if detail_item:
                        return detail_item
        elif isinstance(payload, dict):
            detail_item = _extract_detail_item_from_candidate(payload)
            if detail_item:
                return detail_item
    return _extract_next_data_payload(html)


def parse_detail_html(html: str, fallback_url: str | None = None) -> dict[str, Any]:
    payload = _parse_detail_payload(html)
    if not payload:
        return {}

    page = payload.get("page") if isinstance(payload.get("page"), dict) else {}
    page_detail = page.get("detail") if isinstance(page.get("detail"), dict) else {}
    detail_meta = payload.get("detail") if isinstance(payload.get("detail"), dict) else {}
    ad_detail = page.get("adDetail") if isinstance(page.get("adDetail"), dict) else payload.get("adDetail") or {}
    ad_props = page.get("adProperties") if isinstance(page.get("adProperties"), list) else payload.get("adProperties") or []
    props_dict = extract_all_props(ad_props)
    pictures = page.get("pictures") or page.get("picture") or payload.get("picture") or payload.get("pictures") or []
    gallery_urls = _extract_picture_urls(pictures)
    lat = ad_detail.get("listLat") or ad_detail.get("lat")
    lon = ad_detail.get("listLon") or ad_detail.get("lon")
    zip_code = (
        extract_zip_code_from_mapping(page_detail, "zipcode", "zipCode", "postalCode")
        or extract_zip_code_from_mapping(detail_meta, "zipcode", "zipCode", "postalCode")
        or extract_zip_code(payload.get("zipcode"), payload.get("zipCode"), payload.get("postalCode"))
        or extract_zip_code_from_mapping(ad_detail, "zipcode", "zipCode", "postalCode")
        or extract_zip_code(
            ad_detail.get("street"),
            ad_detail.get("description"),
            ad_detail.get("body"),
            html,
        )
    )
    ad_date_raw = (
        page_detail.get("adDate")
        or detail_meta.get("adDate")
        or payload.get("adDate")
        or ad_detail.get("listTime")
    )
    listing_created_at = _unix_seconds_to_iso8601(ad_date_raw) or ad_date_raw
    updated_raw = (
        page_detail.get("lastUpdated")
        or detail_meta.get("lastUpdated")
        or page_detail.get("modified")
        or payload.get("modified")
        or ad_detail.get("lastUpdated")
    )
    listing_updated_at = _unix_seconds_to_iso8601(updated_raw) or updated_raw

    detail = {
        "listing_url": fallback_url,
        "zip_code": zip_code,
        "zipcode": zip_code,
        "listing_created_at": listing_created_at,
        "created_at": listing_created_at,
        "listing_updated_at": listing_updated_at,
        "listing_status": page_detail.get("status") or detail_meta.get("status") or payload.get("status") or ad_detail.get("status") or page.get("pageType"),
        "seller_name": ad_detail.get("sellerName") or payload.get("sellerName"),
        "seller_id": ad_detail.get("sellerId") or payload.get("sellerAccountId") or payload.get("sellerId"),
        "seller_public_account_id": ad_detail.get("sellerPublicAccountId") or payload.get("sellerPublicAccountId"),
        "seller_professional": ad_detail.get("professionalAd") if ad_detail.get("professionalAd") is not None else payload.get("professionalAd"),
        "seller_type": "professional"
        if (ad_detail.get("professionalAd") if ad_detail.get("professionalAd") is not None else payload.get("professionalAd"))
        else "private",
        "main_image_url": gallery_urls[0] if gallery_urls else None,
        "gallery_urls_json": compact_json(gallery_urls) if gallery_urls else None,
        "amenities_json": compact_json(ad_props) if ad_props else None,
        "subcategory": ad_detail.get("subCategory") or payload.get("subCategory") or payload.get("subCategoryId"),
        "category": page.get("category") or ad_detail.get("mainCategory") or payload.get("category") or payload.get("mainCategory"),
        "real_estate_type": props_dict.get("real_estate_type") or payload.get("realEstateType"),
        "property_type": props_dict.get("property_type") or page.get("category") or payload.get("mainCategory"),
        "total_area_m2": props_dict.get("size"),
        "area": props_dict.get("size"),
        "bedrooms": props_dict.get("rooms"),
        "bathrooms": props_dict.get("bathrooms"),
        "parking": props_dict.get("garage_spaces"),
        "suites": props_dict.get("suites"),
        "floor": props_dict.get("floor"),
        "furnished": props_dict.get("furnished"),
        "condo_fee": props_dict.get("condominium"),
        "condo_fee_brl": props_dict.get("condominium"),
        "iptu": props_dict.get("iptu"),
        "iptu_brl": props_dict.get("iptu"),
        "price": page_detail.get("price") or detail_meta.get("price") or payload.get("price"),
        "description": ad_detail.get("body") or ad_detail.get("description") or payload.get("description"),
        "address": ad_detail.get("street"),
        "city": ad_detail.get("municipality") or payload.get("municipality"),
        "state": ad_detail.get("state") or page.get("state") or payload.get("state"),
        "neighbourhood": ad_detail.get("neighbourhood") or payload.get("neighbourhood"),
        "region_name": page.get("region"),
        "page_type": page.get("pageType"),
        "category_id": page_detail.get("category_id") or detail_meta.get("category_id"),
        "city_id": page_detail.get("city_id") or detail_meta.get("city_id"),
        "state_id": page_detail.get("state_id") or detail_meta.get("state_id"),
        "parent_category_id": page_detail.get("parent_category_id") or detail_meta.get("parent_category_id"),
        "is_eligible_ad": page_detail.get("isEligibleAd") if page_detail.get("isEligibleAd") is not None else detail_meta.get("isEligibleAd"),
        "is_shared": page_detail.get("isShared") if page_detail.get("isShared") is not None else detail_meta.get("isShared"),
        "save_data": page_detail.get("saveData") if page_detail.get("saveData") is not None else detail_meta.get("saveData"),
        "connection_type": page_detail.get("connectionType") or detail_meta.get("connectionType"),
        "cpu_cores": page_detail.get("cpuCores") or detail_meta.get("cpuCores"),
        "download_link": page_detail.get("downloadLink") or detail_meta.get("downloadLink"),
        "last_internal_source": page_detail.get("lastInternalSource") or detail_meta.get("lastInternalSource"),
        "olx_pay_json": compact_json(page_detail.get("olxPay")) if page_detail.get("olxPay") is not None else compact_json(detail_meta.get("olxPay")),
        "olx_delivery_json": compact_json(page_detail.get("olxDelivery")) if page_detail.get("olxDelivery") is not None else compact_json(detail_meta.get("olxDelivery")),
        "vehicle_report_json": compact_json(page_detail.get("vehicleReport")) if page_detail.get("vehicleReport") is not None else compact_json(detail_meta.get("vehicleReport")),
        "memory_status_json": compact_json(page_detail.get("memoryStatus")) if page_detail.get("memoryStatus") is not None else compact_json(detail_meta.get("memoryStatus")),
        "vehicle_tags_json": compact_json(page_detail.get("vehicleTags")) if page_detail.get("vehicleTags") is not None else compact_json(detail_meta.get("vehicleTags")),
        "lat": lat,
        "lon": lon,
    }
    return detail


def enrich_records_with_details(
    records: List[Dict[str, Any]],
    session: requests.Session,
    metrics: Dict[str, Any],
    min_delay_seconds: float,
    max_delay_seconds: float,
    target_delay_seconds: float | None,
    max_consecutive_failures: int,
) -> List[Dict[str, Any]]:
    if not records:
        return records

    throttle = AdaptiveThrottle(
        min_delay_seconds=min_delay_seconds,
        max_delay_seconds=max_delay_seconds,
        target_delay_seconds=target_delay_seconds,
    )
    consecutive_failures = 0
    filled_keys = (
        "zip_code",
        "listing_created_at",
        "listing_updated_at",
        "seller_name",
        "seller_public_account_id",
        "seller_professional",
        "main_image_url",
        "gallery_urls_json",
        "amenities_json",
        "condo_fee_brl",
        "iptu_brl",
        "description",
        "lat",
        "lon",
    )
    enriched_records: List[Dict[str, Any]] = []

    for record in records:
        listing_url = record.get("listing_url") or record.get("url")
        if not listing_url:
            enriched_records.append(record)
            continue

        print(f"[INFO] detalhe_olx url={listing_url}")
        metrics["detail_requests"] += 1
        html, status_code = fetch_page(str(listing_url), session, metrics)
        if not html:
            metrics["detail_failures"] += 1
            consecutive_failures += 1
            next_delay = throttle.failure(status_code=status_code)
            metrics["detail_backoffs"] += 1
            print(
                f"[WARN] detalhe_olx_falhou status={status_code} "
                f"falhas_consecutivas={consecutive_failures} next_delay={next_delay:.2f}s"
            )
            enriched_records.append(record)
            if consecutive_failures >= max_consecutive_failures:
                print("[WARN] limite de falhas em detalhe OLX atingido, preservando dados coletados")
                enriched_records.extend(records[len(enriched_records):])
                break
            slept = throttle.sleep()
            metrics["throttle_sleep_seconds"] += slept
            continue

        try:
            detail = parse_detail_html(html, fallback_url=str(listing_url))
        except Exception as exc:
            metrics["detail_failures"] += 1
            consecutive_failures += 1
            next_delay = throttle.failure(status_code=status_code)
            metrics["detail_backoffs"] += 1
            print(
                f"[WARN] detalhe_olx_parse_falhou error={exc} "
                f"falhas_consecutivas={consecutive_failures} next_delay={next_delay:.2f}s"
            )
            enriched_records.append(record)
            if consecutive_failures >= max_consecutive_failures:
                print("[WARN] limite de falhas em detalhe OLX atingido, preservando dados coletados")
                enriched_records.extend(records[len(enriched_records):])
                break
            slept = throttle.sleep()
            metrics["throttle_sleep_seconds"] += slept
            continue
        if detail_has_useful_data(detail):
            merged = merge_record(record, detail)
            metrics["detail_successes"] += 1
            metrics["detail_fields_filled"] += count_filled_fields(detail, filled_keys)
            consecutive_failures = 0
            enriched_records.append(merged)
        else:
            metrics["detail_failures"] += 1
            consecutive_failures += 1
            next_delay = throttle.failure(status_code=status_code)
            metrics["detail_backoffs"] += 1
            print(
                f"[WARN] detalhe_olx_sem_campos_uteis "
                f"falhas_consecutivas={consecutive_failures} next_delay={next_delay:.2f}s"
            )
            enriched_records.append(record)
            if consecutive_failures >= max_consecutive_failures:
                print("[WARN] limite de falhas em detalhe OLX atingido, preservando dados coletados")
                enriched_records.extend(records[len(enriched_records):])
                break
        slept = throttle.sleep()
        metrics["throttle_sleep_seconds"] += slept

    return enriched_records


def scrape_all(
    *,
    base_url: str,
    max_pages: int,
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
    metrics = init_metrics(label)
    throttle = AdaptiveThrottle(
        min_delay_seconds=min_delay_seconds,
        max_delay_seconds=max_delay_seconds,
        target_delay_seconds=target_delay_seconds,
    )
    session = requests.Session()
    all_records: List[Dict[str, Any]] = []
    seen_ids: Set[str] = set()
    consecutive_failures = 0
    low_yield_pages = 0

    for page in range(1, max_pages + 1):
        url = f"{base_url}?o={page}"
        print(f"[INFO] Pagina {page}: {url}")
        html, status_code = fetch_page(url, session, metrics)

        if not html:
            consecutive_failures += 1
            next_delay = throttle.failure(status_code=status_code)
            print(
                f"[WARN] pagina {page} sem HTML status={status_code} "
                f"falhas_consecutivas={consecutive_failures} next_delay={next_delay:.2f}s"
            )
            if consecutive_failures >= max_consecutive_failures:
                metrics["stop_reason"] = f"max_consecutive_failures:{consecutive_failures}"
                break
            slept = throttle.sleep()
            metrics["throttle_sleep_seconds"] += slept
            continue

        records = parse_listing_html(html)
        metrics["pages_processed"] += 1
        metrics["items_seen"] += len(records)
        if not records:
            metrics["stop_reason"] = "empty_page"
            break

        new_count = 0
        for rec in records:
            pid = rec.get("property_id")
            if pid and pid not in seen_ids:
                seen_ids.add(pid)
                all_records.append(rec)
                new_count += 1

        metrics["items_kept"] += new_count
        print(f"[INFO] pagina {page}: novos={new_count} total={len(all_records)}")
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
        min_delay_seconds=detail_min_delay_seconds,
        max_delay_seconds=detail_max_delay_seconds,
        target_delay_seconds=detail_target_delay_seconds,
        max_consecutive_failures=detail_max_consecutive_failures,
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
