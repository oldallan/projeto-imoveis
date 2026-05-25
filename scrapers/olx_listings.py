from __future__ import annotations

import html as html_lib
import json
from pathlib import Path
from typing import Any, Dict, List

import scrapy
from scrapy.http import Request, Response

from scrapers.http_metrics import init_metrics
from scrapers.io_utils import load_csv_records, save_parquet_records
from scrapers.listings_resume import (
    BaseListingsSpider,
    build_incomplete_output_path,
    build_listing_resume_key,
    build_resume_paths,
    cleanup_incomplete_outputs,
    cleanup_resume_runtime,
    default_resume_dir,
    load_resume_state,
    run_batched_scrapy_collection,
    save_resume_state,
    utc_now_iso,
)
from scrapers.logging_utils import log_warn
from scrapers.olx_shared import *  # noqa: F403
from scrapers.scrapy_runner import run_spider
from scrapers.scrapy_support import build_scrapy_settings as build_base_scrapy_settings

OLX_RESUME_KEY_FIELD = "key"


def extract_all_props(props: List[Dict[str, Any]]) -> Dict[str, Any]:
    extracted: dict[str, Any] = {}
    for prop in props:
        name = str(prop.get("name") or "").strip().lower()
        if name:
            extracted[name] = prop.get("value")
    return extracted


def _clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text = html_lib.unescape(str(value))
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text or None


def _extract_meta_description(html: str) -> str | None:
    patterns = (
        r'<meta[^>]+name=["\']description["\'][^>]+content=["\'](.*?)["\']',
        r'<meta[^>]+property=["\']og:description["\'][^>]+content=["\'](.*?)["\']',
        r'<meta[^>]+content=["\'](.*?)["\'][^>]+name=["\']description["\']',
        r'<meta[^>]+content=["\'](.*?)["\'][^>]+property=["\']og:description["\']',
    )
    for pattern in patterns:
        match = re.search(pattern, html, flags=re.IGNORECASE | re.DOTALL)
        if not match:
            continue
        description = _clean_text(match.group(1))
        if description:
            return description
    return None


def _extract_json_ld_description(html: str) -> str | None:
    for match in re.finditer(
        r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        html,
        flags=re.DOTALL | re.IGNORECASE,
    ):
        try:
            payload = json.loads(match.group(1))
        except json.JSONDecodeError:
            continue

        entries = payload if isinstance(payload, list) else [payload]
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            description = _clean_text(entry.get("description"))
            if description:
                return description
    return None


def _find_first_nested_text(value: Any, candidate_keys: set[str]) -> str | None:
    if isinstance(value, dict):
        for key in candidate_keys:
            description = _clean_text(value.get(key))
            if description:
                return description
        for nested_value in value.values():
            description = _find_first_nested_text(nested_value, candidate_keys)
            if description:
                return description
    elif isinstance(value, list):
        for item in value:
            description = _find_first_nested_text(item, candidate_keys)
            if description:
                return description
    return None


def _first_present(mapping: Dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = mapping.get(key)
        if value not in (None, ""):
            return value
    return None


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


def _extract_initial_data_ad(html: str) -> dict[str, Any] | None:
    match = re.search(
        r'<script[^>]+id=["\']initial-data["\'][^>]+data-json=["\'](.*?)["\'][^>]*>',
        html,
        flags=re.DOTALL | re.IGNORECASE,
    )
    if not match:
        return None

    try:
        payload = json.loads(html_lib.unescape(match.group(1)))
    except json.JSONDecodeError:
        return None

    ad = payload.get("ad")
    return ad if isinstance(ad, dict) else None


def _extract_listing_page_item_from_candidate(candidate: dict[str, Any]) -> dict[str, Any] | None:
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


def _derive_business_type_from_url(url: str | None) -> str | None:
    lowered = str(url or "").lower()
    if "/aluguel/" in lowered:
        return "rent"
    if "/venda/" in lowered:
        return "sale"
    return None


def _has_required_listing_keys(record: Dict[str, Any]) -> bool:
    business_type = str(record.get("business_type") or "").strip()
    property_id = str(record.get("property_id") or "").strip()
    listing_url = str(record.get("listing_url") or "").strip()
    return bool(business_type and (property_id or listing_url))


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


def _derive_listing_id_from_url(url: str | None) -> str | None:
    if not url:
        return None
    match = LISTING_ID_PATTERN.search(str(url).strip())
    if not match:
        return None
    return match.group(1)


def _coerce_optional_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    normalized = str(value).strip().lower()
    if not normalized:
        return None
    if normalized in {"1", "true", "sim", "yes"}:
        return True
    if normalized in {"0", "false", "nao", "não", "no"}:
        return False
    return None


def _parse_listing_page_payload(html: str) -> dict[str, Any] | None:
    for payload in _iter_datalayer_payloads(html):
        if isinstance(payload, list):
            for item in payload:
                if isinstance(item, dict):
                    listing_page_item = _extract_listing_page_item_from_candidate(item)
                    if listing_page_item:
                        return listing_page_item
        elif isinstance(payload, dict):
            listing_page_item = _extract_listing_page_item_from_candidate(payload)
            if listing_page_item:
                return listing_page_item
    return _extract_next_data_payload(html)


def parse_listing_page_html(
    html: str,
    business_type: str | None = None,
    fallback_url: str | None = None,
) -> dict[str, Any]:
    payload = _parse_listing_page_payload(html)
    if not payload:
        return {}

    page = payload.get("page") if isinstance(payload.get("page"), dict) else {}
    page_detail = page.get("detail") if isinstance(page.get("detail"), dict) else {}
    detail_meta = payload.get("detail") if isinstance(payload.get("detail"), dict) else {}
    ad_detail = page.get("adDetail") if isinstance(page.get("adDetail"), dict) else payload.get("adDetail") or {}
    ad_props = page.get("adProperties") if isinstance(page.get("adProperties"), list) else payload.get("adProperties") or []
    initial_data_ad = _extract_initial_data_ad(html) or {}
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
        or ad_detail.get("origListTime")
    )
    listing_created_at = _unix_seconds_to_iso8601(ad_date_raw) or ad_date_raw
    updated_raw = (
        page_detail.get("lastUpdated")
        or detail_meta.get("lastUpdated")
        or page_detail.get("modified")
        or payload.get("modified")
        or ad_detail.get("listTime")
    )
    listing_updated_at = _unix_seconds_to_iso8601(updated_raw) or updated_raw
    listing_id = (
        page_detail.get("list_id")
        or detail_meta.get("list_id")
        or payload.get("list_id")
        or ad_detail.get("listId")
        or ad_detail.get("list_id")
        or payload.get("listId")
        or _derive_listing_id_from_url(fallback_url)
    )
    ad_id = (
        page_detail.get("ad_id")
        or detail_meta.get("ad_id")
        or payload.get("ad_id")
        or ad_detail.get("adId")
        or ad_detail.get("ad_id")
        or payload.get("adId")
    )
    seller_professional = _coerce_optional_bool(ad_detail.get("professionalAd"))
    if seller_professional is None:
        seller_professional = _coerce_optional_bool(payload.get("professionalAd"))
    description = (
        _clean_text(initial_data_ad.get("body"))
        or _clean_text(initial_data_ad.get("description"))
        or _extract_json_ld_description(html)
        or _extract_meta_description(html)
        or _clean_text(ad_detail.get("body"))
        or _clean_text(ad_detail.get("description"))
        or _clean_text(page_detail.get("description"))
        or _clean_text(detail_meta.get("description"))
        or _clean_text(payload.get("body"))
        or _clean_text(payload.get("description"))
        or _find_first_nested_text(
            {"page": page, "detail": detail_meta, "payload": payload},
            {"body", "description", "descriptionHtml", "adDescription", "seoDescription"},
        )
    )

    return {
        "listing_url": fallback_url,
        "property_id": str(listing_id) if listing_id is not None else None,
        #"listing_id": str(listing_id) if listing_id is not None else None,
        "business_type": _derive_business_type_from_url(fallback_url) or business_type,
        "ad_id": str(ad_id) if ad_id is not None else None,
        #"zip_code": zip_code,
        "zipcode": zip_code,
        "listing_created_at": listing_created_at,
        #"created_at": listing_created_at,
        #"listing_updated_at": listing_updated_at,
        #"listing_status": page_detail.get("status") or detail_meta.get("status") or payload.get("status") or ad_detail.get("status") or page.get("pageType"),
        #"seller_name": ad_detail.get("sellerName") or payload.get("sellerName"),
        #"seller_id": ad_detail.get("sellerId") or payload.get("sellerAccountId") or payload.get("sellerId"),
        #"seller_public_account_id": ad_detail.get("sellerPublicAccountId") or payload.get("sellerPublicAccountId"),
        #"seller_professional": seller_professional,
        #"seller_type": "professional"
        #if seller_professional is True
        #else "private",
        #"main_image_url": gallery_urls[0] if gallery_urls else None,
        #"gallery_urls_json": compact_json(gallery_urls) if gallery_urls else None,
        "amenities_json": compact_json(ad_props) if ad_props else None,
        "property_type": ad_detail.get("subCategory") or payload.get("subCategory") or payload.get("subCategoryId"),
        #"category": page.get("category") or ad_detail.get("mainCategory") or payload.get("category") or payload.get("mainCategory"),
        "real_estate_type": props_dict.get("real_estate_type") or payload.get("realEstateType"),
        #"property_type": props_dict.get("property_type") or page.get("category") or payload.get("mainCategory"),
        "total_area_m2": props_dict.get("size"),
        #"area": props_dict.get("size"),
        "bedrooms": props_dict.get("rooms"),
        "bathrooms": props_dict.get("bathrooms"),
        "parking": props_dict.get("garage_spaces"),
        "suites": props_dict.get("suites"),
        "floor": props_dict.get("floor"),
        #"furnished": props_dict.get("furnished"),
        #"condo_fee": props_dict.get("condominium"),
        "condo_fee_brl": _first_present(
            props_dict,
            "condominium",
            "condominio",
            "condo_fee",
            "condominium_fee",
        ),
        #"iptu": props_dict.get("iptu"),
        "iptu_brl": props_dict.get("iptu"),
        "price": page_detail.get("price") or detail_meta.get("price") or payload.get("price"),
        "description": description,
        "street": ad_detail.get("street"),
        "city": ad_detail.get("municipality") or payload.get("municipality"),
        "state": ad_detail.get("state") or page.get("state") or payload.get("state"),
        "neighbourhood": ad_detail.get("neighbourhood") or payload.get("neighbourhood"),
        #"region_name": page.get("region"),
        #"page_type": page.get("pageType"),
        #"category_id": page_detail.get("category_id") or detail_meta.get("category_id"),
        #"city_id": page_detail.get("city_id") or detail_meta.get("city_id"),
        #"state_id": page_detail.get("state_id") or detail_meta.get("state_id"),
        #"parent_category_id": page_detail.get("parent_category_id") or detail_meta.get("parent_category_id"),
        #"is_eligible_ad": page_detail.get("isEligibleAd") if page_detail.get("isEligibleAd") is not None else detail_meta.get("isEligibleAd"),
        #"is_shared": page_detail.get("isShared") if page_detail.get("isShared") is not None else detail_meta.get("isShared"),
        #"save_data": page_detail.get("saveData") if page_detail.get("saveData") is not None else detail_meta.get("saveData"),
        #"connection_type": page_detail.get("connectionType") or detail_meta.get("connectionType"),
        #"cpu_cores": page_detail.get("cpuCores") or detail_meta.get("cpuCores"),
        #"download_link": page_detail.get("downloadLink") or detail_meta.get("downloadLink"),
        #"last_internal_source": page_detail.get("lastInternalSource") or detail_meta.get("lastInternalSource"),
        #"olx_pay_json": compact_json(page_detail.get("olxPay")) if page_detail.get("olxPay") is not None else compact_json(detail_meta.get("olxPay")),
        #"olx_delivery_json": compact_json(page_detail.get("olxDelivery")) if page_detail.get("olxDelivery") is not None else compact_json(detail_meta.get("olxDelivery")),
        #"vehicle_report_json": compact_json(page_detail.get("vehicleReport")) if page_detail.get("vehicleReport") is not None else compact_json(detail_meta.get("vehicleReport")),
        #"memory_status_json": compact_json(page_detail.get("memoryStatus")) if page_detail.get("memoryStatus") is not None else compact_json(detail_meta.get("memoryStatus")),
        #"vehicle_tags_json": compact_json(page_detail.get("vehicleTags")) if page_detail.get("vehicleTags") is not None else compact_json(detail_meta.get("vehicleTags")),
        "lat": lat,
        "lon": lon,
    }


def build_scrapy_settings(
    *,
    verbose: bool = False,
    retry_times: int = 2,
    autothrottle_start_delay: float = 1.0,
    autothrottle_max_delay: float = 8.0,
    autothrottle_target_concurrency: float = 1.0,
    concurrent_requests: int = 2,
    concurrent_requests_per_domain: int = 1,
    download_delay: float = 1.0,
    download_timeout: int = 30,
    jobdir: str | None = None,
) -> dict[str, Any]:
    return build_base_scrapy_settings(
        user_agent=DEFAULT_USER_AGENT,
        default_headers=HEADERS,
        verbose=verbose,
        retry_times=retry_times,
        autothrottle_start_delay=autothrottle_start_delay,
        autothrottle_max_delay=autothrottle_max_delay,
        autothrottle_target_concurrency=autothrottle_target_concurrency,
        concurrent_requests=concurrent_requests,
        concurrent_requests_per_domain=concurrent_requests_per_domain,
        download_delay=download_delay,
        randomize_download_delay=True,
        download_timeout=download_timeout,
        impersonate=DEFAULT_IMPERSONATE_BROWSER,
        jobdir=jobdir,
    )


class OlxListingsSpider(BaseListingsSpider):
    name = "olx_listings"
    allowed_domains = ["olx.com.br", "www.olx.com.br", "sp.olx.com.br"]

    def build_request(self, record: Dict[str, Any], *, scheduled_index: int) -> Request | None:
        listing_url = str(record.get("listing_url") or record.get("url") or "").strip()
        if not listing_url:
            return None
        return Request(
            url=listing_url,
            callback=self.parse_listing_response,
            errback=self.handle_request_error,
            headers=HEADERS,
            meta={
                "listing_url": listing_url,
                "business_type": record.get("business_type"),
                "scheduled_index": scheduled_index,
                "handle_httpstatus_all": True,
            },
        )

    def parse_listing_response(self, response: Response):
        if int(response.status) == 410:
            scheduled_index = int(response.meta["scheduled_index"])
            self._record_http_result(response, success=False)
            self.metrics["listing_page_failures"] += 1
            self.metrics["listing_page_not_founds"] += 1
            self._mark_terminal_processed(
                response.meta.get("_resume_record") or response.meta,
                status="not_found",
                scheduled_index=scheduled_index,
                url=response.url,
            )
            log_warn(
                "listing_collection_item_not_found",
                label=self.label,
                processed=f"{scheduled_index}/{self.total_records}",
                status=410,
            )
            self._finalize_attempt()
            return None
        return super().parse_listing_response(response)

    def parse_record(self, response: Response) -> dict[str, Any]:
        record = parse_listing_page_html(
            response.text,
            business_type=response.meta.get("business_type"),
            fallback_url=str(response.meta["listing_url"]),
        )
        resume_record = response.meta.get("_resume_record")
        resume_key = build_listing_resume_key(resume_record) if isinstance(resume_record, dict) else None
        if resume_key:
            record[OLX_RESUME_KEY_FIELD] = resume_key
        return record


def strip_olx_resume_keys(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [
        {key: value for key, value in record.items() if key != OLX_RESUME_KEY_FIELD}
        for record in records
    ]


def run_scrapy_collection(
    *,
    records: List[Dict[str, Any]],
    label: str,
    max_consecutive_failures: int,
    listings_output_path: str,
    listings_parquet_output_path: str,
    resume_dir: str | None = None,
    verbose: bool = False,
    retry_times: int = 2,
    autothrottle_start_delay: float = 1.0,
    autothrottle_max_delay: float = 8.0,
    autothrottle_target_concurrency: float = 1.0,
    concurrent_requests: int = 2,
    concurrent_requests_per_domain: int = 1,
    download_delay: float = 1.0,
    download_timeout: int = 30,
) -> tuple[List[Dict[str, Any]], Dict[str, Any]]:
    return run_batched_scrapy_collection(
        records=records,
        label=label,
        max_consecutive_failures=max_consecutive_failures,
        listings_output_path=listings_output_path,
        listings_parquet_output_path=listings_parquet_output_path,
        spider_cls=OlxListingsSpider,
        build_scrapy_settings=build_scrapy_settings,
        run_spider=run_spider,
        resume_dir=resume_dir,
        verbose=verbose,
        retry_times=retry_times,
        autothrottle_start_delay=autothrottle_start_delay,
        autothrottle_max_delay=autothrottle_max_delay,
        autothrottle_target_concurrency=autothrottle_target_concurrency,
        concurrent_requests=concurrent_requests,
        concurrent_requests_per_domain=concurrent_requests_per_domain,
        download_delay=download_delay,
        download_timeout=download_timeout,
    )


def collect_listings_from_file(
    *,
    input_path: str,
    listings_output_path: str,
    listings_parquet_output_path: str,
    max_consecutive_failures: int,
    label: str,
    resume_dir: str | None = None,
    verbose: bool = False,
    retry_times: int = 2,
    autothrottle_start_delay: float = 1.0,
    autothrottle_max_delay: float = 8.0,
    autothrottle_target_concurrency: float = 1.0,
    concurrent_requests: int = 2,
    concurrent_requests_per_domain: int = 1,
    download_delay: float = 1.0,
    download_timeout: int = 30,
) -> dict[str, Any] | None:
    base_records = load_csv_records(input_path)
    if not base_records:
        return {
            "input_rows": 0,
            "output_rows": 0,
            "no_op": True,
        }

    resolved_resume_dir = default_resume_dir(
        label=label,
        listings_output_path=listings_output_path,
    ) if resume_dir is None else Path(resume_dir)
    resume_paths = build_resume_paths(resolved_resume_dir)

    listings_records, metrics = run_scrapy_collection(
        records=base_records,
        label=label,
        max_consecutive_failures=max_consecutive_failures,
        listings_output_path=listings_output_path,
        listings_parquet_output_path=listings_parquet_output_path,
        resume_dir=str(resolved_resume_dir),
        verbose=verbose,
        retry_times=retry_times,
        autothrottle_start_delay=autothrottle_start_delay,
        autothrottle_max_delay=autothrottle_max_delay,
        autothrottle_target_concurrency=autothrottle_target_concurrency,
        concurrent_requests=concurrent_requests,
        concurrent_requests_per_domain=concurrent_requests_per_domain,
        download_delay=download_delay,
        download_timeout=download_timeout,
    )
    print(f"[INFO] metrics={json.dumps(metrics, ensure_ascii=False)}")
    if metrics.get("stop_reason") == "max_consecutive_failures":
        output_records = strip_olx_resume_keys(listings_records)
        incomplete_output_path = build_incomplete_output_path(listings_output_path)
        incomplete_parquet_output_path = build_incomplete_output_path(listings_parquet_output_path)
        save_csv(output_records, filename=incomplete_output_path)
        save_parquet_records(output_records, filename=incomplete_parquet_output_path)
        failed_state = load_resume_state(resume_paths["state_json"])
        failed_state.update(
            {
                "status": "failed_terminal",
                "updated_at": utc_now_iso(),
                "metrics": metrics,
                "output_rows": len(output_records),
                "incomplete_output_path": str(incomplete_output_path),
                "incomplete_parquet_output_path": str(incomplete_parquet_output_path),
                "incomplete_output_rows": len(output_records),
            }
        )
        save_resume_state(resume_paths["state_json"], failed_state)
        raise RuntimeError(f"{label} abortado por max_consecutive_failures")

    if int(metrics.get("pending_records", 0) or 0) > 0:
        output_records = strip_olx_resume_keys(listings_records)
        incomplete_output_path = build_incomplete_output_path(listings_output_path)
        incomplete_parquet_output_path = build_incomplete_output_path(listings_parquet_output_path)
        save_csv(output_records, filename=incomplete_output_path)
        save_parquet_records(output_records, filename=incomplete_parquet_output_path)
        in_progress_state = load_resume_state(resume_paths["state_json"])
        in_progress_state.update(
            {
                "status": "in_progress",
                "updated_at": utc_now_iso(),
                "metrics": metrics,
                "output_rows": len(output_records),
                "pending_rows": int(metrics.get("pending_records", 0) or 0),
                "incomplete_output_path": str(incomplete_output_path),
                "incomplete_parquet_output_path": str(incomplete_parquet_output_path),
                "incomplete_output_rows": len(output_records),
            }
        )
        save_resume_state(resume_paths["state_json"], in_progress_state)
        raise RuntimeError(f"{label} ainda possui listings pendentes para retomada")

    output_records = strip_olx_resume_keys(listings_records)
    temp_csv_path = Path(listings_output_path).with_suffix(Path(listings_output_path).suffix + ".tmp")
    temp_parquet_path = Path(listings_parquet_output_path).with_suffix(Path(listings_parquet_output_path).suffix + ".tmp")
    save_csv(output_records, filename=str(temp_csv_path))
    save_parquet_records(output_records, filename=temp_parquet_path)
    temp_csv_path.replace(listings_output_path)
    temp_parquet_path.replace(listings_parquet_output_path)
    cleanup_incomplete_outputs(listings_output_path, listings_parquet_output_path)
    completed_state = load_resume_state(resume_paths["state_json"])
    completed_state.update(
        {
            "status": "completed",
            "updated_at": utc_now_iso(),
            "input_rows": len(base_records),
            "output_rows": len(output_records),
            "output_path": str(listings_output_path),
            "parquet_output_path": str(listings_parquet_output_path),
            "metrics": metrics,
            "incomplete_output_path": None,
            "incomplete_parquet_output_path": None,
            "incomplete_output_rows": 0,
        }
    )
    save_resume_state(resume_paths["state_json"], completed_state)
    cleanup_resume_runtime(resume_paths["jobdir"], resume_paths["partial_jsonl"], resume_paths["processed_jsonl"])
    return {
        "input_rows": len(base_records),
        "output_rows": len(output_records),
        "resume_state_path": str(resume_paths["state_json"]),
    }
