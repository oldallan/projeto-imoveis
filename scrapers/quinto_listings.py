from __future__ import annotations

import html as html_lib
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping

import scrapy
from scrapy.http import Request, Response

from scrapers.http_metrics import init_metrics
from scrapers.io_utils import load_csv_records
from scrapers.logging_utils import log_warn
from scrapers.listings_resume import (
    BaseListingsSpider,
    build_listing_resume_key,
    build_incomplete_output_path,
    build_resume_paths,
    cleanup_incomplete_outputs,
    cleanup_resume_runtime,
    default_resume_dir,
    load_resume_state,
    run_batched_scrapy_collection,
    save_resume_state,
    utc_now_iso,
)
from scrapers.quinto_shared import *  # noqa: F403
from scrapers.scrapy_runner import run_spider
from scrapers.scrapy_support import build_scrapy_settings as build_base_scrapy_settings


class MissingNextDataError(ValueError):
    """Raised when a QuintoAndar HTTP 200 page omits __NEXT_DATA__."""


class MissingInitialStateError(ValueError):
    """Raised when a QuintoAndar HTTP 200 page has no usable initialState."""


def extract_next_data(html: str) -> Dict[str, Any]:
    match = re.search(
        r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>',
        html,
        flags=re.DOTALL,
    )
    if not match:
        raise MissingNextDataError("Nao foi possivel localizar __NEXT_DATA__.")
    return json.loads(match.group(1))


def _derive_property_id_from_listing_url(url: str | None) -> str | None:
    match = re.search(r"/imovel/(\d+)", str(url or ""))
    if not match:
        return None
    return match.group(1)


def _derive_business_type_from_listing_url(url: str | None) -> str | None:
    lowered = str(url or "").lower()
    if "/alugar" in lowered:
        return "rent"
    if "/comprar" in lowered:
        return "sale"
    return None


def _has_required_listing_keys(record: Dict[str, Any]) -> bool:
    business_type = str(record.get("business_type") or "").strip()
    property_id = str(record.get("property_id") or "").strip()
    listing_url = str(record.get("listing_url") or "").strip()
    return bool(business_type and (property_id or listing_url))


def _compose_business_type(values: Iterable[str | None]) -> str | None:
    normalized = {
        str(value).strip().lower()
        for value in values
        if str(value or "").strip().lower() in {"rent", "sale"}
    }
    if normalized == {"rent", "sale"}:
        return "rent|sale"
    if "rent" in normalized:
        return "rent"
    if "sale" in normalized:
        return "sale"
    return None


def _group_quinto_records(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    grouped_by_id: dict[str, dict[str, Any]] = {}
    grouped_records: list[dict[str, Any]] = []

    for record in records:
        item = dict(record)
        listing_url = str(item.get("listing_url") or "").strip()
        property_id = str(
            item.get("listing_id")
            or item.get("property_id")
            or _derive_property_id_from_listing_url(listing_url)
            or ""
        ).strip()
        business_type = str(item.get("business_type") or _derive_business_type_from_listing_url(listing_url) or "").strip().lower()

        if not property_id:
            item["grouped_business_types"] = [business_type] if business_type in {"rent", "sale"} else []
            item["primary_business_type"] = business_type or None
            grouped_records.append(item)
            continue

        existing = grouped_by_id.get(property_id)
        if existing is None:
            item["listing_id"] = property_id
            item["property_id"] = property_id
            item["grouped_business_types"] = [business_type] if business_type in {"rent", "sale"} else []
            item["primary_business_type"] = business_type or None
            grouped_by_id[property_id] = item
            grouped_records.append(item)
            continue

        existing_types = list(existing.get("grouped_business_types") or [])
        if business_type in {"rent", "sale"} and business_type not in existing_types:
            existing_types.append(business_type)
        existing["grouped_business_types"] = existing_types

    return grouped_records


def _clean_description_text(value: Any) -> str | None:
    if value is None:
        return None
    text = html_lib.unescape(str(value)).strip()
    return text or None


def _extract_meta_description(html: str) -> str | None:
    match = re.search(
        r'<meta\s+name="description"\s+content="([^"]*)"',
        html,
        flags=re.IGNORECASE,
    )
    if not match:
        return None
    return _clean_description_text(match.group(1))


def _extract_json_ld_description(html: str) -> str | None:
    for match in re.finditer(
        r'<script[^>]+type="application/ld\+json"[^>]*>(.*?)</script>',
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
            description = _clean_description_text(entry.get("description"))
            if description:
                return description
    return None


def _index_listings_by_business_context(listings: Any) -> dict[str, dict[str, Any]]:
    indexed: dict[str, dict[str, Any]] = {}
    if not isinstance(listings, list):
        return indexed
    for listing in listings:
        if not isinstance(listing, dict):
            continue
        context = str(listing.get("businessContext") or "").strip().upper()
        if context in {"RENT", "SALE"} and context not in indexed:
            indexed[context] = listing
    return indexed


def _coalesce_house_value(house_info: Mapping[str, Any], house_state: Mapping[str, Any], key: str) -> Any:
    value = house_info.get(key)
    if value is not None:
        return value
    return house_state.get(key)


def _resolve_quinto_business_type(
    house_state: Mapping[str, Any],
    house_info: Mapping[str, Any],
    grouped_business_types: Iterable[str | None] | None,
    primary_business_type: str | None,
    fallback_url: str | None,
) -> str | None:
    for_rent = _coalesce_house_value(house_info, house_state, "forRent")
    for_sale = _coalesce_house_value(house_info, house_state, "forSale")

    if for_rent is True and for_sale is True:
        return "rent|sale"
    if for_rent is True:
        return "rent"
    if for_sale is True:
        return "sale"

    grouped_value = _compose_business_type(grouped_business_types or [])
    if grouped_value:
        return grouped_value

    primary = _compose_business_type([primary_business_type])
    if primary:
        return primary

    return _derive_business_type_from_listing_url(fallback_url)


def parse_listing_page_html(
    html: str,
    business_type: str | None = None,
    fallback_url: str | None = None,
    grouped_business_types: Iterable[str | None] | None = None,
    primary_business_type: str | None = None,
) -> dict[str, Any]:
    next_data = extract_next_data(html)
    page_props = next_data["props"]["pageProps"]
    state = page_props.get("initialState")
    if not isinstance(state, Mapping):
        raise MissingInitialStateError("Nao foi possivel localizar um initialState valido.")
    house_state = state.get("house") or {}
    house_info = ((house_state.get("houseInfo")) or {})
    listings_by_context = _index_listings_by_business_context(house_info.get("listings"))
    generated_description = house_info.get("generatedDescription") or {}
    range_floor = house_info.get("rangeFloor") or {}
    remarks_description = _clean_description_text(house_info.get("remarks"))
    structured_description = (
        _extract_json_ld_description(html)
        or _extract_meta_description(html)
    )
    long_description = (
        _clean_description_text(generated_description.get("longDescription"))
        or remarks_description
        or structured_description
    )
    short_rent_description = _clean_description_text(generated_description.get("shortRentDescription"))
    short_sale_description = _clean_description_text(generated_description.get("shortSaleDescription"))
    address = normalize_address_value(house_info.get("address"))
    photos = house_info.get("photos") or []
    photo_urls = [
        f"https://images.quintoandar.com.br/{photo.get('url')}"
        for photo in photos
        if isinstance(photo, dict) and photo.get("url")
    ]
    rent_listing = listings_by_context.get("RENT") or {}
    sale_listing = listings_by_context.get("SALE") or {}
    resolved_business_type = _resolve_quinto_business_type(
        house_state,
        house_info,
        grouped_business_types=grouped_business_types,
        primary_business_type=primary_business_type or business_type,
        fallback_url=fallback_url,
    )

    return {
        "listing_url": fallback_url,
        "property_id": _derive_property_id_from_listing_url(fallback_url),
        "business_type": resolved_business_type,
        "display_id": house_info.get("displayId"),
        #"zip_code": address["zip_code"],
        "zipcode": address["zip_code"],
        "street": address["address"],
        "neighbourhood": address["neighbourhood"],
        "city": address["city"],
        "state": address["state"],
        "lat": address["lat"],
        "lon": address["lon"],
        "rent_listing_created_at": rent_listing.get("firstPublicationDate"),
        "sale_listing_created_at": sale_listing.get("firstPublicationDate"),
        "rent_last_publication_date": rent_listing.get("lastPublicationDate"),
        "sale_last_publication_date": sale_listing.get("lastPublicationDate"),
        "rent_listing_status": rent_listing.get("status"),
        "sale_listing_status": sale_listing.get("status"),
        "construction_year": _coalesce_house_value(house_info, house_state, "constructionYear"),
        "range_floor_min": range_floor.get("min"),
        "range_floor_max": range_floor.get("max"),
        "for_rent": _coalesce_house_value(house_info, house_state, "forRent"),
        "for_sale": _coalesce_house_value(house_info, house_state, "forSale"),
        "property_type": house_info.get("type"),
        "total_area_m2": house_info.get("area"),
        "bedrooms": house_info.get("bedrooms"),
        "bathrooms": house_info.get("bathrooms"),
        "parking_spots": (
            house_info.get("parkingSpots")
            or house_info.get("parkingSpaces")
            or house_info.get("garageSpots")
            or house_info.get("garageSpaces")
            or house_info.get("parking")
            or 0
        ),
        "suites": house_info.get("suites"),
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
        "sale_price_brl": house_info.get("salePrice"),
        "rent_price_brl": house_info.get("rentPrice"),
        #"main_image_url": photo_urls[0] if photo_urls else None,
        #"gallery_urls_json": compact_json(photo_urls) if photo_urls else None,
        "amenities_json": compact_json(house_info.get("amenities")),
        "comfort_commodities_json": compact_json(house_info.get("comfortCommodities")),
        "practicality_commodities_json": compact_json(house_info.get("practicalityCommodities")),
        "installations_json": compact_json(house_info.get("installations")),
        "pois_json": compact_json(house_info.get("placesNearby")),
        "long_description": long_description,
        "description": long_description
        or short_rent_description
        or short_sale_description
        or structured_description,
        "short_rent_description": short_rent_description,
        "short_sale_description": short_sale_description,
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
        user_agent=HEADERS["User-Agent"],
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
        impersonate="chrome110",
        jobdir=jobdir,
    )


class QuintoListingsSpider(BaseListingsSpider):
    name = "quinto_listings"
    allowed_domains = ["quintoandar.com.br", "www.quintoandar.com.br"]
    terminal_not_found_statuses = {404, 500}
    missing_next_data_attempt_limit = 3
    missing_initial_state_attempt_limit = 3

    def build_request(self, record: Dict[str, Any], *, scheduled_index: int) -> Request | None:
        listing_url = str(record.get("listing_url") or "").strip()
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
                "primary_business_type": record.get("primary_business_type") or record.get("business_type"),
                "grouped_business_types": list(record.get("grouped_business_types") or []),
                "scheduled_index": scheduled_index,
                "handle_httpstatus_all": True,
            },
        )

    def parse_record(self, response: Response) -> dict[str, Any]:
        return parse_listing_page_html(
            response.text,
            business_type=response.meta.get("business_type"),
            fallback_url=str(response.meta["listing_url"]),
            grouped_business_types=response.meta.get("grouped_business_types"),
            primary_business_type=response.meta.get("primary_business_type"),
        )

    def handle_parse_error(self, response: Response, exc: Exception):
        if int(response.status) == 200 and isinstance(exc, MissingNextDataError):
            return self._handle_missing_payload_error(
                response,
                exc,
                error_code="missing_next_data",
                attempts=self.missing_next_data_attempts,
                attempt_limit=self.missing_next_data_attempt_limit,
                failure_metric="listing_page_missing_next_data_failures",
                terminal_metric="listing_page_missing_next_data_terminal",
            )
        if int(response.status) == 200 and isinstance(exc, MissingInitialStateError):
            return self._handle_missing_payload_error(
                response,
                exc,
                error_code="missing_initial_state",
                attempts=self.missing_initial_state_attempts,
                attempt_limit=self.missing_initial_state_attempt_limit,
                failure_metric="listing_page_missing_initial_state_failures",
                terminal_metric="listing_page_missing_initial_state_terminal",
            )
        return super().handle_parse_error(response, exc)

    def _handle_missing_payload_error(
        self,
        response: Response,
        exc: Exception,
        *,
        error_code: str,
        attempts: dict[str, int],
        attempt_limit: int,
        failure_metric: str,
        terminal_metric: str,
    ):
        scheduled_index = int(response.meta["scheduled_index"])
        resume_record = response.meta.get("_resume_record") or response.meta
        resume_key = build_listing_resume_key(resume_record)
        previous_attempts = int(attempts.get(str(resume_key), 0) or 0)
        attempt = previous_attempts + 1
        if resume_key:
            attempts[str(resume_key)] = attempt

        self.metrics[failure_metric] += 1
        property_id = str(
            resume_record.get("property_id")
            or resume_record.get("listing_id")
            or ""
        ).strip() or None

        if attempt < attempt_limit:
            log_warn(
                f"listing_collection_item_{error_code}_retry",
                label=self.label,
                processed=f"{scheduled_index}/{self.total_records}",
                url=response.url,
                property_id=property_id,
                status=response.status,
                attempt=attempt,
                attempt_limit=attempt_limit,
                error=exc,
            )
            self.metrics["listing_page_requests"] += 1
            self._persist_runtime_state(status="in_progress")
            return response.request.replace(dont_filter=True)

        self.metrics[terminal_metric] += 1
        self._mark_terminal_processed(
            resume_record,
            status=error_code,
            scheduled_index=scheduled_index,
            url=response.url,
        )
        if resume_key:
            attempts.pop(str(resume_key), None)
        log_warn(
            f"listing_collection_item_{error_code}_terminal",
            label=self.label,
            processed=f"{scheduled_index}/{self.total_records}",
            url=response.url,
            property_id=property_id,
            status=response.status,
            attempt=attempt,
            attempt_limit=attempt_limit,
            error=exc,
        )
        self._finalize_attempt()
        return None

    def handle_parse_success(self, response: Response) -> None:
        resume_record = response.meta.get("_resume_record") or response.meta
        resume_key = build_listing_resume_key(resume_record)
        if resume_key:
            self.missing_next_data_attempts.pop(str(resume_key), None)
            self.missing_initial_state_attempts.pop(str(resume_key), None)


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
    grouped_records = _group_quinto_records(records)
    return run_batched_scrapy_collection(
        records=grouped_records,
        label=label,
        max_consecutive_failures=max_consecutive_failures,
        listings_output_path=listings_output_path,
        listings_parquet_output_path=listings_parquet_output_path,
        spider_cls=QuintoListingsSpider,
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
        incomplete_output_path = build_incomplete_output_path(listings_output_path)
        incomplete_parquet_output_path = build_incomplete_output_path(listings_parquet_output_path)
        save_csv(listings_records, filename=incomplete_output_path)
        save_parquet(listings_records, filename=incomplete_parquet_output_path)
        failed_state = load_resume_state(resume_paths["state_json"])
        failed_state.update(
            {
                "status": "failed_terminal",
                "updated_at": utc_now_iso(),
                "metrics": metrics,
                "output_rows": len(listings_records),
                "incomplete_output_path": str(incomplete_output_path),
                "incomplete_parquet_output_path": str(incomplete_parquet_output_path),
                "incomplete_output_rows": len(listings_records),
            }
        )
        save_resume_state(resume_paths["state_json"], failed_state)
        raise RuntimeError(f"{label} abortado por max_consecutive_failures")

    if int(metrics.get("pending_records", 0) or 0) > 0:
        incomplete_output_path = build_incomplete_output_path(listings_output_path)
        incomplete_parquet_output_path = build_incomplete_output_path(listings_parquet_output_path)
        save_csv(listings_records, filename=incomplete_output_path)
        save_parquet(listings_records, filename=incomplete_parquet_output_path)
        in_progress_state = load_resume_state(resume_paths["state_json"])
        in_progress_state.update(
            {
                "status": "in_progress",
                "updated_at": utc_now_iso(),
                "metrics": metrics,
                "output_rows": len(listings_records),
                "pending_rows": int(metrics.get("pending_records", 0) or 0),
                "incomplete_output_path": str(incomplete_output_path),
                "incomplete_parquet_output_path": str(incomplete_parquet_output_path),
                "incomplete_output_rows": len(listings_records),
            }
        )
        save_resume_state(resume_paths["state_json"], in_progress_state)
        raise RuntimeError(f"{label} ainda possui listings pendentes para retomada")

    temp_csv_path = Path(listings_output_path).with_suffix(Path(listings_output_path).suffix + ".tmp")
    temp_parquet_path = Path(listings_parquet_output_path).with_suffix(Path(listings_parquet_output_path).suffix + ".tmp")
    save_csv(listings_records, filename=str(temp_csv_path))
    save_parquet(listings_records, filename=str(temp_parquet_path))
    temp_csv_path.replace(listings_output_path)
    temp_parquet_path.replace(listings_parquet_output_path)
    cleanup_incomplete_outputs(listings_output_path, listings_parquet_output_path)
    completed_state = load_resume_state(resume_paths["state_json"])
    completed_state.update(
        {
            "status": "completed",
            "updated_at": utc_now_iso(),
            "input_rows": len(base_records),
            "output_rows": len(listings_records),
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
        "output_rows": len(listings_records),
        "resume_state_path": str(resume_paths["state_json"]),
    }
