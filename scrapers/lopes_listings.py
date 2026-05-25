from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

import scrapy
from scrapy.http import Request, Response

from scrapers.http_metrics import init_metrics
from scrapers.io_utils import load_csv_records
from scrapers.listings_resume import (
    BaseListingsSpider,
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
from scrapers.lopes_shared import *  # noqa: F403
from scrapers.scrapy_runner import run_spider
from scrapers.scrapy_support import build_scrapy_settings as build_base_scrapy_settings


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


def find_nested_map(data: Any) -> Dict[str, Any]:
    if isinstance(data, dict):
        map_value = data.get("map")
        if isinstance(map_value, dict):
            return map_value
        for value in data.values():
            found = find_nested_map(value)
            if found:
                return found
    elif isinstance(data, list):
        for item in data:
            found = find_nested_map(item)
            if found:
                return found
    return {}


def _derive_property_id_from_listing_url(url: str | None) -> str | None:
    match = re.search(r"/imovel/([^/?#]+)", str(url or ""))
    if not match:
        return None
    return match.group(1)


def _derive_business_type_from_listing_url(url: str | None) -> str | None:
    slug_match = re.search(r"/imovel/[^/]+/([^/?#]+)", str(url or "").lower())
    if slug_match:
        slug = slug_match.group(1)
        if slug.startswith("venda-"):
            return "sale"
        if slug.startswith("aluguel-"):
            return "rent"
    return None


def _has_required_listing_keys(record: Dict[str, Any]) -> bool:
    business_type = str(record.get("business_type") or "").strip()
    property_id = str(record.get("property_id") or "").strip()
    listing_url = str(record.get("listing_url") or "").strip()
    return bool(business_type and (property_id or listing_url))


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    try:
        return float(str(value).strip())
    except (TypeError, ValueError):
        return None


def parse_listing_page_html(html: str, fallback_url: str | None = None) -> dict[str, Any]:
    match = re.search(r'<script id="ng-state" type="application/json">\s*(\{[\s\S]*?\})\s*</script>', html)
    if not match:
        return {}
    data = json.loads(match.group(1))
    product = find_nested_product(data)
    if not product:
        return {}
    map_data = find_nested_map(data)
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
    listing_url = absolute_url(seo.get("url"), BASE_SITE_URL) or fallback_url
    return {
        "description": product.get("description"),
        "property_id": _derive_property_id_from_listing_url(listing_url),
        "business_type": _derive_business_type_from_listing_url(listing_url),
        "address": address.get("formatted") or address.get("street"),
        "street": address.get("street"),
        "city": address.get("city"),
        "neighbourhood": address.get("neighborhood"),
        "state": address.get("state"),
        "zipcode": None,
        "lat": _to_float(map_data.get("lat")),
        "lon": _to_float(map_data.get("lng")),
        "property_type": product.get("name"),
        "total_area_m2": extract_attr_number(attributes, "total_area_attr") or extract_attr_number(attributes, "area_attr"),
        "area": extract_attr_number(attributes, "area_attr"),
        "suites": extract_attr_number(attributes, "suite_attr") or 0,
        "bedrooms": extract_attr_number(attributes, "bedroom_attr"),
        "bathrooms": extract_attr_number(attributes, "bathroom_attr"),
        "parking": extract_attr_number(attributes, "parking_lots_attr") or 0,
        "condominium_name": condominium.get("name"),
        #"condominium_id": condominium.get("id"),
        "condominium_url": absolute_url(condominium.get("url"), BASE_SITE_URL),
        "condominium_amenities_json": compact_json(condominium.get("amenities")),
        "features_json": compact_json(features),
        "pois_json": compact_json(pois),
        #"advertiser_name": advertiser.get("name") or advertiser.get("shortName"),
        #"advertiser_id": listing_owner.get("id"),
        #"seller_type": listing_owner.get("type"),
        #"main_image_url": photo_urls[0] if photo_urls else None,
        #"gallery_urls_json": compact_json(photo_urls) if photo_urls else None,
        "condo_fee_brl": prices.get("condominium"),
        "sale_price_brl": prices.get("sale"),
        "rent_price_brl": prices.get("rent"),
        "total_rent_price_brl": prices.get("fullMonthlyPrice"),
        "listing_url": listing_url,
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
        user_agent=LISTING_HEADERS["User-Agent"],
        default_headers=LISTING_HEADERS,
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


class LopesListingsSpider(BaseListingsSpider):
    name = "lopes_listings"
    allowed_domains = ["lopes.com.br", "www.lopes.com.br"]

    def build_request(self, record: Dict[str, Any], *, scheduled_index: int) -> Request | None:
        listing_url = str(record.get("listing_url") or "").strip()
        if not listing_url:
            return None
        return Request(
            url=listing_url,
            callback=self.parse_listing_response,
            errback=self.handle_request_error,
            headers=LISTING_HEADERS,
            meta={
                "listing_url": listing_url,
                "scheduled_index": scheduled_index,
                "handle_httpstatus_all": True,
            },
        )

    def parse_record(self, response: Response) -> dict[str, Any]:
        return parse_listing_page_html(response.text, fallback_url=str(response.meta["listing_url"]))


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
        spider_cls=LopesListingsSpider,
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
