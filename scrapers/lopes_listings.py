from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List
from urllib.parse import parse_qsl, urlparse

import pandas as pd
import scrapy
from scrapy.http import Request, Response

from scrapers.http_metrics import init_metrics
from scrapers.io_utils import load_csv_records
from scrapers.logging_utils import log_warn
from scrapers.listings_resume import (
    BaseListingsSpider,
    TERMINAL_NO_OUTPUT_STATUSES,
    build_listing_resume_key,
    build_incomplete_output_path,
    build_resume_paths,
    cleanup_incomplete_outputs,
    cleanup_resume_runtime,
    default_resume_dir,
    load_resume_state,
    load_jsonl_records,
    dedupe_listing_records,
    run_batched_scrapy_collection,
    save_resume_state,
    utc_now_iso,
)
from scrapers.lopes_state import (
    claim_daily_batch,
    finalize_claim,
    get_metadata,
    queue_metrics,
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
    terminal_not_found_statuses = {404, 410}

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
        parsed = parse_listing_page_html(response.text, fallback_url=str(response.meta["listing_url"]))
        resume_record = response.meta.get("_resume_record") or {}
        if parsed:
            parsed.update(
                {
                    "queue_listing_url": resume_record.get("listing_url") or response.meta["listing_url"],
                    "discovered_at": resume_record.get("discovered_at"),
                    "sitemap_lastmod": resume_record.get("sitemap_lastmod") or resume_record.get("lastmod"),
                    "sitemap_generation_id": resume_record.get("sitemap_generation_id"),
                    "collection_priority": resume_record.get("collection_priority"),
                }
            )
        return parsed

    @staticmethod
    def _is_lopes_host(hostname: str | None) -> bool:
        normalized = str(hostname or "").strip().lower()
        return normalized == "lopes.com.br" or normalized.endswith(".lopes.com.br")

    @classmethod
    def _is_listing_to_not_found_redirect(cls, original_url: str, final_url: str) -> bool:
        original = urlparse(original_url)
        final = urlparse(final_url)
        original_path = original.path.rstrip("/").lower()
        final_path = final.path.rstrip("/").lower()
        query_keys = {key.lower() for key, _ in parse_qsl(final.query, keep_blank_values=True)}
        return (
            cls._is_lopes_host(original.hostname)
            and cls._is_lopes_host(final.hostname)
            and (original_path == "/imovel" or original_path.startswith("/imovel/"))
            and (final_path == "/busca" or final_path.startswith("/busca/"))
            and "notfound" in query_keys
        )

    def handle_response_before_parse(self, response: Response) -> bool:
        original_url = str(response.meta.get("listing_url") or response.request.url)
        final_url = str(response.url)
        if not self._is_listing_to_not_found_redirect(original_url, final_url):
            return False

        scheduled_index = int(response.meta["scheduled_index"])
        resume_record = response.meta.get("_resume_record") or response.meta
        resume_key = build_listing_resume_key(resume_record)
        property_id = str(
            resume_record.get("property_id")
            or resume_record.get("listing_id")
            or _derive_property_id_from_listing_url(original_url)
            or ""
        ).strip() or None

        self.metrics["listing_page_failures"] += 1
        self.metrics["listing_page_not_founds"] += 1
        self.metrics["listing_page_redirected_to_not_found"] += 1
        self._mark_terminal_processed(
            resume_record,
            status="redirected_to_not_found",
            scheduled_index=scheduled_index,
            url=final_url,
        )
        log_warn(
            "listing_collection_item_redirected_to_not_found",
            label=self.label,
            processed=f"{scheduled_index}/{self.total_records}",
            original_url=original_url,
            final_url=final_url,
            property_id=property_id,
            status=response.status,
            terminal_status="redirected_to_not_found",
            resume_key=resume_key,
        )
        self._finalize_attempt()
        return True


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
    state_db_path: str | None = None,
    daily_limit: int = 10_000,
    new_quota: int = 8_000,
    refresh_quota: int = 2_000,
) -> dict[str, Any] | None:
    if state_db_path:
        return _collect_persistent_queue(
            listings_output_path=listings_output_path,
            listings_parquet_output_path=listings_parquet_output_path,
            state_db_path=state_db_path,
            max_consecutive_failures=max_consecutive_failures,
            label=label,
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
            daily_limit=daily_limit,
            new_quota=new_quota,
            refresh_quota=refresh_quota,
        )
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


def _collect_persistent_queue(
    *,
    listings_output_path: str,
    listings_parquet_output_path: str,
    state_db_path: str,
    max_consecutive_failures: int,
    label: str,
    resume_dir: str | None,
    verbose: bool,
    retry_times: int,
    autothrottle_start_delay: float,
    autothrottle_max_delay: float,
    autothrottle_target_concurrency: float,
    concurrent_requests: int,
    concurrent_requests_per_domain: int,
    download_delay: float,
    download_timeout: int,
    daily_limit: int,
    new_quota: int,
    refresh_quota: int,
) -> dict[str, Any]:
    run_date = infer_run_date_from_output_path(listings_output_path) or utc_now_iso()[:10]
    claim_token = f"lopes:{run_date}"
    claimed_records = claim_daily_batch(
        state_db_path,
        claim_token=claim_token,
        daily_limit=daily_limit,
        new_quota=new_quota,
        refresh_quota=refresh_quota,
    )
    resolved_resume_dir = (
        default_resume_dir(label=label, listings_output_path=listings_output_path)
        if resume_dir is None
        else Path(resume_dir)
    ) / "queue"
    resume_paths = build_resume_paths(resolved_resume_dir)
    listings_records: list[dict[str, Any]] = []
    scraper_metrics: dict[str, Any] = {}
    terminal_urls: set[str] = set()
    attempted_urls: set[str] = set()

    try:
        if claimed_records:
            listings_records, scraper_metrics = run_scrapy_collection(
                records=claimed_records,
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
            claimed_urls = {str(record["listing_url"]) for record in claimed_records}
            claimed_by_id = {
                str(record.get("listing_id") or "").strip(): str(record["listing_url"])
                for record in claimed_records
                if str(record.get("listing_id") or "").strip()
            }
            for record in load_jsonl_records(resume_paths["processed_jsonl"]):
                ledger_url = str(record.get("listing_url") or "").strip()
                property_id = str(record.get("property_id") or "").strip()
                resolved_url: str | None = None
                if ledger_url in claimed_urls:
                    resolved_url = ledger_url
                elif property_id in claimed_by_id:
                    resolved_url = claimed_by_id[property_id]
                if resolved_url:
                    attempted_urls.add(resolved_url)
                    if str(record.get("status") or "") in TERMINAL_NO_OUTPUT_STATUSES:
                        terminal_urls.add(resolved_url)
        successful_urls = {
            str(record.get("queue_listing_url") or record.get("listing_url") or "").strip()
            for record in listings_records
            if str(record.get("queue_listing_url") or record.get("listing_url") or "").strip()
        }
        attempted_urls.update(successful_urls)
        state_metrics = finalize_claim(
            state_db_path,
            claim_token=claim_token,
            successful_urls=successful_urls,
            terminal_urls=terminal_urls,
            attempted_urls=attempted_urls,
            error=str(scraper_metrics.get("stop_reason") or "transient_failure"),
        ) if claimed_records else queue_metrics(state_db_path)
    except Exception as exc:
        finalize_claim(
            state_db_path,
            claim_token=claim_token,
            successful_urls=set(),
            terminal_urls=set(),
            attempted_urls=set(),
            error=str(exc),
        )
        raise

    bootstrap_path_text = get_metadata(state_db_path, "bootstrap_results_path")
    bootstrap_published = get_metadata(state_db_path, "bootstrap_results_published") == "1"
    bootstrap_records: list[dict[str, Any]] = []
    if bootstrap_path_text and not bootstrap_published and Path(bootstrap_path_text).exists():
        bootstrap_frame = pd.read_parquet(bootstrap_path_text)
        bootstrap_frame = bootstrap_frame.where(pd.notna(bootstrap_frame), None)
        bootstrap_records = bootstrap_frame.to_dict(orient="records")

    output_records = dedupe_listing_records([*bootstrap_records, *listings_records])
    if output_records:
        temp_csv_path = Path(listings_output_path).with_suffix(Path(listings_output_path).suffix + ".tmp")
        temp_parquet_path = Path(listings_parquet_output_path).with_suffix(Path(listings_parquet_output_path).suffix + ".tmp")
        save_csv(output_records, filename=str(temp_csv_path))
        save_parquet(output_records, filename=str(temp_parquet_path))
        temp_csv_path.replace(listings_output_path)
        temp_parquet_path.replace(listings_parquet_output_path)
        cleanup_incomplete_outputs(listings_output_path, listings_parquet_output_path)

    cleanup_resume_runtime(resume_paths["jobdir"], resume_paths["partial_jsonl"], resume_paths["processed_jsonl"])
    return {
        "input_rows": len(claimed_records),
        "output_rows": len(output_records),
        "no_op": not output_records,
        "selected_today": len(claimed_records),
        "bootstrap_rows_included": len(bootstrap_records),
        "scraper_metrics": scraper_metrics,
        **state_metrics,
    }
