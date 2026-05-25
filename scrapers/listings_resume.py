from __future__ import annotations

import csv
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Sequence

import scrapy
from scrapy.exceptions import CloseSpider
from scrapy.http import Request, Response

from scrapers.discovery_incremental import infer_run_date_from_output_path
from scrapers.http_metrics import init_metrics, record_request
from scrapers.io_utils import save_parquet_records
from scrapers.logging_utils import (
    log_listing_collection_item,
    log_listing_collection_phase_end,
    log_listing_collection_phase_start,
    log_listing_collection_progress,
    log_warn,
)


DEFAULT_FLUSH_BATCH_SIZE = 500
DEFAULT_INCOMPLETE_SNAPSHOT_BATCH_SIZE = 1000
DEFAULT_LISTING_BATCH_SIZE = 500
TERMINAL_NO_OUTPUT_STATUSES = {"not_found", "skipped_no_url"}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def build_resume_paths(resume_dir: str | Path) -> dict[str, Path]:
    root = Path(resume_dir)
    return {
        "root": root,
        "jobdir": root / "jobdir",
        "current_jobdir": root / "jobdir" / "current",
        "partial_jsonl": root / "records.partial.jsonl",
        "processed_jsonl": root / "processed.partial.jsonl",
        "state_json": root / "resume_state.json",
    }


def default_resume_dir(*, label: str, listings_output_path: str | Path, project_root: Path | None = None) -> Path:
    run_date = infer_run_date_from_output_path(listings_output_path) or datetime.now().strftime("%d-%m-%Y")
    root = (project_root or Path.cwd()).resolve()
    return root / "artifacts" / run_date / "collect_listings" / label


def _write_json_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(path.suffix + ".tmp")
    temp_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    temp_path.replace(path)


def load_resume_state(path: str | Path) -> dict[str, Any]:
    state_path = Path(path)
    if not state_path.exists():
        return {}
    try:
        return json.loads(state_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def save_resume_state(path: str | Path, payload: Mapping[str, Any]) -> None:
    _write_json_atomic(Path(path), payload)


def append_jsonl_records(path: str | Path, records: Iterable[Mapping[str, Any]]) -> int:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    written = 0
    with output_path.open("a", encoding="utf-8", newline="\n") as handle:
        for record in records:
            handle.write(json.dumps(dict(record), ensure_ascii=False) + "\n")
            written += 1
    return written


def load_jsonl_records(path: str | Path) -> list[dict[str, Any]]:
    input_path = Path(path)
    if not input_path.exists():
        return []
    records: list[dict[str, Any]] = []
    with input_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            text = line.strip()
            if not text:
                continue
            try:
                value = json.loads(text)
            except json.JSONDecodeError:
                continue
            if isinstance(value, dict):
                records.append(value)
    return records


def _normalize_key_part(value: Any) -> str:
    return str(value or "").strip()


def build_listing_resume_key(record: Mapping[str, Any]) -> str | None:
    explicit_key = _normalize_key_part(record.get("key"))
    if explicit_key:
        return explicit_key
    for key in ("property_id", "listing_id"):
        value = _normalize_key_part(record.get(key))
        if value:
            return f"id:{value}"
    listing_url = _normalize_key_part(record.get("listing_url") or record.get("url"))
    if listing_url:
        return f"url:{listing_url}"
    return f"record:{json.dumps(dict(record), sort_keys=True, ensure_ascii=False, default=str)}"


def build_processed_ledger_entry(
    record: Mapping[str, Any],
    *,
    status: str,
    scheduled_index: int | None = None,
    label: str | None = None,
    url: str | None = None,
) -> dict[str, Any] | None:
    key = build_listing_resume_key(record)
    if not key:
        return None
    payload = {
        "key": key,
        "status": status,
        "updated_at": utc_now_iso(),
        "listing_url": _normalize_key_part(url or record.get("listing_url") or record.get("url")) or None,
        "property_id": _normalize_key_part(record.get("property_id") or record.get("listing_id")) or None,
        "business_type": _normalize_key_part(record.get("business_type") or record.get("primary_business_type")) or None,
    }
    if label:
        payload["label"] = label
    if scheduled_index is not None:
        payload["scheduled_index"] = scheduled_index
    return payload


def load_processed_record_keys(path: str | Path) -> set[str]:
    keys: set[str] = set()
    for record in load_jsonl_records(path):
        status = str(record.get("status") or "").strip()
        if status and status not in TERMINAL_NO_OUTPUT_STATUSES:
            continue
        key = build_listing_resume_key(record)
        if key:
            keys.add(key)
    return keys


def load_saved_record_keys(path: str | Path) -> set[str]:
    keys: set[str] = set()
    for record in load_jsonl_records(path):
        key = build_listing_resume_key(record)
        if key:
            keys.add(key)
    return keys


def load_completed_record_keys(partial_jsonl_path: str | Path, processed_jsonl_path: str | Path) -> set[str]:
    return load_saved_record_keys(partial_jsonl_path) | load_processed_record_keys(processed_jsonl_path)


def pending_listing_records(
    records: Sequence[Mapping[str, Any]],
    *,
    partial_jsonl_path: str | Path,
    processed_jsonl_path: str | Path,
) -> list[dict[str, Any]]:
    completed_keys = load_completed_record_keys(partial_jsonl_path, processed_jsonl_path)
    pending: list[dict[str, Any]] = []
    seen_input_keys: set[str] = set()
    for record in records:
        item = dict(record)
        key = build_listing_resume_key(item)
        if key and key in seen_input_keys:
            continue
        if key:
            seen_input_keys.add(key)
        if key and key in completed_keys:
            continue
        pending.append(item)
    return pending


def dedupe_listing_records(records: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    deduped: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str, str]] = set()
    for record in records:
        item = dict(record)
        source = str(item.get("source") or "").strip()
        business_type = str(item.get("business_type") or "").strip()
        property_id = str(item.get("property_id") or "").strip()
        listing_url = str(item.get("listing_url") or "").strip()
        key = (source, business_type, property_id, "")
        if not property_id:
            key = (source, business_type, "", listing_url)
        if not property_id and not listing_url:
            continue
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return deduped


def cleanup_resume_runtime(
    jobdir_path: str | Path,
    partial_jsonl_path: str | Path,
    processed_jsonl_path: str | Path | None = None,
) -> None:
    jobdir = Path(jobdir_path)
    partial = Path(partial_jsonl_path)
    if jobdir.exists():
        shutil.rmtree(jobdir, ignore_errors=True)
    if partial.exists():
        partial.unlink(missing_ok=True)
    if processed_jsonl_path is not None:
        processed = Path(processed_jsonl_path)
        if processed.exists():
            processed.unlink(missing_ok=True)


def build_incomplete_output_path(path: str | Path) -> Path:
    output_path = Path(path)
    return output_path.with_name(f"incomplete_{output_path.name}")


def save_listing_records_snapshot(
    records: list[Mapping[str, Any]],
    *,
    csv_path: str | Path,
    parquet_path: str | Path,
) -> None:
    csv_output_path = Path(csv_path)
    csv_output_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = sorted({key for record in records for key in record.keys()})
    with csv_output_path.open("w", newline="", encoding="utf-8-sig") as file:
        if fieldnames:
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(records)
    save_parquet_records(records, parquet_path)


def cleanup_incomplete_outputs(*paths: str | Path) -> None:
    for path in paths:
        build_incomplete_output_path(path).unlink(missing_ok=True)


def restore_metrics(metrics: dict[str, Any], saved_metrics: Mapping[str, Any] | None) -> dict[str, Any]:
    if not isinstance(saved_metrics, Mapping):
        return metrics
    restored = dict(metrics)
    for key in restored:
        if key in saved_metrics:
            restored[key] = saved_metrics[key]
    return restored


def sync_resume_metrics_from_ledgers(
    metrics: dict[str, Any],
    *,
    partial_jsonl_path: str | Path,
    processed_jsonl_path: str | Path,
) -> dict[str, Any]:
    saved_records = dedupe_listing_records(load_jsonl_records(partial_jsonl_path))
    completed_keys = load_completed_record_keys(partial_jsonl_path, processed_jsonl_path)
    metrics["items_kept"] = len(saved_records)
    metrics["listing_page_successes"] = max(int(metrics.get("listing_page_successes", 0) or 0), len(saved_records))
    metrics["pages_processed"] = len(completed_keys)
    return metrics


def run_batched_scrapy_collection(
    *,
    records: list[dict[str, Any]],
    label: str,
    max_consecutive_failures: int,
    listings_output_path: str,
    listings_parquet_output_path: str,
    spider_cls: type[BaseException] | type,
    build_scrapy_settings: Callable[..., dict[str, Any]],
    run_spider: Callable[..., None],
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
    batch_size: int = DEFAULT_LISTING_BATCH_SIZE,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    resolved_resume_dir = default_resume_dir(
        label=label,
        listings_output_path=listings_output_path,
    ) if resume_dir is None else Path(resume_dir)
    resume_paths = build_resume_paths(resolved_resume_dir)
    saved_state = load_resume_state(resume_paths["state_json"])
    metrics = restore_metrics(init_metrics(label), saved_state.get("metrics"))
    metrics["items_seen"] = len(records)
    metrics["batch_size"] = batch_size
    metrics.setdefault("batches_started", 0)
    metrics.setdefault("pending_records", 0)
    metrics = sync_resume_metrics_from_ledgers(
        metrics,
        partial_jsonl_path=resume_paths["partial_jsonl"],
        processed_jsonl_path=resume_paths["processed_jsonl"],
    )
    initial_consecutive_failures = (
        int(saved_state.get("consecutive_failures") or 0)
        if saved_state.get("status") == "in_progress"
        else 0
    )

    while True:
        pending = pending_listing_records(
            records,
            partial_jsonl_path=resume_paths["partial_jsonl"],
            processed_jsonl_path=resume_paths["processed_jsonl"],
        )
        metrics["pending_records"] = len(pending)
        if not pending:
            metrics["stop_reason"] = "completed"
            break
        if metrics.get("stop_reason") == "max_consecutive_failures":
            break

        batch_records = pending[: max(1, int(batch_size))]
        completed_before = len(
            load_completed_record_keys(
                resume_paths["partial_jsonl"],
                resume_paths["processed_jsonl"],
            )
        )
        current_jobdir = resume_paths["current_jobdir"]
        if current_jobdir.exists():
            shutil.rmtree(current_jobdir, ignore_errors=True)

        metrics["batches_started"] = int(metrics.get("batches_started", 0) or 0) + 1
        save_resume_state(
            resume_paths["state_json"],
            {
                "label": label,
                "status": "in_progress",
                "updated_at": utc_now_iso(),
                "input_rows": len(records),
                "pending_rows": len(pending),
                "current_batch_rows": len(batch_records),
                "output_path": str(listings_output_path),
                "parquet_output_path": str(listings_parquet_output_path),
                "partial_jsonl_path": str(resume_paths["partial_jsonl"]),
                "processed_jsonl_path": str(resume_paths["processed_jsonl"]),
                "jobdir": str(resume_paths["jobdir"]),
                "current_jobdir": str(current_jobdir),
                "metrics": metrics,
                "consecutive_failures": initial_consecutive_failures,
                "pages_processed": int(metrics.get("pages_processed", 0) or 0),
            },
        )

        collector: dict[str, Any] = {"records": [], "metrics": metrics}
        run_spider(
            spider_cls,
            settings=build_scrapy_settings(
                verbose=verbose,
                retry_times=retry_times,
                autothrottle_start_delay=autothrottle_start_delay,
                autothrottle_max_delay=autothrottle_max_delay,
                autothrottle_target_concurrency=autothrottle_target_concurrency,
                concurrent_requests=concurrent_requests,
                concurrent_requests_per_domain=concurrent_requests_per_domain,
                download_delay=download_delay,
                download_timeout=download_timeout,
                jobdir=str(current_jobdir),
            ),
            records=batch_records,
            collector=collector,
            max_consecutive_failures=max_consecutive_failures,
            partial_jsonl_path=str(resume_paths["partial_jsonl"]),
            processed_jsonl_path=str(resume_paths["processed_jsonl"]),
            resume_state_path=str(resume_paths["state_json"]),
            output_path=str(listings_output_path),
            parquet_output_path=str(listings_parquet_output_path),
            initial_consecutive_failures=initial_consecutive_failures,
            total_input_records=len(records),
            label=label,
            verbose=verbose,
        )
        saved_keys = load_saved_record_keys(resume_paths["partial_jsonl"])
        collector_records_to_persist = [
            record
            for record in collector.get("records", [])
            if (build_listing_resume_key(record) and build_listing_resume_key(record) not in saved_keys)
        ]
        if collector_records_to_persist:
            append_jsonl_records(resume_paths["partial_jsonl"], collector_records_to_persist)
        metrics = sync_resume_metrics_from_ledgers(
            metrics,
            partial_jsonl_path=resume_paths["partial_jsonl"],
            processed_jsonl_path=resume_paths["processed_jsonl"],
        )
        completed_after = len(
            load_completed_record_keys(
                resume_paths["partial_jsonl"],
                resume_paths["processed_jsonl"],
            )
        )
        if metrics.get("stop_reason") == "max_consecutive_failures":
            break
        if completed_after <= completed_before:
            metrics["stop_reason"] = "pending_transient_failures"
            break

    partial_records = load_jsonl_records(resume_paths["partial_jsonl"])
    deduped_records = dedupe_listing_records(partial_records)
    metrics["output_rows"] = len(deduped_records)
    metrics["pending_records"] = len(
        pending_listing_records(
            records,
            partial_jsonl_path=resume_paths["partial_jsonl"],
            processed_jsonl_path=resume_paths["processed_jsonl"],
        )
    )
    return deduped_records, metrics


class BaseListingsSpider(scrapy.Spider):
    request_headers: dict[str, str] = {}
    terminal_not_found_statuses = {404}

    def __init__(
        self,
        *,
        records: list[dict[str, Any]],
        collector: dict[str, Any],
        max_consecutive_failures: int,
        label: str,
        partial_jsonl_path: str | None = None,
        processed_jsonl_path: str | None = None,
        resume_state_path: str | None = None,
        output_path: str | None = None,
        parquet_output_path: str | None = None,
        flush_batch_size: int = DEFAULT_FLUSH_BATCH_SIZE,
        incomplete_snapshot_batch_size: int = DEFAULT_INCOMPLETE_SNAPSHOT_BATCH_SIZE,
        initial_consecutive_failures: int = 0,
        total_input_records: int | None = None,
        verbose: bool = False,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.records = records
        self.collector = collector
        self.metrics = collector["metrics"]
        self.max_consecutive_failures = max_consecutive_failures
        self.label = label
        resolved_resume_dir = default_resume_dir(
            label=label,
            listings_output_path=output_path or f"raw/{datetime.now().strftime('%d-%m-%Y')}/{label}/{label}_listings.csv",
        )
        resolved_resume_paths = build_resume_paths(resolved_resume_dir)
        self.partial_jsonl_path = str(partial_jsonl_path or resolved_resume_paths["partial_jsonl"])
        self.processed_jsonl_path = str(processed_jsonl_path or resolved_resume_paths["processed_jsonl"])
        self.resume_state_path = str(resume_state_path or resolved_resume_paths["state_json"])
        self.output_path = str(output_path or (resolved_resume_dir / f"{label}_listings.csv"))
        self.parquet_output_path = str(parquet_output_path or Path(self.output_path).with_suffix(".parquet"))
        saved_state = load_resume_state(self.resume_state_path)
        self.incomplete_output_path = str(build_incomplete_output_path(self.output_path))
        self.incomplete_parquet_output_path = str(build_incomplete_output_path(self.parquet_output_path))
        self.incomplete_snapshot_batch_size = max(1, int(incomplete_snapshot_batch_size))
        self.incomplete_snapshot_rows = int(saved_state.get("incomplete_output_rows") or 0)
        self.flush_batch_size = max(1, int(flush_batch_size))
        self.verbose = verbose
        self.total_records = len(records)
        self.global_total_records = int(total_input_records) if total_input_records is not None else self.total_records
        self.pending_records: list[dict[str, Any]] = []
        self.consecutive_failures = max(0, int(initial_consecutive_failures))
        self.completed_attempts = int(self.metrics.get("pages_processed", 0) or 0)
        self.batch_start_completed_attempts = self.completed_attempts
        self.collector.setdefault("records", [])
        self.state = {
            "consecutive_failures": self.consecutive_failures,
            "pages_processed": self.completed_attempts,
            "metrics": dict(self.metrics),
            "incomplete_output_path": self.incomplete_output_path,
            "incomplete_parquet_output_path": self.incomplete_parquet_output_path,
            "incomplete_output_rows": self.incomplete_snapshot_rows,
        }

    def start_requests(self):
        log_listing_collection_phase_start(self.label, total=self.total_records)
        for scheduled_index, record in enumerate(self.records, start=1):
            request = self.build_request(record, scheduled_index=scheduled_index)
            if request is None:
                self._mark_terminal_processed(record, status="skipped_no_url", scheduled_index=scheduled_index)
                self.completed_attempts += 1
                self.metrics["pages_processed"] = self.completed_attempts
                self._persist_runtime_state(status="in_progress")
                self._log_progress(self.completed_attempts)
                continue

            request.meta["_resume_record"] = dict(record)

            log_listing_collection_item(
                self.label,
                processed=self.completed_attempts + scheduled_index,
                total=self.total_records,
                url=request.url,
                verbose=self.verbose,
            )
            self.metrics["listing_page_requests"] += 1
            self.metrics["listing_page_in_flight_peak"] = max(
                int(self.metrics.get("listing_page_in_flight_peak", 0) or 0),
                min(self.metrics["listing_page_requests"], self._parallel_limit()),
            )
            yield request

    async def start(self):
        for request in self.start_requests():
            yield request

    def _parallel_limit(self) -> int:
        crawler = getattr(self, "crawler", None)
        if crawler is None:
            return 1
        concurrent_requests = int(crawler.settings.getint("CONCURRENT_REQUESTS", 1) or 1)
        concurrent_per_domain = int(crawler.settings.getint("CONCURRENT_REQUESTS_PER_DOMAIN", 1) or 1)
        return max(1, min(concurrent_requests, concurrent_per_domain))

    def build_request(self, record: dict[str, Any], *, scheduled_index: int) -> Request | None:
        raise NotImplementedError

    def parse_record(self, response: Response) -> dict[str, Any]:
        raise NotImplementedError

    def _record_http_result(self, response_or_meta: Any, *, success: bool) -> None:
        meta = response_or_meta.meta if hasattr(response_or_meta, "meta") else response_or_meta
        record_request(
            self.metrics,
            success=success,
            elapsed_seconds=float(meta.get("download_latency") or 0.0),
            retries=int(meta.get("retry_times") or 0),
        )

    def _flush_pending_records(self) -> None:
        if not self.pending_records:
            return
        append_jsonl_records(self.partial_jsonl_path, self.pending_records)
        self.pending_records.clear()

    def _write_incomplete_snapshot(self) -> None:
        self._flush_pending_records()
        records = dedupe_listing_records(load_jsonl_records(self.partial_jsonl_path) or self.collector["records"])
        if not records:
            return
        save_listing_records_snapshot(
            records,
            csv_path=self.incomplete_output_path,
            parquet_path=self.incomplete_parquet_output_path,
        )
        self.incomplete_snapshot_rows = len(records)

    def _persist_runtime_state(self, *, status: str) -> None:
        payload = {
            "label": self.label,
            "status": status,
            "updated_at": utc_now_iso(),
            "input_rows": self.total_records,
            "output_path": self.output_path,
            "parquet_output_path": self.parquet_output_path,
            "partial_jsonl_path": self.partial_jsonl_path,
            "processed_jsonl_path": self.processed_jsonl_path,
            "incomplete_output_path": self.incomplete_output_path,
            "incomplete_parquet_output_path": self.incomplete_parquet_output_path,
            "incomplete_output_rows": self.incomplete_snapshot_rows,
            "metrics": dict(self.metrics),
            "consecutive_failures": self.consecutive_failures,
            "pages_processed": self.completed_attempts,
        }
        self.state.update(payload)
        save_resume_state(self.resume_state_path, payload)

    def _finalize_attempt(self, *, success: bool = False, count_failure: bool = False) -> None:
        self.completed_attempts += 1
        self.metrics["pages_processed"] = self.completed_attempts
        if success:
            self.consecutive_failures = 0
        elif count_failure:
            self.consecutive_failures += 1

        self._persist_runtime_state(status="in_progress")

        if count_failure and self.consecutive_failures >= self.max_consecutive_failures:
            self.metrics["stop_reason"] = "max_consecutive_failures"
            self._persist_runtime_state(status="failed_terminal")
            log_warn(
                "listing_collection_phase_aborted",
                label=self.label,
                reason="max_consecutive_failures",
            )
            self._log_progress(self.completed_attempts)
            raise CloseSpider("max_consecutive_failures")

        self._log_progress(self.completed_attempts)

    def _log_progress(self, processed_count: int) -> None:
        batch_processed = max(0, processed_count - self.batch_start_completed_attempts)
        log_listing_collection_progress(
            self.label,
            processed=processed_count,
            total=self.global_total_records,
            batch_processed=batch_processed,
            batch_total=self.total_records,
            success=self.metrics["listing_page_successes"],
            failures=self.metrics["listing_page_failures"],
            verbose=self.verbose,
        )

    def _accept_listing_record(self, record: dict[str, Any]) -> None:
        self.pending_records.append(record)
        self.collector["records"].append(record)
        if len(self.pending_records) >= self.flush_batch_size:
            self._flush_pending_records()
        items_kept = int(self.metrics.get("items_kept", 0) or 0)
        if items_kept and items_kept % self.incomplete_snapshot_batch_size == 0:
            self._write_incomplete_snapshot()

    def _mark_terminal_processed(
        self,
        record: Mapping[str, Any],
        *,
        status: str,
        scheduled_index: int | None = None,
        url: str | None = None,
    ) -> None:
        entry = build_processed_ledger_entry(
            record,
            status=status,
            scheduled_index=scheduled_index,
            label=self.label,
            url=url,
        )
        if entry is not None:
            append_jsonl_records(self.processed_jsonl_path, [entry])

    def parse_listing_response(self, response: Response):
        scheduled_index = int(response.meta["scheduled_index"])
        status = int(response.status)
        self._record_http_result(response, success=status == 200)

        if status in self.terminal_not_found_statuses:
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
                status=status,
            )
            self._finalize_attempt()
            return None

        if status == 500:
            self.metrics["listing_page_failures"] += 1
            log_warn(
                "listing_collection_item_failed",
                label=self.label,
                processed=f"{scheduled_index}/{self.total_records}",
                status=status,
            )
            self._finalize_attempt()
            return None

        if status != 200:
            self.metrics["listing_page_failures"] += 1
            log_warn(
                "listing_collection_item_failed",
                label=self.label,
                processed=f"{scheduled_index}/{self.total_records}",
                status=status,
                consecutive_failures=self.consecutive_failures + 1,
            )
            self._finalize_attempt(count_failure=True)
            return None

        try:
            listing_record = self.parse_record(response)
        except Exception as exc:
            self.metrics["listing_page_failures"] += 1
            log_warn(
                "listing_collection_item_parse_failed",
                label=self.label,
                processed=f"{scheduled_index}/{self.total_records}",
                error=exc,
                consecutive_failures=self.consecutive_failures + 1,
            )
            self._finalize_attempt(count_failure=True)
            return None

        if self.has_required_listing_keys(listing_record):
            self.metrics["listing_page_successes"] += 1
            self.metrics["items_kept"] += 1
            self._accept_listing_record(listing_record)
            self._finalize_attempt(success=True)
            return None

        self.metrics["listing_page_failures"] += 1
        log_warn(
            "listing_collection_item_empty",
            label=self.label,
            processed=f"{scheduled_index}/{self.total_records}",
            consecutive_failures=self.consecutive_failures + 1,
        )
        self._finalize_attempt(count_failure=True)
        return None

    def handle_request_error(self, failure: Any):
        request = failure.request
        scheduled_index = int(request.meta["scheduled_index"])
        self._record_http_result(request.meta, success=False)
        self.metrics["listing_page_failures"] += 1
        status = getattr(getattr(failure.value, "response", None), "status", None)
        count_failure = status != 500
        log_warn(
            "listing_collection_item_failed",
            label=self.label,
            processed=f"{scheduled_index}/{self.total_records}",
            status=status,
            error=failure.value,
            consecutive_failures=(self.consecutive_failures + 1) if count_failure else None,
        )
        self._finalize_attempt(count_failure=count_failure)
        return None

    def has_required_listing_keys(self, record: dict[str, Any]) -> bool:
        business_type = str(record.get("business_type") or "").strip()
        property_id = str(record.get("property_id") or "").strip()
        listing_url = str(record.get("listing_url") or "").strip()
        return bool(business_type and (property_id or listing_url))

    def closed(self, reason: str) -> None:
        self._flush_pending_records()
        if int(self.metrics.get("items_kept", 0) or 0) > self.incomplete_snapshot_rows:
            self._write_incomplete_snapshot()
        status = "completed"
        if self.metrics.get("stop_reason") == "max_consecutive_failures" or reason == "max_consecutive_failures":
            status = "failed_terminal"
        elif reason != "finished":
            status = "in_progress"
        if self.metrics["stop_reason"] is None:
            self.metrics["stop_reason"] = "completed" if reason == "finished" else (reason or "completed")
        self._persist_runtime_state(status=status)
        log_listing_collection_phase_end(
            self.label,
            processed=self.metrics["pages_processed"],
            success=self.metrics["listing_page_successes"],
            failures=self.metrics["listing_page_failures"],
        )
