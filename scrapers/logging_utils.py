from __future__ import annotations

from typing import Any


LISTING_COLLECTION_PROGRESS_INTERVAL = 25


def _format_fields(**fields: Any) -> str:
    parts: list[str] = []
    for key, value in fields.items():
        if value is None:
            continue
        if isinstance(value, float):
            parts.append(f"{key}={value:.2f}")
        else:
            parts.append(f"{key}={value}")
    return " ".join(parts)


def log_info(event: str, **fields: Any) -> None:
    suffix = _format_fields(**fields)
    print(f"[INFO] {event}" + (f" {suffix}" if suffix else ""))


def log_warn(event: str, **fields: Any) -> None:
    suffix = _format_fields(**fields)
    print(f"[WARN] {event}" + (f" {suffix}" if suffix else ""))


def should_log_progress(
    processed: int,
    total: int,
    *,
    verbose: bool,
    interval: int = LISTING_COLLECTION_PROGRESS_INTERVAL,
) -> bool:
    if total <= 0:
        return False
    if verbose:
        return True
    return processed == 1 or processed == total or processed % interval == 0


def log_listings_phase_start(label: str, *, max_pages: int | None = None, mode: str = "collect") -> None:
    log_info("listings_phase_start", label=label, mode=mode, max_pages=max_pages)


def log_listings_page(label: str, *, page: int, url: str | None = None, verbose: bool = False) -> None:
    if verbose:
        log_info("listings_page", label=label, page=page, url=url)


def log_listings_page_result(
    label: str,
    *,
    page: int,
    new: int,
    total: int,
    seen: int | None = None,
    kept: int | None = None,
) -> None:
    log_info(
        "listings_page_result",
        label=label,
        page=page,
        new=new,
        total=total,
        seen=seen,
        kept=kept,
    )


def log_listings_phase_end(label: str, *, pages_processed: int, items_kept: int, stop_reason: str | None) -> None:
    log_info(
        "listings_phase_end",
        label=label,
        pages_processed=pages_processed,
        items_kept=items_kept,
        stop_reason=stop_reason,
    )


def log_listing_collection_phase_start(
    label: str,
    *,
    total: int,
    mode: str = "collect_from_listing_urls",
) -> None:
    log_info("listing_collection_phase_start", label=label, mode=mode, total=total)


def log_listing_collection_item(
    label: str,
    *,
    processed: int,
    total: int,
    url: str | None,
    verbose: bool = False,
) -> None:
    if verbose:
        log_info("listing_collection_item", label=label, processed=processed, total=total, url=url)


def log_listing_collection_progress(
    label: str,
    *,
    processed: int,
    total: int,
    batch_processed: int | None = None,
    batch_total: int | None = None,
    success: int,
    failures: int,
    verbose: bool = False,
) -> None:
    if not should_log_progress(processed, total, verbose=verbose):
        return
    batch_status = None
    if batch_processed is not None and batch_total is not None:
        batch_status = f"{batch_processed}/{batch_total}"
    log_info(
        "listing_collection_progress",
        label=label,
        processed=f"{processed}/{total}",
        batch_status=batch_status,
        success=success,
        failures=failures,
    )


def log_listing_collection_phase_end(label: str, *, processed: int, success: int, failures: int) -> None:
    log_info(
        "listing_collection_phase_end",
        label=label,
        processed=processed,
        success=success,
        failures=failures,
    )
