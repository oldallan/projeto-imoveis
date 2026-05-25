from __future__ import annotations

from typing import Any


def init_metrics(label: str) -> dict[str, Any]:
    return {
        "label": label,
        "requests": 0,
        "successes": 0,
        "failures": 0,
        "retries": 0,
        "http_seconds": 0.0,
        "items_seen": 0,
        "items_kept": 0,
        "pages_processed": 0,
        "listing_page_requests": 0,
        "listing_page_successes": 0,
        "listing_page_failures": 0,
        "listing_page_not_founds": 0,
        "listing_page_in_flight_peak": 0,
        "stop_reason": None,
    }


def record_request(metrics: dict[str, Any], *, success: bool, elapsed_seconds: float, retries: int = 0) -> None:
    metrics["requests"] += 1
    metrics["http_seconds"] += elapsed_seconds
    metrics["retries"] += retries
    if success:
        metrics["successes"] += 1
    else:
        metrics["failures"] += 1
