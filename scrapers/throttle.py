from __future__ import annotations

import random
import time
from dataclasses import dataclass
from typing import Any


@dataclass
class AdaptiveThrottle:
    min_delay_seconds: float
    max_delay_seconds: float
    target_delay_seconds: float | None = None
    backoff_factor: float = 1.6
    recovery_factor: float = 0.85

    def __post_init__(self) -> None:
        base_delay = self.target_delay_seconds or self.min_delay_seconds
        self.current_delay_seconds = self._clamp(base_delay)

    def success(self, elapsed_seconds: float | None = None) -> float:
        if elapsed_seconds is not None:
            baseline = max(self.min_delay_seconds, elapsed_seconds * 1.1)
        else:
            baseline = self.current_delay_seconds * self.recovery_factor
        self.current_delay_seconds = self._clamp(baseline)
        return self.current_delay_seconds

    def failure(self, *, status_code: int | None = None) -> float:
        factor = self.backoff_factor
        if status_code in {403, 429}:
            factor *= 1.35
        self.current_delay_seconds = self._clamp(self.current_delay_seconds * factor)
        return self.current_delay_seconds

    def sleep(self, *, jitter_ratio: float = 0.15) -> float:
        jitter = self.current_delay_seconds * jitter_ratio
        sleep_seconds = self._clamp(
            random.uniform(
                max(self.min_delay_seconds, self.current_delay_seconds - jitter),
                min(self.max_delay_seconds, self.current_delay_seconds + jitter),
            )
        )
        time.sleep(sleep_seconds)
        return sleep_seconds

    def snapshot(self) -> dict[str, float]:
        return {
            "min_delay_seconds": self.min_delay_seconds,
            "max_delay_seconds": self.max_delay_seconds,
            "current_delay_seconds": self.current_delay_seconds,
        }

    def _clamp(self, value: float) -> float:
        return max(self.min_delay_seconds, min(self.max_delay_seconds, value))


def init_metrics(label: str) -> dict[str, Any]:
    return {
        "label": label,
        "requests": 0,
        "successes": 0,
        "failures": 0,
        "retries": 0,
        "throttle_sleep_seconds": 0.0,
        "http_seconds": 0.0,
        "items_seen": 0,
        "items_kept": 0,
        "pages_processed": 0,
        "detail_requests": 0,
        "detail_html_requests": 0,
        "detail_api_requests": 0,
        "detail_successes": 0,
        "detail_html_successes": 0,
        "detail_api_successes": 0,
        "detail_failures": 0,
        "detail_backoffs": 0,
        "detail_fields_filled": 0,
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
