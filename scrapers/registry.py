from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable

from scrapers import lopes, olx, quinto
from scrapers.output_paths import build_dated_output_path


@dataclass(frozen=True)
class ScraperDefinition:
    name: str
    source: str
    discovery_filename: str
    listings_filename: str
    run_discovery: Callable[..., Any]
    run_collection: Callable[..., dict[str, Any] | None]
    discovery_metadata: dict[str, Any] = field(default_factory=dict)
    discovery_options: dict[str, Any] = field(default_factory=dict)
    collection_options: dict[str, Any] = field(default_factory=dict)

    def discovery_output_path(self, run_date: str) -> str:
        return build_dated_output_path(self.source, self.discovery_filename, run_date=run_date)

    def discovery_parquet_output_path(self, run_date: str) -> str:
        return str(build_dated_output_path(self.source, self.discovery_filename, run_date=run_date)).replace(".csv", ".parquet")

    def listings_output_path(self, run_date: str) -> str:
        return build_dated_output_path(self.source, self.listings_filename, run_date=run_date)

    def listings_parquet_output_path(self, run_date: str) -> str:
        return str(build_dated_output_path(self.source, self.listings_filename, run_date=run_date)).replace(".csv", ".parquet")


def get_scraper_definitions() -> list[ScraperDefinition]:
    return [
        ScraperDefinition(
            name="olx",
            source="olx",
            discovery_filename="olx_discovery.csv",
            listings_filename="olx_listings.csv",
            run_discovery=olx.collect_discovery,
            run_collection=olx.collect_listings,
            discovery_metadata={
                "discovery_mode": "delta",
                "delta_rule": "new_or_price_changed",
            },
            discovery_options={
                "max_pages": 100,
            },
            collection_options={
                "retry_times": 2,
                "autothrottle_start_delay": 1.0,
                "autothrottle_max_delay": 8.0,
                "autothrottle_target_concurrency": 1.0,
                "concurrent_requests": 3,
                "concurrent_requests_per_domain": 3,
                "download_delay": 1.0,
                "download_timeout": 30,
                "max_consecutive_failures": 100,
            },
        ),
        ScraperDefinition(
            name="lopes",
            source="lopes",
            discovery_filename="lopes_discovery.csv",
            listings_filename="lopes_listings.csv",
            run_discovery=lopes.collect_discovery,
            run_collection=lopes.collect_listings,
            discovery_metadata={
                "discovery_mode": "delta",
                "delta_rule": "new_or_lastmod_changed",
            },
            collection_options={
                "retry_times": 2,
                "autothrottle_start_delay": 1.0,
                "autothrottle_max_delay": 8.0,
                "autothrottle_target_concurrency": 1.0,
                "concurrent_requests": 3,
                "concurrent_requests_per_domain": 3,
                "download_delay": 1.0,
                "download_timeout": 30,
                "max_consecutive_failures": 100,
            },
        ),
        ScraperDefinition(
            name="quinto",
            source="quinto",
            discovery_filename="quinto_discovery.csv",
            listings_filename="quinto_listings.csv",
            run_discovery=quinto.collect_discovery,
            run_collection=quinto.collect_listings,
            discovery_metadata={
                "discovery_mode": "delta",
                "delta_rule": "new_or_lastmod_changed",
            },
            collection_options={
                "retry_times": 2,
                "autothrottle_start_delay": 1.0,
                "autothrottle_max_delay": 8.0,
                "autothrottle_target_concurrency": 1.0,
                "concurrent_requests": 3,
                "concurrent_requests_per_domain": 3,
                "download_delay": 1.0,
                "download_timeout": 30,
                "max_consecutive_failures": 100,
            },
        ),
    ]
