from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable

from scrapers import (
    lopes_aluguel,
    lopes_venda,
    olx_aluguel,
    olx_venda,
    quinto_aluguel,
    quinto_venda,
)
from scrapers.output_paths import build_dated_output_path


@dataclass(frozen=True)
class ScraperDefinition:
    name: str
    source: str
    filename: str
    runner: Callable[..., str | None]
    domain_group: str | None = None
    params: dict[str, Any] = field(default_factory=dict)

    def output_path(self, run_date: str) -> str:
        return build_dated_output_path(self.source, self.filename, run_date=run_date)


def get_scraper_definitions() -> list[ScraperDefinition]:
    return [
        ScraperDefinition(
            name="olx_venda",
            source="olx",
            filename="olx_venda.csv",
            runner=olx_venda.run,
            domain_group="olx",
            params={
                "max_pages": 10,
                "min_delay_seconds": 2.0,
                "max_delay_seconds": 8.0,
                "target_delay_seconds": 3.0,
                "max_consecutive_failures": 2,
                "early_stop_on_low_yield": 10,
                "detail_min_delay_seconds": 2.0,
                "detail_max_delay_seconds": 6.0,
                "detail_target_delay_seconds": 2.5,
                "detail_max_consecutive_failures": 3,
            },
        ),
        ScraperDefinition(
            name="olx_aluguel",
            source="olx",
            filename="olx_aluguel.csv",
            runner=olx_aluguel.run,
            domain_group="olx",
            params={
                "max_pages": 10,
                "min_delay_seconds": 2.0,
                "max_delay_seconds": 8.0,
                "target_delay_seconds": 3.0,
                "max_consecutive_failures": 2,
                "early_stop_on_low_yield": 10,
                "detail_min_delay_seconds": 2.0,
                "detail_max_delay_seconds": 6.0,
                "detail_target_delay_seconds": 2.5,
                "detail_max_consecutive_failures": 3,
            },
        ),
        ScraperDefinition(
            name="lopes_venda",
            source="lopes",
            filename="lopes_venda.csv",
            runner=lopes_venda.run,
            domain_group="lopes",
            params={
                "max_pages": 25,
                "lines_per_page": 23,
                "min_delay_seconds": 1.5,
                "max_delay_seconds": 6.0,
                "target_delay_seconds": 2.5,
                "retry_min_delay_seconds": 2.0,
                "retry_max_delay_seconds": 6.0,
                "max_consecutive_failures": 2,
                "early_stop_on_low_yield": 5,
                "detail_min_delay_seconds": 1.5,
                "detail_max_delay_seconds": 5.0,
                "detail_target_delay_seconds": 2.0,
                "detail_max_consecutive_failures": 3,
            },
        ),
        ScraperDefinition(
            name="lopes_aluguel",
            source="lopes",
            filename="lopes_aluguel.csv",
            runner=lopes_aluguel.run,
            domain_group="lopes",
            params={
                "max_pages": 25,
                "lines_per_page": 23,
                "min_delay_seconds": 1.5,
                "max_delay_seconds": 6.0,
                "target_delay_seconds": 2.5,
                "retry_min_delay_seconds": 2.0,
                "retry_max_delay_seconds": 6.0,
                "max_consecutive_failures": 2,
                "early_stop_on_low_yield": 5,
                "detail_min_delay_seconds": 1.5,
                "detail_max_delay_seconds": 5.0,
                "detail_target_delay_seconds": 2.0,
                "detail_max_consecutive_failures": 3,
            },
        ),
        ScraperDefinition(
            name="quinto_venda",
            source="quinto",
            filename="quinto_venda.csv",
            runner=quinto_venda.run,
            domain_group="quinto",
            params={
                "max_batches": 40,
                "min_delay_seconds": 1.5,
                "max_delay_seconds": 5.0,
                "target_delay_seconds": 2.0,
                "max_consecutive_failures": 2,
                "early_stop_on_low_yield": 2,
                "detail_min_delay_seconds": 1.5,
                "detail_max_delay_seconds": 5.0,
                "detail_target_delay_seconds": 2.0,
                "detail_max_consecutive_failures": 3,
            },
        ),
        ScraperDefinition(
            name="quinto_aluguel",
            source="quinto",
            filename="quinto_aluguel.csv",
            runner=quinto_aluguel.run,
            domain_group="quinto",
            params={
                "max_batches": 40,
                "min_delay_seconds": 1.5,
                "max_delay_seconds": 5.0,
                "target_delay_seconds": 2.0,
                "max_consecutive_failures": 2,
                "early_stop_on_low_yield": 2,
                "detail_min_delay_seconds": 1.5,
                "detail_max_delay_seconds": 5.0,
                "detail_target_delay_seconds": 2.0,
                "detail_max_consecutive_failures": 3,
            },
        ),
    ]
