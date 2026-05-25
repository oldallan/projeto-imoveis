from pathlib import Path

from scrapers.lopes_discovery import collect_discovery_to_file
from scrapers.lopes_listings import collect_listings_from_file
from scrapers.output_paths import build_dated_output_path


DEFAULT_DISCOVERY_OUTPUT_PATH = build_dated_output_path("lopes", "lopes_discovery.csv")
DEFAULT_DISCOVERY_PARQUET_OUTPUT_PATH = str(Path(DEFAULT_DISCOVERY_OUTPUT_PATH).with_suffix(".parquet"))
DEFAULT_LISTINGS_OUTPUT_PATH = build_dated_output_path("lopes", "lopes_listings.csv")
DEFAULT_LISTINGS_PARQUET_OUTPUT_PATH = str(Path(DEFAULT_LISTINGS_OUTPUT_PATH).with_suffix(".parquet"))


def collect_discovery(
    output_path: str = DEFAULT_DISCOVERY_OUTPUT_PATH,
    parquet_output_path: str | None = None,
    verbose: bool = False,
) -> dict[str, object] | None:
    return collect_discovery_to_file(
        output_path=output_path,
        parquet_output_path=parquet_output_path or str(Path(output_path).with_suffix(".parquet")),
        verbose=verbose,
    )


def collect_listings(
    input_path: str = DEFAULT_DISCOVERY_OUTPUT_PATH,
    listings_output_path: str = DEFAULT_LISTINGS_OUTPUT_PATH,
    listings_parquet_output_path: str = DEFAULT_LISTINGS_PARQUET_OUTPUT_PATH,
    max_consecutive_failures: int = 5,
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
) -> dict[str, object] | None:
    return collect_listings_from_file(
        input_path=input_path,
        listings_output_path=listings_output_path,
        listings_parquet_output_path=listings_parquet_output_path,
        max_consecutive_failures=max_consecutive_failures,
        label="lopes",
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
