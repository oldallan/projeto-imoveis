from scrapers.lopes_common import save_csv, scrape_all
from scrapers.output_paths import build_dated_output_path


BASE_API_URL = "https://apis.lopes.com.br/portal-home/v2/search/cache/rent/br/sp/sao-paulo"
DEFAULT_OUTPUT_PATH = build_dated_output_path("lopes", "lopes_aluguel.csv")


def run(
    output_path: str = DEFAULT_OUTPUT_PATH,
    max_pages: int = 25,
    lines_per_page: int = 23,
    min_delay_seconds: float = 1.5,
    max_delay_seconds: float = 6.0,
    retry_min_delay_seconds: float = 2.0,
    retry_max_delay_seconds: float = 6.0,
    target_delay_seconds: float | None = None,
    max_consecutive_failures: int = 2,
    early_stop_on_low_yield: int = 0,
    detail_min_delay_seconds: float = 1.5,
    detail_max_delay_seconds: float = 5.0,
    detail_target_delay_seconds: float | None = 2.0,
    detail_max_consecutive_failures: int = 3,
) -> str | None:
    records = scrape_all(
        base_api_url=BASE_API_URL,
        business_type="rent",
        max_pages=max_pages,
        lines_per_page=lines_per_page,
        min_delay_seconds=min_delay_seconds,
        max_delay_seconds=max_delay_seconds,
        retry_min_delay_seconds=retry_min_delay_seconds,
        retry_max_delay_seconds=retry_max_delay_seconds,
        target_delay_seconds=target_delay_seconds,
        max_consecutive_failures=max_consecutive_failures,
        early_stop_on_low_yield=early_stop_on_low_yield,
        detail_min_delay_seconds=detail_min_delay_seconds,
        detail_max_delay_seconds=detail_max_delay_seconds,
        detail_target_delay_seconds=detail_target_delay_seconds,
        detail_max_consecutive_failures=detail_max_consecutive_failures,
        label="lopes_aluguel",
    )
    if not records:
        print("[WARN] Lopes aluguel sem dados coletados")
        return None
    save_csv(records, filename=output_path)
    return output_path


if __name__ == "__main__":
    output = run()
    if output:
        print(f"[OK] arquivo gerado: {output}")
