from scrapers.olx_common import save_csv, scrape_all
from scrapers.output_paths import build_dated_output_path


BASE_URL = "https://www.olx.com.br/imoveis/aluguel/estado-sp/sao-paulo-e-regiao"
DEFAULT_OUTPUT_PATH = build_dated_output_path("olx", "olx_aluguel.csv")


def run(
    output_path: str = DEFAULT_OUTPUT_PATH,
    max_pages: int = 10,
    min_delay_seconds: float = 2.0,
    max_delay_seconds: float = 8.0,
    target_delay_seconds: float | None = None,
    max_consecutive_failures: int = 2,
    early_stop_on_low_yield: int = 0,
    detail_min_delay_seconds: float = 2.0,
    detail_max_delay_seconds: float = 6.0,
    detail_target_delay_seconds: float | None = 2.5,
    detail_max_consecutive_failures: int = 3,
) -> str | None:
    records = scrape_all(
        base_url=BASE_URL,
        max_pages=max_pages,
        min_delay_seconds=min_delay_seconds,
        max_delay_seconds=max_delay_seconds,
        target_delay_seconds=target_delay_seconds,
        max_consecutive_failures=max_consecutive_failures,
        early_stop_on_low_yield=early_stop_on_low_yield,
        detail_min_delay_seconds=detail_min_delay_seconds,
        detail_max_delay_seconds=detail_max_delay_seconds,
        detail_target_delay_seconds=detail_target_delay_seconds,
        detail_max_consecutive_failures=detail_max_consecutive_failures,
        label="olx_aluguel",
    )
    if not records:
        print("[WARN] OLX aluguel sem dados coletados")
        return None
    save_csv(records, filename=output_path)
    return output_path


if __name__ == "__main__":
    output = run()
    if output:
        print(f"[OK] arquivo gerado: {output}")
