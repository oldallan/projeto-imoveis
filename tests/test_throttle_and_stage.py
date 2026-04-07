from __future__ import annotations

import csv
import shutil
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd

from pipelines.daily_snapshot import enrich_listings_with_canonical_id, project_listings_output_columns
from pipelines.historical_store import update_historical_store
from scrapers.registry import ScraperDefinition
from scrapers.throttle import AdaptiveThrottle, init_metrics, record_request
from stages.collect_general_listings import CollectGeneralListingsStage


class DummyLogger:
    def info(self, *args, **kwargs):
        return None

    def exception(self, *args, **kwargs):
        return None


class AdaptiveThrottleTests(unittest.TestCase):
    def test_failure_increases_and_success_recovers(self):
        throttle = AdaptiveThrottle(min_delay_seconds=1.0, max_delay_seconds=10.0, target_delay_seconds=2.0)

        failed_delay = throttle.failure(status_code=429)
        recovered_delay = throttle.success()

        self.assertGreater(failed_delay, 2.0)
        self.assertLess(recovered_delay, failed_delay)
        self.assertGreaterEqual(recovered_delay, 1.0)

    def test_record_request_updates_metrics(self):
        metrics = init_metrics("demo")
        record_request(metrics, success=True, elapsed_seconds=0.25, retries=1)
        record_request(metrics, success=False, elapsed_seconds=0.75, retries=0)

        self.assertEqual(metrics["requests"], 2)
        self.assertEqual(metrics["successes"], 1)
        self.assertEqual(metrics["failures"], 1)
        self.assertEqual(metrics["retries"], 1)
        self.assertAlmostEqual(metrics["http_seconds"], 1.0)


class CollectGeneralListingsStageTests(unittest.TestCase):
    def test_stage_runs_in_parallel_by_round(self):
        stage = CollectGeneralListingsStage()
        logger = DummyLogger()
        root = Path("tests_runtime")
        if root.exists():
            shutil.rmtree(root)
        root.mkdir(parents=True, exist_ok=True)

        try:
            context = SimpleNamespace(run_date="06-04-2026")

            def make_runner(name: str):
                def runner(output_path: str, **kwargs):
                    time.sleep(0.2)
                    output = Path(output_path)
                    output.parent.mkdir(parents=True, exist_ok=True)
                    with open(output, "w", newline="", encoding="utf-8") as file:
                        writer = csv.DictWriter(file, fieldnames=["property_id"])
                        writer.writeheader()
                        writer.writerow({"property_id": name})
                    return str(output)

                return runner

            scrapers = [
                ScraperDefinition("olx_venda", "olx", "olx_venda.csv", make_runner("olx_venda"), "olx"),
                ScraperDefinition("olx_aluguel", "olx", "olx_aluguel.csv", make_runner("olx_aluguel"), "olx"),
                ScraperDefinition("lopes_venda", "lopes", "lopes_venda.csv", make_runner("lopes_venda"), "lopes"),
                ScraperDefinition("lopes_aluguel", "lopes", "lopes_aluguel.csv", make_runner("lopes_aluguel"), "lopes"),
                ScraperDefinition("quinto_venda", "quinto", "quinto_venda.csv", make_runner("quinto_venda"), "quinto"),
                ScraperDefinition("quinto_aluguel", "quinto", "quinto_aluguel.csv", make_runner("quinto_aluguel"), "quinto"),
            ]

            def output_path(self, run_date: str) -> str:
                return str(root / run_date / self.source / self.filename)

            started_at = time.perf_counter()
            with patch("stages.collect_general_listings.get_scraper_definitions", return_value=scrapers), patch.object(
                ScraperDefinition,
                "output_path",
                output_path,
            ):
                artifacts, metrics, errors = stage.run(context, None, logger)
            elapsed = time.perf_counter() - started_at
        finally:
            shutil.rmtree(root, ignore_errors=True)

        self.assertEqual(len(artifacts), 6)
        self.assertEqual(metrics["successful_scrapers"], 6)
        self.assertEqual(metrics["failed_scrapers"], 0)
        self.assertEqual(errors, [])
        self.assertLess(elapsed, 0.9)


class HistoricalStoreRegressionTests(unittest.TestCase):
    def test_project_listings_output_columns_removes_description_fields(self):
        listings_df = pd.DataFrame(
            [
                {
                    "source": "olx",
                    "business_type": "sale",
                    "property_id": "1",
                    "listing_url": "https://example.com/1",
                    "description": "desc curta",
                    "long_description": "desc longa",
                    "scraped_at": "2026-04-06T20:30:21.788509+00:00",
                }
            ]
        )

        projected = project_listings_output_columns(listings_df)

        self.assertNotIn("description", projected.columns)
        self.assertNotIn("long_description", projected.columns)

    def test_enrich_listings_with_canonical_id_uses_full_join_key_when_available(self):
        listings_df = pd.DataFrame(
            [
                {
                    "source": "lopes",
                    "business_type": "rent",
                    "property_id": "REO105140",
                    "listing_url": "https://www.lopes.com.br/imovel/REO105140",
                    "scraped_at": "2026-04-06T20:30:21.788509+00:00",
                }
            ]
        )
        link_df = pd.DataFrame(
            [
                {
                    "canonical_property_id": "rent-canonical",
                    "source": "lopes",
                    "business_type": "rent",
                    "property_id": "REO105140",
                    "listing_url": "https://www.lopes.com.br/imovel/REO105140",
                    "scraped_at": "2026-04-06T20:30:21.788509+00:00",
                },
                {
                    "canonical_property_id": "sale-canonical",
                    "source": "lopes",
                    "business_type": "sale",
                    "property_id": "REO105140",
                    "listing_url": "https://www.lopes.com.br/imovel/REO105140",
                    "scraped_at": "2026-04-06T15:18:17.465397+00:00",
                },
            ]
        )

        enriched = enrich_listings_with_canonical_id(listings_df, link_df)

        self.assertEqual(len(enriched), 1)
        self.assertEqual(enriched.loc[0, "canonical_property_id"], "rent-canonical")

    def test_update_historical_store_preserves_uniqueness_with_sale_rent_shared_url(self):
        output_dir = Path("tests_runtime_historical_store")
        output_dir.mkdir(parents=True, exist_ok=True)
        try:
            existing = pd.DataFrame(
                [
                    {
                        "source": "lopes",
                        "business_type": "sale",
                        "property_id": "REO105140",
                        "listing_url": "https://www.lopes.com.br/imovel/REO105140",
                        "city": "Sao Paulo",
                        "neighbourhood": "Moema, Sao Paulo",
                        "address": "Avenida Acoce",
                        "state": "SP",
                        "zip_code": "04075-000",
                        "lat": pd.NA,
                        "lon": pd.NA,
                        "property_type": "Apartamento",
                        "area_m2": 405,
                        "total_area_m2": 405,
                        "bedrooms": 4,
                        "bathrooms": 6,
                        "parking_spots": 4,
                        "suites": 4,
                        "floor": pd.NA,
                        "furnished": False,
                        "accepts_pets": True,
                        "condominium_name": pd.NA,
                        "condominium_id": pd.NA,
                        "amenities_json": pd.NA,
                        "installations_json": pd.NA,
                        "sale_price_brl": 1000000,
                        "rent_price_brl": pd.NA,
                        "scraped_at": "2026-04-06T15:18:17.465397+00:00",
                        "first_seen_at": "2026-04-06T15:18:17.465397+00:00",
                        "last_seen_at": "2026-04-06T15:18:17.465397+00:00",
                        "created_at": "2026-04-06T15:18:17.465397+00:00",
                        "updated_at": "2026-04-06T15:18:17.465397+00:00",
                    }
                ]
            )
            existing.to_parquet(output_dir / "listings_unificados.parquet", index=False)

            snapshot = pd.DataFrame(
                [
                    {
                        "canonical_property_id": "rent-canonical",
                        "source": "lopes",
                        "business_type": "rent",
                        "property_id": "REO105140",
                        "listing_url": "https://www.lopes.com.br/imovel/REO105140",
                        "city": "Sao Paulo",
                        "neighbourhood": "Indianopolis",
                        "address": "Avenida Acoce - Indianopolis - Sao Paulo/SP",
                        "state": "SP",
                        "zip_code": "04075-000",
                        "lat": pd.NA,
                        "lon": pd.NA,
                        "property_type": "Apartamento",
                        "area_m2": 405,
                        "total_area_m2": 405,
                        "bedrooms": 4,
                        "bathrooms": 6,
                        "parking_spots": 4,
                        "suites": 4,
                        "floor": pd.NA,
                        "furnished": False,
                        "accepts_pets": True,
                        "condominium_name": pd.NA,
                        "condominium_id": pd.NA,
                        "amenities_json": pd.NA,
                        "installations_json": pd.NA,
                        "sale_price_brl": pd.NA,
                        "rent_price_brl": 8000,
                        "scraped_at": "2026-04-06T20:30:21.788509+00:00",
                    }
                ]
            )

            output = update_historical_store(snapshot, output_dir)
            listings = output["listings"]
            upsert_keys = listings[["source", "business_type", "property_id"]].fillna("").astype(str).agg("|".join, axis=1)
        finally:
            shutil.rmtree(output_dir, ignore_errors=True)

        self.assertTrue(upsert_keys.is_unique)
        self.assertEqual(len(listings.loc[listings["property_id"] == "REO105140"]), 2)
        self.assertNotIn("description", listings.columns)
        self.assertNotIn("long_description", listings.columns)


if __name__ == "__main__":
    unittest.main()
