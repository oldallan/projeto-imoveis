from __future__ import annotations

import csv
import json
import shutil
import time
import unittest
from io import StringIO
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd

from cli import build_parser
from pipelines.daily_snapshot import attach_canonical_id, build_daily_snapshot, project_listings_output_columns
from pipelines.historical_store import update_historical_store
from scrapers.http_metrics import init_metrics, record_request
from scrapers.listings_resume import build_resume_paths
from scrapers.logging_utils import log_listing_collection_item, log_listing_collection_progress
from scrapers.registry import (
    ScraperDefinition,
    get_scraper_definitions,
)
from stages.build_daily_snapshot import BuildDailySnapshotStage
from stages.collect_discovery import CollectDiscoveryStage
from stages.collect_listings import CollectListingsStage
from stages.update_historical_store import UpdateHistoricalStoreStage
from workflow.manifest import write_json
from workflow.models import ArtifactRecord
from workflow.paths import build_context, build_scoped_output_dir, stage_manifest_path
from workflow.runner import PipelineRunner


class DummyLogger:
    def info(self, *args, **kwargs):
        return None

    def exception(self, *args, **kwargs):
        return None


class HttpMetricsTests(unittest.TestCase):
    def test_record_request_updates_metrics(self):
        metrics = init_metrics("demo")
        record_request(metrics, success=True, elapsed_seconds=0.25, retries=1)
        record_request(metrics, success=False, elapsed_seconds=0.75, retries=0)

        self.assertEqual(metrics["requests"], 2)
        self.assertEqual(metrics["successes"], 1)
        self.assertEqual(metrics["failures"], 1)
        self.assertEqual(metrics["retries"], 1)
        self.assertAlmostEqual(metrics["http_seconds"], 1.0)


class CollectDiscoveryStageTests(unittest.TestCase):
    def test_stage_runs_all_sources_in_parallel(self):
        stage = CollectDiscoveryStage()
        logger = DummyLogger()
        root = Path("tests_runtime")
        if root.exists():
            shutil.rmtree(root)
        root.mkdir(parents=True, exist_ok=True)

        try:
            context = build_context("06-04-2026", root)

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
                ScraperDefinition(
                    "lopes",
                    "lopes",
                    "lopes_discovery.csv",
                    "lopes_listings.csv",
                    make_runner("lopes"),
                    lambda **_: None,
                ),
                ScraperDefinition(
                    "quinto",
                    "quinto",
                    "quinto_discovery.csv",
                    "quinto_listings.csv",
                    make_runner("quinto"),
                    lambda **_: None,
                ),
                ScraperDefinition(
                    "olx",
                    "olx",
                    "olx_discovery.csv",
                    "olx_listings.csv",
                    make_runner("olx"),
                    lambda **_: None,
                ),
            ]

            started_at = time.perf_counter()
            with patch("stages.collect_discovery.get_scraper_definitions", return_value=scrapers):
                artifacts, metrics, errors = stage.run(context, None, logger)
            elapsed = time.perf_counter() - started_at
        finally:
            shutil.rmtree(root, ignore_errors=True)

        self.assertEqual(len(artifacts), 3)
        self.assertEqual(metrics["successful_scrapers"], 3)
        self.assertEqual(metrics["failed_scrapers"], 0)
        self.assertEqual(errors, [])
        self.assertLess(elapsed, 0.9)

    def test_stage_passes_verbose_to_scraper_runner(self):
        stage = CollectDiscoveryStage()
        logger = DummyLogger()
        root = Path("tests_runtime_verbose_collect")
        if root.exists():
            shutil.rmtree(root)
        root.mkdir(parents=True, exist_ok=True)

        captured: list[bool] = []

        try:
            context = build_context("06-04-2026", root)

            def runner(output_path: str, verbose: bool = False, **kwargs):
                captured.append(verbose)
                output = Path(output_path)
                output.parent.mkdir(parents=True, exist_ok=True)
                with open(output, "w", newline="", encoding="utf-8") as file:
                    writer = csv.DictWriter(file, fieldnames=["property_id"])
                    writer.writeheader()
                    writer.writerow({"property_id": "lopes"})
                return str(output)

            scraper = ScraperDefinition(
                "lopes",
                "lopes",
                "lopes_discovery.csv",
                "lopes_listings.csv",
                runner,
                lambda **_: None,
                discovery_metadata={
                    "discovery_mode": "delta",
                    "delta_rule": "new_or_lastmod_changed",
                },
            )

            with patch("stages.collect_discovery.get_scraper_definitions", return_value=[scraper]):
                stage.run(context, None, logger, stage_options={"verbose": True})
        finally:
            shutil.rmtree(root, ignore_errors=True)

        self.assertEqual(captured, [True])

    def test_validate_allows_empty_delta_artifact_for_incremental_discovery(self):
        stage = CollectDiscoveryStage()
        logger = DummyLogger()
        root = Path("tests_runtime_empty_discovery_delta")
        if root.exists():
            shutil.rmtree(root)
        root.mkdir(parents=True, exist_ok=True)

        try:
            context = build_context("06-04-2026", root)

            def runner(output_path: str, verbose: bool = False, **kwargs):
                output = Path(output_path)
                output.parent.mkdir(parents=True, exist_ok=True)
                with open(output, "w", newline="", encoding="utf-8") as file:
                    writer = csv.DictWriter(file, fieldnames=["business_type", "lastmod", "listing_id", "listing_url"])
                    writer.writeheader()
                return {
                    "output_path": str(output),
                    "metrics": {"delta_rows": 0},
                }

            scraper = ScraperDefinition(
                "lopes",
                "lopes",
                "lopes_discovery.csv",
                "lopes_listings.csv",
                runner,
                lambda **_: None,
                discovery_metadata={
                    "discovery_mode": "delta",
                    "delta_rule": "new_or_lastmod_changed",
                },
            )

            with patch("stages.collect_discovery.get_scraper_definitions", return_value=[scraper]):
                artifacts, metrics, errors = stage.run(context, None, logger)
                validations = stage.validate(
                    context,
                    None,
                    SimpleNamespace(artifacts=artifacts, metrics=metrics),
                    logger,
                )
        finally:
            shutil.rmtree(root, ignore_errors=True)

        self.assertEqual(errors, [])
        self.assertEqual(metrics["source_results"][0]["runner_metrics"]["delta_rows"], 0)
        self.assertTrue(all(validation.passed for validation in validations))

    def test_stage_filters_selected_sources(self):
        stage = CollectDiscoveryStage()
        logger = DummyLogger()
        root = Path("tests_runtime_discovery_filtered")
        if root.exists():
            shutil.rmtree(root)
        root.mkdir(parents=True, exist_ok=True)

        try:
            context = build_context("06-04-2026", root)
            called_sources: list[str] = []

            def make_runner(name: str):
                def runner(output_path: str, **kwargs):
                    called_sources.append(name)
                    output = Path(output_path)
                    output.parent.mkdir(parents=True, exist_ok=True)
                    with open(output, "w", newline="", encoding="utf-8") as file:
                        writer = csv.DictWriter(file, fieldnames=["property_id"])
                        writer.writeheader()
                        writer.writerow({"property_id": name})
                    return str(output)

                return runner

            scrapers = [
                ScraperDefinition("olx", "olx", "olx_discovery.csv", "olx_listings.csv", make_runner("olx"), lambda **_: None),
                ScraperDefinition("lopes", "lopes", "lopes_discovery.csv", "lopes_listings.csv", make_runner("lopes"), lambda **_: None),
                ScraperDefinition("quinto", "quinto", "quinto_discovery.csv", "quinto_listings.csv", make_runner("quinto"), lambda **_: None),
            ]

            with patch("stages.collect_discovery.get_scraper_definitions", return_value=scrapers):
                artifacts, metrics, errors = stage.run(
                    context,
                    None,
                    logger,
                    stage_options={"sources": ["lopes"]},
                )
        finally:
            shutil.rmtree(root, ignore_errors=True)

        self.assertEqual(called_sources, ["lopes"])
        self.assertEqual([artifact.name for artifact in artifacts], ["lopes"])
        self.assertEqual(metrics["configured_scrapers"], 1)
        self.assertEqual(metrics["selected_sources"], ["lopes"])
        self.assertEqual(errors, [])


class CollectListingsStageTests(unittest.TestCase):
    def test_stage_collects_all_sources_from_discovery_manifest(self):
        stage = CollectListingsStage()
        logger = DummyLogger()
        root = Path("tests_runtime_collect_listings")
        if root.exists():
            shutil.rmtree(root)
        root.mkdir(parents=True, exist_ok=True)

        try:
            context = build_context("06-04-2026", root)
            input_manifest = {
                "status": "success",
                "artifacts": [
                    {
                        "name": "lopes",
                        "path": str((root / "06-04-2026" / "lopes" / "lopes_discovery.csv").resolve()),
                        "format": "csv",
                        "rows": 1,
                        "metadata": {"source": "lopes", "artifact_role": "discovery"},
                    },
                    {
                        "name": "quinto",
                        "path": str((root / "06-04-2026" / "quinto" / "quinto_discovery.csv").resolve()),
                        "format": "csv",
                        "rows": 1,
                        "metadata": {"source": "quinto", "artifact_role": "discovery"},
                    },
                    {
                        "name": "olx",
                        "path": str((root / "06-04-2026" / "olx" / "olx_discovery.csv").resolve()),
                        "format": "csv",
                        "rows": 1,
                        "metadata": {"source": "olx", "artifact_role": "discovery"},
                    },
                ],
            }

            for artifact in input_manifest["artifacts"]:
                csv_path = Path(str(artifact["path"]))
                csv_path.parent.mkdir(parents=True, exist_ok=True)
                with open(csv_path, "w", newline="", encoding="utf-8") as file:
                    writer = csv.DictWriter(file, fieldnames=["listing_url", "business_type"])
                    writer.writeheader()
                    writer.writerow({"listing_url": "https://example.com/1", "business_type": "sale"})

            def make_runner(name: str):
                def runner(input_path: str, listings_output_path: str, verbose: bool = False, **kwargs):
                    time.sleep(0.2)
                    output = Path(listings_output_path)
                    output.parent.mkdir(parents=True, exist_ok=True)
                    with open(output, "w", newline="", encoding="utf-8") as file:
                        writer = csv.DictWriter(file, fieldnames=["property_id", "listing_url", "business_type"])
                        writer.writeheader()
                        writer.writerow(
                            {
                                "property_id": name,
                                "listing_url": "https://example.com/1",
                                "business_type": "sale",
                            }
                        )
                    return {"input_rows": 1, "output_rows": 1}

                return runner

            scrapers = [
                ScraperDefinition("olx", "olx", "olx_discovery.csv", "olx_listings.csv", lambda **_: None, make_runner("olx")),
                ScraperDefinition("lopes", "lopes", "lopes_discovery.csv", "lopes_listings.csv", lambda **_: None, make_runner("lopes")),
                ScraperDefinition("quinto", "quinto", "quinto_discovery.csv", "quinto_listings.csv", lambda **_: None, make_runner("quinto")),
            ]

            started_at = time.perf_counter()
            with patch("stages.collect_listings.get_scraper_definitions", return_value=scrapers):
                artifacts, metrics, errors = stage.run(context, input_manifest, logger)
            elapsed = time.perf_counter() - started_at
        finally:
            shutil.rmtree(root, ignore_errors=True)

        self.assertEqual({artifact.name for artifact in artifacts}, {"olx", "lopes", "quinto"})
        self.assertEqual(metrics["successful_scrapers"], 3)
        self.assertEqual(metrics["failed_scrapers"], 0)
        self.assertEqual(metrics["no_op_scrapers"], 0)
        self.assertEqual(errors, [])
        self.assertLess(elapsed, 0.9)

    def test_stage_filters_selected_sources(self):
        stage = CollectListingsStage()
        logger = DummyLogger()
        root = Path("tests_runtime_collect_listings_filtered")
        if root.exists():
            shutil.rmtree(root)
        root.mkdir(parents=True, exist_ok=True)

        try:
            context = build_context("06-04-2026", root)
            input_manifest = {
                "status": "success",
                "artifacts": [
                    {
                        "name": "lopes",
                        "path": str((root / "06-04-2026" / "lopes" / "lopes_discovery.csv").resolve()),
                        "format": "csv",
                        "rows": 1,
                        "metadata": {"source": "lopes", "artifact_role": "discovery"},
                    },
                    {
                        "name": "olx",
                        "path": str((root / "06-04-2026" / "olx" / "olx_discovery.csv").resolve()),
                        "format": "csv",
                        "rows": 1,
                        "metadata": {"source": "olx", "artifact_role": "discovery"},
                    },
                ],
            }
            for artifact in input_manifest["artifacts"]:
                csv_path = Path(str(artifact["path"]))
                csv_path.parent.mkdir(parents=True, exist_ok=True)
                with open(csv_path, "w", newline="", encoding="utf-8") as file:
                    writer = csv.DictWriter(file, fieldnames=["listing_url", "business_type"])
                    writer.writeheader()
                    writer.writerow({"listing_url": "https://example.com/1", "business_type": "sale"})

            called_sources: list[str] = []

            def make_runner(name: str):
                def runner(input_path: str, listings_output_path: str, verbose: bool = False, **kwargs):
                    called_sources.append(name)
                    output = Path(listings_output_path)
                    output.parent.mkdir(parents=True, exist_ok=True)
                    with open(output, "w", newline="", encoding="utf-8") as file:
                        writer = csv.DictWriter(file, fieldnames=["property_id", "listing_url", "business_type"])
                        writer.writeheader()
                        writer.writerow(
                            {
                                "property_id": name,
                                "listing_url": "https://example.com/1",
                                "business_type": "sale",
                            }
                        )
                    return {"input_rows": 1, "output_rows": 1}

                return runner

            scrapers = [
                ScraperDefinition("olx", "olx", "olx_discovery.csv", "olx_listings.csv", lambda **_: None, make_runner("olx")),
                ScraperDefinition("lopes", "lopes", "lopes_discovery.csv", "lopes_listings.csv", lambda **_: None, make_runner("lopes")),
            ]

            with patch("stages.collect_listings.get_scraper_definitions", return_value=scrapers):
                artifacts, metrics, errors = stage.run(
                    context,
                    input_manifest,
                    logger,
                    stage_options={"sources": ["lopes"]},
                )
        finally:
            shutil.rmtree(root, ignore_errors=True)

        self.assertEqual(called_sources, ["lopes"])
        self.assertEqual([artifact.name for artifact in artifacts], ["lopes"])
        self.assertEqual(metrics["selected_sources"], ["lopes"])
        self.assertEqual(errors, [])

    def test_stage_fails_when_requested_source_has_no_discovery_artifact(self):
        stage = CollectListingsStage()
        logger = DummyLogger()
        context = build_context("06-04-2026", Path("tests_runtime_missing_source"))
        input_manifest = {
            "status": "success",
            "artifacts": [
                {
                    "name": "olx",
                    "path": "/tmp/olx_discovery.csv",
                    "format": "csv",
                    "rows": 1,
                    "metadata": {"source": "olx", "artifact_role": "discovery"},
                }
            ],
        }

        with self.assertRaises(ValueError):
            stage.run(
                context,
                input_manifest,
                logger,
                stage_options={"sources": ["lopes"]},
            )

    def test_stage_accepts_all_sources_no_op(self):
        stage = CollectListingsStage()
        logger = DummyLogger()
        root = Path("tests_runtime_details_noop")
        if root.exists():
            shutil.rmtree(root)
        root.mkdir(parents=True, exist_ok=True)

        try:
            input_csv = root / "06-04-2026" / "lopes" / "lopes_discovery.csv"
            input_csv.parent.mkdir(parents=True, exist_ok=True)
            with open(input_csv, "w", newline="", encoding="utf-8") as file:
                writer = csv.DictWriter(file, fieldnames=["business_type", "lastmod", "listing_id", "listing_url"])
                writer.writeheader()

            context = build_context("06-04-2026", root)
            input_manifest = {
                "status": "success",
                "artifacts": [
                    {
                        "name": "lopes",
                        "path": str(input_csv.resolve()),
                        "format": "csv",
                        "rows": 0,
                        "metadata": {
                            "source": "lopes",
                            "artifact_role": "discovery",
                            "discovery_mode": "delta",
                            "delta_rule": "new_or_lastmod_changed",
                        },
                    }
                ],
            }

            scraper = ScraperDefinition(
                "lopes",
                "lopes",
                "lopes_discovery.csv",
                "lopes_listings.csv",
                lambda **_: None,
                lambda **_: {"input_rows": 0, "output_rows": 0, "no_op": True},
                "lopes",
            )

            with patch("stages.collect_listings.get_scraper_definitions", return_value=[scraper]):
                artifacts, metrics, errors = stage.run(
                    context,
                    input_manifest,
                    logger,
                )
                validations = stage.validate(
                    context,
                    input_manifest,
                    SimpleNamespace(artifacts=artifacts, metrics=metrics),
                    logger,
                )
        finally:
            shutil.rmtree(root, ignore_errors=True)

        self.assertEqual(artifacts, [])
        self.assertEqual(errors, [])
        self.assertTrue(metrics["source_results"][0]["no_op"])
        self.assertTrue(metrics["all_sources_no_op"])
        self.assertTrue(all(validation.passed for validation in validations))

    def test_stage_skips_source_already_completed_for_same_run_date(self):
        stage = CollectListingsStage()
        logger = DummyLogger()
        root = Path("tests_runtime_collect_listings_resume")
        if root.exists():
            shutil.rmtree(root)
        root.mkdir(parents=True, exist_ok=True)

        try:
            run_date = "06-04-2026"
            context = build_context(run_date, root)
            input_csv = context.raw_dir / "lopes" / "lopes_discovery.csv"
            input_csv.parent.mkdir(parents=True, exist_ok=True)
            with open(input_csv, "w", newline="", encoding="utf-8") as file:
                writer = csv.DictWriter(file, fieldnames=["listing_url", "business_type"])
                writer.writeheader()
                writer.writerow({"listing_url": "https://example.com/1", "business_type": "sale"})

            output_csv = context.raw_dir / "lopes" / "lopes_listings.csv"
            output_csv.parent.mkdir(parents=True, exist_ok=True)
            with open(output_csv, "w", newline="", encoding="utf-8") as file:
                writer = csv.DictWriter(file, fieldnames=["property_id", "listing_url", "business_type"])
                writer.writeheader()
                writer.writerow(
                    {
                        "property_id": "REO1",
                        "listing_url": "https://example.com/1",
                        "business_type": "sale",
                    }
                )

            resume_paths = build_resume_paths(context.artifacts_run_dir / "collect_listings" / "lopes")
            write_json(
                resume_paths["state_json"],
                {
                    "status": "completed",
                    "output_rows": 1,
                    "output_path": str(output_csv.resolve()),
                },
            )

            input_manifest = {
                "status": "success",
                "artifacts": [
                    {
                        "name": "lopes",
                        "path": str(input_csv.resolve()),
                        "format": "csv",
                        "rows": 1,
                        "metadata": {"source": "lopes", "artifact_role": "discovery"},
                    }
                ],
            }

            runner_mock = patch(
                "stages.collect_listings.get_scraper_definitions",
                return_value=[
                    ScraperDefinition(
                        "lopes",
                        "lopes",
                        "lopes_discovery.csv",
                        "lopes_listings.csv",
                        lambda **_: None,
                        lambda **_: (_ for _ in ()).throw(AssertionError("runner nao deveria ser chamado")),
                    )
                ],
            )
            with runner_mock:
                artifacts, metrics, errors = stage.run(context, input_manifest, logger)
        finally:
            shutil.rmtree(root, ignore_errors=True)

        self.assertEqual(errors, [])
        self.assertEqual(len(artifacts), 1)
        self.assertTrue(metrics["source_results"][0]["skipped_completed"])


class LoggingUtilsTests(unittest.TestCase):
    def test_listing_collection_item_only_logs_in_verbose_mode(self):
        with patch("sys.stdout", new=StringIO()) as stream:
            log_listing_collection_item("quinto", processed=1, total=10, url="https://example.com", verbose=False)
            quiet_output = stream.getvalue()

        with patch("sys.stdout", new=StringIO()) as stream:
            log_listing_collection_item("quinto", processed=1, total=10, url="https://example.com", verbose=True)
            verbose_output = stream.getvalue()

        self.assertEqual(quiet_output, "")
        self.assertIn("listing_collection_item", verbose_output)

    def test_listing_collection_progress_logs_in_standard_mode_on_interval_boundaries(self):
        with patch("sys.stdout", new=StringIO()) as stream:
            log_listing_collection_progress(
                "lopes",
                processed=1,
                total=100,
                success=1,
                failures=0,
                verbose=False,
            )
            first_output = stream.getvalue()

        with patch("sys.stdout", new=StringIO()) as stream:
            log_listing_collection_progress(
                "lopes",
                processed=2,
                total=100,
                success=2,
                failures=0,
                verbose=False,
            )
            second_output = stream.getvalue()

        self.assertIn("listing_collection_progress", first_output)
        self.assertEqual(second_output, "")


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

    def test_attach_canonical_id_uses_full_join_key_when_available(self):
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

        enriched = attach_canonical_id(listings_df, link_df)

        self.assertEqual(len(enriched), 1)
        self.assertEqual(enriched.loc[0, "canonical_property_id"], "rent-canonical")

    def test_attach_canonical_id_propagates_property_level_sale_and_rent_prices(self):
        listings_df = pd.DataFrame(
            [
                {
                    "source": "olx",
                    "business_type": "rent",
                    "property_id": "1493832184",
                    "listing_url": "https://example.com/rent",
                    "sale_price_brl": pd.NA,
                    "rent_price_brl": 25000,
                    "scraped_at": "2026-04-06T20:30:21.788509+00:00",
                },
                {
                    "source": "olx",
                    "business_type": "sale",
                    "property_id": "1493832177",
                    "listing_url": "https://example.com/sale",
                    "sale_price_brl": 5900000,
                    "rent_price_brl": pd.NA,
                    "scraped_at": "2026-04-06T15:18:17.465397+00:00",
                },
            ]
        )
        link_df = pd.DataFrame(
            [
                {
                    "canonical_property_id": "shared-canonical",
                    "source": "olx",
                    "business_type": "rent",
                    "property_id": "1493832184",
                    "listing_url": "https://example.com/rent",
                    "scraped_at": "2026-04-06T20:30:21.788509+00:00",
                },
                {
                    "canonical_property_id": "shared-canonical",
                    "source": "olx",
                    "business_type": "sale",
                    "property_id": "1493832177",
                    "listing_url": "https://example.com/sale",
                    "scraped_at": "2026-04-06T15:18:17.465397+00:00",
                },
            ]
        )
        properties_df = pd.DataFrame(
            [
                {
                    "canonical_property_id": "shared-canonical",
                    "sale_price_brl": 5900000,
                    "rent_price_brl": 25000,
                    "is_for_sale": True,
                    "is_for_rent": True,
                    "listing_mode": "sale_rent",
                }
            ]
        )

        enriched = attach_canonical_id(listings_df, link_df, properties_df)

        self.assertEqual(len(enriched), 2)
        self.assertTrue((enriched["sale_price_brl"] == 5900000).all())
        self.assertTrue((enriched["rent_price_brl"] == 25000).all())
        self.assertTrue(enriched["is_for_sale"].all())
        self.assertTrue(enriched["is_for_rent"].all())
        self.assertTrue((enriched["listing_mode"] == "sale_rent").all())

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

    def test_update_historical_store_accepts_rent_sale_business_type_key(self):
        output_dir = Path("tests_runtime_historical_store_rent_sale")
        output_dir.mkdir(parents=True, exist_ok=True)
        try:
            existing = pd.DataFrame(
                [
                    {
                        "source": "quinto",
                        "business_type": "sale",
                        "property_id": "892884382",
                        "listing_url": "https://www.quintoandar.com.br/imovel/892884382/comprar/apartamento",
                        "city": "Sao Paulo",
                        "neighbourhood": "Jardim Paulista",
                        "address": "Rua Exemplo",
                        "state": "SP",
                        "zip_code": "01414-001",
                        "lat": pd.NA,
                        "lon": pd.NA,
                        "property_type": "Apartamento",
                        "area_m2": 180,
                        "total_area_m2": 180,
                        "bedrooms": 3,
                        "bathrooms": 4,
                        "parking_spots": 2,
                        "suites": 1,
                        "floor": 5,
                        "furnished": False,
                        "accepts_pets": True,
                        "condominium_name": pd.NA,
                        "condominium_id": pd.NA,
                        "amenities_json": pd.NA,
                        "installations_json": pd.NA,
                        "sale_price_brl": 4200000,
                        "rent_price_brl": pd.NA,
                        "scraped_at": "2026-04-14T10:00:00+00:00",
                        "first_seen_at": "2026-04-14T10:00:00+00:00",
                        "last_seen_at": "2026-04-14T10:00:00+00:00",
                        "created_at": "2026-04-14T10:00:00+00:00",
                        "updated_at": "2026-04-14T10:00:00+00:00",
                    }
                ]
            )
            existing.to_parquet(output_dir / "listings_unificados.parquet", index=False)

            snapshot = pd.DataFrame(
                [
                    {
                        "canonical_property_id": "quinto-892884382",
                        "source": "quinto",
                        "business_type": "rent|sale",
                        "property_id": "892884382",
                        "listing_url": "https://www.quintoandar.com.br/imovel/892884382/alugar/apartamento",
                        "city": "Sao Paulo",
                        "neighbourhood": "Jardim Paulista",
                        "address": "Rua Exemplo",
                        "state": "SP",
                        "zip_code": "01414-001",
                        "lat": pd.NA,
                        "lon": pd.NA,
                        "property_type": "Apartamento",
                        "area_m2": 180,
                        "total_area_m2": 180,
                        "bedrooms": 3,
                        "bathrooms": 4,
                        "parking_spots": 2,
                        "suites": 1,
                        "floor": 5,
                        "furnished": False,
                        "accepts_pets": True,
                        "condominium_name": pd.NA,
                        "condominium_id": pd.NA,
                        "amenities_json": pd.NA,
                        "installations_json": pd.NA,
                        "sale_price_brl": 4200000,
                        "rent_price_brl": 12000,
                        "scraped_at": "2026-04-15T10:00:00+00:00",
                    }
                ]
            )

            output = update_historical_store(snapshot, output_dir)
            listings = output["listings"]
            upsert_keys = listings[["source", "business_type", "property_id"]].fillna("").astype(str).agg("|".join, axis=1)
        finally:
            shutil.rmtree(output_dir, ignore_errors=True)

        self.assertTrue(upsert_keys.is_unique)
        self.assertIn("quinto|rent|sale|892884382", upsert_keys.tolist())
        self.assertEqual(len(listings.loc[listings["property_id"] == "892884382"]), 2)

    def test_update_historical_store_applies_insert_and_update_across_incremental_days(self):
        output_dir = Path("tests_runtime_historical_store_incremental")
        output_dir.mkdir(parents=True, exist_ok=True)
        try:
            day_one_snapshot = pd.DataFrame(
                [
                    {
                        "canonical_property_id": "lopes-1",
                        "source": "lopes",
                        "business_type": "sale",
                        "property_id": "REO2000001",
                        "listing_url": "https://www.lopes.com.br/imovel/REO2000001/venda-apartamento",
                        "city": "Sao Paulo",
                        "neighbourhood": "Moema",
                        "address": "Rua A, 100",
                        "state": "SP",
                        "zip_code": "04000-000",
                        "lat": pd.NA,
                        "lon": pd.NA,
                        "property_type": "Apartamento",
                        "area_m2": 80,
                        "total_area_m2": 80,
                        "bedrooms": 2,
                        "bathrooms": 2,
                        "parking_spots": 1,
                        "suites": 1,
                        "floor": 10,
                        "furnished": False,
                        "accepts_pets": True,
                        "condominium_name": pd.NA,
                        "condominium_id": pd.NA,
                        "amenities_json": pd.NA,
                        "installations_json": pd.NA,
                        "sale_price_brl": 1000000,
                        "rent_price_brl": pd.NA,
                        "scraped_at": "2026-04-10T10:00:00+00:00",
                    }
                ]
            )
            first_output = update_historical_store(day_one_snapshot, output_dir)

            day_two_delta = pd.DataFrame(
                [
                    {
                        "canonical_property_id": "lopes-1",
                        "source": "lopes",
                        "business_type": "sale",
                        "property_id": "REO2000001",
                        "listing_url": "https://www.lopes.com.br/imovel/REO2000001/venda-apartamento",
                        "city": "Sao Paulo",
                        "neighbourhood": "Moema",
                        "address": "Rua A, 100",
                        "state": "SP",
                        "zip_code": "04000-000",
                        "lat": pd.NA,
                        "lon": pd.NA,
                        "property_type": "Apartamento",
                        "area_m2": 80,
                        "total_area_m2": 80,
                        "bedrooms": 2,
                        "bathrooms": 2,
                        "parking_spots": 1,
                        "suites": 1,
                        "floor": 10,
                        "furnished": False,
                        "accepts_pets": True,
                        "condominium_name": pd.NA,
                        "condominium_id": pd.NA,
                        "amenities_json": pd.NA,
                        "installations_json": pd.NA,
                        "sale_price_brl": 1100000,
                        "rent_price_brl": pd.NA,
                        "scraped_at": "2026-04-11T10:00:00+00:00",
                    },
                    {
                        "canonical_property_id": "lopes-2",
                        "source": "lopes",
                        "business_type": "sale",
                        "property_id": "REO2000002",
                        "listing_url": "https://www.lopes.com.br/imovel/REO2000002/venda-apartamento",
                        "city": "Sao Paulo",
                        "neighbourhood": "Pinheiros",
                        "address": "Rua B, 200",
                        "state": "SP",
                        "zip_code": "05400-000",
                        "lat": pd.NA,
                        "lon": pd.NA,
                        "property_type": "Apartamento",
                        "area_m2": 70,
                        "total_area_m2": 70,
                        "bedrooms": 2,
                        "bathrooms": 1,
                        "parking_spots": 1,
                        "suites": 0,
                        "floor": 6,
                        "furnished": False,
                        "accepts_pets": True,
                        "condominium_name": pd.NA,
                        "condominium_id": pd.NA,
                        "amenities_json": pd.NA,
                        "installations_json": pd.NA,
                        "sale_price_brl": 900000,
                        "rent_price_brl": pd.NA,
                        "scraped_at": "2026-04-11T11:00:00+00:00",
                    },
                ]
            )
            second_output = update_historical_store(day_two_delta, output_dir)
            listings = second_output["listings"]
        finally:
            shutil.rmtree(output_dir, ignore_errors=True)

        self.assertEqual(first_output["inserted_count"], 1)
        self.assertEqual(first_output["updated_count"], 0)
        self.assertEqual(second_output["inserted_count"], 1)
        self.assertEqual(second_output["updated_count"], 1)
        self.assertEqual(len(listings), 2)
        updated_row = listings.loc[listings["property_id"] == "REO2000001"].iloc[0]
        inserted_row = listings.loc[listings["property_id"] == "REO2000002"].iloc[0]
        self.assertEqual(updated_row["sale_price_brl"], 1100000)
        self.assertEqual(inserted_row["sale_price_brl"], 900000)


class PipelineRunnerFlowTests(unittest.TestCase):
    def test_select_stages_uses_fixed_four_stage_sequence(self):
        runner = PipelineRunner()
        self.assertEqual(
            list(runner._select_stages(None)),
            ["collect_discovery", "collect_listings", "build_daily_snapshot", "update_historical_store"],
        )

    def test_stage_options_include_verbose_for_discovery_and_collect(self):
        runner = PipelineRunner()

        self.assertEqual(
            runner._stage_options("collect_discovery", True),
            {"verbose": True},
        )
        self.assertEqual(
            runner._stage_options("collect_listings", True),
            {"verbose": True},
        )
        self.assertEqual(
            runner._stage_options("build_daily_snapshot", True),
            {},
        )

    def test_stage_options_include_selected_sources_for_any_stage(self):
        runner = PipelineRunner()

        self.assertEqual(
            runner._stage_options("collect_listings", True, sources=["lopes", "olx"]),
            {"verbose": True, "sources": ["lopes", "olx"]},
        )
        self.assertEqual(
            runner._stage_options("build_daily_snapshot", False, sources=["lopes"]),
            {"sources": ["lopes"]},
        )

    def test_build_context_can_separate_project_root_and_output_root(self):
        project_root = Path("tests_runtime_project_root").resolve()
        output_root = Path("tests_runtime_output_root").resolve()

        context = build_context("07-04-2026", project_root, output_root=output_root)

        self.assertEqual(context.project_root, project_root)
        self.assertEqual(context.output_root, output_root)
        self.assertEqual(context.raw_dir, output_root / "raw" / "07-04-2026")
        self.assertEqual(context.processed_dir, output_root / "processed")
        self.assertEqual(context.artifacts_run_dir, output_root / "artifacts" / "07-04-2026")
        self.assertEqual(context.logs_run_dir, output_root / "logs" / "07-04-2026")
        self.assertEqual(context.to_dict()["output_root"], str(output_root))

    def test_build_snapshot_requires_collect_listings_manifest(self):
        runtime_dir = Path("tests_runtime_runner")
        runtime_dir.mkdir(parents=True, exist_ok=True)
        try:
            runner = PipelineRunner(project_root=runtime_dir)
            context = build_context("07-04-2026", runtime_dir)
            discovery_manifest_path = stage_manifest_path(context, "collect_discovery")
            collect_manifest_path = stage_manifest_path(context, "collect_listings")
            write_json(discovery_manifest_path, {"status": "success"})
            write_json(collect_manifest_path, {"status": "success"})

            selected = runner._default_input_manifest(context, "build_daily_snapshot")
        finally:
            shutil.rmtree(runtime_dir, ignore_errors=True)

        self.assertEqual(selected, collect_manifest_path)

    def test_collect_listings_requires_discovery_manifest_by_default(self):
        runtime_dir = Path("tests_runtime_runner_manifests")
        runtime_dir.mkdir(parents=True, exist_ok=True)
        try:
            runner = PipelineRunner(project_root=runtime_dir)
            context = build_context("07-04-2026", runtime_dir)
            discovery_manifest_path = stage_manifest_path(context, "collect_discovery")
            write_json(discovery_manifest_path, {"status": "success"})

            selected = runner._default_input_manifest(context, "collect_listings")
        finally:
            shutil.rmtree(runtime_dir, ignore_errors=True)

        self.assertEqual(selected, discovery_manifest_path)

    def test_filtered_collect_listings_requires_filtered_discovery_manifest_by_default(self):
        runtime_dir = Path("tests_runtime_runner_manifests_filtered")
        runtime_dir.mkdir(parents=True, exist_ok=True)
        try:
            runner = PipelineRunner(project_root=runtime_dir)
            context = build_context("07-04-2026", runtime_dir)
            scoped_discovery_manifest_path = stage_manifest_path(context, "collect_discovery", sources=["lopes"])
            write_json(scoped_discovery_manifest_path, {"status": "success"})

            selected = runner._default_input_manifest(context, "collect_listings", sources=["lopes"])
        finally:
            shutil.rmtree(runtime_dir, ignore_errors=True)

        self.assertEqual(selected, scoped_discovery_manifest_path)

    def test_filtered_collect_listings_falls_back_to_unscoped_discovery_manifest(self):
        runtime_dir = Path("tests_runtime_runner_manifests_filtered_fallback")
        runtime_dir.mkdir(parents=True, exist_ok=True)
        try:
            runner = PipelineRunner(project_root=runtime_dir)
            context = build_context("07-04-2026", runtime_dir)
            discovery_manifest_path = stage_manifest_path(context, "collect_discovery")
            write_json(discovery_manifest_path, {"status": "success"})

            selected = runner._default_input_manifest(context, "collect_listings", sources=["lopes"])
        finally:
            shutil.rmtree(runtime_dir, ignore_errors=True)

        self.assertEqual(selected, discovery_manifest_path)

    def test_run_stage_forwards_sources_to_stage_options(self):
        runtime_dir = Path("tests_runtime_runner_stage_sources")
        runtime_dir.mkdir(parents=True, exist_ok=True)
        try:
            runner = PipelineRunner(project_root=runtime_dir)
            context = build_context("07-04-2026", runtime_dir)
            discovery_manifest_path = stage_manifest_path(context, "collect_discovery", sources=["lopes"])
            write_json(discovery_manifest_path, {"status": "success"})
            captured: dict[str, object] = {}

            def fake_execute(context_arg, input_manifest_path=None, stage_options=None):
                captured["input_manifest_path"] = input_manifest_path
                captured["stage_options"] = stage_options
                return SimpleNamespace(status="success", to_dict=lambda: {})

            with patch("workflow.runner.get_stage", return_value=SimpleNamespace(execute=fake_execute)):
                runner.run_stage("collect_listings", "07-04-2026", sources=["lopes"])
        finally:
            shutil.rmtree(runtime_dir, ignore_errors=True)

        self.assertEqual(captured["input_manifest_path"], discovery_manifest_path)
        self.assertEqual(captured["stage_options"], {"verbose": False, "sources": ["lopes"]})

    def test_run_all_stops_after_discovery_when_no_new_links_exist(self):
        runtime_dir = Path("tests_runtime_runner_short_circuit")
        runtime_dir.mkdir(parents=True, exist_ok=True)
        try:
            runner = PipelineRunner(project_root=runtime_dir)
            discovery_result = SimpleNamespace(
                status="success",
                blocked=False,
                output_manifest=str(runtime_dir / "collect_discovery.json"),
                metrics={"new_links_total": 0},
                to_dict=lambda: {
                    "stage_name": "collect_discovery",
                    "status": "success",
                    "metrics": {"new_links_total": 0},
                },
            )
            collect_stage = SimpleNamespace(execute=lambda *args, **kwargs: None)
            with patch("workflow.runner.get_stage", side_effect=[SimpleNamespace(execute=lambda *args, **kwargs: discovery_result), collect_stage]):
                payload = runner.run_all(run_date="07-04-2026")
        finally:
            shutil.rmtree(runtime_dir, ignore_errors=True)

        self.assertEqual(payload["status"], "success")
        self.assertEqual(payload["stop_reason"], "no_new_links_after_discovery")
        self.assertEqual([result["stage_name"] for result in payload["results"]], ["collect_discovery"])

    def test_run_all_skips_existing_successful_discovery_manifest(self):
        runtime_dir = Path("tests_runtime_runner_skip_discovery")
        runtime_dir.mkdir(parents=True, exist_ok=True)
        captured: dict[str, object] = {"requested_stages": []}
        try:
            runner = PipelineRunner(project_root=runtime_dir)
            context = build_context("07-04-2026", runtime_dir)
            discovery_manifest_path = stage_manifest_path(context, "collect_discovery")
            write_json(
                discovery_manifest_path,
                {
                    "stage_name": "collect_discovery",
                    "status": "success",
                    "output_manifest": str(discovery_manifest_path),
                    "metrics": {"new_links_total": 3},
                },
            )

            def fake_get_stage(stage_name):
                captured["requested_stages"].append(stage_name)

                def execute(context_arg, input_manifest_path=None, stage_options=None):
                    captured["input_manifest_path"] = input_manifest_path
                    return SimpleNamespace(
                        status="failed",
                        blocked=True,
                        output_manifest=None,
                        metrics={},
                        to_dict=lambda: {"stage_name": stage_name, "status": "failed"},
                    )

                return SimpleNamespace(execute=execute)

            with patch("workflow.runner.get_stage", side_effect=fake_get_stage):
                payload = runner.run_all(run_date="07-04-2026")
        finally:
            shutil.rmtree(runtime_dir, ignore_errors=True)

        self.assertEqual(captured["requested_stages"], ["collect_listings"])
        self.assertEqual(captured["input_manifest_path"], discovery_manifest_path)
        self.assertTrue(payload["results"][0]["skipped"])
        self.assertEqual(payload["results"][0]["skip_reason"], "existing_success_manifest")

    def test_run_all_force_discovery_executes_even_when_success_manifest_exists(self):
        runtime_dir = Path("tests_runtime_runner_force_discovery")
        runtime_dir.mkdir(parents=True, exist_ok=True)
        captured: dict[str, object] = {"execute_calls": 0}
        try:
            runner = PipelineRunner(project_root=runtime_dir)
            context = build_context("07-04-2026", runtime_dir)
            discovery_manifest_path = stage_manifest_path(context, "collect_discovery")
            write_json(discovery_manifest_path, {"stage_name": "collect_discovery", "status": "success"})
            discovery_result = SimpleNamespace(
                status="success",
                blocked=False,
                output_manifest=str(discovery_manifest_path),
                metrics={"new_links_total": 0},
                to_dict=lambda: {
                    "stage_name": "collect_discovery",
                    "status": "success",
                    "metrics": {"new_links_total": 0},
                },
            )

            def execute(*args, **kwargs):
                captured["execute_calls"] += 1
                return discovery_result

            with patch("workflow.runner.get_stage", return_value=SimpleNamespace(execute=execute)):
                payload = runner.run_all(run_date="07-04-2026", force_discovery=True)
        finally:
            shutil.rmtree(runtime_dir, ignore_errors=True)

        self.assertEqual(captured["execute_calls"], 1)
        self.assertNotIn("skipped", payload["results"][0])

    def test_run_all_executes_discovery_when_existing_manifest_failed(self):
        runtime_dir = Path("tests_runtime_runner_failed_discovery_manifest")
        runtime_dir.mkdir(parents=True, exist_ok=True)
        captured: dict[str, object] = {"execute_calls": 0}
        try:
            runner = PipelineRunner(project_root=runtime_dir)
            context = build_context("07-04-2026", runtime_dir)
            discovery_manifest_path = stage_manifest_path(context, "collect_discovery")
            write_json(discovery_manifest_path, {"stage_name": "collect_discovery", "status": "failed"})
            discovery_result = SimpleNamespace(
                status="success",
                blocked=False,
                output_manifest=str(discovery_manifest_path),
                metrics={"new_links_total": 0},
                to_dict=lambda: {
                    "stage_name": "collect_discovery",
                    "status": "success",
                    "metrics": {"new_links_total": 0},
                },
            )

            def execute(*args, **kwargs):
                captured["execute_calls"] += 1
                return discovery_result

            with patch("workflow.runner.get_stage", return_value=SimpleNamespace(execute=execute)):
                payload = runner.run_all(run_date="07-04-2026")
        finally:
            shutil.rmtree(runtime_dir, ignore_errors=True)

        self.assertEqual(captured["execute_calls"], 1)
        self.assertNotIn("skipped", payload["results"][0])


class RegistryAndSnapshotTests(unittest.TestCase):
    def test_registry_uses_single_definition_per_source(self):
        definitions = get_scraper_definitions()
        names = [definition.name for definition in definitions]

        self.assertEqual(names, ["olx", "lopes", "quinto"])

    def test_registry_exposes_discovery_and_collection_callables(self):
        definitions = get_scraper_definitions()

        self.assertTrue(all(callable(definition.run_discovery) for definition in definitions))
        self.assertTrue(all(callable(definition.run_collection) for definition in definitions))

    def test_detail_utils_module_was_removed_from_scrapers(self):
        self.assertFalse(Path("scrapers/detail_utils.py").exists())
        for shared_path in (
            Path("scrapers/olx_shared.py"),
            Path("scrapers/lopes_shared.py"),
            Path("scrapers/quinto_shared.py"),
        ):
            self.assertNotIn("detail_utils", shared_path.read_text(encoding="utf-8"))

    def test_throttle_module_was_removed_from_scrapers(self):
        self.assertFalse(Path("scrapers/throttle.py").exists())
        for source_path in Path("scrapers").glob("*.py"):
            self.assertNotIn("scrapers.throttle", source_path.read_text(encoding="utf-8"))

    def test_build_snapshot_reads_only_collect_listings_artifacts(self):
        stage = BuildDailySnapshotStage()
        manifest = {
            "stage_name": "collect_listings",
            "artifacts": [
                {
                    "name": "lopes",
                    "path": "/tmp/lopes_listings.csv",
                    "format": "csv",
                    "metadata": {"source": "lopes", "artifact_role": "listings"},
                },
                {
                    "name": "quinto",
                    "path": "/tmp/quinto_listings.csv",
                    "format": "csv",
                    "metadata": {"source": "quinto", "artifact_role": "listings"},
                },
                {
                    "name": "olx",
                    "path": "/tmp/olx_listings.csv",
                    "format": "csv",
                    "metadata": {"source": "olx", "artifact_role": "listings"},
                },
            ],
        }

        raw_files, manifest_source = stage._resolve_snapshot_inputs(manifest)

        self.assertEqual(raw_files, ["/tmp/lopes_listings.csv", "/tmp/quinto_listings.csv", "/tmp/olx_listings.csv"])
        self.assertEqual(manifest_source, "collect_listings")

    def test_build_snapshot_filters_sources_and_uses_scoped_output_dir(self):
        stage = BuildDailySnapshotStage()
        logger = DummyLogger()
        runtime_dir = Path("tests_runtime_build_snapshot_scoped")
        shutil.rmtree(runtime_dir, ignore_errors=True)
        runtime_dir.mkdir(parents=True, exist_ok=True)

        try:
            context = build_context("14-04-2026", runtime_dir)
            input_manifest = {
                "status": "success",
                "stage_name": "collect_listings",
                "artifacts": [
                    {
                        "name": "lopes",
                        "path": str((runtime_dir / "raw" / "lopes.csv").resolve()),
                        "format": "csv",
                        "metadata": {"source": "lopes", "artifact_role": "listings"},
                    },
                    {
                        "name": "olx",
                        "path": str((runtime_dir / "raw" / "olx.csv").resolve()),
                        "format": "csv",
                        "metadata": {"source": "olx", "artifact_role": "listings"},
                    },
                ],
            }
            captured: dict[str, object] = {}

            def fake_build_daily_snapshot(files, output_dir):
                captured["files"] = files
                captured["output_dir"] = output_dir
                output_dir.mkdir(parents=True, exist_ok=True)
                listings_path = output_dir / "listings_unificados.parquet"
                properties_path = output_dir / "properties_unified.parquet"
                links_path = output_dir / "property_listing_link.parquet"
                listings_csv_path = output_dir / "listings_unificados.csv"
                pd.DataFrame([{"source": "lopes", "business_type": "sale", "property_id": "1", "listing_url": "https://example.com"}]).to_parquet(listings_path, index=False)
                pd.DataFrame([{"canonical_property_id": "lopes-1"}]).to_parquet(properties_path, index=False)
                pd.DataFrame([{"canonical_property_id": "lopes-1"}]).to_parquet(links_path, index=False)
                pd.DataFrame([{"source": "lopes"}]).to_csv(listings_csv_path, index=False)
                return {
                    "listings": pd.read_parquet(listings_path),
                    "properties": pd.read_parquet(properties_path),
                    "links": pd.read_parquet(links_path),
                    "paths": {
                        "listings": listings_path,
                        "properties": properties_path,
                        "links": links_path,
                        "listings_csv": listings_csv_path,
                    },
                    "metrics": {},
                }

            with patch("stages.build_daily_snapshot.build_daily_snapshot", side_effect=fake_build_daily_snapshot):
                artifacts, metrics, errors = stage.run(
                    context,
                    input_manifest,
                    logger,
                    stage_options={"sources": ["lopes"]},
                )
        finally:
            shutil.rmtree(runtime_dir, ignore_errors=True)

        expected_output_dir = build_scoped_output_dir(context.processed_run_dir, ["lopes"])
        self.assertEqual(captured["files"], [input_manifest["artifacts"][0]["path"]])
        self.assertEqual(Path(captured["output_dir"]), expected_output_dir)
        self.assertEqual(metrics["selected_sources"], ["lopes"])
        self.assertEqual(metrics["source_scope"], "lopes")
        self.assertTrue(all(artifact.metadata["sources"] == ["lopes"] for artifact in artifacts))
        self.assertEqual(errors, [])

    def test_build_daily_snapshot_enriches_olx_missing_address_and_coordinates_from_zip_code(self):
        runtime_dir = Path("tests_runtime_snapshot_zipcode")
        shutil.rmtree(runtime_dir, ignore_errors=True)
        (runtime_dir / "raw").mkdir(parents=True, exist_ok=True)
        csv_path = runtime_dir / "raw" / "olx_listings.csv"
        output_dir = runtime_dir / "processed" / "14-04-2026"

        try:
            cache_dir = runtime_dir / "artifacts" / "_cache"
            cache_dir.mkdir(parents=True, exist_ok=True)
            (cache_dir / "base_ceps.csv").write_text(
                "cep,logradouro,localidade,id_municipio,nome_municipio,sigla_uf,estabelecimentos,centroide\n"
                "04000000,Rua Teste,Moema,,Sao Paulo,SP,,POINT(-46.6 -23.5)\n",
                encoding="utf-8",
            )
            with open(csv_path, "w", newline="", encoding="utf-8") as file:
                writer = csv.DictWriter(
                    file,
                    fieldnames=[
                        "property_id",
                        "listing_url",
                        "business_type",
                        "city",
                        "state",
                        "neighbourhood",
                        "zip_code",
                        "area_m2",
                        "bedrooms",
                        "bathrooms",
                    ],
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "property_id": "123",
                        "listing_url": "https://www.olx.com.br/imoveis/venda/item-123",
                        "business_type": "sale",
                        "city": "Sao Paulo",
                        "state": "SP",
                        "neighbourhood": "Moema",
                        "zip_code": "04000-000",
                        "area_m2": "120",
                        "bedrooms": "3",
                        "bathrooms": "2",
                    }
                )

            snapshot = build_daily_snapshot([str(csv_path)], output_dir)
        finally:
            shutil.rmtree(runtime_dir, ignore_errors=True)

        listings = snapshot["listings"]
        properties = snapshot["properties"]
        self.assertEqual(listings.loc[0, "address"], "Rua Teste")
        self.assertEqual(listings.loc[0, "lat"], -23.5)
        self.assertEqual(listings.loc[0, "lon"], -46.6)
        self.assertIn("rua teste", properties.loc[0, "canonical_property_id"])
        self.assertEqual(snapshot["metrics"]["zip_code_address_filled_count"], 1)
        self.assertEqual(snapshot["metrics"]["zip_code_coordinates_filled_count"], 1)


class UpdateHistoricalStoreStageTests(unittest.TestCase):
    def test_stage_filters_snapshot_rows_and_uses_scoped_output_dir(self):
        stage = UpdateHistoricalStoreStage()
        logger = DummyLogger()
        runtime_dir = Path("tests_runtime_update_historical_scoped")
        shutil.rmtree(runtime_dir, ignore_errors=True)
        runtime_dir.mkdir(parents=True, exist_ok=True)

        try:
            context = build_context("14-04-2026", runtime_dir)
            processed_run_dir = build_scoped_output_dir(context.processed_run_dir, ["lopes"])
            processed_run_dir.mkdir(parents=True, exist_ok=True)
            listings_path = processed_run_dir / "listings_unificados.parquet"
            properties_path = processed_run_dir / "properties_unified.parquet"
            links_path = processed_run_dir / "property_listing_link.parquet"
            pd.DataFrame(
                [
                    {"source": "lopes", "business_type": "sale", "property_id": "l-1", "listing_url": "https://example.com/l-1"},
                    {"source": "olx", "business_type": "sale", "property_id": "o-1", "listing_url": "https://example.com/o-1"},
                ]
            ).to_parquet(listings_path, index=False)
            pd.DataFrame([{"canonical_property_id": "x"}]).to_parquet(properties_path, index=False)
            pd.DataFrame([{"canonical_property_id": "x"}]).to_parquet(links_path, index=False)
            input_manifest = {
                "status": "success",
                "artifacts": [
                    {"name": "daily_listings", "path": str(listings_path.resolve())},
                    {"name": "daily_properties", "path": str(properties_path.resolve())},
                    {"name": "daily_property_listing_link", "path": str(links_path.resolve())},
                ],
            }
            captured: dict[str, object] = {}

            def fake_update_historical_store(snapshot_listings, processed_dir):
                captured["sources"] = sorted(snapshot_listings["source"].tolist())
                captured["processed_dir"] = processed_dir
                processed_dir.mkdir(parents=True, exist_ok=True)
                historical_listings = processed_dir / "listings_unificados.parquet"
                historical_properties = processed_dir / "properties_unified.parquet"
                historical_links = processed_dir / "property_listing_link.parquet"
                snapshot_listings.to_parquet(historical_listings, index=False)
                pd.DataFrame([{"canonical_property_id": "lopes-1"}]).to_parquet(historical_properties, index=False)
                pd.DataFrame([{"canonical_property_id": "lopes-1"}]).to_parquet(historical_links, index=False)
                return {
                    "listings": snapshot_listings,
                    "properties": pd.read_parquet(historical_properties),
                    "links": pd.read_parquet(historical_links),
                    "inserted_count": len(snapshot_listings),
                    "updated_count": 0,
                    "paths": {
                        "listings": historical_listings,
                        "properties": historical_properties,
                        "links": historical_links,
                    },
                }

            with patch("stages.update_historical_store.update_historical_store", side_effect=fake_update_historical_store):
                artifacts, metrics, errors = stage.run(
                    context,
                    input_manifest,
                    logger,
                    stage_options={"sources": ["lopes"]},
                )
        finally:
            shutil.rmtree(runtime_dir, ignore_errors=True)

        expected_output_dir = build_scoped_output_dir(context.processed_dir, ["lopes"])
        self.assertEqual(captured["sources"], ["lopes"])
        self.assertEqual(Path(captured["processed_dir"]), expected_output_dir)
        self.assertEqual(metrics["incoming_snapshot_count"], 1)
        self.assertEqual(metrics["selected_sources"], ["lopes"])
        self.assertTrue(all(artifact.metadata["sources"] == ["lopes"] for artifact in artifacts))
        self.assertEqual(errors, [])


class CliParserTests(unittest.TestCase):
    def test_list_stages_does_not_require_output_path(self):
        parser = build_parser()
        args = parser.parse_args(["list-stages"])

        self.assertEqual(args.command, "list-stages")

    def test_run_all_requires_output_path(self):
        parser = build_parser()

        with self.assertRaises(SystemExit):
            parser.parse_args(["run-all"])

    def test_run_stage_requires_output_path(self):
        parser = build_parser()

        with self.assertRaises(SystemExit):
            parser.parse_args(["run-stage", "collect_discovery"])

    def test_run_all_accepts_verbose(self):
        parser = build_parser()
        args = parser.parse_args(["run-all", "--output-path", "runtime", "--verbose"])

        self.assertTrue(args.verbose)
        self.assertEqual(args.output_path, "runtime")

    def test_run_all_accepts_force_discovery(self):
        parser = build_parser()
        args = parser.parse_args(["run-all", "--output-path", "runtime", "--force-discovery"])

        self.assertTrue(args.force_discovery)

    def test_run_stage_accepts_verbose(self):
        parser = build_parser()
        args = parser.parse_args(["run-stage", "collect_listings", "--output-path", "runtime", "--verbose"])

        self.assertTrue(args.verbose)
        self.assertEqual(args.output_path, "runtime")

    def test_run_stage_accepts_collect_discovery(self):
        parser = build_parser()
        args = parser.parse_args(["run-stage", "collect_discovery", "--output-path", "runtime"])

        self.assertEqual(args.stage_name, "collect_discovery")

    def test_run_stage_accepts_collect_listings(self):
        parser = build_parser()
        args = parser.parse_args(["run-stage", "collect_listings", "--output-path", "runtime"])

        self.assertEqual(args.stage_name, "collect_listings")

    def test_run_stage_accepts_single_source_filter(self):
        parser = build_parser()
        args = parser.parse_args(["run-stage", "collect_listings", "--output-path", "runtime", "--sources", "lopes"])

        self.assertEqual(args.sources, ["lopes"])

    def test_run_stage_accepts_multiple_source_filters(self):
        parser = build_parser()
        args = parser.parse_args(["run-stage", "build_daily_snapshot", "--output-path", "runtime", "--sources", "lopes", "olx"])

        self.assertEqual(args.sources, ["lopes", "olx"])


if __name__ == "__main__":
    unittest.main()
