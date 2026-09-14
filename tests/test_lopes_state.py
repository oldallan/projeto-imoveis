from __future__ import annotations

import shutil
import sqlite3
import json
import csv
import unittest
from types import SimpleNamespace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from scrapers.lopes_discovery import bootstrap_legacy_state_if_available
from scrapers.lopes_listings import collect_listings_from_file
from scrapers.registry import ScraperDefinition
from scrapers.lopes_state import (
    bootstrap_inventory,
    claim_daily_batch,
    finalize_claim,
    queue_metrics,
    reconcile_inventory,
    get_metadata,
)
from workflow.runner import PipelineRunner
from workflow.paths import build_context
from stages.collect_listings import CollectListingsStage
from pipelines.historical_store import _apply_history_metadata


class DummyLogger:
    def info(self, *args, **kwargs):
        return None

    def exception(self, *args, **kwargs):
        return None


def record(identifier: str, lastmod: str = "2026-01-01") -> dict[str, str]:
    return {
        "listing_url": f"https://www.lopes.com.br/imovel/{identifier}/venda-apartamento",
        "listing_id": identifier,
        "business_type": "sale",
        "lastmod": lastmod,
    }


class LopesStateTests(unittest.TestCase):
    def setUp(self):
        self.root = Path("tests_runtime_lopes_state")
        shutil.rmtree(self.root, ignore_errors=True)
        self.db_path = self.root / "state" / "lopes_state.sqlite3"

    def tearDown(self):
        shutil.rmtree(self.root, ignore_errors=True)

    def test_reconcile_classifies_new_refresh_unchanged_and_removed(self):
        previous = [record("A"), record("B"), record("C")]
        current = [record("A"), record("B", "2026-02-01"), record("D")]

        metrics = bootstrap_inventory(
            self.db_path,
            previous_records=previous,
            current_records=current,
            discovered_at="2026-02-01T00:00:00+00:00",
        )

        self.assertEqual(metrics["new_rows"], 1)
        self.assertEqual(metrics["refresh_rows"], 1)
        self.assertEqual(metrics["unchanged_rows"], 1)
        self.assertEqual(metrics["removed_rows"], 1)
        claimed = claim_daily_batch(self.db_path, claim_token="run-1", daily_limit=10, new_quota=8, refresh_quota=2)
        self.assertEqual({item["listing_id"] for item in claimed}, {"B", "D"})
        self.assertEqual(
            {item["listing_id"]: item["collection_priority"] for item in claimed},
            {"B": "refresh", "D": "new"},
        )
        connection = sqlite3.connect(self.db_path)
        try:
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM inventory WHERE listing_url LIKE '%/C/%'").fetchone()[0], 0)
        finally:
            connection.close()

    def test_same_generation_is_idempotent(self):
        records = [record("A"), record("B")]
        first_delta, _ = reconcile_inventory(self.db_path, records, discovered_at="2026-01-01T00:00:00+00:00")
        second_delta, metrics = reconcile_inventory(self.db_path, records, discovered_at="2026-01-02T00:00:00+00:00")

        self.assertEqual(len(first_delta), 2)
        self.assertEqual(len(second_delta), 0)
        self.assertTrue(metrics["idempotent_generation"])
        self.assertEqual(queue_metrics(self.db_path)["backlog_remaining"], 2)

    def test_claim_applies_quotas_and_reallocates_unused_capacity(self):
        records = [record(f"N{index}") for index in range(12)]
        reconcile_inventory(self.db_path, records, discovered_at="2026-01-01T00:00:00+00:00")
        claimed = claim_daily_batch(
            self.db_path,
            claim_token="run-1",
            daily_limit=10,
            new_quota=8,
            refresh_quota=2,
        )
        second_claim = claim_daily_batch(
            self.db_path,
            claim_token="run-2",
            daily_limit=10,
            new_quota=8,
            refresh_quota=2,
        )

        self.assertEqual(len(claimed), 10)
        self.assertEqual(len(second_claim), 2)
        self.assertTrue({item["listing_url"] for item in claimed}.isdisjoint({item["listing_url"] for item in second_claim}))

    def test_claim_reserves_eight_new_and_two_refresh_when_both_exist(self):
        previous = [record(f"R{index}") for index in range(4)]
        current = [record(f"R{index}", "2026-02-01") for index in range(4)]
        current += [record(f"N{index}", "2026-02-01") for index in range(10)]
        bootstrap_inventory(
            self.db_path,
            previous_records=previous,
            current_records=current,
            discovered_at="2026-02-01T00:00:00+00:00",
        )

        claimed = claim_daily_batch(self.db_path, claim_token="quota", daily_limit=10, new_quota=8, refresh_quota=2)
        priorities = [item["collection_priority"] for item in claimed]

        self.assertEqual(priorities.count("new"), 8)
        self.assertEqual(priorities.count("refresh"), 2)

    def test_claim_lease_and_retry_terminalization(self):
        start = datetime(2026, 1, 1, tzinfo=timezone.utc)
        reconcile_inventory(self.db_path, [record("A")], discovered_at=start.isoformat())
        first = claim_daily_batch(self.db_path, claim_token="old", daily_limit=1, now=start)
        self.assertEqual(len(first), 1)
        self.assertEqual(claim_daily_batch(self.db_path, claim_token="new", daily_limit=1, now=start + timedelta(hours=11)), [])
        reclaimed = claim_daily_batch(self.db_path, claim_token="new", daily_limit=1, now=start + timedelta(hours=13))
        self.assertEqual(len(reclaimed), 1)

        attempt_time = start + timedelta(hours=13)
        for attempt in range(5):
            metrics = finalize_claim(
                self.db_path,
                claim_token=f"attempt-{attempt}" if attempt else "new",
                successful_urls=set(),
                terminal_urls=set(),
                now=attempt_time,
            )
            if attempt < 4:
                attempt_time += timedelta(days=(1, 2, 4, 7)[attempt])
                claim = claim_daily_batch(
                    self.db_path,
                    claim_token=f"attempt-{attempt + 1}",
                    daily_limit=1,
                    now=attempt_time,
                )
                self.assertEqual(len(claim), 1)

        self.assertEqual(metrics["failed_terminal"], 1)
        self.assertEqual(metrics["backlog_remaining"], 0)

    def test_new_generation_drops_obsolete_pending_work(self):
        reconcile_inventory(self.db_path, [record("A"), record("B")], discovered_at="2026-01-01T00:00:00+00:00")
        delta, metrics = reconcile_inventory(
            self.db_path,
            [record("B", "2026-03-01"), record("C", "2026-03-01")],
            discovered_at="2026-03-01T00:00:00+00:00",
        )

        self.assertEqual({item["listing_id"] for item in delta}, {"B", "C"})
        self.assertEqual(metrics["removed_rows"], 1)
        claimed = claim_daily_batch(self.db_path, claim_token="new-generation", daily_limit=10)
        self.assertEqual({item["listing_id"] for item in claimed}, {"B", "C"})

        finalize_claim(
            self.db_path,
            claim_token="new-generation",
            successful_urls={item["listing_url"] for item in claimed},
            terminal_urls=set(),
        )
        reappeared, _ = reconcile_inventory(
            self.db_path,
            [record("A", "2026-04-01"), record("B", "2026-03-01"), record("C", "2026-03-01")],
            discovered_at="2026-04-01T00:00:00+00:00",
        )
        self.assertEqual(len(reappeared), 1)
        self.assertEqual(reappeared[0]["listing_id"], "A")
        self.assertEqual(reappeared[0]["collection_priority"], "new")

    def test_persistent_collection_limits_daily_work_and_publishes_successes(self):
        reconcile_inventory(
            self.db_path,
            [record("A"), record("B"), record("C")],
            discovered_at="2026-01-01T00:00:00+00:00",
        )
        output = self.root / "raw" / "02-01-2026" / "lopes" / "lopes_listings.csv"

        def fake_collection(*, records, **kwargs):
            return [
                {
                    "property_id": item["listing_id"],
                    "business_type": item["business_type"],
                    "listing_url": item["listing_url"],
                    "queue_listing_url": item["listing_url"],
                    "discovered_at": item["discovered_at"],
                    "sitemap_lastmod": item["sitemap_lastmod"],
                    "sitemap_generation_id": item["sitemap_generation_id"],
                    "collection_priority": item["collection_priority"],
                }
                for item in records
            ], {"stop_reason": "completed"}

        with patch("scrapers.lopes_listings.run_scrapy_collection", side_effect=fake_collection):
            result = collect_listings_from_file(
                input_path=str(self.root / "unused.csv"),
                listings_output_path=str(output),
                listings_parquet_output_path=str(output.with_suffix(".parquet")),
                max_consecutive_failures=100,
                label="lopes",
                resume_dir=str(self.root / "resume"),
                state_db_path=str(self.db_path),
                daily_limit=2,
                new_quota=2,
                refresh_quota=0,
            )

        self.assertEqual(result["selected_today"], 2)
        self.assertEqual(result["output_rows"], 2)
        self.assertEqual(result["backlog_remaining"], 1)
        frame = pd.read_csv(output)
        self.assertEqual(len(frame), 2)
        self.assertTrue(frame["sitemap_generation_id"].notna().all())

    def test_bootstrap_imports_legacy_successes_once(self):
        output_root = self.root / "output"
        current_path = output_root / "raw" / "10-09-2026" / "lopes" / "lopes_discovery.csv"
        previous_path = output_root / "raw" / "20-04-2026" / "lopes" / "lopes_discovery.csv"
        current_path.parent.mkdir(parents=True, exist_ok=True)
        previous_path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame([record("A", "2026-09-10"), record("B", "2026-09-10")]).to_csv(current_path, index=False)
        pd.DataFrame([record("A", "2026-01-02"), record("C", "2026-01-02")]).to_csv(previous_path, index=False)
        partial_path = output_root / "artifacts" / "10-09-2026" / "collect_listings" / "lopes" / "records.partial.jsonl"
        partial_path.parent.mkdir(parents=True, exist_ok=True)
        partial_path.write_text(
            json.dumps({"property_id": "A", "business_type": "sale", "listing_url": record("A")["listing_url"]}) + "\n",
            encoding="utf-8",
        )
        manifest_path = output_root / "artifacts" / "10-09-2026" / "collect_discovery" / "manifest.json"
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        manifest_path.write_text(
            json.dumps(
                {
                    "status": "success",
                    "artifacts": [{"path": str(current_path), "format": "csv", "rows": 2, "metadata": {"source": "lopes"}}],
                    "metrics": {"source_results": [{"source": "lopes", "runner_metrics": {"previous_output_path": str(previous_path)}}]},
                }
            ),
            encoding="utf-8",
        )
        db_path = output_root / "state" / "lopes" / "lopes_state.sqlite3"

        metrics = bootstrap_legacy_state_if_available(output_root, db_path)
        second = bootstrap_legacy_state_if_available(output_root, db_path)

        self.assertTrue(metrics["bootstrapped"])
        self.assertEqual(metrics["new_rows"], 1)
        self.assertEqual(metrics["refresh_rows"], 1)
        self.assertEqual(metrics["removed_rows"], 1)
        self.assertEqual(metrics["backlog_remaining"], 1)
        self.assertFalse(second["bootstrapped"])
        self.assertEqual(get_metadata(db_path, "bootstrap_results_published"), "0")
        self.assertTrue(Path(str(get_metadata(db_path, "bootstrap_results_path"))).exists())

    def test_runner_continues_after_zero_delta_when_lopes_has_backlog(self):
        output_root = self.root / "output"
        db_path = output_root / "state" / "lopes" / "lopes_state.sqlite3"
        reconcile_inventory(db_path, [record("A")], discovered_at="2026-01-01T00:00:00+00:00")
        calls: list[str] = []

        def fake_stage(stage_name):
            def execute(*args, **kwargs):
                calls.append(stage_name)
                if stage_name == "collect_discovery":
                    return SimpleNamespace(
                        status="success", blocked=False, output_manifest=str(self.root / "discovery.json"),
                        metrics={"new_links_total": 0}, artifacts=[],
                        to_dict=lambda: {"stage_name": stage_name, "status": "success", "metrics": {"new_links_total": 0}},
                    )
                return SimpleNamespace(
                    status="success", blocked=False, output_manifest=str(self.root / "listings.json"),
                    metrics={}, artifacts=[],
                    to_dict=lambda: {"stage_name": stage_name, "status": "success", "metrics": {}, "artifacts": []},
                )
            return SimpleNamespace(execute=execute)

        with patch("workflow.runner.get_stage", side_effect=fake_stage):
            payload = PipelineRunner(project_root=self.root).run_all(
                run_date="02-01-2026",
                output_root=output_root,
            )

        self.assertEqual(calls, ["collect_discovery", "collect_listings"])
        self.assertEqual(payload["stop_reason"], "no_listing_outputs")

    def test_collection_stage_isolates_one_source_failure(self):
        context = build_context("02-01-2026", self.root)
        artifacts = []
        for source in ("lopes", "olx"):
            path = context.raw_dir / source / f"{source}_discovery.csv"
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("listing_url,business_type\nhttps://example.com/1,sale\n", encoding="utf-8")
            artifacts.append(
                {"path": str(path), "format": "csv", "rows": 1, "metadata": {"source": source, "artifact_role": "discovery"}}
            )

        def olx_runner(*, listings_output_path, **kwargs):
            output = Path(listings_output_path)
            output.parent.mkdir(parents=True, exist_ok=True)
            with output.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(handle, fieldnames=["property_id", "listing_url", "business_type"])
                writer.writeheader()
                writer.writerow({"property_id": "1", "listing_url": "https://example.com/1", "business_type": "sale"})
            return {"input_rows": 1, "output_rows": 1}

        scrapers = [
            ScraperDefinition("lopes", "lopes", "lopes_discovery.csv", "lopes_listings.csv", lambda **_: None, lambda **_: (_ for _ in ()).throw(RuntimeError("offline"))),
            ScraperDefinition("olx", "olx", "olx_discovery.csv", "olx_listings.csv", lambda **_: None, olx_runner),
        ]
        stage = CollectListingsStage()
        with patch("stages.collect_listings.get_scraper_definitions", return_value=scrapers):
            outputs, metrics, errors = stage.run(
                context,
                {"status": "success", "artifacts": artifacts},
                DummyLogger(),
            )

        self.assertEqual(errors, [])
        self.assertEqual(metrics["successful_scrapers"], 1)
        self.assertEqual(metrics["failed_scrapers"], 1)
        self.assertEqual([artifact.metadata["source"] for artifact in outputs], ["olx"])

    def test_history_uses_discovery_and_scrape_timestamps(self):
        existing = pd.DataFrame(columns=["source", "business_type", "property_id", "first_seen_at", "created_at"])
        incoming = pd.DataFrame(
            [{
                "source": "lopes", "business_type": "sale", "property_id": "A",
                "discovered_at": "2026-01-01T00:00:00+00:00",
                "scraped_at": "2026-01-05T12:00:00+00:00",
            }]
        )

        enriched, inserted, updated = _apply_history_metadata(existing, incoming)

        self.assertEqual(inserted, 1)
        self.assertEqual(updated, 0)
        self.assertEqual(enriched.iloc[0]["first_seen_at"], "2026-01-01T00:00:00+00:00")
        self.assertEqual(enriched.iloc[0]["last_seen_at"], "2026-01-05T12:00:00+00:00")


if __name__ == "__main__":
    unittest.main()
