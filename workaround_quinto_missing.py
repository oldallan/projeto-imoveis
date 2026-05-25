from __future__ import annotations

import argparse
import csv
import re
from pathlib import Path
from typing import Any

import pandas as pd

from scrapers.io_utils import save_parquet_records
from scrapers.quinto import collect_listings


DEFAULT_RUN_DATE = "24-04-2026"
PROPERTY_ID_RE = re.compile(r"/imovel/(\d+)")


def property_id_from_url(url: Any) -> str:
    match = PROPERTY_ID_RE.search(str(url or ""))
    return match.group(1) if match else ""


def row_property_id(row: dict[str, Any]) -> str:
    for key in ("property_id", "listing_id"):
        value = str(row.get(key) or "").strip()
        if value:
            return value
    return property_id_from_url(row.get("listing_url"))


def build_paths(project_root: Path, run_date: str) -> dict[str, Path]:
    quinto_dir = project_root / "raw" / run_date / "quinto"
    return {
        "discovery": quinto_dir / "quinto_discovery.csv",
        "current_listings": quinto_dir / "quinto_listings.csv",
        "missing_discovery": quinto_dir / "quinto_missing_discovery.csv",
        "missing_listings": quinto_dir / "quinto_missing_listings.csv",
        "missing_listings_parquet": quinto_dir / "quinto_missing_listings.parquet",
        "merged_listings": quinto_dir / "quinto_listings_recovered.csv",
        "merged_listings_parquet": quinto_dir / "quinto_listings_recovered.parquet",
        "resume_dir": project_root / "artifacts" / run_date / "collect_listings" / "quinto_missing_workaround",
    }


def build_missing_discovery(paths: dict[str, Path]) -> dict[str, int]:
    collected_ids: set[str] = set()
    current_rows = 0
    with paths["current_listings"].open("r", encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            current_rows += 1
            property_id = row_property_id(row)
            if property_id:
                collected_ids.add(property_id)

    discovery_rows = 0
    missing_rows = 0
    missing_property_ids: set[str] = set()
    paths["missing_discovery"].parent.mkdir(parents=True, exist_ok=True)
    with paths["discovery"].open("r", encoding="utf-8-sig", newline="") as source:
        reader = csv.DictReader(source)
        fieldnames = list(reader.fieldnames or [])
        with paths["missing_discovery"].open("w", encoding="utf-8-sig", newline="") as target:
            writer = csv.DictWriter(target, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            for row in reader:
                discovery_rows += 1
                property_id = row_property_id(row)
                if property_id and property_id in collected_ids:
                    continue
                writer.writerow(row)
                missing_rows += 1
                if property_id:
                    missing_property_ids.add(property_id)

    return {
        "current_listing_rows": current_rows,
        "collected_property_ids": len(collected_ids),
        "discovery_rows": discovery_rows,
        "missing_discovery_rows": missing_rows,
        "missing_property_ids": len(missing_property_ids),
    }


def collect_missing(paths: dict[str, Path], *, verbose: bool) -> dict[str, Any] | None:
    return collect_listings(
        input_path=str(paths["missing_discovery"]),
        listings_output_path=str(paths["missing_listings"]),
        listings_parquet_output_path=str(paths["missing_listings_parquet"]),
        resume_dir=str(paths["resume_dir"]),
        max_consecutive_failures=100,
        verbose=verbose,
        retry_times=2,
        autothrottle_start_delay=1.0,
        autothrottle_max_delay=8.0,
        autothrottle_target_concurrency=1.0,
        concurrent_requests=3,
        concurrent_requests_per_domain=3,
        download_delay=1.0,
        download_timeout=30,
    )


def merge_outputs(paths: dict[str, Path]) -> dict[str, int]:
    current = pd.read_csv(paths["current_listings"])
    missing = pd.read_csv(paths["missing_listings"])
    merged = pd.concat([current, missing], ignore_index=True, sort=False)

    if "property_id" in merged.columns:
        dedupe_key = merged["property_id"].fillna("").astype(str).str.strip()
        fallback_key = (
            merged["listing_url"].fillna("").astype(str).str.strip()
            if "listing_url" in merged.columns
            else pd.Series([""] * len(merged))
        )
        merged["_dedupe_key"] = dedupe_key.where(dedupe_key != "", fallback_key)
        merged = merged.drop_duplicates(subset=["_dedupe_key"], keep="first")
        merged = merged.drop(columns=["_dedupe_key"])

    paths["merged_listings"].parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(paths["merged_listings"], index=False, encoding="utf-8-sig")
    save_parquet_records(merged.to_dict(orient="records"), paths["merged_listings_parquet"])
    return {
        "current_rows": len(current),
        "missing_rows": len(missing),
        "merged_rows": len(merged),
    }


def print_paths(paths: dict[str, Path]) -> None:
    for name, path in paths.items():
        print(f"{name}={path}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Workaround para coletar listings faltantes do Quinto.")
    parser.add_argument("action", choices=["paths", "build-missing", "collect-missing", "merge"])
    parser.add_argument("--date", default=DEFAULT_RUN_DATE)
    parser.add_argument("--project-root", default=".")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    project_root = Path(args.project_root).resolve()
    paths = build_paths(project_root, args.date)

    if args.action == "paths":
        print_paths(paths)
        return 0
    if args.action == "build-missing":
        print(build_missing_discovery(paths))
        print(f"missing_discovery={paths['missing_discovery']}")
        return 0
    if args.action == "collect-missing":
        print(collect_missing(paths, verbose=args.verbose))
        return 0
    if args.action == "merge":
        print(merge_outputs(paths))
        print(f"merged_listings={paths['merged_listings']}")
        print(f"merged_listings_parquet={paths['merged_listings_parquet']}")
        return 0
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
