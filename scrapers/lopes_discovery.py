from __future__ import annotations

from datetime import datetime, timezone

from scrapers.io_utils import load_csv_records, save_parquet_records
from scrapers.listings_resume import load_jsonl_records
from scrapers.lopes_state import (
    bootstrap_inventory,
    generation_fingerprint,
    get_metadata,
    reconcile_inventory,
    set_metadata,
    state_db_path,
)
from scrapers.lopes_shared import *  # noqa: F403

def _iter_sitemap_entries(xml_text: str, tag_name: str) -> List[dict[str, str | None]]:
    namespace = "{http://www.sitemaps.org/schemas/sitemap/0.9}"
    entries: List[dict[str, str | None]] = []
    stream = StringIO(xml_text)
    current: dict[str, str | None] | None = None
    for event, elem in ET.iterparse(stream, events=("start", "end")):
        if event == "start" and elem.tag == f"{namespace}{tag_name}":
            current = {"loc": None, "lastmod": None}
            continue
        if event == "end" and current is not None:
            if elem.tag == f"{namespace}loc":
                current["loc"] = (elem.text or "").strip() or None
            elif elem.tag == f"{namespace}lastmod":
                current["lastmod"] = (elem.text or "").strip() or None
            elif elem.tag == f"{namespace}{tag_name}":
                if current.get("loc"):
                    entries.append(current)
                current = None
                elem.clear()
    return entries


def parse_sitemap_index(xml_text: str) -> List[str]:
    return [
        str(entry["loc"])
        for entry in _iter_sitemap_entries(xml_text, "sitemap")
        if entry.get("loc") and re.search(r"/sitemap-imoveis(?:-\d+)?\.xml$", str(entry["loc"]))
    ]


def _derive_business_type_from_listing_url(listing_url: str) -> str | None:
    match = re.search(r"/imovel/[^/]+/([^/]+)", listing_url)
    if not match:
        return None
    slug = match.group(1).lower()
    if slug.startswith("venda-"):
        return "sale"
    if slug.startswith("aluguel-"):
        return "rent"
    return None


def _derive_listing_id_from_listing_url(listing_url: str) -> str | None:
    match = re.search(r"/imovel/([^/]+)/", listing_url)
    if not match:
        return None
    return match.group(1)


def parse_listing_sitemap(xml_text: str) -> List[Dict[str, str | None]]:
    records: List[Dict[str, str | None]] = []
    for entry in _iter_sitemap_entries(xml_text, "url"):
        listing_url = entry.get("loc")
        if not listing_url:
            continue
        if "/imovel/" not in listing_url:
            continue
        records.append(
            {
                "listing_url": listing_url,
                "lastmod": entry.get("lastmod"),
                "listing_id": _derive_listing_id_from_listing_url(listing_url),
                "business_type": _derive_business_type_from_listing_url(listing_url),
            }
        )
    return records


def _fetch_text(url: str, session: requests.Session) -> str:
    response = session.get(url, impersonate="chrome110", timeout=60)
    response.raise_for_status()
    return response.text


def _deduplicate_discovery_records(records: List[Dict[str, str | None]]) -> List[Dict[str, str | None]]:
    deduped: dict[str, Dict[str, str | None]] = {}
    for record in records:
        listing_url = record.get("listing_url")
        if not listing_url:
            continue
        previous = deduped.get(listing_url)
        if previous is None:
            deduped[listing_url] = dict(record)
            continue
        previous_lastmod = previous.get("lastmod")
        current_lastmod = record.get("lastmod")
        if current_lastmod and (not previous_lastmod or current_lastmod > previous_lastmod):
            deduped[listing_url] = dict(record)
    return list(deduped.values())


def collect_discovery_records(verbose: bool = False) -> List[Dict[str, str | None]]:
    session = requests.Session()
    sitemap_index = _fetch_text(SITEMAP_INDEX_URL, session)
    sitemap_urls = parse_sitemap_index(sitemap_index)
    if not sitemap_urls:
        raise RuntimeError("o indice Lopes nao retornou sitemaps de imoveis validos")
    all_records: List[Dict[str, str | None]] = []
    for sitemap_url in sitemap_urls:
        if verbose:
            print(f"[INFO] lopes_discovery_sitemap url={sitemap_url}")
        sitemap_xml = _fetch_text(sitemap_url, session)
        all_records.extend(parse_listing_sitemap(sitemap_xml))
    return _deduplicate_discovery_records(all_records)


def _date_from_discovery_path(path: Path) -> str:
    return path.parent.parent.name


def _discovered_at_from_run_date(run_date: str | None) -> str:
    if not run_date:
        return datetime.now(timezone.utc).isoformat()
    return datetime.strptime(run_date, "%d-%m-%Y").replace(tzinfo=timezone.utc).isoformat()


def _latest_legacy_manifest(output_root: Path) -> tuple[Path, dict[str, Any], dict[str, Any]] | None:
    candidates: list[tuple[datetime, Path, dict[str, Any], dict[str, Any]]] = []
    for manifest_path in (output_root / "artifacts").glob("*/collect_discovery/manifest.json"):
        try:
            run_date = datetime.strptime(manifest_path.parent.parent.name, "%d-%m-%Y")
            payload = json.loads(manifest_path.read_text(encoding="utf-8-sig"))
        except (OSError, ValueError, json.JSONDecodeError):
            continue
        if payload.get("status") != "success":
            continue
        artifact = next(
            (
                item for item in payload.get("artifacts", [])
                if item.get("format") == "csv"
                and item.get("metadata", {}).get("source") == "lopes"
                and int(item.get("rows") or 0) > 0
                and Path(str(item.get("path") or "")).exists()
            ),
            None,
        )
        source_result = next(
            (item for item in payload.get("metrics", {}).get("source_results", []) if item.get("source") == "lopes"),
            None,
        )
        if artifact and source_result:
            candidates.append((run_date, manifest_path, artifact, source_result))
    if not candidates:
        return None
    _, manifest_path, artifact, source_result = max(candidates, key=lambda item: item[0])
    return manifest_path, artifact, source_result


def bootstrap_legacy_state_if_available(output_root: Path, db_path: Path) -> dict[str, Any]:
    if (
        get_metadata(db_path, "legacy_bootstrap_completed") == "1"
        or get_metadata(db_path, "active_generation_id") is not None
    ):
        return {"bootstrapped": False}
    legacy = _latest_legacy_manifest(output_root)
    if legacy is None:
        return {"bootstrapped": False}
    _, artifact, source_result = legacy
    current_path = Path(str(artifact["path"]))
    previous_path_text = str((source_result.get("runner_metrics") or {}).get("previous_output_path") or "")
    previous_path = Path(previous_path_text) if previous_path_text else None
    if previous_path is None or not previous_path.exists():
        return {"bootstrapped": False}

    current_records = load_csv_records(current_path)
    previous_records = load_csv_records(previous_path)
    run_date = _date_from_discovery_path(current_path)
    discovered_at = _discovered_at_from_run_date(run_date)
    partial_path = output_root / "artifacts" / run_date / "collect_listings" / "lopes" / "records.partial.jsonl"
    completed_records = load_jsonl_records(partial_path)
    completed_urls = {
        str(record.get("listing_url") or "").strip()
        for record in completed_records
        if str(record.get("listing_url") or "").strip()
    }
    metrics = bootstrap_inventory(
        db_path,
        previous_records=previous_records,
        current_records=current_records,
        discovered_at=discovered_at,
        completed_urls=completed_urls,
    )
    if completed_records:
        previous_urls = {str(record.get("listing_url") or "").strip() for record in previous_records}
        generation_id = generation_fingerprint(current_records)
        current_by_url = {str(record.get("listing_url") or "").strip(): record for record in current_records}
        enriched: list[dict[str, Any]] = []
        for record in completed_records:
            listing_url = str(record.get("listing_url") or "").strip()
            discovery = current_by_url.get(listing_url, {})
            enriched.append(
                {
                    **record,
                    "discovered_at": discovered_at,
                    "sitemap_lastmod": discovery.get("lastmod"),
                    "sitemap_generation_id": generation_id,
                    "collection_priority": "refresh" if listing_url in previous_urls else "new",
                }
            )
        bootstrap_path = db_path.parent / "bootstrap_results.parquet"
        save_parquet_records(enriched, bootstrap_path)
        set_metadata(db_path, "bootstrap_results_path", str(bootstrap_path))
        set_metadata(db_path, "bootstrap_results_published", "0")
    return metrics

def collect_discovery_to_file(
    *,
    output_path: str,
    parquet_output_path: str,
    previous_output_path: str | None = None,
    verbose: bool = False,
) -> dict[str, Any] | None:
    records = collect_discovery_records(verbose=verbose)
    if not records:
        print("[WARN] Lopes discovery sem dados coletados")
        return None

    run_date = infer_run_date_from_output_path(output_path)
    resolved_previous_output_path = previous_output_path
    if not resolved_previous_output_path and run_date:
        previous_path = find_previous_output(
            run_date=run_date,
            source="lopes",
            filename=DISCOVERY_FILENAME,
            project_root=infer_output_root_from_output_path(output_path),
        )
        resolved_previous_output_path = str(previous_path) if previous_path else None

    output_root = infer_output_root_from_output_path(output_path)
    if output_root is None:
        previous_state = load_previous_lastmod_state(resolved_previous_output_path)
        delta_records, incremental_metrics = build_incremental_discovery_delta(records, previous_state)
    else:
        db_path = state_db_path(output_root)
        state_existed = db_path.exists()
        bootstrap_metrics = bootstrap_legacy_state_if_available(output_root, db_path)
        if (
            not state_existed
            and not bootstrap_metrics.get("bootstrapped")
            and resolved_previous_output_path
            and Path(resolved_previous_output_path).exists()
        ):
            bootstrap_metrics = bootstrap_inventory(
                db_path,
                previous_records=load_csv_records(resolved_previous_output_path),
                current_records=records,
                discovered_at=_discovered_at_from_run_date(run_date),
            )
        delta_records, incremental_metrics = reconcile_inventory(
            db_path,
            records,
            discovered_at=_discovered_at_from_run_date(run_date),
        )
        incremental_metrics["state_db_path"] = str(db_path)
        incremental_metrics["bootstrap"] = bootstrap_metrics
        incremental_metrics["previous_output_path"] = resolved_previous_output_path

    save_csv(delta_records, filename=output_path, fieldnames=DISCOVERY_FIELDNAMES)
    save_parquet(delta_records, filename=parquet_output_path)

    if verbose:
        print(
            "[INFO] lopes_discovery_metrics="
            + json.dumps(
                incremental_metrics,
                ensure_ascii=False,
            )
        )
    return {
        "output_path": output_path,
        "metrics": incremental_metrics,
    }

