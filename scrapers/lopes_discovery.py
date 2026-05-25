from __future__ import annotations

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
    all_records: List[Dict[str, str | None]] = []
    for sitemap_url in sitemap_urls:
        if verbose:
            print(f"[INFO] lopes_discovery_sitemap url={sitemap_url}")
        sitemap_xml = _fetch_text(sitemap_url, session)
        all_records.extend(parse_listing_sitemap(sitemap_xml))
    return _deduplicate_discovery_records(all_records)

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
        )
        resolved_previous_output_path = str(previous_path) if previous_path else None

    previous_state = load_previous_lastmod_state(resolved_previous_output_path)
    delta_records, incremental_metrics = build_incremental_discovery_delta(records, previous_state)

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

