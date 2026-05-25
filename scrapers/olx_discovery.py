from __future__ import annotations

import json
import time

import scrapy

from scrapy.http import Request, Response

from scrapers.discovery_incremental import _has_non_empty_csv_rows, infer_output_root_from_output_path
from scrapers.logging_utils import log_info
from scrapers.olx_shared import *  # noqa: F403
from scrapers.scrapy_runner import run_spider
from scrapers.scrapy_support import build_scrapy_settings as build_base_scrapy_settings

class OlxDiscoverySpider(scrapy.Spider):
    name = "olx_discovery"
    allowed_domains = ["olx.com.br", "www.olx.com.br", "sp.olx.com.br"]
    empty_page_retry_limit = 1
    empty_page_advance_delay_seconds = 120
    min_pages_before_empty_stop = 90
    stale_overlap_page_limit = 3

    def __init__(
        self,
        *,
        run_date: str,
        max_pages: int,
        previous_state: PreviousRunState,
        collector: dict[str, Any],
        verbose: bool = False,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.run_date = run_date
        self.max_pages = max_pages
        self.previous_state = previous_state
        self.collector = collector
        self.verbose = verbose
        self.empty_page_retry_limit = max(0, int(kwargs.pop("empty_page_retry_limit", self.empty_page_retry_limit)))
        self.empty_page_advance_delay_seconds = max(
            0,
            int(kwargs.pop("empty_page_advance_delay_seconds", self.empty_page_advance_delay_seconds)),
        )
        self.min_pages_before_empty_stop = max(
            1,
            int(kwargs.pop("min_pages_before_empty_stop", self.min_pages_before_empty_stop)),
        )
        self.stale_overlap_page_limit = max(1, int(kwargs.pop("stale_overlap_page_limit", self.stale_overlap_page_limit)))
        self.flow_configs = [
            FlowConfig(name="sale", base_url=SALE_BASE_URL),
            FlowConfig(name="rent", base_url=RENT_BASE_URL),
        ]
        self.metrics_by_flow = {
            flow_config.name: FlowMetrics(flow=flow_config.name)
            for flow_config in self.flow_configs
        }
        self.seen_urls_by_flow = {flow_config.name: set() for flow_config in self.flow_configs}
        self.stale_overlap_pages_by_flow = {flow_config.name: 0 for flow_config in self.flow_configs}
        self.started_flows: set[str] = set()

    async def start(self):
        first_flow = self._flow_config_by_index(0)
        if first_flow is not None:
            first_request = self._start_flow(flow_config=first_flow)
            if first_request is not None:
                yield first_request

    def _build_request(
        self,
        *,
        flow_config: FlowConfig,
        page: int,
        empty_page_retry_count: int = 0,
    ) -> Request:
        url = build_listing_page_url(flow_config.base_url, page)
        if self.verbose:
            log_info("olx_discovery_page", flow=flow_config.name, page=page, url=url)
        return Request(
            url=url,
            callback=self.parse_listing_response,
            errback=self.handle_request_error,
            headers=HEADERS,
            dont_filter=empty_page_retry_count > 0,
            meta={
                "flow_name": flow_config.name,
                "base_url": flow_config.base_url,
                "page": page,
                "empty_page_retry_count": empty_page_retry_count,
            },
        )

    def _log_page_result(
        self,
        *,
        flow_name: str,
        page: int,
        items_seen: int,
        items_kept: int,
        duplicates_in_run: int,
        same_price_ignored: int,
        invalid_records: int,
    ) -> None:
        if not self.verbose:
            return
        log_info(
            "olx_discovery_page_result",
            flow=flow_name,
            page=page,
            items_seen=items_seen,
            items_kept=items_kept,
            duplicates_in_run=duplicates_in_run,
            same_price_ignored=same_price_ignored,
            invalid_records=invalid_records,
        )

    def _log_stop(self, *, flow_name: str, page: int, stop_reason: str) -> None:
        if not self.verbose:
            return
        log_info("olx_discovery_stop", flow=flow_name, page=page, stop_reason=stop_reason)

    def _log_retry(self, *, flow_name: str, page: int, retry_count: int, retry_reason: str) -> None:
        if not self.verbose:
            return
        log_info(
            "olx_discovery_retry",
            flow=flow_name,
            page=page,
            retry_count=retry_count,
            retry_reason=retry_reason,
        )

    def _flow_config_by_index(self, index: int) -> FlowConfig | None:
        if 0 <= index < len(self.flow_configs):
            return self.flow_configs[index]
        return None

    def _next_flow_config(self, flow_name: str) -> FlowConfig | None:
        for index, flow_config in enumerate(self.flow_configs):
            if flow_config.name == flow_name:
                return self._flow_config_by_index(index + 1)
        return None

    def _start_flow(self, *, flow_config: FlowConfig) -> Request | None:
        if flow_config.name in self.started_flows:
            return None
        self.started_flows.add(flow_config.name)
        return self._build_request(flow_config=flow_config, page=1)

    def _schedule_next_flow(self, *, current_flow_name: str):
        next_flow = self._next_flow_config(current_flow_name)
        if next_flow is None:
            return
        next_request = self._start_flow(flow_config=next_flow)
        if next_request is not None:
            yield next_request

    def parse_listing_response(self, response: Response):
        flow_name = str(response.meta["flow_name"])
        base_url = str(response.meta["base_url"])
        page = int(response.meta["page"])
        empty_page_retry_count = int(response.meta.get("empty_page_retry_count", 0) or 0)
        metrics = self.metrics_by_flow[flow_name]

        parsed_records = parse_listing_page(response.text, self.run_date)
        metrics.pages_scanned += 1
        metrics.items_seen += len(parsed_records)

        if not parsed_records:
            self._log_page_result(
                flow_name=flow_name,
                page=page,
                items_seen=0,
                items_kept=0,
                duplicates_in_run=0,
                same_price_ignored=0,
                invalid_records=0,
            )
            if empty_page_retry_count < self.empty_page_retry_limit:
                next_retry_count = empty_page_retry_count + 1
                self._log_retry(
                    flow_name=flow_name,
                    page=page,
                    retry_count=next_retry_count,
                    retry_reason="empty_page",
                )
                yield self._build_request(
                    flow_config=FlowConfig(name=flow_name, base_url=base_url),
                    page=page,
                    empty_page_retry_count=next_retry_count,
                )
                return

            if page < self.min_pages_before_empty_stop and page < self.max_pages:
                if self.empty_page_advance_delay_seconds > 0:
                    time.sleep(self.empty_page_advance_delay_seconds)
                yield self._build_request(
                    flow_config=FlowConfig(name=flow_name, base_url=base_url),
                    page=page + 1,
                )
                return

            if page >= self.max_pages:
                metrics.stop_reason = "max_pages_reached"
                self._log_stop(flow_name=flow_name, page=page, stop_reason=metrics.stop_reason)
                yield from self._schedule_next_flow(current_flow_name=flow_name)
                return

            metrics.stop_reason = "empty_page"
            self._log_stop(flow_name=flow_name, page=page, stop_reason=metrics.stop_reason)
            yield from self._schedule_next_flow(current_flow_name=flow_name)
            return

        page_result = process_page_records(
            flow=flow_name,
            parsed_records=parsed_records,
            previous_state=self.previous_state,
            seen_urls=self.seen_urls_by_flow[flow_name],
        )
        metrics.duplicates_in_run += page_result.duplicates_in_run
        metrics.same_price_ignored += page_result.same_price_ignored
        metrics.invalid_records += page_result.invalid_records
        metrics.items_kept += len(page_result.kept_records)
        self.collector["records"].extend(page_result.kept_records)
        self.collector["invalid_records"].extend(page_result.invalid_samples)
        self._log_page_result(
            flow_name=flow_name,
            page=page,
            items_seen=len(parsed_records),
            items_kept=len(page_result.kept_records),
            duplicates_in_run=page_result.duplicates_in_run,
            same_price_ignored=page_result.same_price_ignored,
            invalid_records=page_result.invalid_records,
        )

        if page_result.page_fully_in_overlap and page_result.useful_overlap_records == 0:
            self.stale_overlap_pages_by_flow[flow_name] += 1
        else:
            self.stale_overlap_pages_by_flow[flow_name] = 0

        if self.stale_overlap_pages_by_flow[flow_name] >= self.stale_overlap_page_limit:
            metrics.stopped_by_old_date = True
            metrics.stop_reason = "older_than_previous_window"
            self._log_stop(flow_name=flow_name, page=page, stop_reason=metrics.stop_reason)
            yield from self._schedule_next_flow(current_flow_name=flow_name)
            return

        if page >= self.max_pages:
            metrics.stop_reason = "max_pages_reached"
            self._log_stop(flow_name=flow_name, page=page, stop_reason=metrics.stop_reason)
            yield from self._schedule_next_flow(current_flow_name=flow_name)
            return

        yield self._build_request(
            flow_config=FlowConfig(name=flow_name, base_url=base_url),
            page=page + 1,
        )

    def handle_request_error(self, failure: Any):
        request = failure.request
        flow_name = str(request.meta["flow_name"])
        page = int(request.meta["page"])
        metrics = self.metrics_by_flow[flow_name]
        status = getattr(getattr(failure.value, "response", None), "status", None)
        metrics.stop_reason = f"request_failed:{status or failure.type.__name__}"
        self._log_stop(flow_name=flow_name, page=page, stop_reason=metrics.stop_reason)
        yield from self._schedule_next_flow(current_flow_name=flow_name)

    def closed(self, reason: str) -> None:
        for metrics in self.metrics_by_flow.values():
            if metrics.stop_reason is None:
                metrics.stop_reason = "crawler_closed"
            self.collector["metrics"].append(metrics.to_dict())


def default_run_date() -> str:
    return datetime.now(BRAZIL_TZ).strftime(DATE_FORMAT)


def default_output_path(run_date: str | None = None) -> str:
    return build_dated_output_path("olx", DEFAULT_DISCOVERY_FILENAME, run_date=run_date or default_run_date())


def default_invalid_output_path(run_date: str | None = None) -> str:
    return build_dated_output_path(
        "olx",
        DEFAULT_INVALID_DISCOVERY_FILENAME,
        run_date=run_date or default_run_date(),
    )


def _derive_invalid_output_path_from_output_path(output_path: str | Path) -> str:
    output = Path(output_path)
    return str(output.with_name(DEFAULT_INVALID_DISCOVERY_FILENAME))


def _infer_run_date_from_output_path(output_path: str | Path) -> str | None:
    output = Path(output_path)
    try:
        return output.parent.parent.name
    except IndexError:
        return None


def build_listing_page_url(base_url: str, page: int) -> str:
    separator = "&" if "?" in base_url else "?"
    if "sf=" in base_url:
        return f"{base_url}{separator}o={page}"
    return f"{base_url}{separator}sf=1&o={page}"


def _parse_run_date(run_date: str | date | datetime) -> date:
    if isinstance(run_date, datetime):
        return run_date.astimezone(BRAZIL_TZ).date()
    if isinstance(run_date, date):
        return run_date
    return datetime.strptime(run_date, DATE_FORMAT).date()


def _normalize_text_token(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    return normalized.encode("ascii", "ignore").decode("ascii").lower().strip()


def parse_card_date(text: str | None, run_date: str | date | datetime) -> str | None:
    if not text:
        return None

    normalized = re.sub(r"\s+", " ", html_unescape(text)).strip()
    reference_date = _parse_run_date(run_date)

    today_match = re.fullmatch(r"Hoje,\s*(\d{1,2}):(\d{2})", normalized, flags=re.IGNORECASE)
    if today_match:
        hour, minute = int(today_match.group(1)), int(today_match.group(2))
        return datetime.combine(reference_date, dt_time(hour=hour, minute=minute), tzinfo=BRAZIL_TZ).isoformat()

    yesterday_match = re.fullmatch(r"Ontem,\s*(\d{1,2}):(\d{2})", normalized, flags=re.IGNORECASE)
    if yesterday_match:
        hour, minute = int(yesterday_match.group(1)), int(yesterday_match.group(2))
        target_date = reference_date - timedelta(days=1)
        return datetime.combine(target_date, dt_time(hour=hour, minute=minute), tzinfo=BRAZIL_TZ).isoformat()

    month_match = re.fullmatch(
        r"(\d{1,2})\s+de\s+([A-Za-z・・・・ｧ・πｵ・｣・・・・｡・δ・・｣ｰ・・▼・｢・・・・ｩ・・曝・ｪ・・ぎ・ｭ・・ｦｿ・ｳ・・ｰｾ・ｴ・・剱・ｵ・・呻ｽｺ]{3,})\.?,\s*(\d{1,2}):(\d{2})",
        normalized,
        flags=re.IGNORECASE,
    )
    if not month_match:
        return None

    day = int(month_match.group(1))
    month_key = _normalize_text_token(month_match.group(2).rstrip("."))
    month = MONTHS_PT_BR.get(month_key[:3], MONTHS_PT_BR.get(month_key))
    if month is None:
        return None

    hour = int(month_match.group(3))
    minute = int(month_match.group(4))
    candidate_date = date(reference_date.year, month, day)
    if candidate_date > reference_date:
        candidate_date = date(reference_date.year - 1, month, day)
    return datetime.combine(candidate_date, dt_time(hour=hour, minute=minute), tzinfo=BRAZIL_TZ).isoformat()


def infer_flow_from_url(listing_url: str) -> str | None:
    lowered = str(listing_url).lower()
    if "/aluguel/" in lowered:
        return "rent"
    if "/venda/" in lowered:
        return "sale"

    normalized = _normalize_text_token(lowered)
    has_rent_hint = any(hint in normalized for hint in ("aluguel", "locacao", "para-alugar", "para-locar"))
    has_sale_hint = any(hint in normalized for hint in ("venda", "a-venda", "para-venda"))
    if has_rent_hint and not has_sale_hint:
        return "rent"
    if has_sale_hint and not has_rent_hint:
        return "sale"
    return None


def _is_listing_href(href: str | None) -> bool:
    if not href:
        return False
    return bool(LISTING_URL_PATTERN.search(href))


def _extract_listing_href(anchor_selector: Selector) -> str | None:
    href = anchor_selector.attrib.get("href")
    if not _is_listing_href(href):
        return None
    return absolute_url(href, BASE_SITE_URL)


def _find_listing_container(selector: Selector) -> Selector | None:
    containers = selector.css('div[class*="AdListing_adListContainer"]')
    if not containers:
        return None
    return containers[0]


def _extract_card_root_from_topbody(topbody_selector: Selector, container_root: Any) -> Selector:
    current = topbody_selector.root
    best = current
    for _ in range(6):
        parent = current.getparent()
        if parent is None or parent == container_root:
            break
        parent_selector = Selector(root=parent)
        has_medium = bool(parent_selector.css('div.olx-adcard__mediumbody'))
        has_bottom = bool(parent_selector.css('div.olx-adcard__bottombody'))
        if has_medium or has_bottom:
            best = parent
        if has_medium and has_bottom:
            return parent_selector
        current = parent
    return Selector(root=best)


def _extract_card_date_text(card_selector: Selector) -> str | None:
    text = card_selector.css('div.olx-adcard__bottombody p.olx-adcard__date::text').get()
    if text:
        normalized = html_unescape(text).strip()
        if normalized:
            return normalized
    text = card_selector.css('div.olx-adcard__bottombody [class*="adcard__date"]::text').get()
    if text:
        normalized = html_unescape(text).strip()
        if normalized:
            return normalized
    return None


def _extract_card_price_brl(card_selector: Selector) -> int | None:
    for text in card_selector.css("div.olx-adcard__mediumbody ::text").getall():
        normalized = html_unescape(text).strip()
        if not normalized:
            continue
        match = PRICE_TEXT_PATTERN.search(normalized)
        if match:
            return normalize_price_brl(match.group(0))
    return None


def _extract_card_title(anchor_selector: Selector, card_selector: Selector) -> str | None:
    for text in anchor_selector.css("::text").getall():
        normalized = html_unescape(text).strip()
        if normalized:
            return normalized
    for text in card_selector.css("div.olx-adcard__topbody ::text").getall():
        normalized = html_unescape(text).strip()
        if normalized:
            return normalized
    return None


def parse_listing_page(html: str, run_date: str | date | datetime) -> list[dict[str, Any]]:
    selector = Selector(text=html)
    records: list[dict[str, Any]] = []
    seen_urls: set[str] = set()
    container_selector = _find_listing_container(selector)
    if container_selector is None:
        return records

    container_root = container_selector.root
    for topbody_selector in container_selector.css('div.olx-adcard__topbody[data-mode="horizontal"]'):
        anchors = topbody_selector.css('a[href*="/imoveis/"]')
        if not anchors:
            continue
        anchor_selector = anchors[0]
        listing_url = _extract_listing_href(anchor_selector)
        if not listing_url or listing_url in seen_urls:
            continue
        seen_urls.add(listing_url)

        card_selector = _extract_card_root_from_topbody(topbody_selector, container_root)
        raw_card_date_text = _extract_card_date_text(card_selector)
        records.append(
            {
                "listing_url": listing_url,
                "price_brl": _extract_card_price_brl(card_selector),
                "listing_posted_at": parse_card_date(raw_card_date_text, run_date),
                "raw_card_date_text": raw_card_date_text,
                "title": _extract_card_title(anchor_selector, card_selector),
            }
        )

    return records


def _parse_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=BRAZIL_TZ)
    return parsed.astimezone(BRAZIL_TZ)


def _derive_invalid_reason(record: dict[str, Any]) -> str:
    listing_url = str(record.get("listing_url") or "").strip()
    price_brl = normalize_price_brl(record.get("price_brl"))
    listing_posted_at = record.get("listing_posted_at")
    raw_card_date_text = str(record.get("raw_card_date_text") or "").strip()
    if not listing_url:
        return "missing_listing_url"
    if price_brl is None:
        return "missing_price"
    if listing_posted_at:
        return "unexpected_invalid_record"
    if raw_card_date_text:
        return "unparsed_card_date_text"
    return "missing_card_date_text"


def process_page_records(
    *,
    flow: str,
    parsed_records: list[dict[str, Any]],
    previous_state: PreviousRunState,
    seen_urls: set[str],
) -> PageProcessResult:
    result = PageProcessResult(kept_records=[], invalid_samples=[])
    newest_previous = previous_state.newest_posted_at_by_flow.get(flow)
    overlap_candidate_count = 0
    newer_than_previous_count = 0

    for record in parsed_records:
        listing_url = str(record.get("listing_url") or "").strip()
        price_brl = normalize_price_brl(record.get("price_brl"))
        listing_posted_at = _parse_iso_datetime(record.get("listing_posted_at"))

        if not listing_url or price_brl is None or listing_posted_at is None:
            result.invalid_records += 1
            result.invalid_samples.append(
                {
                    "flow": flow,
                    "invalid_reason": _derive_invalid_reason(record),
                    "listing_url": listing_url or None,
                    "price_brl": price_brl,
                    "listing_posted_at": record.get("listing_posted_at"),
                    "raw_card_date_text": record.get("raw_card_date_text"),
                    "title": record.get("title"),
                }
            )
            continue

        in_overlap_window = newest_previous is not None and listing_posted_at <= newest_previous
        if in_overlap_window:
            overlap_candidate_count += 1
        else:
            newer_than_previous_count += 1

        if listing_url in seen_urls:
            result.duplicates_in_run += 1
            continue
        seen_urls.add(listing_url)

        if listing_url in previous_state.price_by_url and previous_state.price_by_url[listing_url] == price_brl:
            result.same_price_ignored += 1
            continue

        if in_overlap_window:
            result.useful_overlap_records += 1

        result.kept_records.append(
            {
                "listing_url": listing_url,
                "business_type": "rent" if flow == "rent" else "sale",
                "price_brl": price_brl,
                "listing_posted_at": listing_posted_at.isoformat(),
            }
        )

    result.page_fully_in_overlap = overlap_candidate_count > 0 and newer_than_previous_count == 0
    result.stop_due_to_old_date = result.page_fully_in_overlap and result.useful_overlap_records == 0
    return result


def build_scrapy_settings(verbose: bool = False) -> dict[str, Any]:
    return build_base_scrapy_settings(
        user_agent=DEFAULT_USER_AGENT,
        default_headers=HEADERS,
        verbose=verbose,
        retry_times=2,
        autothrottle_start_delay=1.0,
        autothrottle_max_delay=8.0,
        autothrottle_target_concurrency=1.0,
        concurrent_requests=2,
        concurrent_requests_per_domain=1,
        download_delay=1.0,
        randomize_download_delay=True,
        download_timeout=30,
        impersonate=DEFAULT_IMPERSONATE_BROWSER,
    )


def run_scrapy_discovery(
    *,
    run_date: str,
    previous_state: PreviousRunState,
    max_pages: int,
    verbose: bool = False,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    collector: dict[str, Any] = {"records": [], "metrics": [], "invalid_records": []}
    run_spider(
        OlxDiscoverySpider,
        settings=build_scrapy_settings(verbose=verbose),
        run_date=run_date,
        max_pages=max_pages,
        previous_state=previous_state,
        collector=collector,
        verbose=verbose,
    )
    return collector["records"], collector["metrics"], collector["invalid_records"]


def find_previous_output(run_date: str, project_root: Path | None = None) -> Path | None:
    root = (project_root or Path.cwd()).resolve()
    raw_dir = root / "raw"
    if not raw_dir.exists():
        return None

    current_date = _parse_run_date(run_date)
    candidates: list[tuple[date, Path]] = []
    for candidate in raw_dir.glob("*/olx/olx_discovery.csv"):
        try:
            candidate_date = _parse_run_date(candidate.parent.parent.name)
        except ValueError:
            continue
        if candidate_date >= current_date:
            continue
        candidates.append((candidate_date, candidate))

    candidates.sort(key=lambda item: item[0], reverse=True)
    for _, candidate in candidates:
        if _has_non_empty_csv_rows(candidate):
            return candidate
    return None


def load_previous_run_state(path: str | Path | None) -> PreviousRunState:
    if not path:
        return PreviousRunState(
            price_by_url={},
            oldest_posted_at_by_flow={},
            newest_posted_at_by_flow={},
            source_path=None,
        )

    previous_path = Path(path)
    if not previous_path.exists():
        return PreviousRunState(
            price_by_url={},
            oldest_posted_at_by_flow={},
            newest_posted_at_by_flow={},
            source_path=str(previous_path),
        )

    price_by_url: dict[str, int | None] = {}
    oldest_posted_at_by_flow: dict[str, datetime] = {}
    newest_posted_at_by_flow: dict[str, datetime] = {}
    with previous_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            listing_url = str(row.get("listing_url") or "").strip()
            if not listing_url:
                continue

            price_by_url[listing_url] = normalize_price_brl(row.get("price_brl"))
            business_type = str(row.get("business_type") or "").strip().lower()
            flow = business_type if business_type in {"rent", "sale"} else infer_flow_from_url(listing_url)
            posted_at = _parse_iso_datetime(str(row.get("listing_posted_at") or "").strip())
            if flow is None or posted_at is None:
                continue

            previous_oldest = oldest_posted_at_by_flow.get(flow)
            if previous_oldest is None or posted_at < previous_oldest:
                oldest_posted_at_by_flow[flow] = posted_at
            previous_newest = newest_posted_at_by_flow.get(flow)
            if previous_newest is None or posted_at > previous_newest:
                newest_posted_at_by_flow[flow] = posted_at

    return PreviousRunState(
        price_by_url=price_by_url,
        oldest_posted_at_by_flow=oldest_posted_at_by_flow,
        newest_posted_at_by_flow=newest_posted_at_by_flow,
        source_path=str(previous_path),
    )


def _sort_records_desc(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        records,
        key=lambda record: _parse_iso_datetime(record.get("listing_posted_at"))
        or datetime.min.replace(tzinfo=BRAZIL_TZ),
        reverse=True,
    )


def collect_discovery_records(
    *,
    run_date: str | None = None,
    max_pages: int = 100,
    previous_output_path: str | None = None,
    verbose: bool = False,
) -> list[dict[str, Any]]:
    resolved_run_date = run_date or default_run_date()
    output_root = infer_output_root_from_output_path(default_output_path(resolved_run_date))
    previous_path = previous_output_path or str(find_previous_output(resolved_run_date, project_root=output_root) or "")
    previous_state = load_previous_run_state(previous_path)
    records, metrics, invalid_records = run_scrapy_discovery(
        run_date=resolved_run_date,
        previous_state=previous_state,
        max_pages=max_pages,
        verbose=verbose,
    )
    sorted_records = _sort_records_desc(records)
    if verbose:
        print(
            "[INFO] olx_discovery_metrics="
            + json.dumps(
                {
                    "run_date": resolved_run_date,
                    "previous_output_path": previous_state.source_path,
                    "records_collected": len(sorted_records),
                    "invalid_records_collected": len(invalid_records),
                    "flows": metrics,
                },
                ensure_ascii=False,
            )
        )
    return sorted_records

def collect_discovery_to_file(
    *,
    output_path: str,
    parquet_output_path: str | None = None,
    previous_output_path: str | None = None,
    invalid_output_path: str | None = None,
    run_date: str | None = None,
    max_pages: int = 100,
    verbose: bool = False,
) -> dict[str, Any]:
    target_output_path = output_path or default_output_path(run_date)
    resolved_run_date = run_date or _infer_run_date_from_output_path(target_output_path) or default_run_date()
    output_root = infer_output_root_from_output_path(target_output_path)
    resolved_previous_output_path = previous_output_path or str(find_previous_output(resolved_run_date, project_root=output_root) or "")
    previous_state = load_previous_run_state(resolved_previous_output_path)
    records, metrics, invalid_records = run_scrapy_discovery(
        run_date=resolved_run_date,
        previous_state=previous_state,
        max_pages=max_pages,
        verbose=verbose,
    )
    sorted_records = _sort_records_desc(records)
    resolved_parquet_output_path = parquet_output_path or str(Path(target_output_path).with_suffix(".parquet"))

    save_csv(sorted_records, filename=target_output_path, fieldnames=DISCOVERY_FIELDNAMES)
    save_parquet_records(sorted_records, filename=resolved_parquet_output_path)
    if invalid_records:
        save_invalid_records_csv(
            invalid_records,
            filename=invalid_output_path or _derive_invalid_output_path_from_output_path(target_output_path),
        )

    runner_metrics = {
        "run_date": resolved_run_date,
        "previous_output_path": previous_state.source_path,
        "records_collected": len(sorted_records),
        "invalid_records_collected": len(invalid_records),
        "flows": metrics,
    }
    if verbose:
        print("[INFO] olx_discovery_metrics=" + json.dumps(runner_metrics, ensure_ascii=False))

    return {
        "output_path": target_output_path,
        "metrics": runner_metrics,
    }
