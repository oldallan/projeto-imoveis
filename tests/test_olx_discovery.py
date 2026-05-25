from __future__ import annotations

import shutil
import unittest
import asyncio
from datetime import datetime
from io import StringIO
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from scrapy import Request
from scrapy.http import HtmlResponse
from scrapy.settings import Settings

from scrapers.olx import collect_discovery
from scrapers.olx_discovery import (
    OlxDiscoverySpider,
    build_listing_page_url,
    build_scrapy_settings,
    collect_discovery_to_file,
    collect_discovery_records,
    default_invalid_output_path,
    default_output_path,
    find_previous_output,
    load_previous_run_state,
    parse_card_date,
    parse_listing_page,
    process_page_records,
)
from scrapers.olx_shared import (
    BRAZIL_TZ,
    PreviousRunState,
    normalize_price_brl,
)
from scrapers.scrapy_support import CurlCffiDownloadHandler


def make_listing_html(cards: list[dict[str, object]]) -> str:
    parts: list[str] = []
    for card in cards:
        title = card.get("title", "Sem titulo")
        href = card.get("href", "")
        price = card.get("price_text", "")
        date_text = card.get("date_text", "")
        extra = card.get("extra_markup", "")
        parts.append(
            f"""
            <div class="olx-adcard">
              <div class="olx-adcard__topbody" data-mode="horizontal">
                <a class="listing-link" href="{href}">
                  <h2>{title}</h2>
                </a>
              </div>
              <div class="olx-adcard__mediumbody">
                <span class="listing-price">{price}</span>
              </div>
              <div class="olx-adcard__bottombody">
                <p class="typo-caption olx-adcard__date">{date_text}</p>
              </div>
              {extra}
            </div>
            """
        )
    return '<html><body><div class="AdListing_adListContainer__ALQla">' + "".join(parts) + "</div></body></html>"


class OlxDiscoveryTests(unittest.TestCase):
    def test_spider_starts_sale_flow_before_rent(self):
        spider = collect_spider(verbose=False, max_pages=3)

        start_requests = asyncio.run(collect_start_requests(spider))

        self.assertEqual(len(start_requests), 1)
        self.assertEqual(start_requests[0].meta["flow_name"], "sale")
        self.assertEqual(start_requests[0].meta["page"], 1)

    def test_build_listing_page_url_appends_sf_and_pagination(self):
        self.assertEqual(
            build_listing_page_url("https://www.olx.com.br/imoveis/venda/estado-sp/sao-paulo-e-regiao", 3),
            "https://www.olx.com.br/imoveis/venda/estado-sp/sao-paulo-e-regiao?sf=1&o=3",
        )
        self.assertEqual(
            build_listing_page_url("https://www.olx.com.br/imoveis/aluguel/estado-sp/sao-paulo-e-regiao?sf=1", 2),
            "https://www.olx.com.br/imoveis/aluguel/estado-sp/sao-paulo-e-regiao?sf=1&o=2",
        )

    def test_normalize_price_brl_handles_numeric_and_text_values(self):
        self.assertEqual(normalize_price_brl(1800), 1800)
        self.assertEqual(normalize_price_brl(1800.0), 1800)
        self.assertEqual(normalize_price_brl("R$ 1.800"), 1800)
        self.assertEqual(normalize_price_brl("R$ 740.000"), 740000)
        self.assertIsNone(normalize_price_brl("A combinar"))

    def test_parse_card_date_handles_today_yesterday_and_month_name(self):
        self.assertEqual(parse_card_date("Hoje, 10:16", "13-04-2026"), "2026-04-13T10:16:00-03:00")
        self.assertEqual(parse_card_date("Ontem, 23:33", "13-04-2026"), "2026-04-12T23:33:00-03:00")
        self.assertEqual(parse_card_date("11 de abr, 03:26", "13-04-2026"), "2026-04-11T03:26:00-03:00")

    def test_parse_card_date_rolls_year_back_when_month_day_is_in_future(self):
        self.assertEqual(parse_card_date("31 de dez, 23:33", "02-01-2026"), "2025-12-31T23:33:00-03:00")

    def test_parse_listing_page_extracts_url_price_and_card_date_from_same_card(self):
        html = make_listing_html(
            [
                {
                    "href": "https://sp.olx.com.br/sao-paulo-e-regiao/imoveis/imovel-1-1234567",
                    "title": "Imovel 1",
                    "price_text": "R$ 1.800",
                    "date_text": "Hoje, 10:16",
                },
                {
                    "href": "https://sp.olx.com.br/sao-paulo-e-regiao/imoveis/imovel-2-7654321",
                    "title": "Imovel 2",
                    "price_text": "R$ 740.000",
                    "date_text": "Ontem, 23:33",
                },
            ]
        )

        parsed = parse_listing_page(html, "13-04-2026")

        self.assertEqual(
            parsed,
            [
                {
                    "listing_url": "https://sp.olx.com.br/sao-paulo-e-regiao/imoveis/imovel-1-1234567",
                    "price_brl": 1800,
                    "listing_posted_at": "2026-04-13T10:16:00-03:00",
                    "raw_card_date_text": "Hoje, 10:16",
                    "title": "Imovel 1",
                },
                {
                    "listing_url": "https://sp.olx.com.br/sao-paulo-e-regiao/imoveis/imovel-2-7654321",
                    "price_brl": 740000,
                    "listing_posted_at": "2026-04-12T23:33:00-03:00",
                    "raw_card_date_text": "Ontem, 23:33",
                    "title": "Imovel 2",
                },
            ],
        )

    def test_parse_listing_page_deduplicates_repeated_anchor_with_same_listing_url(self):
        html = """
        <html><body>
          <div class="AdListing_adListContainer__ALQla">
            <div class="olx-adcard">
              <div class="olx-adcard__topbody" data-mode="horizontal">
                <a href="https://sp.olx.com.br/sao-paulo-e-regiao/imoveis/imovel-1-1234567"><h2>Imovel 1</h2></a>
                <a href="https://sp.olx.com.br/sao-paulo-e-regiao/imoveis/imovel-1-1234567">Ver mais</a>
              </div>
              <div class="olx-adcard__mediumbody"><span class="listing-price">R$ 2.500</span></div>
              <div class="olx-adcard__bottombody"><p class="typo-caption olx-adcard__date">Hoje, 12:00</p></div>
            </div>
          </div>
        </body></html>
        """

        parsed = parse_listing_page(html, "13-04-2026")

        self.assertEqual(len(parsed), 1)
        self.assertEqual(parsed[0]["listing_url"], "https://sp.olx.com.br/sao-paulo-e-regiao/imoveis/imovel-1-1234567")

    def test_process_page_records_ignores_same_price_and_tracks_useful_overlap_records(self):
        html = make_listing_html(
            [
                {
                    "href": "https://sp.olx.com.br/sao-paulo-e-regiao/imoveis/novo-1-1000001",
                    "title": "Novo 1",
                    "price_text": "R$ 2.000",
                    "date_text": "Hoje, 11:00",
                },
                {
                    "href": "https://sp.olx.com.br/sao-paulo-e-regiao/imoveis/repetido-mesmo-preco-1000002",
                    "title": "Mesmo preco",
                    "price_text": "R$ 3.000",
                    "date_text": "Hoje, 10:30",
                },
                {
                    "href": "https://sp.olx.com.br/sao-paulo-e-regiao/imoveis/repetido-preco-novo-1000003",
                    "title": "Preco novo",
                    "price_text": "R$ 3.700",
                    "date_text": "Hoje, 10:00",
                },
                {
                    "href": "https://sp.olx.com.br/sao-paulo-e-regiao/imoveis/antigo-demais-1000004",
                    "title": "Antigo",
                    "price_text": "R$ 4.000",
                    "date_text": "Hoje, 08:59",
                },
            ]
        )
        parsed_records = parse_listing_page(html, "13-04-2026")
        previous_state = PreviousRunState(
            price_by_url={
                "https://sp.olx.com.br/sao-paulo-e-regiao/imoveis/repetido-mesmo-preco-1000002": 3000,
                "https://sp.olx.com.br/sao-paulo-e-regiao/imoveis/repetido-preco-novo-1000003": 3500,
            },
            oldest_posted_at_by_flow={"sale": parse_dt("2026-04-13T09:00:00-03:00")},
            newest_posted_at_by_flow={"sale": parse_dt("2026-04-13T10:30:00-03:00")},
            source_path="raw/12-04-2026/olx/olx_discovery.csv",
        )

        result = process_page_records(
            flow="sale",
            parsed_records=parsed_records,
            previous_state=previous_state,
            seen_urls=set(),
        )

        self.assertEqual(
            result.kept_records,
            [
                {
                    "listing_url": "https://sp.olx.com.br/sao-paulo-e-regiao/imoveis/novo-1-1000001",
                    "business_type": "sale",
                    "price_brl": 2000,
                    "listing_posted_at": "2026-04-13T11:00:00-03:00",
                },
                {
                    "listing_url": "https://sp.olx.com.br/sao-paulo-e-regiao/imoveis/repetido-preco-novo-1000003",
                    "business_type": "sale",
                    "price_brl": 3700,
                    "listing_posted_at": "2026-04-13T10:00:00-03:00",
                },
                {
                    "listing_url": "https://sp.olx.com.br/sao-paulo-e-regiao/imoveis/antigo-demais-1000004",
                    "business_type": "sale",
                    "price_brl": 4000,
                    "listing_posted_at": "2026-04-13T08:59:00-03:00",
                },
            ],
        )
        self.assertEqual(result.same_price_ignored, 1)
        self.assertEqual(result.useful_overlap_records, 2)
        self.assertFalse(result.page_fully_in_overlap)
        self.assertFalse(result.stop_due_to_old_date)

    def test_process_page_records_marks_sterile_overlap_page(self):
        html = make_listing_html(
            [
                {
                    "href": "https://sp.olx.com.br/sao-paulo-e-regiao/imoveis/repetido-mesmo-preco-1000002",
                    "title": "Mesmo preco",
                    "price_text": "R$ 3.000",
                    "date_text": "Hoje, 08:30",
                }
            ]
        )
        parsed_records = parse_listing_page(html, "13-04-2026")
        previous_state = PreviousRunState(
            price_by_url={
                "https://sp.olx.com.br/sao-paulo-e-regiao/imoveis/repetido-mesmo-preco-1000002": 3000,
            },
            oldest_posted_at_by_flow={"sale": parse_dt("2026-04-13T08:30:00-03:00")},
            newest_posted_at_by_flow={"sale": parse_dt("2026-04-13T09:00:00-03:00")},
            source_path="raw/12-04-2026/olx/olx_discovery.csv",
        )

        result = process_page_records(
            flow="sale",
            parsed_records=parsed_records,
            previous_state=previous_state,
            seen_urls=set(),
        )

        self.assertEqual(result.kept_records, [])
        self.assertEqual(result.same_price_ignored, 1)
        self.assertEqual(result.useful_overlap_records, 0)
        self.assertTrue(result.page_fully_in_overlap)
        self.assertTrue(result.stop_due_to_old_date)

    def test_process_page_records_collects_missing_card_date_reason(self):
        parsed_records = [
            {
                "listing_url": "https://sp.olx.com.br/sao-paulo-e-regiao/imoveis/item-sem-data-1234567",
                "price_brl": 2500,
                "listing_posted_at": None,
                "raw_card_date_text": None,
                "title": "Item invalido",
            }
        ]

        result = process_page_records(
            flow="rent",
            parsed_records=parsed_records,
            previous_state=PreviousRunState(price_by_url={}, oldest_posted_at_by_flow={}),
            seen_urls=set(),
        )

        self.assertEqual(result.invalid_records, 1)
        self.assertEqual(
            result.invalid_samples,
            [
                {
                    "flow": "rent",
                    "invalid_reason": "missing_card_date_text",
                    "listing_url": "https://sp.olx.com.br/sao-paulo-e-regiao/imoveis/item-sem-data-1234567",
                    "price_brl": 2500,
                    "listing_posted_at": None,
                    "raw_card_date_text": None,
                    "title": "Item invalido",
                }
            ],
        )

    def test_process_page_records_collects_unparsed_card_date_reason(self):
        parsed_records = [
            {
                "listing_url": "https://sp.olx.com.br/sao-paulo-e-regiao/imoveis/item-invalido-1234567",
                "price_brl": 2500,
                "listing_posted_at": None,
                "raw_card_date_text": "12 minutos",
                "title": "Item invalido",
            }
        ]

        result = process_page_records(
            flow="rent",
            parsed_records=parsed_records,
            previous_state=PreviousRunState(price_by_url={}, oldest_posted_at_by_flow={}),
            seen_urls=set(),
        )

        self.assertEqual(result.invalid_records, 1)
        self.assertEqual(result.invalid_samples[0]["invalid_reason"], "unparsed_card_date_text")

    def test_process_page_records_collects_missing_price_reason(self):
        parsed_records = [
            {
                "listing_url": "https://sp.olx.com.br/sao-paulo-e-regiao/imoveis/item-sem-preco-1234567",
                "price_brl": None,
                "listing_posted_at": "2026-04-13T10:16:00-03:00",
                "raw_card_date_text": "Hoje, 10:16",
                "title": "Item sem preco",
            }
        ]

        result = process_page_records(
            flow="sale",
            parsed_records=parsed_records,
            previous_state=PreviousRunState(price_by_url={}, oldest_posted_at_by_flow={}),
            seen_urls=set(),
        )

        self.assertEqual(result.invalid_records, 1)
        self.assertEqual(result.invalid_samples[0]["invalid_reason"], "missing_price")

    def test_build_scrapy_settings_enables_autothrottle(self):
        settings = build_scrapy_settings(verbose=False)
        self.assertTrue(settings["AUTOTHROTTLE_ENABLED"])
        self.assertEqual(settings["CONCURRENT_REQUESTS_PER_DOMAIN"], 1)
        self.assertEqual(settings["DOWNLOAD_DELAY"], 1.0)
        self.assertEqual(settings["AUTOTHROTTLE_TARGET_CONCURRENCY"], 1.0)
        self.assertEqual(settings["DOWNLOAD_HANDLERS"]["https"], "scrapers.scrapy_support.CurlCffiDownloadHandler")

    def test_verbose_spider_logs_page_progress_and_max_pages_stop(self):
        spider = collect_spider(verbose=True, max_pages=1)
        with patch("sys.stdout", new=StringIO()) as stream:
            request = spider._build_request(flow_config=spider.flow_configs[0], page=1)
            response = HtmlResponse(
                url=request.url,
                body=make_listing_html(
                    [
                        {
                            "href": "https://sp.olx.com.br/sao-paulo-e-regiao/imoveis/imovel-1-1234567",
                            "title": "Imovel 1",
                            "price_text": "R$ 1.800",
                            "date_text": "Hoje, 10:16",
                        }
                    ]
                ).encode("utf-8"),
                encoding="utf-8",
                request=request,
            )
            follow_ups = list(spider.parse_listing_response(response))
            output = stream.getvalue()

        self.assertEqual(len(follow_ups), 1)
        self.assertEqual(follow_ups[0].meta["flow_name"], "rent")
        self.assertEqual(follow_ups[0].meta["page"], 1)
        self.assertIn("olx_discovery_page flow=sale page=1", output)
        self.assertIn("olx_discovery_page_result flow=sale page=1", output)
        self.assertIn("items_seen=1", output)
        self.assertIn("items_kept=1", output)
        self.assertIn("olx_discovery_stop flow=sale page=1 stop_reason=max_pages_reached", output)
        self.assertIn("olx_discovery_page flow=rent page=1", output)

    def test_spider_does_not_log_intermediate_progress_when_not_verbose(self):
        spider = collect_spider(verbose=False, max_pages=1)
        request = spider._build_request(flow_config=spider.flow_configs[0], page=1)
        response = HtmlResponse(
            url=request.url,
            body=make_listing_html(
                [
                    {
                        "href": "https://sp.olx.com.br/sao-paulo-e-regiao/imoveis/imovel-1-1234567",
                        "title": "Imovel 1",
                        "price_text": "R$ 1.800",
                        "date_text": "Hoje, 10:16",
                    }
                ]
            ).encode("utf-8"),
            encoding="utf-8",
            request=request,
        )

        with patch("sys.stdout", new=StringIO()) as stream:
            list(spider.parse_listing_response(response))
            output = stream.getvalue()

        self.assertEqual(output, "")

    def test_spider_advances_empty_page_until_minimum_page_threshold(self):
        spider = collect_spider(verbose=True, max_pages=5, empty_page_retry_limit=0, min_pages_before_empty_stop=4)
        request = spider._build_request(flow_config=spider.flow_configs[0], page=1)
        response = HtmlResponse(
            url=request.url,
            body=b"<html><body></body></html>",
            encoding="utf-8",
            request=request,
        )

        with patch("scrapers.olx_discovery.time.sleep") as mocked_sleep, patch("sys.stdout", new=StringIO()) as stream:
            follow_ups = list(spider.parse_listing_response(response))
            output = stream.getvalue()

        self.assertEqual(len(follow_ups), 1)
        self.assertEqual(follow_ups[0].meta["flow_name"], "sale")
        self.assertEqual(follow_ups[0].meta["page"], 2)
        mocked_sleep.assert_called_once_with(60)
        self.assertIn("olx_discovery_page_result flow=sale page=1", output)
        self.assertIn("items_seen=0", output)
        self.assertIn("items_kept=0", output)
        self.assertNotIn("olx_discovery_stop flow=sale page=1 stop_reason=empty_page", output)

    def test_verbose_spider_logs_empty_page_stop_after_minimum_page_threshold(self):
        spider = collect_spider(verbose=True, max_pages=6, empty_page_retry_limit=0, min_pages_before_empty_stop=4)
        request = spider._build_request(flow_config=spider.flow_configs[0], page=4)
        response = HtmlResponse(
            url=request.url,
            body=b"<html><body></body></html>",
            encoding="utf-8",
            request=request,
        )

        with patch("scrapers.olx_discovery.time.sleep") as mocked_sleep, patch("sys.stdout", new=StringIO()) as stream:
            follow_ups = list(spider.parse_listing_response(response))
            output = stream.getvalue()

        self.assertEqual(len(follow_ups), 1)
        self.assertEqual(follow_ups[0].meta["flow_name"], "rent")
        mocked_sleep.assert_not_called()
        self.assertIn("olx_discovery_page_result flow=sale page=4", output)
        self.assertIn("items_seen=0", output)
        self.assertIn("items_kept=0", output)
        self.assertIn("olx_discovery_stop flow=sale page=4 stop_reason=empty_page", output)

    def test_spider_retries_empty_page_once_before_stopping(self):
        spider = collect_spider(verbose=True, max_pages=3, empty_page_retry_limit=1)
        request = spider._build_request(flow_config=spider.flow_configs[0], page=72)
        response = HtmlResponse(
            url=request.url,
            body=b"<html><body></body></html>",
            encoding="utf-8",
            request=request,
        )

        with patch("sys.stdout", new=StringIO()) as stream:
            follow_ups = list(spider.parse_listing_response(response))
            output = stream.getvalue()

        self.assertEqual(len(follow_ups), 1)
        retry_request = follow_ups[0]
        self.assertEqual(retry_request.meta["page"], 72)
        self.assertEqual(retry_request.meta["empty_page_retry_count"], 1)
        self.assertTrue(retry_request.dont_filter)
        self.assertIn("olx_discovery_retry flow=sale page=72 retry_count=1 retry_reason=empty_page", output)
        self.assertNotIn("olx_discovery_stop flow=sale page=72 stop_reason=empty_page", output)

    def test_verbose_spider_logs_old_date_stop_after_stale_overlap_pages(self):
        spider = collect_spider(
            verbose=True,
            max_pages=5,
            previous_state=PreviousRunState(
                price_by_url={
                    "https://sp.olx.com.br/sao-paulo-e-regiao/imoveis/imovel-antigo-1234567": 1800,
                },
                oldest_posted_at_by_flow={"sale": parse_dt("2026-04-13T08:30:00-03:00")},
                newest_posted_at_by_flow={"sale": parse_dt("2026-04-13T09:00:00-03:00")},
            ),
            stale_overlap_page_limit=3,
        )
        with patch("sys.stdout", new=StringIO()) as stream:
            follow_ups = []
            for page in (1, 2, 3):
                request = spider._build_request(flow_config=spider.flow_configs[0], page=page)
                response = HtmlResponse(
                    url=request.url,
                    body=make_listing_html(
                        [
                            {
                                "href": "https://sp.olx.com.br/sao-paulo-e-regiao/imoveis/imovel-antigo-1234567",
                                "title": "Imovel antigo",
                                "price_text": "R$ 1.800",
                                "date_text": "Hoje, 08:30",
                            }
                        ]
                    ).encode("utf-8"),
                    encoding="utf-8",
                    request=request,
                )
                follow_ups = list(spider.parse_listing_response(response))
            output = stream.getvalue()

        self.assertEqual(len(follow_ups), 1)
        self.assertEqual(follow_ups[0].meta["flow_name"], "rent")
        self.assertIn("olx_discovery_page_result flow=sale page=3", output)
        self.assertIn("items_seen=1", output)
        self.assertIn("items_kept=0", output)
        self.assertIn("olx_discovery_stop flow=sale page=3 stop_reason=older_than_previous_window", output)

    def test_verbose_spider_logs_request_failed_stop(self):
        spider = collect_spider(verbose=True, max_pages=3)
        request = spider._build_request(flow_config=spider.flow_configs[0], page=2)
        failure = SimpleNamespace(
            request=request,
            value=SimpleNamespace(response=SimpleNamespace(status=503)),
            type=RuntimeError,
        )

        with patch("sys.stdout", new=StringIO()) as stream:
            follow_ups = list(spider.handle_request_error(failure))
            output = stream.getvalue()

        self.assertEqual(len(follow_ups), 1)
        self.assertEqual(follow_ups[0].meta["flow_name"], "rent")
        self.assertIn("olx_discovery_stop flow=sale page=2 stop_reason=request_failed:503", output)

    def test_curl_cffi_handler_strips_compression_headers_from_scrapy_response(self):
        handler = CurlCffiDownloadHandler(Settings({"DOWNLOAD_TIMEOUT": 30}), crawler=None)
        request = Request("https://www.olx.com.br/imoveis")
        upstream_response = SimpleNamespace(
            status_code=200,
            url="https://www.olx.com.br/imoveis",
            headers={
                "content-type": "text/html; charset=utf-8",
                "content-encoding": "gzip",
                "content-length": "999",
                "transfer-encoding": "chunked",
            },
            content=b"<html><body>ok</body></html>",
            encoding="utf-8",
        )

        scrapy_response = handler._build_scrapy_response(request, upstream_response)

        self.assertIsInstance(scrapy_response, HtmlResponse)
        self.assertNotIn(b"Content-Encoding", scrapy_response.headers)
        self.assertNotIn(b"Transfer-Encoding", scrapy_response.headers)
        self.assertEqual(scrapy_response.headers[b"Content-Length"], b"28")

    def test_collect_discovery_records_scans_sale_and_rent_and_sorts_desc(self):
        with patch("scrapers.olx_discovery.find_previous_output", return_value=None), patch(
            "scrapers.olx_discovery.run_scrapy_discovery",
            return_value=(
                [
                    {
                        "listing_url": "https://sp.olx.com.br/sao-paulo-e-regiao/imoveis/venda-1-1234567",
                        "business_type": "sale",
                        "price_brl": 750000,
                        "listing_posted_at": "2026-04-13T09:00:00-03:00",
                    },
                    {
                        "listing_url": "https://sp.olx.com.br/sao-paulo-e-regiao/imoveis/aluguel-1-1234568",
                        "business_type": "rent",
                        "price_brl": 3500,
                        "listing_posted_at": "2026-04-13T10:00:00-03:00",
                    },
                ],
                [
                    {"flow": "sale", "stop_reason": "empty_page"},
                    {"flow": "rent", "stop_reason": "empty_page"},
                ],
                [],
            ),
        ):
            records = collect_discovery_records(run_date="13-04-2026", max_pages=2)

        self.assertEqual(
            records,
            [
                {
                    "listing_url": "https://sp.olx.com.br/sao-paulo-e-regiao/imoveis/aluguel-1-1234568",
                    "business_type": "rent",
                    "price_brl": 3500,
                    "listing_posted_at": "2026-04-13T10:00:00-03:00",
                },
                {
                    "listing_url": "https://sp.olx.com.br/sao-paulo-e-regiao/imoveis/venda-1-1234567",
                    "business_type": "sale",
                    "price_brl": 750000,
                    "listing_posted_at": "2026-04-13T09:00:00-03:00",
                },
            ],
        )

    def test_find_previous_output_uses_latest_prior_run(self):
        runtime_root = Path("tests_runtime_olx_discovery_previous")
        try:
            (runtime_root / "raw" / "10-04-2026" / "olx").mkdir(parents=True, exist_ok=True)
            (runtime_root / "raw" / "12-04-2026" / "olx").mkdir(parents=True, exist_ok=True)
            (runtime_root / "raw" / "14-04-2026" / "olx").mkdir(parents=True, exist_ok=True)
            (runtime_root / "raw" / "10-04-2026" / "olx" / "olx_discovery.csv").write_text(
                "listing_url,business_type,price_brl,listing_posted_at\n"
                "https://sp.olx.com.br/imoveis/venda/item-1-1234567,sale,700000,2026-04-10T10:00:00-03:00\n",
                encoding="utf-8-sig",
            )
            expected = (runtime_root / "raw" / "12-04-2026" / "olx" / "olx_discovery.csv").resolve()
            expected.write_text(
                "listing_url,business_type,price_brl,listing_posted_at\n"
                "https://sp.olx.com.br/imoveis/venda/item-2-1234568,sale,710000,2026-04-12T09:00:00-03:00\n",
                encoding="utf-8-sig",
            )
            (runtime_root / "raw" / "14-04-2026" / "olx" / "olx_discovery.csv").write_text(
                "listing_url,business_type,price_brl,listing_posted_at\n"
                "https://sp.olx.com.br/imoveis/venda/item-3-1234569,sale,720000,2026-04-14T09:00:00-03:00\n",
                encoding="utf-8-sig",
            )

            found = find_previous_output("13-04-2026", project_root=runtime_root)
        finally:
            shutil.rmtree(runtime_root, ignore_errors=True)

        self.assertEqual(found, expected)

    def test_find_previous_output_skips_empty_snapshots_and_returns_none_when_all_are_empty(self):
        runtime_root = Path("tests_runtime_olx_discovery_previous_empty")
        try:
            (runtime_root / "raw" / "10-04-2026" / "olx").mkdir(parents=True, exist_ok=True)
            (runtime_root / "raw" / "12-04-2026" / "olx").mkdir(parents=True, exist_ok=True)
            non_empty = runtime_root / "raw" / "10-04-2026" / "olx" / "olx_discovery.csv"
            empty = runtime_root / "raw" / "12-04-2026" / "olx" / "olx_discovery.csv"
            non_empty.write_text(
                "listing_url,business_type,price_brl,listing_posted_at\n"
                "https://sp.olx.com.br/imoveis/venda/item-1-1234567,sale,700000,2026-04-10T10:00:00-03:00\n",
                encoding="utf-8-sig",
            )
            empty.write_text(
                "listing_url,business_type,price_brl,listing_posted_at\n",
                encoding="utf-8-sig",
            )

            found = find_previous_output("13-04-2026", project_root=runtime_root)
            non_empty.unlink()
            found_without_non_empty = find_previous_output("13-04-2026", project_root=runtime_root)
        finally:
            shutil.rmtree(runtime_root, ignore_errors=True)

        self.assertEqual(found, non_empty.resolve())
        self.assertIsNone(found_without_non_empty)

    def test_load_previous_run_state_builds_price_map_and_oldest_by_flow(self):
        runtime_dir = Path("tests_runtime_olx_discovery_state")
        csv_path = runtime_dir / "olx_discovery.csv"
        runtime_dir.mkdir(parents=True, exist_ok=True)
        try:
            csv_path.write_text(
                "listing_url,business_type,price_brl,listing_posted_at\n"
                "https://sp.olx.com.br/imoveis/venda/item-1-1234567,sale,700000,2026-04-12T10:00:00-03:00\n"
                "https://sp.olx.com.br/imoveis/venda/item-2-1234568,sale,710000,2026-04-11T09:00:00-03:00\n"
                "https://sp.olx.com.br/imoveis/aluguel/item-3-1234569,rent,3500,2026-04-12T08:00:00-03:00\n",
                encoding="utf-8-sig",
            )
            state = load_previous_run_state(csv_path)
        finally:
            shutil.rmtree(runtime_dir, ignore_errors=True)

        self.assertEqual(state.price_by_url["https://sp.olx.com.br/imoveis/venda/item-1-1234567"], 700000)
        self.assertEqual(state.oldest_posted_at_by_flow["sale"].isoformat(), "2026-04-11T09:00:00-03:00")
        self.assertEqual(state.oldest_posted_at_by_flow["rent"].isoformat(), "2026-04-12T08:00:00-03:00")
        self.assertEqual(state.newest_posted_at_by_flow["sale"].isoformat(), "2026-04-12T10:00:00-03:00")
        self.assertEqual(state.newest_posted_at_by_flow["rent"].isoformat(), "2026-04-12T08:00:00-03:00")

    def test_collect_discovery_to_file_writes_unified_csv_with_expected_columns(self):
        output_dir = Path("tests_runtime_olx_discovery_csv")
        output_path = output_dir / "olx_discovery.csv"
        invalid_output_path = output_dir / "olx_discovery_invalid_records.csv"
        output_dir.mkdir(parents=True, exist_ok=True)
        try:
            with patch(
                "scrapers.olx_discovery.run_scrapy_discovery",
                return_value=(
                    [
                        {
                            "listing_url": "https://sp.olx.com.br/sao-paulo-e-regiao/imoveis/item-1-1234567",
                            "business_type": "rent",
                            "price_brl": 3500,
                            "listing_posted_at": "2026-04-13T10:00:00-03:00",
                        }
                    ],
                    [{"flow": "sale", "stop_reason": "max_pages_reached"}],
                    [
                        {
                            "flow": "sale",
                            "invalid_reason": "unparsed_card_date_text",
                            "listing_url": "https://sp.olx.com.br/sao-paulo-e-regiao/imoveis/item-invalido-1234568",
                            "price_brl": 3100,
                            "listing_posted_at": None,
                            "raw_card_date_text": "12 minutos",
                            "title": "Item invalido",
                        }
                    ],
                ),
            ):
                returned = collect_discovery_to_file(
                    run_date="13-04-2026",
                    output_path=str(output_path),
                    parquet_output_path=str(output_dir / "olx_discovery.parquet"),
                    invalid_output_path=str(invalid_output_path),
                )
            contents = output_path.read_text(encoding="utf-8-sig")
            invalid_contents = invalid_output_path.read_text(encoding="utf-8-sig")
        finally:
            shutil.rmtree(output_dir, ignore_errors=True)

        self.assertEqual(returned["output_path"], str(output_path))
        self.assertIn("listing_url,business_type,price_brl,listing_posted_at", contents)
        self.assertIn("item-1-1234567", contents)
        self.assertIn("invalid_reason", invalid_contents)
        self.assertIn("unparsed_card_date_text", invalid_contents)

    def test_collect_discovery_to_file_verbose_prints_metrics(self):
        output_dir = Path("tests_runtime_olx_discovery_verbose_metrics")
        output_path = output_dir / "olx_discovery.csv"
        parquet_path = output_dir / "olx_discovery.parquet"
        output_dir.mkdir(parents=True, exist_ok=True)
        try:
            with patch(
                "scrapers.olx_discovery.run_scrapy_discovery",
                return_value=(
                    [
                        {
                            "listing_url": "https://sp.olx.com.br/sao-paulo-e-regiao/imoveis/item-1-1234567",
                            "business_type": "sale",
                            "price_brl": 700000,
                            "listing_posted_at": "2026-04-13T10:00:00-03:00",
                        }
                    ],
                    [{"flow": "sale", "stop_reason": "max_pages_reached"}],
                    [],
                ),
            ), patch("sys.stdout", new=StringIO()) as stream:
                collect_discovery_to_file(
                    run_date="13-04-2026",
                    output_path=str(output_path),
                    parquet_output_path=str(parquet_path),
                    verbose=True,
                )
                output = stream.getvalue()
        finally:
            shutil.rmtree(output_dir, ignore_errors=True)

        self.assertIn("olx_discovery_metrics=", output)
        self.assertIn("\"records_collected\": 1", output)

    def test_collect_discovery_to_file_without_verbose_does_not_print_metrics(self):
        output_dir = Path("tests_runtime_olx_discovery_quiet_metrics")
        output_path = output_dir / "olx_discovery.csv"
        parquet_path = output_dir / "olx_discovery.parquet"
        output_dir.mkdir(parents=True, exist_ok=True)
        try:
            with patch(
                "scrapers.olx_discovery.run_scrapy_discovery",
                return_value=(
                    [
                        {
                            "listing_url": "https://sp.olx.com.br/sao-paulo-e-regiao/imoveis/item-1-1234567",
                            "business_type": "sale",
                            "price_brl": 700000,
                            "listing_posted_at": "2026-04-13T10:00:00-03:00",
                        }
                    ],
                    [{"flow": "sale", "stop_reason": "max_pages_reached"}],
                    [],
                ),
            ), patch("sys.stdout", new=StringIO()) as stream:
                collect_discovery_to_file(
                    run_date="13-04-2026",
                    output_path=str(output_path),
                    parquet_output_path=str(parquet_path),
                    verbose=False,
                )
                output = stream.getvalue()
        finally:
            shutil.rmtree(output_dir, ignore_errors=True)

        self.assertEqual(output, "")

    def test_collect_discovery_to_file_uses_latest_non_empty_prior_snapshot(self):
        runtime_root = Path("tests_runtime_olx_discovery_non_empty_baseline")
        output_path = runtime_root / "raw" / "13-04-2026" / "olx" / "olx_discovery.csv"
        parquet_path = output_path.with_suffix(".parquet")
        first_snapshot = runtime_root / "raw" / "11-04-2026" / "olx" / "olx_discovery.csv"
        empty_snapshot = runtime_root / "raw" / "12-04-2026" / "olx" / "olx_discovery.csv"
        try:
            first_snapshot.parent.mkdir(parents=True, exist_ok=True)
            empty_snapshot.parent.mkdir(parents=True, exist_ok=True)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            first_snapshot.write_text(
                "listing_url,business_type,price_brl,listing_posted_at\n"
                "https://sp.olx.com.br/imoveis/venda/item-1-1234567,sale,700000,2026-04-11T10:00:00-03:00\n",
                encoding="utf-8-sig",
            )
            empty_snapshot.write_text(
                "listing_url,business_type,price_brl,listing_posted_at\n",
                encoding="utf-8-sig",
            )

            with patch(
                "scrapers.olx_discovery.find_previous_output",
                return_value=first_snapshot.resolve(),
            ), patch(
                "scrapers.olx_discovery.run_scrapy_discovery",
                return_value=(
                    [
                        {
                            "listing_url": "https://sp.olx.com.br/imoveis/venda/item-1-1234567",
                            "business_type": "sale",
                            "price_brl": 700000,
                            "listing_posted_at": "2026-04-11T10:00:00-03:00",
                        }
                    ],
                    [{"flow": "sale", "stop_reason": "empty_page"}],
                    [],
                ),
            ) as mocked_run:
                returned = collect_discovery_to_file(
                    run_date="13-04-2026",
                    output_path=str(output_path),
                    parquet_output_path=str(parquet_path),
                )
        finally:
            shutil.rmtree(runtime_root, ignore_errors=True)

        previous_state = mocked_run.call_args.kwargs["previous_state"]
        self.assertEqual(returned["metrics"]["previous_output_path"], str(first_snapshot.resolve()))
        self.assertEqual(previous_state.source_path, str(first_snapshot.resolve()))
        self.assertEqual(
            previous_state.price_by_url["https://sp.olx.com.br/imoveis/venda/item-1-1234567"],
            700000,
        )
        self.assertEqual(returned["metrics"]["records_collected"], 1)

    def test_collect_discovery_to_file_prefers_explicit_previous_output_even_if_empty(self):
        runtime_root = Path("tests_runtime_olx_discovery_explicit_previous")
        output_path = runtime_root / "raw" / "13-04-2026" / "olx" / "olx_discovery.csv"
        parquet_path = output_path.with_suffix(".parquet")
        explicit_previous = runtime_root / "raw" / "12-04-2026" / "olx" / "olx_discovery.csv"
        older_non_empty = runtime_root / "raw" / "11-04-2026" / "olx" / "olx_discovery.csv"
        try:
            explicit_previous.parent.mkdir(parents=True, exist_ok=True)
            older_non_empty.parent.mkdir(parents=True, exist_ok=True)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            explicit_previous.write_text(
                "listing_url,business_type,price_brl,listing_posted_at\n",
                encoding="utf-8-sig",
            )
            older_non_empty.write_text(
                "listing_url,business_type,price_brl,listing_posted_at\n"
                "https://sp.olx.com.br/imoveis/venda/item-1-1234567,sale,700000,2026-04-11T10:00:00-03:00\n",
                encoding="utf-8-sig",
            )

            with patch(
                "scrapers.olx_discovery.run_scrapy_discovery",
                return_value=([], [{"flow": "sale", "stop_reason": "empty_page"}], []),
            ) as mocked_run:
                returned = collect_discovery_to_file(
                    run_date="13-04-2026",
                    output_path=str(output_path),
                    parquet_output_path=str(parquet_path),
                    previous_output_path=str(explicit_previous),
                )
        finally:
            shutil.rmtree(runtime_root, ignore_errors=True)

        previous_state = mocked_run.call_args.kwargs["previous_state"]
        self.assertEqual(previous_state.source_path, str(explicit_previous))
        self.assertEqual(previous_state.price_by_url, {})
        self.assertEqual(returned["metrics"]["previous_output_path"], str(explicit_previous))

    def test_collect_wrapper_derives_invalid_output_path_from_output_path(self):
        output_dir = Path("tests_runtime_olx_discovery_wrapper")
        output_path = output_dir / "06-04-2026" / "olx" / "olx_discovery.csv"
        try:
            with patch(
                "scrapers.olx.collect_discovery_to_file",
                return_value={"output_path": str(output_path), "metrics": {}},
            ) as mocked_collect_discovery_to_file:
                returned = collect_discovery(output_path=str(output_path), max_pages=100, verbose=True)
        finally:
            shutil.rmtree(output_dir, ignore_errors=True)

        self.assertEqual(returned["output_path"], str(output_path))
        mocked_collect_discovery_to_file.assert_called_once_with(
            output_path=str(output_path),
            parquet_output_path=str(output_path.with_suffix(".parquet")),
            max_pages=100,
            verbose=True,
        )

    def test_default_output_path_uses_olx_discovery_filename(self):
        self.assertEqual(default_output_path("13-04-2026"), "raw\\13-04-2026\\olx\\olx_discovery.csv")

    def test_default_invalid_output_path_uses_invalid_filename(self):
        self.assertEqual(
            default_invalid_output_path("13-04-2026"),
            "raw\\13-04-2026\\olx\\olx_discovery_invalid_records.csv",
        )


def parse_dt(value: str) -> datetime:
    return datetime.fromisoformat(value).astimezone(BRAZIL_TZ)


async def collect_start_requests(spider: OlxDiscoverySpider):
    return [request async for request in spider.start()]


def collect_spider(
    *,
    verbose: bool,
    max_pages: int,
    previous_state: PreviousRunState | None = None,
    empty_page_retry_limit: int = 1,
    empty_page_advance_delay_seconds: int = 60,
    min_pages_before_empty_stop: int = 4,
    stale_overlap_page_limit: int = 3,
):
    return OlxDiscoverySpider(
        run_date="13-04-2026",
        max_pages=max_pages,
        previous_state=previous_state or PreviousRunState(price_by_url={}, oldest_posted_at_by_flow={}),
        collector={"records": [], "metrics": [], "invalid_records": []},
        verbose=verbose,
        empty_page_retry_limit=empty_page_retry_limit,
        empty_page_advance_delay_seconds=empty_page_advance_delay_seconds,
        min_pages_before_empty_stop=min_pages_before_empty_stop,
        stale_overlap_page_limit=stale_overlap_page_limit,
    )


if __name__ == "__main__":
    unittest.main()
