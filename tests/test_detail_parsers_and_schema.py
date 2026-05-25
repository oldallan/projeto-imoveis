from __future__ import annotations

import json
import shutil
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import pandas as pd
import requests
from scrapy.exceptions import CloseSpider
from scrapy.http import HtmlResponse

import scrapers.lopes as lopes_module
import scrapers.quinto as quinto_module
from pipelines.dedupe import build_unified_tables
from pipelines.normalize import CANONICAL_COLUMNS, normalize_data
from pipelines.zipcode_enrichment import ZipCodeEnricher
from scrapers.discovery_incremental import (
    IncrementalDiscoveryState,
    build_incremental_discovery_delta,
    find_previous_output,
)
from scrapers.http_metrics import init_metrics
from scrapers.io_utils import save_parquet_records
from scrapers.listings_resume import build_resume_paths, load_jsonl_records, pending_listing_records
from scrapers.lopes_discovery import (
    collect_discovery_records as collect_lopes_discovery_records,
    parse_listing_sitemap as parse_lopes_listing_sitemap,
    parse_sitemap_index as parse_lopes_sitemap_index,
)
from scrapers.lopes_discovery import collect_discovery_to_file as collect_lopes_discovery_to_file
from scrapers.lopes_listings import (
    LopesListingsSpider,
    build_scrapy_settings as build_lopes_listings_scrapy_settings,
    collect_listings_from_file as collect_lopes_listings_from_file,
    parse_listing_page_html as parse_lopes_listing_page_html,
    run_scrapy_collection as run_lopes_scrapy_collection,
)
from scrapers.logging_utils import log_listing_collection_progress
from scrapers.olx_listings import (
    OlxListingsSpider,
    build_scrapy_settings as build_olx_listings_scrapy_settings,
    collect_listings_from_file as collect_olx_listings_from_file,
    parse_listing_page_html as parse_olx_listing_page_html,
)
from scrapers.quinto_discovery import (
    collect_discovery_to_file as collect_quinto_discovery_to_file,
    parse_listing_sitemap as parse_quinto_listing_sitemap,
    parse_sitemap_index as parse_quinto_sitemap_index,
)
from scrapers.quinto_listings import (
    QuintoListingsSpider,
    build_scrapy_settings as build_quinto_listings_scrapy_settings,
    collect_listings_from_file as collect_quinto_listings_from_file,
    parse_listing_page_html as parse_quinto_listing_page_html,
    run_scrapy_collection as run_quinto_scrapy_collection,
)
from scrapers.quinto_shared import (
    normalize_address_value as normalize_quinto_address_value,
    save_csv as save_quinto_csv,
    save_parquet as save_quinto_parquet,
)


class ListingParserTests(unittest.TestCase):
    def test_incremental_discovery_uses_inferred_watermark_overlap_window(self):
        previous_state = IncrementalDiscoveryState(
            lastmod_by_url={
                "https://example.com/known": "2026-04-28",
                "https://example.com/changed": "2026-04-28",
            },
            source_path="/tmp/previous.csv",
            watermark_lastmod=pd.Timestamp("2026-04-28").date(),
        )

        delta, metrics = build_incremental_discovery_delta(
            [
                {
                    "listing_url": "https://example.com/old-new",
                    "lastmod": "2026-04-26",
                    "listing_id": "old-new",
                    "business_type": "sale",
                },
                {
                    "listing_url": "https://example.com/window-new",
                    "lastmod": "2026-04-27",
                    "listing_id": "window-new",
                    "business_type": "sale",
                },
                {
                    "listing_url": "https://example.com/window-new",
                    "lastmod": "2026-04-27",
                    "listing_id": "window-new",
                    "business_type": "sale",
                },
                {
                    "listing_url": "https://example.com/known",
                    "lastmod": "2026-04-28",
                    "listing_id": "known",
                    "business_type": "rent",
                },
                {
                    "listing_url": "https://example.com/changed",
                    "lastmod": "2026-04-29",
                    "listing_id": "changed",
                    "business_type": "rent",
                },
                {
                    "listing_url": "https://example.com/no-lastmod",
                    "lastmod": None,
                    "listing_id": "no-lastmod",
                    "business_type": "rent",
                },
            ],
            previous_state,
        )

        self.assertEqual(
            [record["listing_url"] for record in delta],
            [
                "https://example.com/changed",
                "https://example.com/window-new",
                "https://example.com/no-lastmod",
            ],
        )
        self.assertEqual(metrics["watermark_lastmod"], "2026-04-28")
        self.assertEqual(metrics["overlap_start_lastmod"], "2026-04-27")
        self.assertEqual(metrics["window_filtered_rows"], 1)
        self.assertEqual(metrics["new_rows"], 2)
        self.assertEqual(metrics["updated_rows"], 1)
        self.assertEqual(metrics["unchanged_rows"], 1)
        self.assertEqual(metrics["delta_rows"], 3)

    def test_lopes_parse_sitemap_index_filters_listing_sitemaps(self):
        xml = """
        <sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
          <sitemap><loc>https://www.lopes.com.br/sitemaps/sitemap-index.xml</loc></sitemap>
          <sitemap><loc>https://www.lopes.com.br/sitemaps/sitemap-imoveis.xml</loc></sitemap>
          <sitemap><loc>https://www.lopes.com.br/sitemaps/sitemap-imoveis-2.xml</loc></sitemap>
          <sitemap><loc>https://www.lopes.com.br/sitemaps/sitemap-noticias.xml</loc></sitemap>
        </sitemapindex>
        """
        parsed = parse_lopes_sitemap_index(xml)
        self.assertEqual(
            parsed,
            [
                "https://www.lopes.com.br/sitemaps/sitemap-imoveis.xml",
                "https://www.lopes.com.br/sitemaps/sitemap-imoveis-2.xml",
            ],
        )

    def test_lopes_parse_listing_sitemap_derives_discovery_fields(self):
        xml = """
        <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
          <url>
            <loc>https://www.lopes.com.br/imovel/REO1000001/venda-apartamento-2-quartos-sao-paulo-moema</loc>
            <lastmod>2026-01-02</lastmod>
          </url>
          <url>
            <loc>https://www.lopes.com.br/imovel/REO1000002/aluguel-apartamento-1-quarto-sao-paulo-pinheiros</loc>
            <lastmod>2026-01-03</lastmod>
          </url>
        </urlset>
        """
        parsed = parse_lopes_listing_sitemap(xml)
        self.assertEqual(
            parsed,
            [
                {
                    "listing_url": "https://www.lopes.com.br/imovel/REO1000001/venda-apartamento-2-quartos-sao-paulo-moema",
                    "lastmod": "2026-01-02",
                    "listing_id": "REO1000001",
                    "business_type": "sale",
                },
                {
                    "listing_url": "https://www.lopes.com.br/imovel/REO1000002/aluguel-apartamento-1-quarto-sao-paulo-pinheiros",
                    "lastmod": "2026-01-03",
                    "listing_id": "REO1000002",
                    "business_type": "rent",
                },
            ],
        )

    def test_lopes_parse_listing_sitemap_ignores_lancamento_urls(self):
        xml = """
        <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
          <url>
            <loc>https://www.lopes.com.br/lancamento/REM1000001/empreendimento-teste</loc>
            <lastmod>2026-01-02</lastmod>
          </url>
          <url>
            <loc>https://www.lopes.com.br/imovel/REO1000002/aluguel-apartamento-1-quarto-sao-paulo-pinheiros</loc>
            <lastmod>2026-01-03</lastmod>
          </url>
        </urlset>
        """
        parsed = parse_lopes_listing_sitemap(xml)
        self.assertEqual(
            parsed,
            [
                {
                    "listing_url": "https://www.lopes.com.br/imovel/REO1000002/aluguel-apartamento-1-quarto-sao-paulo-pinheiros",
                    "lastmod": "2026-01-03",
                    "listing_id": "REO1000002",
                    "business_type": "rent",
                }
            ],
        )

    def test_lopes_collect_discovery_records_deduplicates_urls_by_latest_lastmod(self):
        sitemap_index_xml = """
        <sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
          <sitemap><loc>https://www.lopes.com.br/sitemaps/sitemap-imoveis.xml</loc></sitemap>
          <sitemap><loc>https://www.lopes.com.br/sitemaps/sitemap-imoveis-2.xml</loc></sitemap>
        </sitemapindex>
        """
        first_sitemap = """
        <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
          <url>
            <loc>https://www.lopes.com.br/imovel/REO1000001/venda-apartamento</loc>
            <lastmod>2026-01-01</lastmod>
          </url>
        </urlset>
        """
        second_sitemap = """
        <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
          <url>
            <loc>https://www.lopes.com.br/imovel/REO1000001/venda-apartamento</loc>
            <lastmod>2026-01-05</lastmod>
          </url>
        </urlset>
        """
        with patch(
            "scrapers.lopes_discovery._fetch_text",
            side_effect=[sitemap_index_xml, first_sitemap, second_sitemap],
        ):
            records = collect_lopes_discovery_records()

        self.assertEqual(
            records,
            [
                {
                    "listing_url": "https://www.lopes.com.br/imovel/REO1000001/venda-apartamento",
                    "lastmod": "2026-01-05",
                    "listing_id": "REO1000001",
                    "business_type": "sale",
                }
            ],
        )

    def test_quinto_parse_sitemap_index_filters_listing_sitemaps(self):
        xml = """
        <sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
          <sitemap><loc>https://www.quintoandar.com.br/sitemap-v2-listings-part-0000.xml</loc></sitemap>
          <sitemap><loc>https://www.quintoandar.com.br/sitemap-v2-listings-part-0001.xml</loc></sitemap>
          <sitemap><loc>https://www.quintoandar.com.br/sitemap-v2-buildings-part-0000.xml</loc></sitemap>
        </sitemapindex>
        """
        parsed = parse_quinto_sitemap_index(xml)
        self.assertEqual(
            parsed,
            [
                "https://www.quintoandar.com.br/sitemap-v2-listings-part-0000.xml",
                "https://www.quintoandar.com.br/sitemap-v2-listings-part-0001.xml",
            ],
        )

    def test_quinto_parse_listing_sitemap_derives_discovery_fields(self):
        xml = """
        <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
          <url>
            <loc>https://www.quintoandar.com.br/imovel/894054331/alugar/apartamento</loc>
            <lastmod>2026-02-01</lastmod>
          </url>
          <url>
            <loc>https://www.quintoandar.com.br/imovel/123456789/comprar/casa</loc>
            <lastmod>2026-02-02</lastmod>
          </url>
        </urlset>
        """
        parsed = parse_quinto_listing_sitemap(xml)
        self.assertEqual(
            parsed,
            [
                {
                    "listing_url": "https://www.quintoandar.com.br/imovel/894054331/alugar/apartamento",
                    "lastmod": "2026-02-01",
                    "listing_id": "894054331",
                    "business_type": "rent",
                },
                {
                    "listing_url": "https://www.quintoandar.com.br/imovel/123456789/comprar/casa",
                    "lastmod": "2026-02-02",
                    "listing_id": "123456789",
                    "business_type": "sale",
                },
            ],
        )

    def test_lopes_listing_collection_progress_logs_first_interval_and_last(self):
        metrics = init_metrics("lopes")
        metrics["listing_page_successes"] = 24

        with patch("builtins.print") as print_mock:
            log_listing_collection_progress(
                "lopes",
                processed=1,
                total=60,
                success=metrics["listing_page_successes"],
                failures=metrics["listing_page_failures"],
            )
            metrics["listing_page_successes"] = 25
            log_listing_collection_progress(
                "lopes",
                processed=25,
                total=60,
                success=metrics["listing_page_successes"],
                failures=metrics["listing_page_failures"],
            )
            log_listing_collection_progress(
                "lopes",
                processed=26,
                total=60,
                success=metrics["listing_page_successes"],
                failures=metrics["listing_page_failures"],
            )
            metrics["listing_page_successes"] = 60
            log_listing_collection_progress(
                "lopes",
                processed=60,
                total=60,
                success=metrics["listing_page_successes"],
                failures=metrics["listing_page_failures"],
            )

        logged_lines = [call.args[0] for call in print_mock.call_args_list]
        self.assertEqual(len(logged_lines), 3)
        self.assertIn("processed=1/60", logged_lines[0])
        self.assertIn("processed=25/60", logged_lines[1])
        self.assertIn("processed=60/60", logged_lines[2])
        self.assertTrue(all("label=lopes" in line for line in logged_lines))

    def test_listing_collection_progress_can_log_global_and_batch_status(self):
        with patch("builtins.print") as print_mock:
            log_listing_collection_progress(
                "quinto",
                processed=11501,
                total=11501,
                batch_processed=338,
                batch_total=338,
                success=11409,
                failures=92,
            )

        logged_line = print_mock.call_args.args[0]
        self.assertIn("listing_collection_progress", logged_line)
        self.assertIn("label=quinto", logged_line)
        self.assertIn("processed=11501/11501", logged_line)
        self.assertIn("batch_status=338/338", logged_line)
        self.assertIn("success=11409", logged_line)
        self.assertIn("failures=92", logged_line)

    def test_parse_olx_listing_page_html(self):
        html = """
        <script>
        window.dataLayer = [
            {"event":"gtm.load"},
            {
                "page": {
                    "pageType": "ad_detail",
                    "category": "Apartamentos",
                    "region": "Sao Paulo",
                    "state": "SP",
                    "detail": {
                        "adDate": 1775455573,
                        "lastUpdated": 1775459999,
                        "zipcode": "04567000",
                        "price": "13500",
                        "category_id": 1020,
                        "city_id": 3343,
                        "state_id": 11,
                        "parent_category_id": 1000,
                        "isEligibleAd": false,
                        "isShared": false,
                        "saveData": false,
                        "connectionType": "4g",
                        "cpuCores": 16,
                        "downloadLink": 10,
                        "lastInternalSource": null,
                        "olxPay": {"enabled": false},
                        "olxDelivery": {"enabled": false},
                        "vehicleReport": {"enabled": false},
                        "memoryStatus": {"deviceMemory": 8},
                        "vehicleTags": []
                    },
                    "pictures": [
                        {"original":"https://img1.jpg"},
                        {"original":"https://img2.jpg"}
                    ],
                    "adDetail": {
                        "sellerName": "Imoveis Pro",
                        "sellerPublicAccountId": "abc",
                        "professionalAd": true,
                        "municipality": "Sao Paulo",
                        "state": "SP",
                        "neighbourhood": "Vila Congonhas",
                        "street": "Rua Exemplo",
                        "body": "descricao completa",
                        "lat": -23.5,
                        "lon": -46.6,
                        "subCategory": "Apartamentos"
                    },
                    "adProperties": [
                        {"name":"size","value":"134m2"},
                        {"name":"rooms","value":"3"},
                        {"name":"bathrooms","value":"5 ou mais"},
                        {"name":"garage_spaces","value":"2"},
                        {"name":"condominium","value":"R$ 1.678"},
                        {"name":"iptu","value":"R$ 981"},
                        {"name":"real_estate_type","value":"Aluguel - apartamento padrao"}
                    ]
                }
            }
        ];
        </script>
        """
        parsed = parse_olx_listing_page_html(
            html,
            fallback_url="https://www.olx.com.br/imoveis/aluguel/sao-paulo/item-1234567",
        )
        self.assertEqual(parsed["listing_created_at"], "2026-04-06T06:06:13+00:00")
        self.assertEqual(parsed["listing_updated_at"], "2026-04-06T07:19:59+00:00")
        self.assertEqual(parsed["zipcode"], "04567-000")
        self.assertEqual(parsed["condo_fee_brl"], "R$ 1.678")
        self.assertEqual(parsed["iptu_brl"], "R$ 981")
        self.assertEqual(parsed["price"], "13500")
        self.assertEqual(parsed["description"], "descricao completa")
        self.assertEqual(parsed["listing_url"], "https://www.olx.com.br/imoveis/aluguel/sao-paulo/item-1234567")
        self.assertEqual(parsed["property_id"], "1234567")
        self.assertEqual(parsed["business_type"], "rent")
        self.assertEqual(parsed["lat"], -23.5)
        self.assertEqual(parsed["lon"], -46.6)
        self.assertEqual(parsed["property_type"], "Apartamentos")
        self.assertEqual(parsed["real_estate_type"], "Aluguel - apartamento padrao")

    def test_parse_olx_listing_page_html_extracts_zip_from_text_when_payload_omits_it(self):
        html = """
        <script>
        window.dataLayer = [
            {
                "page": {
                    "pageType": "ad_detail",
                    "adDetail": {
                        "street": "Rua Teste, CEP 04567-000",
                        "body": "Apartamento em Moema"
                    },
                    "adProperties": []
                }
            }
        ];
        </script>
        """
        parsed = parse_olx_listing_page_html(html)
        self.assertEqual(parsed["zipcode"], "04567-000")

    def test_parse_olx_listing_page_html_falls_back_to_meta_description_and_condominio_alias(self):
        html = """
        <meta property="og:description" content="  Descricao vinda da meta  ">
        <script>
        window.dataLayer = [
            {
                "page": {
                    "pageType": "ad_detail",
                    "adDetail": {},
                    "adProperties": [
                        {"name":"condominio","value":"R$ 2.100"}
                    ]
                }
            }
        ];
        </script>
        """
        parsed = parse_olx_listing_page_html(
            html,
            business_type="sale",
            fallback_url="https://sp.olx.com.br/sao-paulo-e-regiao/imoveis/exemplo-1493833725",
        )
        self.assertEqual(parsed["description"], "Descricao vinda da meta")
        self.assertEqual(parsed["condo_fee_brl"], "R$ 2.100")

    def test_parse_olx_listing_page_html_prefers_initial_data_full_description_over_truncated_payload(self):
        html = """
        <script>
        window.dataLayer = [
            {
                "page": {
                    "pageType": "ad_detail",
                    "detail": {
                        "description": "Descricao truncada..."
                    },
                    "adDetail": {},
                    "adProperties": []
                }
            }
        ];
        </script>
        <script id="initial-data" type="text/plain" data-json="{&quot;ad&quot;:{&quot;body&quot;:&quot;Linha 1&lt;br&gt;&lt;br&gt;Linha 2 completa&quot;}}"></script>
        """
        parsed = parse_olx_listing_page_html(
            html,
            business_type="sale",
            fallback_url="https://sp.olx.com.br/sao-paulo-e-regiao/imoveis/exemplo-1493833725",
        )
        self.assertEqual(parsed["description"], "Linha 1 Linha 2 completa")

    def test_parse_olx_listing_page_html_uses_record_business_type_when_detail_url_has_no_flow(self):
        html = """
        <script>
        window.dataLayer = [
            {
                "page": {
                    "pageType": "ad_detail",
                    "adDetail": {
                        "sellerName": "Corretora X"
                    },
                    "adProperties": []
                }
            }
        ];
        </script>
        """
        parsed = parse_olx_listing_page_html(
            html,
            business_type="sale",
            fallback_url="https://sp.olx.com.br/sao-paulo-e-regiao/imoveis/exemplo-1493833725",
        )
        self.assertEqual(parsed["property_id"], "1493833725")
        self.assertEqual(parsed["business_type"], "sale")

    def test_parse_quinto_listing_page_html(self):
        payload = {
            "props": {
                "pageProps": {
                    "initialState": {
                        "house": {
                            "constructionYear": 1998,
                            "forRent": True,
                            "forSale": False,
                            "houseInfo": {
                                "displayId": "2354331",
                                "status": "publicado",
                                "lastPublishedDate": "2025-10-06T14:26:20.000Z",
                                "type": "Apartamento",
                                "area": 90,
                                "bedrooms": 3,
                                "bathrooms": 2,
                                "parkingSpots": 2,
                                "suites": 1,
                                "acceptsPets": False,
                                "hasFurniture": False,
                                "homeProtection": 35,
                                "tenantServiceFee": 53,
                                "rentalGuarantee": {"minValue": 520, "maxValue": 1560},
                                "condoType": "IncluidoNoAluguel",
                                "condoPrice": 0,
                                "iptu": 34,
                                "address": {
                                    "street": "Rua dos Guatambus",
                                    "neighborhood": "Parada Inglesa",
                                    "city": "Sao Paulo",
                                    "stateName": "Sao Paulo",
                                    "zipCode": "02316-080",
                                    "lat": -23.4620491,
                                    "lng": -46.5932235,
                                },
                                "rangeFloor": {"min": 3, "max": 7},
                                "photos": [{"url": "original895054331.jpg"}],
                                "amenities": [{"text": "Banheira", "value": "NAO"}],
                                "comfortCommodities": [{"text": "Rua silenciosa", "value": "SIM"}],
                                "practicalityCommodities": [{"text": "Box", "value": "NAO"}],
                                "installations": [{"text": "Playground", "value": "NAO"}],
                                "placesNearby": [{"name": "Colegio"}],
                                "listings": [
                                    {
                                        "businessContext": "RENT",
                                        "firstPublicationDate": "2025-08-12T22:43:30.000+0000",
                                        "lastPublicationDate": "2025-10-06T14:26:20.000Z",
                                        "status": "ACTIVE",
                                        "houseAgents": [{"name": "Ayrton"}],
                                    }
                                ],
                                "generatedDescription": {
                                    "longDescription": "descricao longa",
                                    "shortRentDescription": "descricao curta",
                                },
                                "rentPrice": 2080,
                            }
                        }
                    }
                }
            }
        }
        html = f'<script id="__NEXT_DATA__" type="application/json">{json.dumps(payload)}</script>'
        parsed = parse_quinto_listing_page_html(
            html,
            business_type="rent",
            fallback_url="https://www.quintoandar.com.br/imovel/895054331/alugar/apartamento",
        )
        self.assertEqual(parsed["zipcode"], "02316-080")
        self.assertEqual(parsed["street"], "Rua dos Guatambus")
        self.assertEqual(parsed["neighbourhood"], "Parada Inglesa")
        self.assertEqual(parsed["tenant_service_fee_brl"], 53)
        self.assertEqual(parsed["rental_guarantee_min_brl"], 520)
        self.assertEqual(parsed["description"], "descricao longa")
        self.assertEqual(parsed["listing_url"], "https://www.quintoandar.com.br/imovel/895054331/alugar/apartamento")
        self.assertEqual(parsed["property_id"], "895054331")
        self.assertEqual(parsed["business_type"], "rent")
        self.assertEqual(parsed["lat"], -23.4620491)
        self.assertEqual(parsed["lon"], -46.5932235)
        self.assertEqual(parsed["construction_year"], 1998)
        self.assertEqual(parsed["range_floor_min"], 3)
        self.assertEqual(parsed["range_floor_max"], 7)
        self.assertTrue(parsed["for_rent"])
        self.assertFalse(parsed["for_sale"])
        self.assertEqual(parsed["property_type"], "Apartamento")
        self.assertEqual(parsed["total_area_m2"], 90)
        self.assertEqual(parsed["bedrooms"], 3)
        self.assertEqual(parsed["bathrooms"], 2)
        self.assertEqual(parsed["parking_spots"], 2)
        self.assertEqual(parsed["suites"], 1)
        self.assertEqual(parsed["rent_listing_created_at"], "2025-08-12T22:43:30.000+0000")
        self.assertEqual(parsed["rent_last_publication_date"], "2025-10-06T14:26:20.000Z")
        self.assertEqual(parsed["rent_listing_status"], "ACTIVE")
        self.assertIsNone(parsed["sale_listing_created_at"])
        self.assertIsNone(parsed["sale_last_publication_date"])
        self.assertIsNone(parsed["sale_listing_status"])

    def test_parse_quinto_listing_page_html_consolidates_dual_mode_regardless_of_listing_order(self):
        listings = [
            {
                "businessContext": "SALE",
                "firstPublicationDate": "2020-06-30T20:48:56.000+0000",
                "lastPublicationDate": "2026-04-10T10:00:00.000Z",
                "status": "SALE_ACTIVE",
            },
            {
                "businessContext": "RENT",
                "firstPublicationDate": "2019-03-10T03:05:11.000+0000",
                "lastPublicationDate": "2026-04-11T10:00:00.000Z",
                "status": "RENT_ACTIVE",
            },
        ]

        def build_html(raw_listings):
            payload = {
                "props": {
                    "pageProps": {
                        "initialState": {
                            "house": {
                                "forRent": True,
                                "forSale": True,
                                "houseInfo": {
                                    "displayId": "2884382",
                                    "address": {"street": "Rua Exemplo"},
                                    "listings": raw_listings,
                                    "generatedDescription": {"longDescription": "descricao consolidada"},
                                    "rentPrice": 12000,
                                    "salePrice": 4200000,
                                },
                            }
                        }
                    }
                }
            }
            return f'<script id="__NEXT_DATA__" type="application/json">{json.dumps(payload)}</script>'

        parsed_sale_first = parse_quinto_listing_page_html(
            build_html(listings),
            fallback_url="https://www.quintoandar.com.br/imovel/892884382/alugar/apartamento",
            grouped_business_types=["rent", "sale"],
            primary_business_type="rent",
        )
        parsed_rent_first = parse_quinto_listing_page_html(
            build_html(list(reversed(listings))),
            fallback_url="https://www.quintoandar.com.br/imovel/892884382/alugar/apartamento",
            grouped_business_types=["rent", "sale"],
            primary_business_type="rent",
        )

        for parsed in (parsed_sale_first, parsed_rent_first):
            self.assertEqual(parsed["business_type"], "rent|sale")
            self.assertEqual(parsed["listing_url"], "https://www.quintoandar.com.br/imovel/892884382/alugar/apartamento")
            self.assertEqual(parsed["rent_listing_created_at"], "2019-03-10T03:05:11.000+0000")
            self.assertEqual(parsed["sale_listing_created_at"], "2020-06-30T20:48:56.000+0000")
            self.assertEqual(parsed["rent_last_publication_date"], "2026-04-11T10:00:00.000Z")
            self.assertEqual(parsed["sale_last_publication_date"], "2026-04-10T10:00:00.000Z")
            self.assertEqual(parsed["rent_listing_status"], "RENT_ACTIVE")
            self.assertEqual(parsed["sale_listing_status"], "SALE_ACTIVE")
            self.assertEqual(parsed["rent_price_brl"], 12000)
            self.assertEqual(parsed["sale_price_brl"], 4200000)

    def test_parse_quinto_listing_page_html_reads_listing_flags_from_house_info(self):
        payload = {
            "props": {
                "pageProps": {
                    "initialState": {
                        "house": {
                            "houseInfo": {
                                "displayId": "2769477",
                                "address": {"street": "Rua Leoncio de Carvalho"},
                                "forRent": True,
                                "forSale": True,
                                "constructionYear": 1962,
                                "listings": [
                                    {
                                        "businessContext": "RENT",
                                        "firstPublicationDate": "2015-02-08T18:52:59.000+0000",
                                        "lastPublicationDate": "2026-04-13T16:53:20.000+0000",
                                        "status": "publicado",
                                    },
                                    {
                                        "businessContext": "SALE",
                                        "firstPublicationDate": "2023-07-31T17:15:14.000+0000",
                                        "lastPublicationDate": "2023-07-31T17:15:14.000+0000",
                                        "status": "publicado",
                                    },
                                ],
                                "generatedDescription": {"longDescription": "descricao atual"},
                                "rentPrice": 5100,
                                "salePrice": 990000,
                            }
                        }
                    }
                }
            }
        }
        html = f'<script id="__NEXT_DATA__" type="application/json">{json.dumps(payload)}</script>'

        parsed = parse_quinto_listing_page_html(
            html,
            fallback_url="https://www.quintoandar.com.br/imovel/892769477/alugar/apartamento-2-quartos-paraiso-sao-paulo",
            grouped_business_types=["rent", "sale"],
            primary_business_type="rent",
        )

        self.assertEqual(parsed["business_type"], "rent|sale")
        self.assertEqual(parsed["construction_year"], 1962)
        self.assertTrue(parsed["for_rent"])
        self.assertTrue(parsed["for_sale"])
        self.assertEqual(parsed["parking_spots"], 0)
        self.assertEqual(parsed["rent_listing_created_at"], "2015-02-08T18:52:59.000+0000")
        self.assertEqual(parsed["sale_listing_created_at"], "2023-07-31T17:15:14.000+0000")

    def test_parse_quinto_listing_page_html_falls_back_to_remarks(self):
        payload = {
            "props": {
                "pageProps": {
                    "initialState": {
                        "house": {
                            "houseInfo": {
                                "address": {"street": "Rua Exemplo"},
                                "generatedDescription": {},
                                "remarks": "Descricao manual do anuncio",
                            }
                        }
                    }
                }
            }
        }
        html = f'<script id="__NEXT_DATA__" type="application/json">{json.dumps(payload)}</script>'

        parsed = parse_quinto_listing_page_html(
            html,
            business_type="sale",
            fallback_url="https://www.quintoandar.com.br/imovel/892856360/comprar/apartamento",
        )

        self.assertEqual(parsed["description"], "Descricao manual do anuncio")
        self.assertEqual(parsed["long_description"], "Descricao manual do anuncio")

    def test_parse_quinto_listing_page_html_falls_back_to_structured_page_description(self):
        payload = {
            "props": {
                "pageProps": {
                    "initialState": {
                        "house": {
                            "houseInfo": {
                                "address": {"street": "Rua Exemplo"},
                                "generatedDescription": {},
                            }
                        }
                    }
                }
            }
        }
        html = (
            f'<meta name="description" content="Resumo curto da pagina">'
            f'<script type="application/ld+json">{json.dumps({"@type": "Apartment", "description": "Descricao estruturada"})}</script>'
            f'<script id="__NEXT_DATA__" type="application/json">{json.dumps(payload)}</script>'
        )

        parsed = parse_quinto_listing_page_html(
            html,
            business_type="sale",
            fallback_url="https://www.quintoandar.com.br/imovel/892884382/comprar/apartamento",
        )

        self.assertEqual(parsed["description"], "Descricao estruturada")
        self.assertEqual(parsed["long_description"], "Descricao estruturada")

    def test_quinto_normalize_address_value_coerces_coordinates(self):
        normalized = normalize_quinto_address_value(
            {
                "street": "Rua dos Guatambus",
                "neighborhood": "Parada Inglesa",
                "city": "Sao Paulo",
                "stateName": "Sao Paulo",
                "zipCode": "02316-080",
                "lat": " -23.4620491 ",
                "lng": "",
            }
        )

        self.assertEqual(normalized["zip_code"], "02316-080")
        self.assertEqual(normalized["neighbourhood"], "Parada Inglesa")
        self.assertEqual(normalized["lat"], -23.4620491)
        self.assertIsNone(normalized["lon"])

        normalized_numeric = normalize_quinto_address_value(
            {
                "street": "Rua dos Guatambus",
                "city": "Sao Paulo",
                "stateName": "Sao Paulo",
                "zipCode": "02316-080",
                "lat": -23.5,
                "lng": -46.6,
            }
        )

        self.assertEqual(normalized_numeric["lat"], -23.5)
        self.assertEqual(normalized_numeric["lon"], -46.6)

    def test_quinto_spider_tolerates_404_without_abort(self):
        metrics = init_metrics("quinto_test")
        records = [
            {"property_id": "1", "listing_url": "https://www.quintoandar.com.br/imovel/1/comprar"},
            {"property_id": "2", "listing_url": "https://www.quintoandar.com.br/imovel/2/comprar"},
        ]
        collector = {"records": [], "metrics": metrics}
        spider = QuintoListingsSpider(
            records=records,
            collector=collector,
            max_consecutive_failures=5,
            label="quinto",
        )
        payload = {
            "props": {
                "pageProps": {
                    "initialState": {
                        "house": {
                            "houseInfo": {
                                "displayId": "abc",
                                "status": "publicado",
                                "address": {"street": "Rua X", "city": "Sao Paulo", "zipCode": "01000-000"},
                                "photos": [{"url": "img.jpg"}],
                                "generatedDescription": {"longDescription": "desc"},
                            }
                        }
                    }
                }
            }
        }
        html = f'<script id="__NEXT_DATA__" type="application/json">{json.dumps(payload)}</script>'
        scheduled = list(spider.start_requests())
        first_request, second_request = scheduled
        success_response = HtmlResponse(
            url=str(first_request.url),
            request=first_request,
            body=html.encode("utf-8"),
            encoding="utf-8",
            status=200,
        )
        spider.parse_listing_response(success_response)

        not_found_response = HtmlResponse(
            url=str(second_request.url),
            request=second_request,
            body=b"",
            encoding="utf-8",
            status=404,
        )
        spider.parse_listing_response(not_found_response)

        self.assertEqual(len(collector["records"]), 1)
        self.assertEqual(collector["records"][0]["listing_url"], "https://www.quintoandar.com.br/imovel/1/comprar")
        self.assertEqual(collector["records"][0]["property_id"], "1")
        self.assertEqual(collector["records"][0]["business_type"], "sale")
        self.assertEqual(metrics["listing_page_requests"], 2)
        self.assertEqual(metrics["listing_page_successes"], 1)
        self.assertEqual(metrics["listing_page_failures"], 1)
        self.assertEqual(metrics["listing_page_not_founds"], 1)
        self.assertNotEqual(metrics["stop_reason"], "max_consecutive_failures")

    def test_quinto_spider_tolerates_500_without_abort(self):
        runtime_dir = Path("tests_runtime_quinto_500_ledger")
        shutil.rmtree(runtime_dir, ignore_errors=True)
        processed_path = runtime_dir / "resume" / "processed.partial.jsonl"
        partial_path = runtime_dir / "resume" / "records.partial.jsonl"
        state_path = runtime_dir / "resume" / "resume_state.json"
        records = [
            {"property_id": "1", "listing_url": "https://www.quintoandar.com.br/imovel/1/comprar"},
            {"property_id": "2", "listing_url": "https://www.quintoandar.com.br/imovel/2/comprar"},
        ]

        try:
            metrics = init_metrics("quinto_500_test")
            collector = {"records": [], "metrics": metrics}
            spider = QuintoListingsSpider(
                records=records,
                collector=collector,
                max_consecutive_failures=2,
                label="quinto",
                partial_jsonl_path=str(partial_path),
                processed_jsonl_path=str(processed_path),
                resume_state_path=str(state_path),
            )

            scheduled = list(spider.start_requests())
            for request in scheduled:
                response = HtmlResponse(
                    url=str(request.url),
                    request=request,
                    body=b"",
                    encoding="utf-8",
                    status=500,
                )
                spider.parse_listing_response(response)
            processed = load_jsonl_records(processed_path)
            pending = pending_listing_records(
                records,
                partial_jsonl_path=partial_path,
                processed_jsonl_path=processed_path,
            )
        finally:
            shutil.rmtree(runtime_dir, ignore_errors=True)

        self.assertEqual(metrics["listing_page_failures"], 2)
        self.assertEqual(metrics["listing_page_not_founds"], 2)
        self.assertEqual(metrics["pages_processed"], 2)
        self.assertEqual(spider.consecutive_failures, 0)
        self.assertNotEqual(metrics["stop_reason"], "max_consecutive_failures")
        self.assertEqual([item["status"] for item in processed], ["not_found", "not_found"])
        self.assertEqual([item["key"] for item in processed], ["id:1", "id:2"])
        self.assertEqual(pending, [])

    def test_parse_lopes_listing_page_html(self):
        payload = {
            "123": {
                "b": {
                    "product": {
                        "name": "Casa",
                        "description": "descricao lopes",
                        "address": {
                            "formatted": "Alameda dos Jurupis - Indianopolis - Sao Paulo/SP",
                            "street": "Alameda dos Jurupis",
                            "city": "Sao Paulo",
                            "neighborhood": "Indianopolis",
                            "state": "Sao Paulo",
                            "zipCode": "04088-001",
                        },
                        "attributes": [
                            {"type": "area_attr", "value": "70m2"},
                            {"type": "total_area_attr", "value": "70m2"},
                            {"type": "suite_attr", "value": "2"},
                        ],
                        "condominium": {
                            "id": "REC27345",
                            "name": "Condominio Teste",
                            "url": "/condominios/sp/teste",
                            "amenities": [{"name": "Piscina"}],
                        },
                        "features": [{"name": "Lavabo"}],
                        "pois": [{"name": "Gastronomia"}],
                        "photos": [{"url": "/REO1/IMG1.JPG"}],
                        "prices": {"condominium": 1731.58, "sale": 1200000, "rent": 0, "fullMonthlyPrice": 0},
                        "advertiser": {"name": "Lopes Office"},
                        "listingOwner": {"id": "648", "type": "INTERNAL"},
                        "seo": {"url": "/imovel/REO882470/venda-apartamento-2-quartos-sao-paulo-moema"},
                        "map": {"lat": -23.6031, "lng": -46.6752},
                    }
                }
            }
        }
        html = f'<script id="ng-state" type="application/json">{json.dumps(payload)}</script>'
        parsed = parse_lopes_listing_page_html(html)
        self.assertEqual(parsed["condominium_name"], "Condominio Teste")
        self.assertEqual(parsed["suites"], 2)
        self.assertIn("IMG1.JPG", parsed["gallery_urls_json"])
        self.assertEqual(parsed["advertiser_name"], "Lopes Office")
        self.assertEqual(parsed["property_id"], "REO882470")
        self.assertEqual(parsed["business_type"], "sale")
        self.assertEqual(parsed["property_type"], "Casa")
        self.assertTrue(pd.isna(parsed["zip_code"]))
        self.assertEqual(parsed["lat"], -23.6031)
        self.assertEqual(parsed["lon"], -46.6752)
        self.assertEqual(
            parsed["listing_url"],
            "https://www.lopes.com.br/imovel/REO882470/venda-apartamento-2-quartos-sao-paulo-moema",
        )

    def test_parse_lopes_listing_page_html_nested_and_partial(self):
        payload = {
            "123": {
                "x": {
                    "y": {
                        "b": {
                            "product": {
                                "description": "descricao parcial",
                                "address": "Rua Alternativa, 50",
                                "attributes": [],
                                "photos": [{"imageUrl": "/REO1/IMG2.JPG"}],
                                "listingOwner": {"id": "999", "type": "BROKER"},
                            }
                        }
                    }
                }
            }
        }
        html = f'<script id="ng-state" type="application/json">{json.dumps(payload)}</script>'
        parsed = parse_lopes_listing_page_html(
            html,
            fallback_url="https://www.lopes.com.br/imovel/REO999999/aluguel-apartamento-sao-paulo",
        )
        self.assertEqual(parsed["address"], "Rua Alternativa, 50")
        self.assertEqual(parsed["advertiser_id"], "999")
        self.assertIn("IMG2.JPG", parsed["gallery_urls_json"])
        self.assertEqual(parsed["property_id"], "REO999999")
        self.assertEqual(parsed["business_type"], "rent")
        self.assertTrue(pd.isna(parsed.get("property_type")))
        self.assertTrue(pd.isna(parsed["zip_code"]))
        self.assertTrue(pd.isna(parsed["lat"]))
        self.assertTrue(pd.isna(parsed["lon"]))

    def test_lopes_spider_collects_listing_data_from_public_html(self):
        metrics = init_metrics("lopes_test")
        collector = {"records": [], "metrics": metrics}
        spider = LopesListingsSpider(
            records=[{"listing_url": "https://www.lopes.com.br/imovel/REO123456/venda-apartamento-sao-paulo"}],
            collector=collector,
            max_consecutive_failures=5,
            label="lopes",
        )
        html = """
        <script id="ng-state" type="application/json">{
            "123": {"b": {"product": {
                "description": "descricao html",
                "address": {"formatted": "Rua Html, 10"},
                "photos": [{"url": "/REO1/HTML.JPG"}]
            }}}
        }</script>
        """
        request = next(iter(spider.start_requests()))
        response = HtmlResponse(
            url=str(request.url),
            request=request,
            body=html.encode("utf-8"),
            encoding="utf-8",
            status=200,
        )
        spider.parse_listing_response(response)

        self.assertEqual(collector["records"][0]["description"], "descricao html")
        self.assertEqual(collector["records"][0]["property_id"], "REO123456")
        self.assertEqual(collector["records"][0]["business_type"], "sale")
        self.assertNotIn("lastmod", collector["records"][0])
        self.assertEqual(metrics["listing_page_requests"], 1)
        self.assertEqual(metrics["listing_page_successes"], 1)
        self.assertEqual(metrics["listing_page_failures"], 0)

    def test_lopes_spider_aborts_after_five_consecutive_useful_failures(self):
        metrics = init_metrics("lopes_abort")
        records = [
            {"listing_url": f"https://www.lopes.com.br/imovel/REO{i}/venda-apartamento"}
            for i in range(5)
        ]
        collector = {"records": [], "metrics": metrics}
        spider = LopesListingsSpider(
            records=records,
            collector=collector,
            max_consecutive_failures=5,
            label="lopes",
        )

        scheduled = list(spider.start_requests())
        for index, request in enumerate(scheduled):
            response = HtmlResponse(
                url=str(request.url),
                request=request,
                body=b"<html></html>",
                encoding="utf-8",
                status=503,
            )
            if index < 4:
                spider.parse_listing_response(response)
            else:
                with self.assertRaises(CloseSpider):
                    spider.parse_listing_response(response)
        self.assertEqual(metrics["stop_reason"], "max_consecutive_failures")
        self.assertEqual(metrics["listing_page_failures"], 5)

    def test_olx_spider_start_enqueues_all_urls_and_reflects_parallel_limit(self):
        class DummySettings:
            def __init__(self, values):
                self.values = values

            def getint(self, key, default=0):
                return self.values.get(key, default)

        metrics = init_metrics("olx_parallel")
        collector = {"records": [], "metrics": metrics}
        spider = OlxListingsSpider(
            records=[
                {"listing_url": "https://www.olx.com.br/imoveis/venda/item-1-1234567"},
                {"listing_url": "https://www.olx.com.br/imoveis/venda/item-2-1234568"},
                {"listing_url": "https://www.olx.com.br/imoveis/venda/item-3-1234569"},
            ],
            collector=collector,
            max_consecutive_failures=5,
            max_parallel_requests=2,
            label="olx",
        )
        spider.crawler = Mock(settings=DummySettings({"CONCURRENT_REQUESTS": 2, "CONCURRENT_REQUESTS_PER_DOMAIN": 2}))

        scheduled = list(spider.start_requests())

        self.assertEqual(len(scheduled), 3)
        self.assertEqual(metrics["listing_page_requests"], 3)
        self.assertEqual(metrics["listing_page_in_flight_peak"], 2)

    def test_olx_spider_uses_discovery_url_as_resume_key_for_saved_details(self):
        runtime_dir = Path("tests_runtime_olx_url_resume_key")
        shutil.rmtree(runtime_dir, ignore_errors=True)
        partial_path = runtime_dir / "resume" / "records.partial.jsonl"
        processed_path = runtime_dir / "resume" / "processed.partial.jsonl"
        state_path = runtime_dir / "resume" / "resume_state.json"
        listing_url = "https://sp.olx.com.br/sao-paulo-e-regiao/imoveis/exemplo-1493833725"
        input_record = {"listing_url": listing_url, "business_type": "sale"}
        html = """
        <script>
        window.dataLayer = [
            {
                "page": {
                    "pageType": "ad_detail",
                    "adDetail": {"body": "Descricao", "municipality": "Sao Paulo"},
                    "adProperties": []
                }
            }
        ];
        </script>
        """

        try:
            metrics = init_metrics("olx_resume_key")
            collector = {"records": [], "metrics": metrics}
            spider = OlxListingsSpider(
                records=[input_record],
                collector=collector,
                max_consecutive_failures=5,
                label="olx",
                partial_jsonl_path=str(partial_path),
                processed_jsonl_path=str(processed_path),
                resume_state_path=str(state_path),
            )
            request = next(iter(spider.start_requests()))
            response = HtmlResponse(
                url=str(request.url),
                request=request,
                body=html.encode("utf-8"),
                encoding="utf-8",
                status=200,
            )

            spider.parse_listing_response(response)
            spider.closed("finished")

            saved_records = load_jsonl_records(partial_path)
            pending = pending_listing_records(
                [input_record],
                partial_jsonl_path=partial_path,
                processed_jsonl_path=processed_path,
            )
        finally:
            shutil.rmtree(runtime_dir, ignore_errors=True)

        self.assertEqual(saved_records[0]["key"], f"url:{listing_url}")
        self.assertEqual(saved_records[0]["property_id"], "1493833725")
        self.assertEqual(pending, [])

    def test_olx_spider_marks_410_as_terminal_removed_listing(self):
        runtime_dir = Path("tests_runtime_olx_410_terminal")
        shutil.rmtree(runtime_dir, ignore_errors=True)
        partial_path = runtime_dir / "resume" / "records.partial.jsonl"
        processed_path = runtime_dir / "resume" / "processed.partial.jsonl"
        state_path = runtime_dir / "resume" / "resume_state.json"
        listing_url = "https://sp.olx.com.br/sao-paulo-e-regiao/imoveis/removido-1493833725"
        input_record = {"listing_url": listing_url, "business_type": "sale"}

        try:
            metrics = init_metrics("olx_410")
            collector = {"records": [], "metrics": metrics}
            spider = OlxListingsSpider(
                records=[input_record],
                collector=collector,
                max_consecutive_failures=5,
                label="olx",
                partial_jsonl_path=str(partial_path),
                processed_jsonl_path=str(processed_path),
                resume_state_path=str(state_path),
            )
            request = next(iter(spider.start_requests()))
            response = HtmlResponse(
                url=str(request.url),
                request=request,
                body=b"",
                encoding="utf-8",
                status=410,
            )

            spider.parse_listing_response(response)
            processed = load_jsonl_records(processed_path)
            pending = pending_listing_records(
                [input_record],
                partial_jsonl_path=partial_path,
                processed_jsonl_path=processed_path,
            )
        finally:
            shutil.rmtree(runtime_dir, ignore_errors=True)

        self.assertEqual(processed[0]["status"], "not_found")
        self.assertEqual(processed[0]["key"], f"url:{listing_url}")
        self.assertEqual(metrics["listing_page_not_founds"], 1)
        self.assertEqual(pending, [])

    def test_lopes_spider_start_enqueues_all_urls_without_manual_replenish(self):
        class DummySettings:
            def __init__(self, values):
                self.values = values

            def getint(self, key, default=0):
                return self.values.get(key, default)

        metrics = init_metrics("lopes_parallel")
        collector = {"records": [], "metrics": metrics}
        spider = LopesListingsSpider(
            records=[
                {"listing_url": "https://www.lopes.com.br/imovel/REO1/venda-apartamento"},
                {"listing_url": "https://www.lopes.com.br/imovel/REO2/venda-apartamento"},
                {"listing_url": "https://www.lopes.com.br/imovel/REO3/venda-apartamento"},
            ],
            collector=collector,
            max_consecutive_failures=5,
            max_parallel_requests=2,
            label="lopes",
        )
        spider.crawler = Mock(settings=DummySettings({"CONCURRENT_REQUESTS": 2, "CONCURRENT_REQUESTS_PER_DOMAIN": 2}))

        scheduled = list(spider.start_requests())
        self.assertEqual(len(scheduled), 3)
        html = """
        <script id="ng-state" type="application/json">{
            "123": {"b": {"product": {
                "description": "descricao html",
                "address": {"formatted": "Rua Html, 10"},
                "photos": [{"url": "/REO1/HTML.JPG"}],
                "seo": {"url": "/imovel/REO1/venda-apartamento"}
            }}}
        }</script>
        """
        response = HtmlResponse(
            url=str(scheduled[0].url),
            request=scheduled[0],
            body=html.encode("utf-8"),
            encoding="utf-8",
            status=200,
        )

        spider.parse_listing_response(response)

        self.assertEqual(metrics["listing_page_requests"], 3)
        self.assertEqual(metrics["pages_processed"], 1)
        self.assertEqual(metrics["listing_page_in_flight_peak"], 2)

    def test_quinto_spider_abort_after_all_urls_are_enqueued(self):
        class DummySettings:
            def __init__(self, values):
                self.values = values

            def getint(self, key, default=0):
                return self.values.get(key, default)

        metrics = init_metrics("quinto_parallel_abort")
        collector = {"records": [], "metrics": metrics}
        spider = QuintoListingsSpider(
            records=[
                {"listing_url": f"https://www.quintoandar.com.br/imovel/{i}/comprar/apartamento"}
                for i in range(1, 8)
            ],
            collector=collector,
            max_consecutive_failures=2,
            max_parallel_requests=3,
            label="quinto",
        )
        spider.crawler = Mock(settings=DummySettings({"CONCURRENT_REQUESTS": 3, "CONCURRENT_REQUESTS_PER_DOMAIN": 3}))

        scheduled = list(spider.start_requests())
        self.assertEqual(len(scheduled), 7)

        first_response = HtmlResponse(
            url=str(scheduled[0].url),
            request=scheduled[0],
            body=b"",
            encoding="utf-8",
            status=503,
        )
        spider.parse_listing_response(first_response)

        second_response = HtmlResponse(
            url=str(scheduled[1].url),
            request=scheduled[1],
            body=b"",
            encoding="utf-8",
            status=503,
        )
        with self.assertRaises(CloseSpider):
            spider.parse_listing_response(second_response)

        self.assertEqual(metrics["stop_reason"], "max_consecutive_failures")
        self.assertEqual(metrics["listing_page_requests"], 7)
        self.assertEqual(metrics["pages_processed"], 2)

    def test_listings_spider_updates_incomplete_snapshot_periodically_and_on_close(self):
        runtime_dir = Path("tests_runtime_periodic_incomplete_snapshot")
        shutil.rmtree(runtime_dir, ignore_errors=True)
        output_path = runtime_dir / "quinto_listings.csv"
        parquet_path = runtime_dir / "quinto_listings.parquet"
        partial_path = runtime_dir / "resume" / "records.partial.jsonl"
        state_path = runtime_dir / "resume" / "resume_state.json"
        payload = {
            "props": {
                "pageProps": {
                    "initialState": {
                        "house": {
                            "houseInfo": {
                                "displayId": "abc",
                                "address": {"street": "Rua X", "city": "Sao Paulo", "zipCode": "01000-000"},
                                "photos": [],
                                "generatedDescription": {"longDescription": "desc"},
                            }
                        }
                    }
                }
            }
        }
        html = f'<script id="__NEXT_DATA__" type="application/json">{json.dumps(payload)}</script>'

        try:
            metrics = init_metrics("quinto_snapshot_test")
            collector = {"records": [], "metrics": metrics}
            spider = QuintoListingsSpider(
                records=[
                    {"listing_url": f"https://www.quintoandar.com.br/imovel/{i}/comprar/apartamento"}
                    for i in range(1, 4)
                ],
                collector=collector,
                max_consecutive_failures=5,
                label="quinto",
                output_path=str(output_path),
                parquet_output_path=str(parquet_path),
                partial_jsonl_path=str(partial_path),
                resume_state_path=str(state_path),
                incomplete_snapshot_batch_size=2,
            )

            scheduled = list(spider.start_requests())
            for request in scheduled[:2]:
                response = HtmlResponse(
                    url=str(request.url),
                    request=request,
                    body=html.encode("utf-8"),
                    encoding="utf-8",
                    status=200,
                )
                spider.parse_listing_response(response)

            incomplete_csv = output_path.with_name(f"incomplete_{output_path.name}")
            incomplete_parquet = parquet_path.with_name(f"incomplete_{parquet_path.name}")
            self.assertTrue(incomplete_csv.exists())
            self.assertTrue(incomplete_parquet.exists())
            self.assertEqual(len(pd.read_csv(incomplete_csv)), 2)
            self.assertEqual(len(pd.read_parquet(incomplete_parquet)), 2)

            response = HtmlResponse(
                url=str(scheduled[2].url),
                request=scheduled[2],
                body=html.encode("utf-8"),
                encoding="utf-8",
                status=200,
            )
            spider.parse_listing_response(response)
            spider.closed("shutdown")

            self.assertEqual(len(pd.read_csv(incomplete_csv)), 3)
            self.assertEqual(len(pd.read_parquet(incomplete_parquet)), 3)
            resume_state = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertEqual(resume_state["incomplete_output_rows"], 3)
            self.assertEqual(resume_state["incomplete_output_path"], str(incomplete_csv))
            self.assertEqual(resume_state["incomplete_parquet_output_path"], str(incomplete_parquet))
        finally:
            shutil.rmtree(runtime_dir, ignore_errors=True)

    def test_listings_build_scrapy_settings_use_autothrottle_curl_cffi_and_jobdir(self):
        for builder in (
            build_olx_listings_scrapy_settings,
            build_lopes_listings_scrapy_settings,
            build_quinto_listings_scrapy_settings,
        ):
            settings = builder(verbose=False, jobdir="artifacts/14-04-2026/collect_listings/demo/jobdir")
            self.assertTrue(settings["AUTOTHROTTLE_ENABLED"])
            self.assertEqual(settings["CONCURRENT_REQUESTS_PER_DOMAIN"], 1)
            self.assertEqual(settings["RETRY_TIMES"], 2)
            self.assertEqual(settings["JOBDIR"], "artifacts/14-04-2026/collect_listings/demo/jobdir")
            self.assertEqual(
                settings["DOWNLOAD_HANDLERS"]["https"],
                "scrapers.scrapy_support.CurlCffiDownloadHandler",
            )

    def test_run_scrapy_collection_uses_runner_and_returns_records(self):
        sample_records = [
            {
                "listing_url": "https://www.quintoandar.com.br/imovel/1/alugar",
                "listing_id": "1",
                "business_type": "rent",
            },
            {
                "listing_url": "https://www.quintoandar.com.br/imovel/1/comprar",
                "listing_id": "1",
                "business_type": "sale",
            },
        ]
        output_dir = Path("tests_runtime_quinto_scrapy_collection")
        if output_dir.exists():
            shutil.rmtree(output_dir)
        captured_records = []

        def fake_run_spider(spider_cls, *, settings, **kwargs):
            captured_records.extend(kwargs["records"])
            collector = kwargs["collector"]
            collector["records"].append(
                {
                    "listing_url": "https://www.quintoandar.com.br/imovel/1/alugar",
                    "property_id": "1",
                    "business_type": "rent|sale",
                }
            )
            collector["metrics"]["listing_page_requests"] = 1
            collector["metrics"]["listing_page_successes"] = 1
            collector["metrics"]["items_kept"] = 1

        try:
            with patch("scrapers.quinto_listings.run_spider", side_effect=fake_run_spider) as mocked:
                records, metrics = run_quinto_scrapy_collection(
                    records=sample_records,
                    label="quinto_test_runner",
                    max_consecutive_failures=5,
                    listings_output_path=str(output_dir / "quinto_listings.csv"),
                    listings_parquet_output_path=str(output_dir / "quinto_listings.parquet"),
                    resume_dir=str(output_dir / "resume"),
                )
        finally:
            shutil.rmtree(output_dir, ignore_errors=True)

        self.assertEqual(records[0]["property_id"], "1")
        self.assertEqual(records[0]["business_type"], "rent|sale")
        self.assertEqual(metrics["listing_page_successes"], 1)
        self.assertEqual(metrics["items_seen"], 1)
        self.assertTrue(mocked.called)
        self.assertIn("JOBDIR", mocked.call_args.kwargs["settings"])
        self.assertIn("partial_jsonl_path", mocked.call_args.kwargs)
        self.assertEqual(len(captured_records), 1)
        self.assertEqual(captured_records[0]["listing_url"], "https://www.quintoandar.com.br/imovel/1/alugar")
        self.assertEqual(captured_records[0]["primary_business_type"], "rent")
        self.assertEqual(captured_records[0]["grouped_business_types"], ["rent", "sale"])

    def test_run_scrapy_collection_batches_listing_requests_at_500(self):
        sample_records = [
            {
                "listing_url": f"https://www.quintoandar.com.br/imovel/{index}/comprar",
                "listing_id": str(index),
                "business_type": "sale",
            }
            for index in range(1, 502)
        ]
        output_dir = Path("tests_runtime_quinto_batching")
        shutil.rmtree(output_dir, ignore_errors=True)
        captured_batch_sizes: list[int] = []

        def fake_run_spider(spider_cls, *, settings, **kwargs):
            captured_batch_sizes.append(len(kwargs["records"]))
            collector = kwargs["collector"]
            for record in kwargs["records"]:
                collector["records"].append(
                    {
                        "listing_url": record["listing_url"],
                        "property_id": record["listing_id"],
                        "business_type": record["business_type"],
                    }
                )

        try:
            with patch("scrapers.quinto_listings.run_spider", side_effect=fake_run_spider):
                records, metrics = run_quinto_scrapy_collection(
                    records=sample_records,
                    label="quinto_batching",
                    max_consecutive_failures=5,
                    listings_output_path=str(output_dir / "quinto_listings.csv"),
                    listings_parquet_output_path=str(output_dir / "quinto_listings.parquet"),
                    resume_dir=str(output_dir / "resume"),
                )
        finally:
            shutil.rmtree(output_dir, ignore_errors=True)

        self.assertEqual(captured_batch_sizes, [500, 1])
        self.assertEqual(len(records), 501)
        self.assertEqual(metrics["pending_records"], 0)

    def test_run_scrapy_collection_resumes_from_partial_records_ignoring_stale_jobdir(self):
        sample_records = [
            {
                "listing_url": "https://www.quintoandar.com.br/imovel/1/comprar",
                "listing_id": "1",
                "business_type": "sale",
            },
            {
                "listing_url": "https://www.quintoandar.com.br/imovel/2/comprar",
                "listing_id": "2",
                "business_type": "sale",
            },
        ]
        output_dir = Path("tests_runtime_quinto_resume_ledger")
        shutil.rmtree(output_dir, ignore_errors=True)
        resume_paths = build_resume_paths(output_dir / "resume")
        resume_paths["partial_jsonl"].parent.mkdir(parents=True, exist_ok=True)
        resume_paths["partial_jsonl"].write_text(
            json.dumps(
                {
                    "listing_url": "https://www.quintoandar.com.br/imovel/1/comprar",
                    "property_id": "1",
                    "business_type": "sale",
                }
            )
            + "\n",
            encoding="utf-8",
        )
        resume_paths["current_jobdir"].mkdir(parents=True, exist_ok=True)
        (resume_paths["current_jobdir"] / "requests.seen").write_text("stale\n", encoding="utf-8")
        captured_records: list[dict[str, object]] = []

        def fake_run_spider(spider_cls, *, settings, **kwargs):
            captured_records.extend(kwargs["records"])
            collector = kwargs["collector"]
            for record in kwargs["records"]:
                collector["records"].append(
                    {
                        "listing_url": record["listing_url"],
                        "property_id": record["listing_id"],
                        "business_type": record["business_type"],
                    }
                )

        try:
            with patch("scrapers.quinto_listings.run_spider", side_effect=fake_run_spider):
                records, metrics = run_quinto_scrapy_collection(
                    records=sample_records,
                    label="quinto_resume_ledger",
                    max_consecutive_failures=5,
                    listings_output_path=str(output_dir / "quinto_listings.csv"),
                    listings_parquet_output_path=str(output_dir / "quinto_listings.parquet"),
                    resume_dir=str(output_dir / "resume"),
                )
        finally:
            shutil.rmtree(output_dir, ignore_errors=True)

        self.assertEqual([record["listing_id"] for record in captured_records], ["2"])
        self.assertEqual(len(records), 2)
        self.assertEqual(metrics["pending_records"], 0)

    def test_listing_spider_marks_404_as_terminal_without_output(self):
        runtime_dir = Path("tests_runtime_quinto_not_found_ledger")
        shutil.rmtree(runtime_dir, ignore_errors=True)
        processed_path = runtime_dir / "resume" / "processed.partial.jsonl"
        partial_path = runtime_dir / "resume" / "records.partial.jsonl"
        state_path = runtime_dir / "resume" / "resume_state.json"

        try:
            metrics = init_metrics("quinto_not_found")
            collector = {"records": [], "metrics": metrics}
            spider = QuintoListingsSpider(
                records=[
                    {
                        "listing_url": "https://www.quintoandar.com.br/imovel/404/comprar",
                        "listing_id": "404",
                        "business_type": "sale",
                    }
                ],
                collector=collector,
                max_consecutive_failures=5,
                label="quinto",
                partial_jsonl_path=str(partial_path),
                processed_jsonl_path=str(processed_path),
                resume_state_path=str(state_path),
            )
            request = next(iter(spider.start_requests()))
            response = HtmlResponse(
                url=str(request.url),
                request=request,
                body=b"",
                encoding="utf-8",
                status=404,
            )
            spider.parse_listing_response(response)
            processed = load_jsonl_records(processed_path)
        finally:
            shutil.rmtree(runtime_dir, ignore_errors=True)

        self.assertEqual(processed[0]["status"], "not_found")
        self.assertEqual(processed[0]["key"], "id:404")

    def test_listing_spider_keeps_transient_failures_pending(self):
        runtime_dir = Path("tests_runtime_quinto_transient_ledger")
        shutil.rmtree(runtime_dir, ignore_errors=True)
        processed_path = runtime_dir / "resume" / "processed.partial.jsonl"
        partial_path = runtime_dir / "resume" / "records.partial.jsonl"
        state_path = runtime_dir / "resume" / "resume_state.json"
        record = {
            "listing_url": "https://www.quintoandar.com.br/imovel/503/comprar",
            "listing_id": "503",
            "business_type": "sale",
        }

        try:
            metrics = init_metrics("quinto_transient")
            collector = {"records": [], "metrics": metrics}
            spider = QuintoListingsSpider(
                records=[record],
                collector=collector,
                max_consecutive_failures=5,
                label="quinto",
                partial_jsonl_path=str(partial_path),
                processed_jsonl_path=str(processed_path),
                resume_state_path=str(state_path),
            )
            request = next(iter(spider.start_requests()))
            response = HtmlResponse(
                url=str(request.url),
                request=request,
                body=b"",
                encoding="utf-8",
                status=503,
            )
            spider.parse_listing_response(response)
            pending = pending_listing_records(
                [record],
                partial_jsonl_path=partial_path,
                processed_jsonl_path=processed_path,
            )
        finally:
            shutil.rmtree(runtime_dir, ignore_errors=True)

        self.assertEqual(pending, [record])
        self.assertFalse(processed_path.exists())

    def _assert_incomplete_outputs_persisted_on_failure(
        self,
        *,
        runtime_dir: Path,
        module_path: str,
        collect_fn,
        label: str,
        listing_url: str,
        property_id: str,
    ) -> None:
        shutil.rmtree(runtime_dir, ignore_errors=True)
        input_path = runtime_dir / f"{label}_discovery.csv"
        output_path = runtime_dir / f"{label}_listings.csv"
        parquet_path = runtime_dir / f"{label}_listings.parquet"
        input_path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(
            [{"listing_url": listing_url, "business_type": "sale"}]
        ).to_csv(input_path, index=False)

        records = [
            {
                "listing_url": listing_url,
                "property_id": property_id,
                "business_type": "sale",
            }
        ]
        metrics = {
            "stop_reason": "max_consecutive_failures",
            "listing_page_failures": 5,
            "listing_page_successes": 1,
        }

        try:
            with patch(f"{module_path}.run_scrapy_collection", return_value=(records, metrics)), patch(
                f"{module_path}.cleanup_resume_runtime"
            ) as mocked_cleanup:
                with self.assertRaises(RuntimeError):
                    collect_fn(
                        input_path=str(input_path),
                        listings_output_path=str(output_path),
                        listings_parquet_output_path=str(parquet_path),
                        max_consecutive_failures=5,
                        label=label,
                        resume_dir=str(runtime_dir / "resume"),
                    )

            incomplete_csv = output_path.with_name(f"incomplete_{output_path.name}")
            incomplete_parquet = parquet_path.with_name(f"incomplete_{parquet_path.name}")
            self.assertTrue(incomplete_csv.exists())
            self.assertTrue(incomplete_parquet.exists())
            self.assertFalse(output_path.exists())
            self.assertFalse(parquet_path.exists())
            mocked_cleanup.assert_not_called()

            resume_state_path = runtime_dir / "resume" / "resume_state.json"
            resume_state = json.loads(resume_state_path.read_text(encoding="utf-8"))
            self.assertEqual(resume_state["status"], "failed_terminal")
            self.assertEqual(resume_state["incomplete_output_path"], str(incomplete_csv))
            self.assertEqual(resume_state["incomplete_parquet_output_path"], str(incomplete_parquet))
            self.assertEqual(resume_state["output_rows"], 1)
        finally:
            shutil.rmtree(runtime_dir, ignore_errors=True)

    def _assert_incomplete_outputs_removed_on_success(
        self,
        *,
        runtime_dir: Path,
        module_path: str,
        collect_fn,
        label: str,
        listing_url: str,
        property_id: str,
    ) -> None:
        shutil.rmtree(runtime_dir, ignore_errors=True)
        input_path = runtime_dir / f"{label}_discovery.csv"
        output_path = runtime_dir / f"{label}_listings.csv"
        parquet_path = runtime_dir / f"{label}_listings.parquet"
        input_path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(
            [{"listing_url": listing_url, "business_type": "sale"}]
        ).to_csv(input_path, index=False)
        incomplete_csv = output_path.with_name(f"incomplete_{output_path.name}")
        incomplete_parquet = parquet_path.with_name(f"incomplete_{parquet_path.name}")
        incomplete_csv.write_text("stale\n", encoding="utf-8")
        incomplete_parquet.write_text("stale\n", encoding="utf-8")
        resume_dir = runtime_dir / "resume"
        resume_paths = build_resume_paths(resume_dir)
        resume_paths["partial_jsonl"].parent.mkdir(parents=True, exist_ok=True)
        resume_paths["partial_jsonl"].write_text("stale\n", encoding="utf-8")
        resume_paths["processed_jsonl"].write_text("stale\n", encoding="utf-8")

        records = [
            {
                "listing_url": listing_url,
                "property_id": property_id,
                "business_type": "sale",
            }
        ]
        metrics = {
            "stop_reason": "completed",
            "listing_page_failures": 0,
            "listing_page_successes": 1,
        }

        try:
            with patch(f"{module_path}.run_scrapy_collection", return_value=(records, metrics)):
                result = collect_fn(
                    input_path=str(input_path),
                        listings_output_path=str(output_path),
                        listings_parquet_output_path=str(parquet_path),
                        max_consecutive_failures=5,
                        label=label,
                        resume_dir=str(resume_dir),
                    )

            self.assertEqual(result["output_rows"], 1)
            self.assertTrue(output_path.exists())
            self.assertTrue(parquet_path.exists())
            self.assertFalse(incomplete_csv.exists())
            self.assertFalse(incomplete_parquet.exists())
            self.assertFalse(resume_paths["partial_jsonl"].exists())
            self.assertFalse(resume_paths["processed_jsonl"].exists())
            resume_state = json.loads((runtime_dir / "resume" / "resume_state.json").read_text(encoding="utf-8"))
            self.assertIsNone(resume_state["incomplete_output_path"])
            self.assertIsNone(resume_state["incomplete_parquet_output_path"])
            self.assertEqual(resume_state["incomplete_output_rows"], 0)
        finally:
            shutil.rmtree(runtime_dir, ignore_errors=True)

    def test_lopes_collect_listings_persists_incomplete_outputs_on_failure(self):
        self._assert_incomplete_outputs_persisted_on_failure(
            runtime_dir=Path("tests_runtime_lopes_incomplete"),
            module_path="scrapers.lopes_listings",
            collect_fn=collect_lopes_listings_from_file,
            label="lopes",
            listing_url="https://www.lopes.com.br/imovel/REO1000001/venda-apartamento",
            property_id="REO1000001",
        )

    def test_olx_collect_listings_persists_incomplete_outputs_on_failure(self):
        self._assert_incomplete_outputs_persisted_on_failure(
            runtime_dir=Path("tests_runtime_olx_incomplete"),
            module_path="scrapers.olx_listings",
            collect_fn=collect_olx_listings_from_file,
            label="olx",
            listing_url="https://www.olx.com.br/imoveis/venda/item-123456789",
            property_id="123456789",
        )

    def test_quinto_collect_listings_persists_incomplete_outputs_on_failure(self):
        self._assert_incomplete_outputs_persisted_on_failure(
            runtime_dir=Path("tests_runtime_quinto_incomplete"),
            module_path="scrapers.quinto_listings",
            collect_fn=collect_quinto_listings_from_file,
            label="quinto",
            listing_url="https://www.quintoandar.com.br/imovel/894054331/comprar/apartamento",
            property_id="894054331",
        )

    def test_collect_listings_removes_incomplete_outputs_on_success(self):
        cases = [
            (
                "lopes",
                Path("tests_runtime_lopes_success_cleanup"),
                "scrapers.lopes_listings",
                collect_lopes_listings_from_file,
                "https://www.lopes.com.br/imovel/REO1000001/venda-apartamento",
                "REO1000001",
            ),
            (
                "olx",
                Path("tests_runtime_olx_success_cleanup"),
                "scrapers.olx_listings",
                collect_olx_listings_from_file,
                "https://www.olx.com.br/imoveis/venda/item-123456789",
                "123456789",
            ),
            (
                "quinto",
                Path("tests_runtime_quinto_success_cleanup"),
                "scrapers.quinto_listings",
                collect_quinto_listings_from_file,
                "https://www.quintoandar.com.br/imovel/894054331/comprar/apartamento",
                "894054331",
            ),
        ]
        for label, runtime_dir, module_path, collect_fn, listing_url, property_id in cases:
            with self.subTest(label=label):
                self._assert_incomplete_outputs_removed_on_success(
                    runtime_dir=runtime_dir,
                    module_path=module_path,
                    collect_fn=collect_fn,
                    label=label,
                    listing_url=listing_url,
                    property_id=property_id,
                )

class SchemaTests(unittest.TestCase):
    def test_normalize_uses_listing_id_and_record_business_type_for_unified_batches(self):
        scraped = {
            "lopes_discovery": [
                {
                    "listing_id": "REO1000001",
                    "listing_url": "https://www.lopes.com.br/imovel/REO1000001/venda-apartamento",
                    "business_type": "sale",
                    "lastmod": "2026-01-02",
                },
                {
                    "listing_id": "894054331",
                    "listing_url": "https://www.quintoandar.com.br/imovel/894054331/alugar/apartamento",
                    "business_type": "rent",
                    "lastmod": "2026-01-03",
                },
            ]
        }

        listings = normalize_data(scraped)

        self.assertEqual(listings.loc[0, "property_id"], "REO1000001")
        self.assertEqual(listings.loc[0, "business_type"], "sale")
        self.assertEqual(listings.loc[1, "property_id"], "894054331")
        self.assertEqual(listings.loc[1, "business_type"], "rent")

    def test_normalize_and_properties_include_new_fields(self):
        scraped = {
            "olx_listings": [
                {
                    "property_id": "1",
                    "listing_url": "https://example.com/1",
                    "title": "Casa",
                    "description": "desc",
                    "business_type": "sale",
                    "city": "Sao Paulo",
                    "state": "SP",
                    "neighbourhood": "Moema",
                    "zip_code": "04000-000",
                    "lat": -23.5,
                    "lon": -46.6,
                    "property_type": "Casa",
                    "area": "120",
                    "total_area_m2": 130,
                    "bedrooms": "3",
                    "bathrooms": "2",
                    "parking": "2",
                    "suites": "1",
                    "floor": "1",
                    "furnished": True,
                    "accepts_pets": True,
                    "sale_price_brl": 1000000,
                    "condo_fee_brl": 500,
                    "iptu_brl": 100,
                    "seller_name": "Imobiliaria",
                    "gallery_urls_json": json.dumps(["https://img"]),
                    "amenities_json": json.dumps([{"name": "Piscina"}]),
                    "installations_json": json.dumps([{"name": "Elevador"}]),
                    "condominium_name": "Condominio X",
                    "condominium_id": "abc",
                    "main_image_url": "https://img",
                    "images_count": 3,
                }
            ]
        }
        listings = normalize_data(scraped)
        properties, _ = build_unified_tables(listings)

        self.assertTrue(set(CANONICAL_COLUMNS).issubset(set(listings.columns)))
        self.assertIn("zip_code", properties.columns)
        self.assertIn("total_area_m2", properties.columns)
        self.assertIn("condominium_name", properties.columns)
        self.assertEqual(properties.loc[0, "zip_code"], "04000-000")

    def test_normalize_accepts_quinto_rent_sale_with_contextual_lifecycle_fields(self):
        scraped = {
            "quinto_listings": [
                {
                    "property_id": "892884382",
                    "listing_url": "https://www.quintoandar.com.br/imovel/892884382/alugar/apartamento",
                    "business_type": "rent|sale",
                    "city": "Sao Paulo",
                    "state": "Sao Paulo",
                    "zipcode": "01414-001",
                    "street": "Rua Exemplo",
                    "lat": -23.56,
                    "lon": -46.66,
                    "total_area_m2": 180,
                    "sale_price_brl": 4200000,
                    "rent_price_brl": 12000,
                    "rent_listing_created_at": "2019-03-10T03:05:11.000+0000",
                    "sale_listing_created_at": "2020-06-30T20:48:56.000+0000",
                    "rent_last_publication_date": "2026-04-11T10:00:00.000Z",
                    "sale_last_publication_date": "2026-04-10T10:00:00.000Z",
                    "rent_listing_status": "RENT_ACTIVE",
                    "sale_listing_status": "SALE_ACTIVE",
                }
            ]
        }

        listings = normalize_data(scraped)
        row = listings.iloc[0]

        self.assertEqual(row["business_type"], "rent|sale")
        self.assertEqual(row["sale_price_brl"], 4200000)
        self.assertEqual(row["rent_price_brl"], 12000)
        self.assertEqual(row["rent_listing_created_at"], "2019-03-10T03:05:11.000+0000")
        self.assertEqual(row["sale_listing_created_at"], "2020-06-30T20:48:56.000+0000")
        self.assertEqual(row["rent_last_publication_date"], "2026-04-11T10:00:00.000Z")
        self.assertEqual(row["sale_last_publication_date"], "2026-04-10T10:00:00.000Z")
        self.assertEqual(row["rent_listing_status"], "RENT_ACTIVE")
        self.assertEqual(row["sale_listing_status"], "SALE_ACTIVE")
        self.assertTrue(pd.isna(row["listing_created_at"]))

    def test_normalize_preserves_current_lopes_listing_url_and_drops_legacy_columns(self):
        scraped = {
            "lopes_listings": [
                {
                    "property_id": "REO1",
                    "listing_url": "https://www.lopes.com.br/imovel/REO1/venda-apartamento",
                    "business_type": "sale",
                    "city": "Sao Paulo",
                    "state": "Sao Paulo",
                    "street": "Rua Exemplo",
                    "total_area_m2": 90,
                    "sale_price_brl": 900000,
                }
            ]
        }

        listings = normalize_data(scraped)
        row = listings.iloc[0]

        self.assertEqual(row["listing_url"], "https://www.lopes.com.br/imovel/REO1/venda-apartamento")
        self.assertNotIn("listing_updated_at", listings.columns)
        self.assertNotIn("listing_status", listings.columns)
        self.assertNotIn("seller_name", listings.columns)
        self.assertNotIn("gallery_urls_json", listings.columns)
        self.assertNotIn("condominium_id", listings.columns)

    def test_build_unified_tables_treats_rent_sale_as_both_contexts(self):
        scraped = {
            "quinto_listings": [
                {
                    "property_id": "892884382",
                    "listing_url": "https://www.quintoandar.com.br/imovel/892884382/alugar/apartamento",
                    "business_type": "rent|sale",
                    "city": "Sao Paulo",
                    "state": "Sao Paulo",
                    "street": "Rua Exemplo",
                    "zipcode": "01414-001",
                    "total_area_m2": 180,
                    "bedrooms": 3,
                    "bathrooms": 4,
                    "sale_price_brl": 4200000,
                    "rent_price_brl": 12000,
                }
            ]
        }

        listings = normalize_data(scraped)
        properties, links = build_unified_tables(listings)

        self.assertEqual(len(properties), 1)
        self.assertTrue(properties.loc[0, "is_for_sale"])
        self.assertTrue(properties.loc[0, "is_for_rent"])
        self.assertEqual(properties.loc[0, "listing_mode"], "sale_rent")
        self.assertEqual(properties.loc[0, "sale_price_brl"], 4200000)
        self.assertEqual(properties.loc[0, "rent_price_brl"], 12000)
        self.assertEqual(links.loc[0, "business_type"], "rent|sale")

    def test_build_unified_tables_preserves_sale_and_rent_prices_from_separate_listings(self):
        listings = normalize_data(
            {
                "lopes_listings": [
                    {
                        "business_type": "sale",
                        "property_id": "REO1",
                        "listing_url": "https://www.lopes.com.br/imovel/REO1/venda",
                        "city": "Sao Paulo",
                        "neighbourhood": "Moema",
                        "street": "Rua Exemplo",
                        "state": "Sao Paulo",
                        "zipcode": "04000-000",
                        "area": 100.0,
                        "total_area_m2": 100.0,
                        "bedrooms": 2,
                        "bathrooms": 2,
                        "parking": 1,
                        "sale_price_brl": 1000000,
                    },
                    {
                        "business_type": "rent",
                        "property_id": "REO1",
                        "listing_url": "https://www.lopes.com.br/imovel/REO1/aluguel",
                        "city": "Sao Paulo",
                        "neighbourhood": "Moema",
                        "street": "Rua Exemplo",
                        "state": "Sao Paulo",
                        "zipcode": "04000-000",
                        "area": 100.0,
                        "total_area_m2": 100.0,
                        "bedrooms": 2,
                        "bathrooms": 2,
                        "parking": 1,
                        "rent_price_brl": 5000,
                    },
                ]
            }
        )

        properties, links = build_unified_tables(listings)

        self.assertEqual(len(properties), 1)
        self.assertEqual(properties.loc[0, "sale_price_brl"], 1000000)
        self.assertEqual(properties.loc[0, "rent_price_brl"], 5000)
        self.assertTrue(properties.loc[0, "is_for_sale"])
        self.assertTrue(properties.loc[0, "is_for_rent"])
        self.assertEqual(properties.loc[0, "listing_mode"], "sale_rent")
        self.assertEqual(len(links), 2)

    def test_build_unified_tables_uses_property_fallback_when_identity_fields_are_blank(self):
        listings = normalize_data(
            {
                "quinto_listings": [
                    {
                        "business_type": "sale",
                        "property_id": "892993961",
                        "listing_url": "https://www.quintoandar.com.br/imovel/892993961/comprar/apartamento",
                        "sale_price_brl": 700000,
                    },
                    {
                        "business_type": "rent",
                        "property_id": "892985301",
                        "listing_url": "https://www.quintoandar.com.br/imovel/892985301/alugar/apartamento",
                        "rent_price_brl": 4500,
                    },
                ]
            }
        )

        properties, links = build_unified_tables(listings)

        self.assertEqual(len(properties), 2)
        self.assertEqual(len(links), 2)
        self.assertTrue(properties["canonical_property_id"].str.contains("quinto\\|892993961|quinto\\|892985301").all())

    def test_normalize_backfills_selected_fields_from_amenities_json(self):
        scraped = {
            "olx_listings": [
                {
                    "property_id": "1",
                    "listing_url": "https://example.com/1",
                    "business_type": "rent",
                    "city": "Sao Paulo",
                    "state": "SP",
                    "neighbourhood": "Moema",
                    "price": "8000",
                    "amenities_json": json.dumps(
                        [
                            {"name": "condominium", "label": "Condominio", "value": "R$ 1.760", "values": None},
                            {"name": "iptu", "label": "IPTU", "value": "R$ 460", "values": None},
                            {"name": "size", "label": "Area util", "value": "96m?", "values": None},
                            {"name": "rooms", "label": "Quartos", "value": "2", "values": None},
                            {"name": "bathrooms", "label": "Banheiros", "value": "3", "values": None},
                            {"name": "garage_spaces", "label": "Vagas na garagem", "value": "1", "values": None},
                            {"name": "suites", "label": "Suites", "value": "2", "values": None},
                            {"name": "floor", "label": "Andar", "value": "7", "values": None},
                            {"name": "furnished", "label": "Mobiliado", "value": "Sim", "values": [{"label": "Mobiliado"}]},
                            {
                                "name": "re_complex_features",
                                "label": "Detalhes do condominio",
                                "value": "Permitido animais, Piscina",
                                "values": [{"label": "Permitido animais"}, {"label": "Piscina"}],
                            },
                            {"name": "real_estate_type", "label": "Tipo", "value": "Aluguel - apartamento padr?o", "values": None},
                        ]
                    ),
                }
            ]
        }
        listings = normalize_data(scraped)
        row = listings.iloc[0]

        self.assertEqual(row["condo_fee_brl"], 1760)
        self.assertEqual(row["iptu_brl"], 460)
        self.assertEqual(row["area_m2"], 96.0)
        self.assertEqual(row["total_area_m2"], 96.0)
        self.assertEqual(row["bedrooms"], 2)
        self.assertEqual(row["bathrooms"], 3)
        self.assertEqual(row["parking_spots"], 1)
        self.assertEqual(row["suites"], 2)
        self.assertEqual(row["floor"], 7)
        self.assertTrue(row["furnished"])
        self.assertTrue(row["has_furniture"])
        self.assertTrue(row["accepts_pets"])
        self.assertEqual(row["property_type"], "Apartamento")

    def test_normalize_does_not_backfill_dates_or_zip_code_from_amenities_json(self):
        scraped = {
            "olx_listings": [
                {
                    "property_id": "1",
                    "listing_url": "https://example.com/1",
                    "business_type": "rent",
                    "city": "Sao Paulo",
                    "state": "SP",
                    "neighbourhood": "Moema",
                    "amenities_json": json.dumps(
                        [
                            {"name": "size", "label": "Area util", "value": "96m?", "values": None},
                            {"name": "rooms", "label": "Quartos", "value": "2", "values": None},
                        ]
                    ),
                }
            ]
        }
        listings = normalize_data(scraped)
        row = listings.iloc[0]

        self.assertTrue(pd.isna(row["listing_created_at"]))
        self.assertTrue(pd.isna(row["zip_code"]))

    def test_normalize_prefers_street_over_address(self):
        scraped = {
            "lopes_listings": [
                {
                    "property_id": "REO1",
                    "listing_url": "https://www.lopes.com.br/imovel/REO1",
                    "business_type": "sale",
                    "city": "Sao Bernardo do Campo",
                    "state": "Sao Paulo",
                    "street": "Rua Tiradentes",
                    "address": "Rua Tiradentes - Santa Terezinha - Sao Bernardo do Campo/SP",
                }
            ]
        }

        listings = normalize_data(scraped)

        self.assertEqual(listings.loc[0, "address"], "Rua Tiradentes")

    def test_normalize_enriches_missing_address_and_coordinates_from_zip_code(self):
        runtime_dir = Path("tests_runtime_zip_enrichment")
        shutil.rmtree(runtime_dir, ignore_errors=True)
        runtime_dir.mkdir(parents=True, exist_ok=True)
        try:
            base_path = runtime_dir / "base_ceps.csv"
            base_path.write_text(
                "cep,logradouro,localidade,id_municipio,nome_municipio,sigla_uf,estabelecimentos,centroide\n"
                "04000000,Rua Teste,Moema,,Sao Paulo,SP,,POINT(-46.6 -23.5)\n",
                encoding="utf-8",
            )
            enricher = ZipCodeEnricher(cache_path=runtime_dir / "cache.json", base_ceps_path=base_path)
            scraped = {
                "olx_listings": [
                    {
                        "property_id": "1",
                        "listing_url": "https://example.com/1",
                        "business_type": "sale",
                        "city": "Sao Paulo",
                        "state": "SP",
                        "neighbourhood": "Moema",
                        "zip_code": "04000-000",
                        "area_m2": 120,
                        "bedrooms": 3,
                        "bathrooms": 2,
                    }
                ]
            }

            listings = normalize_data(scraped, zip_enricher=enricher)
        finally:
            shutil.rmtree(runtime_dir, ignore_errors=True)

        row = listings.iloc[0]
        self.assertEqual(row["address"], "Rua Teste")
        self.assertEqual(row["lat"], -23.5)
        self.assertEqual(row["lon"], -46.6)

    def test_normalize_does_not_overwrite_existing_address_or_coordinates_from_zip_code(self):
        runtime_dir = Path("tests_runtime_zip_enrichment_preserve")
        shutil.rmtree(runtime_dir, ignore_errors=True)
        runtime_dir.mkdir(parents=True, exist_ok=True)
        try:
            enricher = ZipCodeEnricher(cache_path=runtime_dir / "cache.json")
            scraped = {
                "olx_listings": [
                    {
                        "property_id": "1",
                        "listing_url": "https://example.com/1",
                        "business_type": "sale",
                        "city": "Sao Paulo",
                        "state": "SP",
                        "neighbourhood": "Moema",
                        "zip_code": "04000-000",
                        "street": "Rua Original",
                        "lat": -23.51,
                        "lon": -46.61,
                    }
                ]
            }

            listings = normalize_data(scraped, zip_enricher=enricher)
        finally:
            shutil.rmtree(runtime_dir, ignore_errors=True)

        row = listings.iloc[0]
        self.assertEqual(row["address"], "Rua Original")
        self.assertEqual(row["lat"], -23.51)
        self.assertEqual(row["lon"], -46.61)


class ZipCodeEnrichmentTests(unittest.TestCase):
    def test_zip_code_enricher_uses_persisted_cache_without_new_requests(self):
        runtime_dir = Path("tests_runtime_zipcode_cache")
        shutil.rmtree(runtime_dir, ignore_errors=True)
        runtime_dir.mkdir(parents=True, exist_ok=True)
        cache_path = runtime_dir / "zipcode_enrichment.json"
        frame = pd.DataFrame(
            [
                {
                    "zip_code": "04567-000",
                    "address": pd.NA,
                    "city": "Sao Paulo",
                    "state": "SP",
                    "lat": pd.NA,
                    "lon": pd.NA,
                }
            ]
        )

        try:
            base_path = runtime_dir / "base_ceps.csv"
            base_path.write_text(
                "cep,logradouro,localidade,id_municipio,nome_municipio,sigla_uf,estabelecimentos,centroide\n"
                "04567000,Rua Cacheada,Brooklin,,Sao Paulo,SP,,POINT(-46.6 -23.5)\n",
                encoding="utf-8",
            )
            enricher = ZipCodeEnricher(cache_path=cache_path, base_ceps_path=base_path)
            enriched_first = enricher.enrich_frame(frame)

            cached_enricher = ZipCodeEnricher(cache_path=cache_path)
            with patch.object(
                cached_enricher,
                "_lookup_local_sources",
                side_effect=AssertionError("cache deveria evitar nova consulta local"),
            ), patch.object(
                cached_enricher,
                "_lookup_brazilguide",
                side_effect=AssertionError("cache deveria evitar nova consulta ao BrazilGuide"),
            ):
                enriched_second = cached_enricher.enrich_frame(frame)
            self.assertTrue(cache_path.exists())
            self.assertEqual(enriched_first.loc[0, "address"], "Rua Cacheada")
            self.assertEqual(enriched_second.loc[0, "address"], "Rua Cacheada")
            self.assertEqual(cached_enricher.last_metrics["zip_codes_consulted"], 0)
            self.assertEqual(cached_enricher.last_metrics["zip_code_cache_hits"], 1)
        finally:
            shutil.rmtree(runtime_dir, ignore_errors=True)

    def test_zip_code_enricher_uses_cepaberto_when_base_ceps_misses_zip(self):
        runtime_dir = Path("tests_runtime_zipcode_cepaberto_fallback")
        shutil.rmtree(runtime_dir, ignore_errors=True)
        runtime_dir.mkdir(parents=True, exist_ok=True)
        base_path = runtime_dir / "base_ceps.csv"
        cepaberto_path = runtime_dir / "cepaberto.csv"
        base_path.write_text(
            "cep,logradouro,localidade,id_municipio,nome_municipio,sigla_uf,estabelecimentos,centroide\n",
            encoding="utf-8",
        )
        cepaberto_path.write_text(
            "cep,logradouro,localidade,id_municipio,nome_municipio,sigla_uf,estabelecimentos,centroide\n"
            "04567000,Rua Cep Aberto,Brooklin,,Sao Paulo,SP,,POINT(-46.7 -23.6)\n",
            encoding="utf-8",
        )

        try:
            enricher = ZipCodeEnricher(
                cache_path=runtime_dir / "cache.json",
                base_ceps_path=base_path,
                cepaberto_path=cepaberto_path,
            )
            payload, _ = enricher.resolve("04567-000", city="Sao Paulo", state="SP", neighbourhood="Brooklin")
        finally:
            shutil.rmtree(runtime_dir, ignore_errors=True)

        self.assertEqual(payload["street"], "Rua Cep Aberto")
        self.assertEqual(payload["lat"], -23.6)
        self.assertEqual(payload["lon"], -46.7)
        self.assertEqual(enricher.last_metrics["zip_code_cepaberto_hits"], 1)

    def test_zip_code_enricher_complements_base_ceps_with_cepaberto_missing_coordinates(self):
        runtime_dir = Path("tests_runtime_zipcode_cepaberto_complement")
        shutil.rmtree(runtime_dir, ignore_errors=True)
        runtime_dir.mkdir(parents=True, exist_ok=True)
        base_path = runtime_dir / "base_ceps.csv"
        cepaberto_path = runtime_dir / "cepaberto.csv"
        base_path.write_text(
            "cep,logradouro,localidade,id_municipio,nome_municipio,sigla_uf,estabelecimentos,centroide\n"
            "04567000,Rua Principal,Brooklin,,Sao Paulo,SP,,\n",
            encoding="utf-8",
        )
        cepaberto_path.write_text(
            "cep,logradouro,localidade,id_municipio,nome_municipio,sigla_uf,estabelecimentos,centroide\n"
            "04567000,Rua Divergente,Brooklin,,Sao Paulo,SP,,POINT(-46.7 -23.6)\n",
            encoding="utf-8",
        )

        try:
            enricher = ZipCodeEnricher(
                cache_path=runtime_dir / "cache.json",
                base_ceps_path=base_path,
                cepaberto_path=cepaberto_path,
            )
            payload, _ = enricher.resolve("04567-000", city="Sao Paulo", state="SP", neighbourhood="Brooklin")
        finally:
            shutil.rmtree(runtime_dir, ignore_errors=True)

        self.assertEqual(payload["street"], "Rua Principal")
        self.assertEqual(payload["lat"], -23.6)
        self.assertEqual(payload["lon"], -46.7)
        self.assertIn("base_ceps", payload["source"])
        self.assertIn("cepaberto", payload["source"])

    def test_zip_code_enricher_uses_brazilguide_when_local_record_has_no_street(self):
        runtime_dir = Path("tests_runtime_zipcode_local_without_street")
        shutil.rmtree(runtime_dir, ignore_errors=True)
        runtime_dir.mkdir(parents=True, exist_ok=True)
        base_path = runtime_dir / "base_ceps.csv"
        cepaberto_path = runtime_dir / "cepaberto.csv"
        base_path.write_text(
            "cep,logradouro,localidade,id_municipio,nome_municipio,sigla_uf,estabelecimentos,centroide\n"
            "01311000,,Bela Vista,,Sao Paulo,SP,,\n",
            encoding="utf-8",
        )
        cepaberto_path.write_text(
            "cep,logradouro,localidade,id_municipio,nome_municipio,sigla_uf,estabelecimentos,centroide\n",
            encoding="utf-8",
        )

        try:
            enricher = ZipCodeEnricher(
                cache_path=runtime_dir / "cache.json",
                base_ceps_path=base_path,
                cepaberto_path=cepaberto_path,
            )
            with patch.object(
                enricher,
                "_lookup_brazilguide",
                return_value={
                    "street": "Alameda São Paulo Golf",
                    "neighbourhood": "Portal da Concórdia Ii (jacaré)",
                    "city": "Cabreúva",
                    "state": "SP",
                    "lat": None,
                    "lon": None,
                    "source": "brazilguide",
                },
            ) as brazilguide_lookup, patch.object(
                enricher,
                "_geocode",
                return_value={"lat": -23.31, "lon": -47.13},
            ):
                payload, _ = enricher.resolve("01311-000", city="Sao Paulo", state="SP", neighbourhood="Bela Vista")
        finally:
            shutil.rmtree(runtime_dir, ignore_errors=True)

        self.assertEqual(brazilguide_lookup.call_count, 1)
        self.assertEqual(payload["street"], "Alameda São Paulo Golf")
        self.assertEqual(payload["lat"], -23.31)
        self.assertIn("base_ceps", payload["source"])
        self.assertIn("brazilguide", payload["source"])

    def test_zip_code_enricher_parses_brazilguide_html_and_appends_success(self):
        runtime_dir = Path("tests_runtime_zipcode_brazilguide")
        shutil.rmtree(runtime_dir, ignore_errors=True)
        runtime_dir.mkdir(parents=True, exist_ok=True)
        base_path = runtime_dir / "base_ceps.csv"
        cepaberto_path = runtime_dir / "cepaberto.csv"
        base_path.write_text(
            "cep,logradouro,localidade,id_municipio,nome_municipio,sigla_uf,estabelecimentos,centroide\n",
            encoding="utf-8",
        )
        cepaberto_path.write_text(
            "cep,logradouro,localidade,id_municipio,nome_municipio,sigla_uf,estabelecimentos,centroide\n",
            encoding="utf-8",
        )
        response = Mock()
        response.text = """
            <a href="/ceps/13318-304">13318-304</a>
            <p class="text-sm text-gray-500">Alameda São Paulo Golf · Portal da Concórdia Ii (jacaré) · Cabreúva/SP</p>
        """
        response.raise_for_status.return_value = None

        try:
            enricher = ZipCodeEnricher(
                cache_path=runtime_dir / "cache.json",
                base_ceps_path=base_path,
                cepaberto_path=cepaberto_path,
            )
            with patch.object(enricher, "_respect_brazilguide_rate_limit", return_value=None), patch.object(
                enricher,
                "_geocode",
                return_value={"lat": -23.3, "lon": -47.1},
            ):
                enricher.session.get = Mock(return_value=response)
                payload, _ = enricher.resolve(
                    "13318-304",
                    city="Cabreúva",
                    state="SP",
                    neighbourhood="Portal Concórdia II (Jacaré)",
                )
            saved_base = base_path.read_text(encoding="utf-8")
        finally:
            shutil.rmtree(runtime_dir, ignore_errors=True)

        self.assertEqual(payload["street"], "Alameda São Paulo Golf")
        self.assertEqual(payload["neighbourhood"], "Portal da Concórdia Ii (jacaré)")
        self.assertEqual(payload["city"], "Cabreúva")
        self.assertEqual(payload["lat"], -23.3)
        self.assertEqual(payload["lon"], -47.1)
        self.assertIn("13318304,Alameda São Paulo Golf,Portal da Concórdia Ii (jacaré)", saved_base)
        enricher.session.get.assert_called_once()
        self.assertEqual(enricher.session.get.call_args.args[0], "https://brazilguide.net/ceps")
        self.assertEqual(enricher.session.get.call_args.kwargs["params"], {"q": "13318-304"})

    def test_zip_code_enricher_negative_caches_brazilguide_failure(self):
        runtime_dir = Path("tests_runtime_zipcode_brazilguide_negative")
        shutil.rmtree(runtime_dir, ignore_errors=True)
        runtime_dir.mkdir(parents=True, exist_ok=True)
        cache_path = runtime_dir / "cache.json"
        base_path = runtime_dir / "base_ceps.csv"
        cepaberto_path = runtime_dir / "cepaberto.csv"
        for path in [base_path, cepaberto_path]:
            path.write_text(
                "cep,logradouro,localidade,id_municipio,nome_municipio,sigla_uf,estabelecimentos,centroide\n",
                encoding="utf-8",
            )
        frame = pd.DataFrame(
            [
                {
                    "zip_code": "01311-000",
                    "address": pd.NA,
                    "city": "Sao Paulo",
                    "state": "SP",
                    "neighbourhood": "Bela Vista",
                    "lat": pd.NA,
                    "lon": pd.NA,
                }
            ]
        )

        try:
            enricher = ZipCodeEnricher(cache_path=cache_path, base_ceps_path=base_path, cepaberto_path=cepaberto_path)
            with patch.object(enricher, "_lookup_brazilguide", return_value=None) as first_lookup:
                enricher.enrich_frame(frame)
            cached_enricher = ZipCodeEnricher(cache_path=cache_path, base_ceps_path=base_path, cepaberto_path=cepaberto_path)
            with patch.object(
                cached_enricher,
                "_lookup_brazilguide",
                side_effect=AssertionError("falha cacheada nao deve repetir BrazilGuide"),
            ):
                cached_enricher.enrich_frame(frame)
        finally:
            shutil.rmtree(runtime_dir, ignore_errors=True)

        self.assertEqual(first_lookup.call_count, 1)
        self.assertEqual(cached_enricher.last_metrics["zip_code_negative_cache_hits"], 1)

    def test_zip_code_enricher_ignores_legacy_cepbrasil_negative_cache(self):
        runtime_dir = Path("tests_runtime_zipcode_legacy_cepbrasil_negative")
        shutil.rmtree(runtime_dir, ignore_errors=True)
        runtime_dir.mkdir(parents=True, exist_ok=True)
        cache_path = runtime_dir / "cache.json"
        cache_path.write_text(
            json.dumps({"13318-304": {"failed": True, "source": "cepbrasil"}}, ensure_ascii=False),
            encoding="utf-8",
        )
        frame = pd.DataFrame(
            [
                {
                    "zip_code": "13318-304",
                    "address": pd.NA,
                    "city": "Cabreúva",
                    "state": "SP",
                    "neighbourhood": "Portal da Concórdia Ii (jacaré)",
                    "lat": pd.NA,
                    "lon": pd.NA,
                }
            ]
        )

        try:
            enricher = ZipCodeEnricher(cache_path=cache_path)
            with patch.object(
                enricher,
                "_lookup_brazilguide",
                return_value={
                    "street": "Alameda São Paulo Golf",
                    "neighbourhood": "Portal da Concórdia Ii (jacaré)",
                    "city": "Cabreúva",
                    "state": "SP",
                    "lat": None,
                    "lon": None,
                    "source": "brazilguide",
                },
            ) as lookup, patch.object(enricher, "_geocode", return_value=None):
                enriched = enricher.enrich_frame(frame)
        finally:
            shutil.rmtree(runtime_dir, ignore_errors=True)

        self.assertEqual(lookup.call_count, 1)
        self.assertEqual(enriched.loc[0, "address"], "Alameda São Paulo Golf")

    def test_zip_code_enricher_does_not_geocode_without_full_street_context(self):
        enricher = ZipCodeEnricher(cache_path=None)
        enricher.session.get = Mock(side_effect=AssertionError("Nominatim nao deveria ser chamado"))

        result = enricher._geocode(
            zip_code="04567-000",
            street=None,
            city="Sao Paulo",
            state="SP",
            neighbourhood="Brooklin",
        )

        self.assertIsNone(result)
        self.assertEqual(enricher.last_metrics["zip_code_geocode_skipped_incomplete_context"], 1)

    def test_zip_code_enricher_keeps_original_record_when_resolution_fails(self):
        runtime_dir = Path("tests_runtime_zipcode_failure")
        shutil.rmtree(runtime_dir, ignore_errors=True)
        runtime_dir.mkdir(parents=True, exist_ok=True)
        try:
            enricher = ZipCodeEnricher(cache_path=runtime_dir / "cache.json")
            frame = pd.DataFrame(
                [
                    {
                        "zip_code": "04567-000",
                        "address": pd.NA,
                        "city": "Sao Paulo",
                        "state": "SP",
                        "lat": pd.NA,
                        "lon": pd.NA,
                    }
                ]
            )
            with patch.object(enricher, "_lookup_brazilguide", return_value=None):
                enriched = enricher.enrich_frame(frame)
        finally:
            shutil.rmtree(runtime_dir, ignore_errors=True)

        self.assertTrue(pd.isna(enriched.loc[0, "address"]))
        self.assertTrue(pd.isna(enriched.loc[0, "lat"]))
        self.assertTrue(pd.isna(enriched.loc[0, "lon"]))

    def test_zip_code_enricher_retries_geocode_for_cached_entry_without_coordinates(self):
        runtime_dir = Path("tests_runtime_zipcode_partial_cache")
        shutil.rmtree(runtime_dir, ignore_errors=True)
        runtime_dir.mkdir(parents=True, exist_ok=True)
        cache_path = runtime_dir / "zipcode_enrichment.json"
        cache_path.write_text(
            json.dumps(
                {
                    "04567-000": {
                        "street": "Rua Cacheada",
                        "city": "Sao Paulo",
                        "state": "SP",
                        "lat": None,
                        "lon": None,
                    }
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        frame = pd.DataFrame(
            [
                    {
                        "zip_code": "04567-000",
                        "address": pd.NA,
                        "city": "Sao Paulo",
                        "state": "SP",
                        "neighbourhood": "Brooklin",
                        "lat": pd.NA,
                        "lon": pd.NA,
                    }
            ]
        )

        try:
            enricher = ZipCodeEnricher(cache_path=cache_path)
            with patch.object(
                enricher,
                "_geocode",
                return_value={"lat": -23.5, "lon": -46.6},
            ) as mocked_geocode:
                enriched = enricher.enrich_frame(frame)

            saved_cache = json.loads(cache_path.read_text(encoding="utf-8"))
        finally:
            shutil.rmtree(runtime_dir, ignore_errors=True)

        self.assertEqual(mocked_geocode.call_count, 1)
        self.assertEqual(enriched.loc[0, "address"], "Rua Cacheada")
        self.assertEqual(enriched.loc[0, "lat"], -23.5)
        self.assertEqual(enriched.loc[0, "lon"], -46.6)
        self.assertEqual(saved_cache["04567-000"]["lat"], -23.5)
        self.assertEqual(saved_cache["04567-000"]["lon"], -46.6)

    def test_zip_code_enricher_does_not_repeat_geocode_for_same_zip_in_one_run(self):
        runtime_dir = Path("tests_runtime_zipcode_dedup")
        shutil.rmtree(runtime_dir, ignore_errors=True)
        runtime_dir.mkdir(parents=True, exist_ok=True)
        frame = pd.DataFrame(
            [
                {
                    "zip_code": "04567-000",
                    "address": pd.NA,
                    "city": "Sao Paulo",
                    "state": "SP",
                    "neighbourhood": "Brooklin",
                    "lat": pd.NA,
                    "lon": pd.NA,
                },
                {
                    "zip_code": "04567-000",
                    "address": pd.NA,
                    "city": "Sao Paulo",
                    "state": "SP",
                    "neighbourhood": "Brooklin",
                    "lat": pd.NA,
                    "lon": pd.NA,
                },
            ]
        )

        try:
            base_path = runtime_dir / "base_ceps.csv"
            base_path.write_text(
                "cep,logradouro,localidade,id_municipio,nome_municipio,sigla_uf,estabelecimentos,centroide\n"
                "04567000,Rua Cacheada,Brooklin,,Sao Paulo,SP,,\n",
                encoding="utf-8",
            )
            enricher = ZipCodeEnricher(cache_path=runtime_dir / "cache.json", base_ceps_path=base_path)
            with patch.object(
                enricher,
                "_geocode",
                return_value={"lat": -23.5, "lon": -46.6},
            ) as mocked_geocode:
                enriched = enricher.enrich_frame(frame)
        finally:
            shutil.rmtree(runtime_dir, ignore_errors=True)

        self.assertEqual(mocked_geocode.call_count, 1)
        self.assertEqual(enriched.loc[0, "lat"], -23.5)
        self.assertEqual(enriched.loc[1, "lat"], -23.5)

    def test_zip_code_enricher_stops_geocode_after_rate_limit(self):
        runtime_dir = Path("tests_runtime_zipcode_rate_limit")
        shutil.rmtree(runtime_dir, ignore_errors=True)
        runtime_dir.mkdir(parents=True, exist_ok=True)
        try:
            enricher = ZipCodeEnricher(cache_path=runtime_dir / "cache.json")
            response = requests.Response()
            response.status_code = 429
            rate_limit_error = requests.HTTPError(response=response)

            with patch.object(
                enricher,
                "_respect_geocode_rate_limit",
                return_value=None,
            ), patch.object(
                enricher.session,
                "get",
                side_effect=rate_limit_error,
            ):
                first_attempt = enricher._geocode(
                    zip_code="04567-000",
                    street="Rua Teste",
                    city="Sao Paulo",
                    state="SP",
                    neighbourhood="Brooklin",
                )
                second_attempt = enricher._geocode(
                    zip_code="04567-001",
                    street="Rua Teste 2",
                    city="Sao Paulo",
                    state="SP",
                    neighbourhood="Brooklin",
                )
        finally:
            shutil.rmtree(runtime_dir, ignore_errors=True)

        self.assertIsNone(first_attempt)
        self.assertIsNone(second_attempt)
        self.assertEqual(enricher.last_metrics["zip_code_geocode_rate_limited"], 1)


class CsvWriterTests(unittest.TestCase):
    def test_save_parquet_records_normalizes_blank_strings_before_writing(self):
        output_dir = Path("tests_runtime_generic_parquet")
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / "records.parquet"
        try:
            save_parquet_records(
                [
                    {"listing_url": "https://example.com/1", "seller_professional": True},
                    {"listing_url": "https://example.com/2", "seller_professional": ""},
                ],
                str(output_path),
            )
            frame = pd.read_parquet(output_path)
        finally:
            shutil.rmtree(output_dir, ignore_errors=True)

        self.assertEqual(len(frame), 2)
        self.assertTrue(pd.isna(frame.loc[1, "seller_professional"]))

    def test_lopes_collect_discovery_to_file_writes_csv_and_parquet(self):
        output_dir = Path("tests_runtime_lopes_discovery")
        csv_path = output_dir / "lopes_discovery.csv"
        parquet_path = output_dir / "lopes_discovery.parquet"
        output_dir.mkdir(parents=True, exist_ok=True)
        try:
            with patch(
                "scrapers.lopes_discovery.collect_discovery_records",
                return_value=[
                    {
                        "listing_url": "https://www.lopes.com.br/imovel/REO1000001/venda-apartamento",
                        "lastmod": "2026-01-02",
                        "listing_id": "REO1000001",
                        "business_type": "sale",
                    }
                ],
            ):
                returned = collect_lopes_discovery_to_file(
                    output_path=str(csv_path),
                    parquet_output_path=str(parquet_path),
                    previous_output_path=None,
                )

            self.assertEqual(returned["output_path"], str(csv_path))
            self.assertEqual(returned["metrics"]["delta_rows"], 1)
            self.assertTrue(csv_path.exists())
            self.assertTrue(parquet_path.exists())
            df = pd.read_parquet(parquet_path)
            self.assertEqual(df.loc[0, "listing_id"], "REO1000001")
        finally:
            shutil.rmtree(output_dir, ignore_errors=True)

    def test_lopes_collect_discovery_to_file_writes_delta_only_and_preserves_empty_lastmod_semantics(self):
        output_dir = Path("tests_runtime_lopes_discovery_delta")
        csv_path = output_dir / "lopes_discovery.csv"
        parquet_path = output_dir / "lopes_discovery.parquet"
        previous_path = output_dir / "previous_lopes_discovery.csv"
        output_dir.mkdir(parents=True, exist_ok=True)
        try:
            pd.DataFrame(
                [
                    {
                        "business_type": "sale",
                        "lastmod": "2026-01-01",
                        "listing_id": "REO1000001",
                        "listing_url": "https://www.lopes.com.br/imovel/REO1000001/venda-apartamento",
                    },
                    {
                        "business_type": "sale",
                        "lastmod": "2026-01-02",
                        "listing_id": "REO1000002",
                        "listing_url": "https://www.lopes.com.br/imovel/REO1000002/venda-apartamento",
                    },
                    {
                        "business_type": "rent",
                        "lastmod": None,
                        "listing_id": "REO1000003",
                        "listing_url": "https://www.lopes.com.br/imovel/REO1000003/aluguel-apartamento",
                    },
                    {
                        "business_type": "sale",
                        "lastmod": "2026-01-05",
                        "listing_id": "REO1000005",
                        "listing_url": "https://www.lopes.com.br/imovel/REO1000005/venda-apartamento",
                    },
                ]
            ).to_csv(previous_path, index=False, encoding="utf-8-sig")

            with patch(
                "scrapers.lopes_discovery.collect_discovery_records",
                return_value=[
                    {
                        "listing_url": "https://www.lopes.com.br/imovel/REO1000001/venda-apartamento",
                        "lastmod": "2026-01-01",
                        "listing_id": "REO1000001",
                        "business_type": "sale",
                    },
                    {
                        "listing_url": "https://www.lopes.com.br/imovel/REO1000002/venda-apartamento",
                        "lastmod": "2026-01-03",
                        "listing_id": "REO1000002",
                        "business_type": "sale",
                    },
                    {
                        "listing_url": "https://www.lopes.com.br/imovel/REO1000003/aluguel-apartamento",
                        "lastmod": None,
                        "listing_id": "REO1000003",
                        "business_type": "rent",
                    },
                    {
                        "listing_url": "https://www.lopes.com.br/imovel/REO1000004/aluguel-apartamento",
                        "lastmod": "2026-01-04",
                        "listing_id": "REO1000004",
                        "business_type": "rent",
                    },
                    {
                        "listing_url": "https://www.lopes.com.br/imovel/REO1000005/venda-apartamento",
                        "lastmod": None,
                        "listing_id": "REO1000005",
                        "business_type": "sale",
                    },
                ],
            ):
                returned = collect_lopes_discovery_to_file(
                    output_path=str(csv_path),
                    parquet_output_path=str(parquet_path),
                    previous_output_path=str(previous_path),
                )

            frame = pd.read_csv(csv_path)
            self.assertEqual(
                frame["listing_url"].tolist(),
                [
                    "https://www.lopes.com.br/imovel/REO1000004/aluguel-apartamento",
                    "https://www.lopes.com.br/imovel/REO1000005/venda-apartamento",
                ],
            )
            self.assertEqual(returned["metrics"]["new_rows"], 1)
            self.assertEqual(returned["metrics"]["updated_rows"], 1)
            self.assertEqual(returned["metrics"]["unchanged_rows"], 1)
            self.assertEqual(returned["metrics"]["watermark_lastmod"], "2026-01-05")
            self.assertEqual(returned["metrics"]["overlap_start_lastmod"], "2026-01-04")
            self.assertEqual(returned["metrics"]["window_filtered_rows"], 2)
        finally:
            shutil.rmtree(output_dir, ignore_errors=True)

    def test_lopes_discovery_uses_latest_non_empty_prior_snapshot(self):
        runtime_root = Path("tests_runtime_lopes_discovery_previous_non_empty")
        output_path = runtime_root / "raw" / "13-01-2026" / "lopes" / "lopes_discovery.csv"
        parquet_path = output_path.with_suffix(".parquet")
        first_snapshot_path = runtime_root / "raw" / "11-01-2026" / "lopes" / "lopes_discovery.csv"
        empty_snapshot_path = runtime_root / "raw" / "12-01-2026" / "lopes" / "lopes_discovery.csv"
        try:
            first_snapshot_path.parent.mkdir(parents=True, exist_ok=True)
            empty_snapshot_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            pd.DataFrame(
                [
                    {
                        "business_type": "sale",
                        "lastmod": "2026-01-01",
                        "listing_id": "REO1000001",
                        "listing_url": "https://www.lopes.com.br/imovel/REO1000001/venda-apartamento",
                    }
                ]
            ).to_csv(first_snapshot_path, index=False, encoding="utf-8-sig")
            empty_snapshot_path.write_text(
                "business_type,lastmod,listing_id,listing_url\n",
                encoding="utf-8-sig",
            )

            found = find_previous_output(
                run_date="13-01-2026",
                source="lopes",
                filename="lopes_discovery.csv",
                project_root=runtime_root,
            )

            with patch(
                "scrapers.lopes_discovery.collect_discovery_records",
                return_value=[
                    {
                        "listing_url": "https://www.lopes.com.br/imovel/REO1000001/venda-apartamento",
                        "lastmod": "2026-01-01",
                        "listing_id": "REO1000001",
                        "business_type": "sale",
                    }
                ],
            ), patch(
                "scrapers.lopes_discovery.find_previous_output",
                return_value=first_snapshot_path.resolve(),
            ):
                returned = collect_lopes_discovery_to_file(
                    output_path=str(output_path),
                    parquet_output_path=str(parquet_path),
                    previous_output_path=None,
                )

            frame = pd.read_csv(output_path)
        finally:
            shutil.rmtree(runtime_root, ignore_errors=True)

        self.assertEqual(found, first_snapshot_path.resolve())
        self.assertTrue(frame.empty)
        self.assertEqual(returned["metrics"]["delta_rows"], 0)
        self.assertEqual(returned["metrics"]["previous_output_path"], str(first_snapshot_path.resolve()))

    def test_quinto_collect_discovery_to_file_writes_csv_and_parquet(self):
        output_dir = Path("tests_runtime_quinto_discovery")
        csv_path = output_dir / "quinto_discovery.csv"
        parquet_path = output_dir / "quinto_discovery.parquet"
        output_dir.mkdir(parents=True, exist_ok=True)
        try:
            with patch(
                "scrapers.quinto_discovery.collect_discovery_records",
                return_value=[
                    {
                        "listing_url": "https://www.quintoandar.com.br/imovel/894054331/alugar/apartamento",
                        "lastmod": "2026-02-01",
                        "listing_id": "894054331",
                        "business_type": "rent",
                    }
                ],
            ):
                returned = collect_quinto_discovery_to_file(
                    output_path=str(csv_path),
                    parquet_output_path=str(parquet_path),
                    previous_output_path=None,
                )

            self.assertEqual(returned["output_path"], str(csv_path))
            self.assertEqual(returned["metrics"]["delta_rows"], 1)
            self.assertTrue(csv_path.exists())
            self.assertTrue(parquet_path.exists())
            df = pd.read_parquet(parquet_path)
            self.assertEqual(df.loc[0, "listing_id"], "894054331")
        finally:
            shutil.rmtree(output_dir, ignore_errors=True)

    def test_quinto_collect_discovery_to_file_writes_header_only_when_delta_is_empty(self):
        output_dir = Path("tests_runtime_quinto_discovery_empty_delta")
        csv_path = output_dir / "quinto_discovery.csv"
        parquet_path = output_dir / "quinto_discovery.parquet"
        previous_path = output_dir / "previous_quinto_discovery.csv"
        output_dir.mkdir(parents=True, exist_ok=True)
        try:
            pd.DataFrame(
                [
                    {
                        "business_type": "rent",
                        "lastmod": "2026-02-01",
                        "listing_id": "894054331",
                        "listing_url": "https://www.quintoandar.com.br/imovel/894054331/alugar/apartamento",
                    }
                ]
            ).to_csv(previous_path, index=False, encoding="utf-8-sig")

            with patch(
                "scrapers.quinto_discovery.collect_discovery_records",
                return_value=[
                    {
                        "listing_url": "https://www.quintoandar.com.br/imovel/894054331/alugar/apartamento",
                        "lastmod": "2026-02-01",
                        "listing_id": "894054331",
                        "business_type": "rent",
                    }
                ],
            ):
                returned = collect_quinto_discovery_to_file(
                    output_path=str(csv_path),
                    parquet_output_path=str(parquet_path),
                    previous_output_path=str(previous_path),
                )

            frame = pd.read_csv(csv_path)
            self.assertTrue(frame.empty)
            self.assertEqual(
                frame.columns.tolist(),
                ["business_type", "lastmod", "listing_id", "listing_url"],
            )
            self.assertEqual(returned["metrics"]["delta_rows"], 0)
            self.assertEqual(returned["metrics"]["unchanged_rows"], 1)
        finally:
            shutil.rmtree(output_dir, ignore_errors=True)

    def test_quinto_discovery_filters_old_urls_when_previous_state_is_delta(self):
        output_dir = Path("tests_runtime_quinto_discovery_delta_watermark")
        csv_path = output_dir / "raw" / "29-04-2026" / "quinto" / "quinto_discovery.csv"
        parquet_path = csv_path.with_suffix(".parquet")
        initial_snapshot_path = output_dir / "raw" / "24-04-2026" / "quinto" / "quinto_discovery.csv"
        previous_delta_path = output_dir / "raw" / "28-04-2026" / "quinto" / "quinto_discovery.csv"
        try:
            initial_snapshot_path.parent.mkdir(parents=True, exist_ok=True)
            previous_delta_path.parent.mkdir(parents=True, exist_ok=True)
            csv_path.parent.mkdir(parents=True, exist_ok=True)
            pd.DataFrame(
                [
                    {
                        "business_type": "rent",
                        "lastmod": "2026-04-23",
                        "listing_id": "old-1",
                        "listing_url": "https://www.quintoandar.com.br/imovel/old-1/alugar/apartamento",
                    }
                ]
            ).to_csv(initial_snapshot_path, index=False, encoding="utf-8-sig")
            pd.DataFrame(
                [
                    {
                        "business_type": "rent",
                        "lastmod": "2026-04-28",
                        "listing_id": "recent-1",
                        "listing_url": "https://www.quintoandar.com.br/imovel/recent-1/alugar/apartamento",
                    }
                ]
            ).to_csv(previous_delta_path, index=False, encoding="utf-8-sig")

            with patch(
                "scrapers.quinto_discovery.collect_discovery_records",
                return_value=[
                    {
                        "listing_url": "https://www.quintoandar.com.br/imovel/old-1/alugar/apartamento",
                        "lastmod": "2026-04-23",
                        "listing_id": "old-1",
                        "business_type": "rent",
                    },
                    {
                        "listing_url": "https://www.quintoandar.com.br/imovel/recent-1/alugar/apartamento",
                        "lastmod": "2026-04-28",
                        "listing_id": "recent-1",
                        "business_type": "rent",
                    },
                    {
                        "listing_url": "https://www.quintoandar.com.br/imovel/new-1/alugar/apartamento",
                        "lastmod": "2026-04-29",
                        "listing_id": "new-1",
                        "business_type": "rent",
                    },
                ],
            ), patch(
                "scrapers.quinto_discovery.find_previous_output",
                return_value=previous_delta_path.resolve(),
            ):
                returned = collect_quinto_discovery_to_file(
                    output_path=str(csv_path),
                    parquet_output_path=str(parquet_path),
                    previous_output_path=None,
                )

            frame = pd.read_csv(csv_path)
        finally:
            shutil.rmtree(output_dir, ignore_errors=True)

        self.assertEqual(
            frame["listing_url"].tolist(),
            ["https://www.quintoandar.com.br/imovel/new-1/alugar/apartamento"],
        )
        self.assertEqual(returned["metrics"]["previous_output_path"], str(previous_delta_path.resolve()))
        self.assertEqual(returned["metrics"]["watermark_lastmod"], "2026-04-28")
        self.assertEqual(returned["metrics"]["overlap_start_lastmod"], "2026-04-27")
        self.assertEqual(returned["metrics"]["window_filtered_rows"], 1)
        self.assertEqual(returned["metrics"]["unchanged_rows"], 1)
        self.assertEqual(returned["metrics"]["new_rows"], 1)

    def test_quinto_discovery_uses_latest_non_empty_prior_snapshot(self):
        runtime_root = Path("tests_runtime_quinto_discovery_previous_non_empty")
        output_path = runtime_root / "raw" / "13-02-2026" / "quinto" / "quinto_discovery.csv"
        parquet_path = output_path.with_suffix(".parquet")
        first_snapshot_path = runtime_root / "raw" / "11-02-2026" / "quinto" / "quinto_discovery.csv"
        empty_snapshot_path = runtime_root / "raw" / "12-02-2026" / "quinto" / "quinto_discovery.csv"
        try:
            first_snapshot_path.parent.mkdir(parents=True, exist_ok=True)
            empty_snapshot_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            pd.DataFrame(
                [
                    {
                        "business_type": "rent",
                        "lastmod": "2026-02-01",
                        "listing_id": "894054331",
                        "listing_url": "https://www.quintoandar.com.br/imovel/894054331/alugar/apartamento",
                    }
                ]
            ).to_csv(first_snapshot_path, index=False, encoding="utf-8-sig")
            empty_snapshot_path.write_text(
                "business_type,lastmod,listing_id,listing_url\n",
                encoding="utf-8-sig",
            )

            found = find_previous_output(
                run_date="13-02-2026",
                source="quinto",
                filename="quinto_discovery.csv",
                project_root=runtime_root,
            )

            with patch(
                "scrapers.quinto_discovery.collect_discovery_records",
                return_value=[
                    {
                        "listing_url": "https://www.quintoandar.com.br/imovel/894054331/alugar/apartamento",
                        "lastmod": "2026-02-01",
                        "listing_id": "894054331",
                        "business_type": "rent",
                    }
                ],
            ), patch(
                "scrapers.quinto_discovery.find_previous_output",
                return_value=first_snapshot_path.resolve(),
            ):
                returned = collect_quinto_discovery_to_file(
                    output_path=str(output_path),
                    parquet_output_path=str(parquet_path),
                    previous_output_path=None,
                )

            frame = pd.read_csv(output_path)
        finally:
            shutil.rmtree(runtime_root, ignore_errors=True)

        self.assertEqual(found, first_snapshot_path.resolve())
        self.assertTrue(frame.empty)
        self.assertEqual(returned["metrics"]["delta_rows"], 0)
        self.assertEqual(returned["metrics"]["previous_output_path"], str(first_snapshot_path.resolve()))

    def test_lopes_wrapper_derives_parquet_output_path_from_output_path(self):
        with patch("scrapers.lopes.collect_discovery_to_file", return_value={"output_path": "ok", "metrics": {}}) as mocked:
            lopes_module.collect_discovery(output_path="raw/01-01-2000/lopes/custom.csv")

        self.assertEqual(mocked.call_args.kwargs["parquet_output_path"], "raw\\01-01-2000\\lopes\\custom.parquet")

    def test_quinto_wrapper_derives_parquet_output_path_from_output_path(self):
        with patch("scrapers.quinto.collect_discovery_to_file", return_value={"output_path": "ok", "metrics": {}}) as mocked:
            quinto_module.collect_discovery(output_path="raw/01-01-2000/quinto/custom.csv")

        self.assertEqual(mocked.call_args.kwargs["parquet_output_path"], "raw\\01-01-2000\\quinto\\custom.parquet")

    def test_quinto_save_csv_accepts_heterogeneous_records(self):
        output_dir = Path("tests_runtime_quinto_csv")
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / "quinto.csv"
        try:
            save_quinto_csv(
                [
                    {"property_id": "1", "listing_url": "https://example.com/1"},
                    {"property_id": "2", "listing_url": "https://example.com/2", "display_id": "abc", "description": "desc"},
                ],
                str(output_path),
            )
            contents = output_path.read_text(encoding="utf-8-sig")
        finally:
            shutil.rmtree(output_dir, ignore_errors=True)

        self.assertIn("display_id", contents)
        self.assertIn("description", contents)

    def test_quinto_save_parquet_tolerates_empty_coordinate_strings_from_payload(self):
        output_dir = Path("tests_runtime_quinto_parquet")
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / "quinto.parquet"
        payload = {
            "props": {
                "pageProps": {
                    "initialState": {
                        "house": {
                            "houseInfo": {
                                "displayId": "2354331",
                                "status": "publicado",
                                "address": {
                                    "street": "Rua dos Guatambus",
                                    "city": "Sao Paulo",
                                    "stateName": "Sao Paulo",
                                    "zipCode": "02316-080",
                                    "lat": "",
                                    "lng": " ",
                                },
                                "photos": [{"url": "original895054331.jpg"}],
                                "generatedDescription": {"longDescription": "descricao longa"},
                            }
                        }
                    }
                }
            }
        }

        try:
            html = f'<script id="__NEXT_DATA__" type="application/json">{json.dumps(payload)}</script>'
            parsed = parse_quinto_listing_page_html(
                html,
                business_type="rent",
                fallback_url="https://www.quintoandar.com.br/imovel/895054331/alugar/apartamento",
            )
            save_quinto_parquet([parsed], str(output_path))
            frame = pd.read_parquet(output_path)
        finally:
            shutil.rmtree(output_dir, ignore_errors=True)

        self.assertEqual(len(frame), 1)
        self.assertTrue(pd.isna(frame.loc[0, "lat"]))
        self.assertTrue(pd.isna(frame.loc[0, "lon"]))

if __name__ == "__main__":
    unittest.main()

