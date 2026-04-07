from __future__ import annotations

import json
import shutil
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import pandas as pd
import requests

from pipelines.dedupe import build_unified_tables
from pipelines.normalize import CANONICAL_COLUMNS, normalize_data
from scrapers.lopes_common import (
    enrich_records_with_details as enrich_lopes_records_with_details,
    maybe_log_detail_progress,
    parse_detail_html as parse_lopes_detail_html,
    parse_detail_payload as parse_lopes_detail_payload,
)
from scrapers.olx_common import parse_detail_html as parse_olx_detail_html
from scrapers.quinto_common import (
    enrich_records_with_details as enrich_quinto_records_with_details,
    normalize_record_from_house,
    parse_detail_html as parse_quinto_detail_html,
)
from scrapers.quinto_common import save_csv as save_quinto_csv
from scrapers.throttle import init_metrics


class DetailParserTests(unittest.TestCase):
    def test_lopes_progress_logs_first_interval_and_last(self):
        metrics = init_metrics("lopes_aluguel")
        metrics["detail_successes"] = 24
        metrics["detail_api_successes"] = 24
        throttle = Mock()
        throttle.snapshot.return_value = {"current_delay_seconds": 2.0}

        with patch("builtins.print") as print_mock:
            maybe_log_detail_progress(
                label="lopes_aluguel",
                processed=1,
                total=60,
                metrics=metrics,
                throttle=throttle,
            )
            metrics["detail_successes"] = 25
            metrics["detail_api_successes"] = 25
            maybe_log_detail_progress(
                label="lopes_aluguel",
                processed=25,
                total=60,
                metrics=metrics,
                throttle=throttle,
            )
            maybe_log_detail_progress(
                label="lopes_aluguel",
                processed=26,
                total=60,
                metrics=metrics,
                throttle=throttle,
            )
            metrics["detail_successes"] = 60
            metrics["detail_api_successes"] = 60
            maybe_log_detail_progress(
                label="lopes_aluguel",
                processed=60,
                total=60,
                metrics=metrics,
                throttle=throttle,
            )

        logged_lines = [call.args[0] for call in print_mock.call_args_list]
        self.assertEqual(len(logged_lines), 3)
        self.assertIn("processed=1/60", logged_lines[0])
        self.assertIn("processed=25/60", logged_lines[1])
        self.assertIn("processed=60/60", logged_lines[2])
        self.assertTrue(all("label=lopes_aluguel" in line for line in logged_lines))

    def test_parse_olx_detail_html(self):
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
        parsed = parse_olx_detail_html(html, fallback_url="https://www.olx.com.br/item")
        self.assertEqual(parsed["listing_created_at"], "2026-04-06T06:06:13+00:00")
        self.assertEqual(parsed["created_at"], "2026-04-06T06:06:13+00:00")
        self.assertEqual(parsed["listing_updated_at"], "2026-04-06T07:19:59+00:00")
        self.assertEqual(parsed["zip_code"], "04567-000")
        self.assertEqual(parsed["seller_name"], "Imoveis Pro")
        self.assertEqual(parsed["condo_fee_brl"], "R$ 1.678")
        self.assertEqual(parsed["iptu_brl"], "R$ 981")
        self.assertEqual(parsed["price"], "13500")
        self.assertEqual(parsed["category_id"], 1020)
        self.assertEqual(parsed["city_id"], 3343)
        self.assertEqual(parsed["state_id"], 11)
        self.assertEqual(parsed["parent_category_id"], 1000)
        self.assertEqual(parsed["connection_type"], "4g")
        self.assertEqual(parsed["cpu_cores"], 16)
        self.assertEqual(parsed["download_link"], 10)
        self.assertEqual(parsed["olx_pay_json"], '{"enabled": false}')
        self.assertEqual(parsed["memory_status_json"], '{"deviceMemory": 8}')
        self.assertEqual(parsed["vehicle_tags_json"], "[]")
        self.assertIn("img1.jpg", parsed["gallery_urls_json"])
        self.assertEqual(parsed["description"], "descricao completa")
        self.assertEqual(parsed["listing_url"], "https://www.olx.com.br/item")

    def test_parse_olx_detail_html_extracts_zip_from_text_when_payload_omits_it(self):
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
        parsed = parse_olx_detail_html(html)
        self.assertEqual(parsed["zip_code"], "04567-000")

    def test_parse_quinto_detail_html(self):
        payload = {
            "props": {
                "pageProps": {
                    "initialState": {
                        "house": {
                            "houseInfo": {
                                "displayId": "2354331",
                                "status": "publicado",
                                "lastPublishedDate": "2025-10-06T14:26:20.000Z",
                                "area": 90,
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
                                    "city": "Sao Paulo",
                                    "stateName": "Sao Paulo",
                                    "zipCode": "02316-080",
                                    "lat": -23.4620491,
                                    "lng": -46.5932235,
                                },
                                "photos": [{"url": "original895054331.jpg"}],
                                "amenities": [{"text": "Banheira", "value": "NAO"}],
                                "comfortCommodities": [{"text": "Rua silenciosa", "value": "SIM"}],
                                "practicalityCommodities": [{"text": "Box", "value": "NAO"}],
                                "installations": [{"text": "Playground", "value": "NAO"}],
                                "placesNearby": [{"name": "Colegio"}],
                                "listings": [{
                                    "firstPublicationDate": "2025-08-12T22:43:30.000+0000",
                                    "houseAgents": [{"name": "Ayrton"}],
                                }],
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
        parsed = parse_quinto_detail_html(html, business_type="rent")
        self.assertEqual(parsed["zip_code"], "02316-080")
        self.assertEqual(parsed["tenant_service_fee_brl"], 53)
        self.assertEqual(parsed["rental_guarantee_min_brl"], 520)
        self.assertIn("original895054331.jpg", parsed["gallery_urls_json"])
        self.assertEqual(parsed["description"], "descricao longa")

    def test_quinto_normalize_listing_address_string_extracts_zip_code(self):
        parsed = normalize_record_from_house(
            {
                "id": "123",
                "address": "Rua Teste, 10 - Sao Paulo/SP - 04567-000",
                "neighbourhood": "Moema",
                "type": "apartamento",
                "area": 80,
                "bedrooms": 2,
            },
            business_type="rent",
        )
        self.assertEqual(parsed["zip_code"], "04567-000")

    def test_quinto_normalize_listing_address_string(self):
        parsed = normalize_record_from_house(
            {
                "id": "123",
                "address": "Rua Teste, 10",
                "neighbourhood": "Moema",
                "type": "apartamento",
                "area": 80,
                "bedrooms": 2,
            },
            business_type="rent",
        )
        self.assertEqual(parsed["address"], "Rua Teste, 10")
        self.assertIsNone(parsed["city"])
        self.assertEqual(parsed["listing_url"], "https://www.quintoandar.com.br/imovel/123/alugar")

    def test_quinto_enrich_counts_detail_requests_once_and_tolerates_404(self):
        metrics = init_metrics("quinto_test")
        records = [
            {"property_id": "1", "listing_url": "https://www.quintoandar.com.br/imovel/1/comprar"},
            {"property_id": "2", "listing_url": "https://www.quintoandar.com.br/imovel/2/comprar"},
        ]
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
        response_404 = Mock()
        response_404.status_code = 404
        http_error = requests.HTTPError("404 Client Error")
        http_error.response = response_404
        with patch(
            "scrapers.quinto_common.fetch_html",
            side_effect=[html, http_error],
        ), patch("scrapers.quinto_common.AdaptiveThrottle.sleep", return_value=0.0), patch(
            "scrapers.quinto_common.AdaptiveThrottle.success", return_value=1.5
        ):
            enriched = enrich_quinto_records_with_details(
                records,
                session=None,
                metrics=metrics,
                business_type="sale",
                min_delay_seconds=1.0,
                max_delay_seconds=2.0,
                target_delay_seconds=1.0,
                max_consecutive_failures=3,
            )
        self.assertEqual(len(enriched), 2)
        self.assertEqual(metrics["detail_requests"], 2)
        self.assertEqual(metrics["detail_successes"], 1)
        self.assertEqual(metrics["detail_failures"], 1)
        self.assertEqual(metrics["detail_backoffs"], 0)

    def test_parse_lopes_detail_html(self):
        payload = {
            "123": {
                "b": {
                    "product": {
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
                    }
                }
            }
        }
        html = f'<script id="ng-state" type="application/json">{json.dumps(payload)}</script>'
        parsed = parse_lopes_detail_html(html)
        self.assertEqual(parsed["condominium_name"], "Condominio Teste")
        self.assertEqual(parsed["suites"], 2)
        self.assertIn("IMG1.JPG", parsed["gallery_urls_json"])
        self.assertEqual(parsed["advertiser_name"], "Lopes Office")

    def test_parse_lopes_detail_payload_extracts_zip_from_formatted_address(self):
        payload = {
            "product": {
                "description": "descricao lopes",
                "address": {
                    "formatted": "Alameda dos Jurupis - Indianopolis - Sao Paulo/SP - 04088-001",
                    "street": "Alameda dos Jurupis",
                    "city": "Sao Paulo",
                    "neighborhood": "Indianopolis",
                    "state": "Sao Paulo",
                },
                "attributes": [],
                "photos": [],
                "prices": {},
            }
        }
        parsed = parse_lopes_detail_payload(payload)
        self.assertEqual(parsed["zip_code"], "04088-001")

    def test_parse_lopes_detail_html_nested_and_partial(self):
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
        parsed = parse_lopes_detail_html(html)
        self.assertEqual(parsed["address"], "Rua Alternativa, 50")
        self.assertEqual(parsed["advertiser_id"], "999")
        self.assertIn("IMG2.JPG", parsed["gallery_urls_json"])

    def test_lopes_enrich_prefers_api_and_skips_html_fallback(self):
        metrics = init_metrics("lopes_test")
        record = {"property_id": "123", "listing_url": "https://www.lopes.com.br/imovel/teste"}
        payload = {
            "product": {
                "description": "descricao api",
                "address": {"formatted": "Rua Api, 20"},
                "photos": [{"imageUrl": "/REO1/API.JPG"}],
            }
        }
        with patch("scrapers.lopes_common.fetch_detail_payload", return_value=payload), patch(
            "scrapers.lopes_common.fetch_html"
        ) as fetch_html_mock, patch("scrapers.lopes_common.AdaptiveThrottle.sleep", return_value=0.0):
            enriched = enrich_lopes_records_with_details(
                [record],
                session=None,
                metrics=metrics,
                detail_min_delay_seconds=1.0,
                detail_max_delay_seconds=2.0,
                detail_target_delay_seconds=1.0,
                detail_max_consecutive_failures=3,
            )
        self.assertEqual(enriched[0]["description"], "descricao api")
        self.assertEqual(metrics["detail_api_requests"], 1)
        self.assertEqual(metrics["detail_api_successes"], 1)
        self.assertEqual(metrics["detail_html_requests"], 0)
        fetch_html_mock.assert_not_called()

    def test_lopes_enrich_uses_html_when_api_has_no_useful_data(self):
        metrics = init_metrics("lopes_test")
        record = {"property_id": "123", "listing_url": "https://www.lopes.com.br/imovel/teste"}
        html = """
        <script id="ng-state" type="application/json">{
            "123": {"b": {"product": {
                "description": "descricao html",
                "address": {"formatted": "Rua Html, 10"},
                "photos": [{"url": "/REO1/HTML.JPG"}]
            }}}
        }</script>
        """
        with patch("scrapers.lopes_common.fetch_detail_payload", return_value={}), patch(
            "scrapers.lopes_common.fetch_html", return_value=html
        ), patch("scrapers.lopes_common.AdaptiveThrottle.sleep", return_value=0.0):
            enriched = enrich_lopes_records_with_details(
                [record],
                session=None,
                metrics=metrics,
                detail_min_delay_seconds=1.0,
                detail_max_delay_seconds=2.0,
                detail_target_delay_seconds=1.0,
                detail_max_consecutive_failures=3,
            )
        self.assertEqual(enriched[0]["description"], "descricao html")
        self.assertEqual(metrics["detail_api_requests"], 1)
        self.assertEqual(metrics["detail_html_requests"], 1)
        self.assertEqual(metrics["detail_html_successes"], 1)


class SchemaTests(unittest.TestCase):
    def test_normalize_and_properties_include_new_fields(self):
        scraped = {
            "olx_venda": [
                {
                    "property_id": "1",
                    "listing_url": "https://example.com/1",
                    "title": "Casa",
                    "description": "desc",
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

    def test_normalize_backfills_selected_fields_from_amenities_json(self):
        scraped = {
            "olx_aluguel": [
                {
                    "property_id": "1",
                    "listing_url": "https://example.com/1",
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
            "olx_aluguel": [
                {
                    "property_id": "1",
                    "listing_url": "https://example.com/1",
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
        self.assertTrue(pd.isna(row["listing_updated_at"]))
        self.assertTrue(pd.isna(row["zip_code"]))


class CsvWriterTests(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()

