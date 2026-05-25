from __future__ import annotations

import csv
import html
import json
from pathlib import Path
import re
import time
import unicodedata
from typing import Any

import pandas as pd
import requests

from scrapers.zip_utils import normalize_zip_code


CACHE_FILENAME = "zipcode_enrichment.json"
BASE_CEPS_FILENAME = "base_ceps.csv"
CEPABERTO_FILENAME = "cepaberto.csv"
DEFAULT_USER_AGENT = "projeto-imoveis-zipcode-enricher/1.0"
REQUEST_TIMEOUT_SECONDS = 5
GEOCODE_MIN_INTERVAL_SECONDS = 1.1
GEOCODE_BLOCK_COOLDOWN_SECONDS = 60.0
BRAZILGUIDE_MIN_INTERVAL_SECONDS = 5.0
BRAZILGUIDE_BASE_URL = "https://brazilguide.net/ceps"

STATE_SLUGS = {
    "AC": "acre",
    "AL": "alagoas",
    "AP": "amapa",
    "AM": "amazonas",
    "BA": "bahia",
    "CE": "ceara",
    "DF": "distrito-federal",
    "ES": "espirito-santo",
    "GO": "goias",
    "MA": "maranhao",
    "MT": "mato-grosso",
    "MS": "mato-grosso-do-sul",
    "MG": "minas-gerais",
    "PA": "para",
    "PB": "paraiba",
    "PR": "parana",
    "PE": "pernambuco",
    "PI": "piaui",
    "RJ": "rio-de-janeiro",
    "RN": "rio-grande-do-norte",
    "RS": "rio-grande-do-sul",
    "RO": "rondonia",
    "RR": "roraima",
    "SC": "santa-catarina",
    "SP": "sao-paulo",
    "SE": "sergipe",
    "TO": "tocantins",
}


def _is_missing(value: Any) -> bool:
    if value is None:
        return True
    if pd.isna(value):
        return True
    return str(value).strip() == ""


def _to_float(value: Any) -> float | None:
    if _is_missing(value):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _zip_digits(value: Any) -> str | None:
    if value is None:
        return None
    digits = re.sub(r"\D+", "", str(value))
    if not digits or len(digits) > 8:
        return None
    return digits.zfill(8)


def _zip_dash(value: Any) -> str | None:
    digits = _zip_digits(value)
    if digits is None:
        normalized = normalize_zip_code(value)
        return normalized
    return f"{digits[:5]}-{digits[5:]}"


def _parse_point(value: Any) -> tuple[float | None, float | None]:
    if _is_missing(value):
        return None, None
    match = re.search(r"POINT\((-?\d+(?:\.\d+)?)\s+(-?\d+(?:\.\d+)?)\)", str(value))
    if not match:
        return None, None
    lon = _to_float(match.group(1))
    lat = _to_float(match.group(2))
    return lat, lon


def _first_present(*values: Any) -> Any:
    for value in values:
        if not _is_missing(value):
            return value
    return None


def _merge_missing(primary: dict[str, Any], secondary: dict[str, Any]) -> dict[str, Any]:
    merged = dict(primary)
    for key in ("street", "neighbourhood", "city", "state", "lat", "lon"):
        if _is_missing(merged.get(key)) and not _is_missing(secondary.get(key)):
            merged[key] = secondary.get(key)
    sources = [source for source in [primary.get("source"), secondary.get("source")] if source]
    if sources:
        merged["source"] = "+".join(dict.fromkeys(sources))
    return merged


def _slugify(value: Any) -> str | None:
    if _is_missing(value):
        return None
    text = str(value).strip()
    state_slug = STATE_SLUGS.get(text.upper())
    if state_slug:
        return state_slug
    normalized = unicodedata.normalize("NFKD", text.casefold())
    ascii_text = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    slug = re.sub(r"[^a-z0-9]+", "-", ascii_text).strip("-")
    return slug or None


def default_cache_path_from_output_dir(output_dir: Path) -> Path:
    project_root = output_dir.resolve().parent.parent
    return project_root / "artifacts" / "_cache" / CACHE_FILENAME


class ZipCodeEnricher:
    def __init__(
        self,
        cache_path: Path | None = None,
        *,
        base_ceps_path: Path | None = None,
        cepaberto_path: Path | None = None,
        session: requests.Session | None = None,
    ) -> None:
        self.cache_path = cache_path
        cache_dir = cache_path.parent if cache_path is not None else None
        self.base_ceps_path = base_ceps_path or (cache_dir / BASE_CEPS_FILENAME if cache_dir else None)
        self.cepaberto_path = cepaberto_path or (cache_dir / CEPABERTO_FILENAME if cache_dir else None)
        self.session = session or requests.Session()
        self.last_metrics = self._empty_metrics()
        self._cache = self._load_cache()
        self._dirty = False
        self._processed_candidates = 0
        self._total_candidates = 0
        self._runtime_resolution_cache: dict[str, tuple[dict[str, Any] | None, bool]] = {}
        self._local_indexes: dict[str, dict[str, dict[str, Any]] | None] = {
            "base_ceps": None,
            "cepaberto": None,
        }
        self._geocode_blocked_until = 0.0
        self._last_geocode_request_at = 0.0
        self._geocode_rate_limit_logged = False
        self._last_brazilguide_request_at = 0.0

    def enrich_frame(self, frame: pd.DataFrame) -> pd.DataFrame:
        self.last_metrics = self._empty_metrics()
        self._processed_candidates = 0
        self._total_candidates = 0
        self._runtime_resolution_cache = {}
        self._geocode_blocked_until = 0.0
        self._last_geocode_request_at = 0.0
        self._geocode_rate_limit_logged = False
        if frame.empty or "zip_code" not in frame.columns:
            print("[INFO] zipcode_enrichment_skip reason=no_rows_or_missing_zip_code_column")
            return frame.copy()

        enriched = frame.copy()
        candidate_indexes: list[int] = []
        for index, row in enriched.iterrows():
            zip_code = _zip_dash(row.get("zip_code"))
            if not zip_code:
                continue
            needs_address = _is_missing(row.get("address"))
            needs_city = _is_missing(row.get("city"))
            needs_state = _is_missing(row.get("state"))
            needs_lat = _is_missing(row.get("lat"))
            needs_lon = _is_missing(row.get("lon"))
            if any([needs_address, needs_city, needs_state, needs_lat, needs_lon]):
                candidate_indexes.append(index)

        self._total_candidates = len(candidate_indexes)
        print(
            "[INFO] zipcode_enrichment_start "
            f"rows={len(enriched)} "
            f"candidates={self._total_candidates} "
            f"cache_entries={len(self._cache)} "
            f"cache_path={self.cache_path}"
        )
        if not candidate_indexes:
            print("[INFO] zipcode_enrichment_skip reason=no_missing_fields_to_enrich")
            return enriched

        for index in candidate_indexes:
            row = enriched.loc[index]
            zip_code = _zip_dash(row.get("zip_code"))
            needs_address = _is_missing(row.get("address"))
            needs_city = _is_missing(row.get("city"))
            needs_state = _is_missing(row.get("state"))
            needs_lat = _is_missing(row.get("lat"))
            needs_lon = _is_missing(row.get("lon"))

            payload, used_cache = self.resolve(
                zip_code,
                city=row.get("city"),
                state=row.get("state"),
                neighbourhood=row.get("neighbourhood"),
            )
            if used_cache:
                self.last_metrics["zip_code_cache_hits"] += 1

            if payload is None:
                self.last_metrics["zip_code_resolution_failures"] += 1
                continue

            if needs_address and payload.get("street"):
                enriched.at[index, "address"] = payload["street"]
                self.last_metrics["zip_code_address_filled_count"] += 1
            if needs_city and payload.get("city"):
                enriched.at[index, "city"] = payload["city"]
            if needs_state and payload.get("state"):
                enriched.at[index, "state"] = payload["state"]

            coordinates_filled = False
            if needs_lat and payload.get("lat") is not None:
                enriched.at[index, "lat"] = payload["lat"]
                coordinates_filled = True
            if needs_lon and payload.get("lon") is not None:
                enriched.at[index, "lon"] = payload["lon"]
                coordinates_filled = True
            if coordinates_filled:
                self.last_metrics["zip_code_coordinates_filled_count"] += 1

            self._processed_candidates += 1
            if (
                self._processed_candidates == 1
                or self._processed_candidates == self._total_candidates
                or self._processed_candidates % 10 == 0
            ):
                print(
                    "[INFO] zipcode_enrichment_progress "
                    f"processed={self._processed_candidates} "
                    f"total={self._total_candidates} "
                    f"cache_hits={self.last_metrics['zip_code_cache_hits']} "
                    f"consulted={self.last_metrics['zip_codes_consulted']} "
                    f"failures={self.last_metrics['zip_code_resolution_failures']}"
                )

        self._save_cache()
        print(
            "[INFO] zipcode_enrichment_end "
            f"metrics={json.dumps(self.last_metrics, ensure_ascii=False)}"
        )
        return enriched

    def resolve(
        self,
        zip_code: str,
        *,
        city: Any = None,
        state: Any = None,
        neighbourhood: Any = None,
    ) -> tuple[dict[str, Any] | None, bool]:
        normalized_zip = _zip_dash(zip_code)
        if not normalized_zip:
            return None, False

        memoized = self._runtime_resolution_cache.get(normalized_zip)
        if memoized is not None:
            payload, used_cache = memoized
            print(
                "[INFO] zipcode_enrichment_runtime_cache_hit "
                f"zip_code={normalized_zip} "
                f"used_cache={used_cache}"
            )
            return (dict(payload) if payload is not None else None), used_cache

        cached = self._cache.get(normalized_zip)
        if cached is not None:
            if cached.get("failed"):
                if cached.get("source") == "cepbrasil":
                    print(
                        "[INFO] zipcode_enrichment_legacy_negative_cache_ignored "
                        f"zip_code={normalized_zip} source=cepbrasil"
                    )
                    cached = None
                else:
                    self.last_metrics["zip_code_negative_cache_hits"] += 1
                    self._runtime_resolution_cache[normalized_zip] = (None, True)
                    return None, True

        if cached is not None:
            cached_payload = dict(cached)
            has_cached_coordinates = (
                cached_payload.get("lat") is not None or cached_payload.get("lon") is not None
            )
            if has_cached_coordinates:
                print(
                    "[INFO] zipcode_enrichment_cache_hit "
                    f"zip_code={normalized_zip} "
                    f"coordinates_found=True"
                )
                self._runtime_resolution_cache[normalized_zip] = (dict(cached_payload), True)
                return cached_payload, True

            print(
                "[INFO] zipcode_enrichment_cache_partial "
                f"zip_code={normalized_zip} "
                "coordinates_found=False retrying_geocode=True"
            )
            refreshed_payload = self._refresh_missing_coordinates(
                normalized_zip,
                cached_payload,
                city=None if _is_missing(city) else str(city),
                state=None if _is_missing(state) else str(state),
                neighbourhood=None if _is_missing(neighbourhood) else str(neighbourhood),
            )
            if refreshed_payload is not None:
                self._runtime_resolution_cache[normalized_zip] = (dict(refreshed_payload), False)
                return refreshed_payload, False

        self.last_metrics["zip_codes_consulted"] += 1
        print(f"[INFO] zipcode_enrichment_lookup_start zip_code={normalized_zip}")
        payload, cache_failure = self._resolve_uncached(
            normalized_zip,
            city=None if _is_missing(city) else str(city),
            state=None if _is_missing(state) else str(state),
            neighbourhood=None if _is_missing(neighbourhood) else str(neighbourhood),
        )
        if payload is None:
            print(f"[WARN] zipcode_enrichment_lookup_failed zip_code={normalized_zip}")
            if cache_failure:
                self._cache[normalized_zip] = {"failed": True, "source": "brazilguide"}
                self._dirty = True
                self._runtime_resolution_cache[normalized_zip] = (None, True)
            return None, False

        self._cache[normalized_zip] = payload
        self._dirty = True
        print(
            "[INFO] zipcode_enrichment_lookup_done "
            f"zip_code={normalized_zip} "
            f"source={payload.get('source')} "
            f"street_found={bool(payload.get('street'))} "
            f"coordinates_found={payload.get('lat') is not None or payload.get('lon') is not None}"
        )
        self._runtime_resolution_cache[normalized_zip] = (dict(payload), False)
        return dict(payload), False

    def _refresh_missing_coordinates(
        self,
        zip_code: str,
        cached_payload: dict[str, Any],
        *,
        city: str | None,
        state: str | None,
        neighbourhood: str | None,
    ) -> dict[str, Any] | None:
        geocoded = self._geocode(
            zip_code=zip_code,
            street=cached_payload.get("street"),
            city=cached_payload.get("city") or city,
            state=cached_payload.get("state") or state,
            neighbourhood=cached_payload.get("neighbourhood") or neighbourhood,
        )
        if not geocoded:
            print(f"[WARN] zipcode_enrichment_cache_partial_geocode_failed zip_code={zip_code}")
            return dict(cached_payload)

        refreshed_payload = dict(cached_payload)
        refreshed_payload["lat"] = geocoded.get("lat")
        refreshed_payload["lon"] = geocoded.get("lon")
        refreshed_payload["source"] = _merge_source(refreshed_payload.get("source"), "nominatim")
        self._cache[zip_code] = refreshed_payload
        self._dirty = True
        print(
            "[INFO] zipcode_enrichment_cache_partial_geocode_done "
            f"zip_code={zip_code} "
            f"lat={refreshed_payload['lat']} "
            f"lon={refreshed_payload['lon']}"
        )
        return refreshed_payload

    def _resolve_uncached(
        self,
        zip_code: str,
        *,
        city: str | None,
        state: str | None,
        neighbourhood: str | None,
    ) -> tuple[dict[str, Any] | None, bool]:
        resolved = self._lookup_local_sources(zip_code)
        if resolved is not None:
            if _is_missing(resolved.get("street")):
                brazilguide_payload = self._lookup_brazilguide(zip_code=zip_code)
                if brazilguide_payload is not None:
                    resolved = _merge_missing(resolved, brazilguide_payload)
                    self._append_brazilguide_success(zip_code, brazilguide_payload)
            if _is_missing(resolved.get("lat")) or _is_missing(resolved.get("lon")):
                geocoded = self._geocode(
                    zip_code=zip_code,
                    street=resolved.get("street"),
                    city=resolved.get("city") or city,
                    state=resolved.get("state") or state,
                    neighbourhood=resolved.get("neighbourhood") or neighbourhood,
                )
                if geocoded:
                    resolved["lat"] = geocoded.get("lat")
                    resolved["lon"] = geocoded.get("lon")
                    resolved["source"] = _merge_source(resolved.get("source"), "nominatim")
            return resolved, False

        print(f"[INFO] zipcode_enrichment_brazilguide_start zip_code={zip_code}")
        brazilguide_payload = self._lookup_brazilguide(zip_code=zip_code)
        if brazilguide_payload is None:
            return None, bool(_zip_digits(zip_code))

        if _is_missing(brazilguide_payload.get("lat")) or _is_missing(brazilguide_payload.get("lon")):
            geocoded = self._geocode(
                zip_code=zip_code,
                street=brazilguide_payload.get("street"),
                city=brazilguide_payload.get("city") or city,
                state=brazilguide_payload.get("state") or state,
                neighbourhood=brazilguide_payload.get("neighbourhood") or neighbourhood,
            )
            if geocoded:
                brazilguide_payload["lat"] = geocoded.get("lat")
                brazilguide_payload["lon"] = geocoded.get("lon")
                brazilguide_payload["source"] = _merge_source(brazilguide_payload.get("source"), "nominatim")

        self._append_brazilguide_success(zip_code, brazilguide_payload)
        return brazilguide_payload, False

    def _lookup_local_sources(self, zip_code: str) -> dict[str, Any] | None:
        primary = self._lookup_local_source("base_ceps", zip_code)
        secondary = self._lookup_local_source("cepaberto", zip_code)
        if primary is None and secondary is None:
            return None
        if primary is None:
            self.last_metrics["zip_code_cepaberto_hits"] += 1
            return secondary
        self.last_metrics["zip_code_base_ceps_hits"] += 1
        if secondary is None:
            return primary
        self.last_metrics["zip_code_cepaberto_hits"] += 1
        return _merge_missing(primary, secondary)

    def _lookup_local_source(self, source_name: str, zip_code: str) -> dict[str, Any] | None:
        index = self._load_local_index(source_name)
        if not index:
            return None
        digits = _zip_digits(zip_code)
        if not digits:
            return None
        record = index.get(digits)
        return dict(record) if record is not None else None

    def _load_local_index(self, source_name: str) -> dict[str, dict[str, Any]]:
        cached = self._local_indexes.get(source_name)
        if cached is not None:
            return cached

        path = self.base_ceps_path if source_name == "base_ceps" else self.cepaberto_path
        index: dict[str, dict[str, Any]] = {}
        if path is None or not path.exists():
            print(f"[WARN] zipcode_enrichment_local_source_missing source={source_name} path={path}")
            self._local_indexes[source_name] = index
            return index

        try:
            for chunk in pd.read_csv(
                path,
                dtype=str,
                chunksize=250000,
                usecols=["cep", "logradouro", "localidade", "nome_municipio", "sigla_uf", "centroide"],
            ):
                for row in chunk.to_dict(orient="records"):
                    digits = _zip_digits(row.get("cep"))
                    if not digits or digits in index:
                        continue
                    lat, lon = _parse_point(row.get("centroide"))
                    index[digits] = {
                        "street": row.get("logradouro") or None,
                        "neighbourhood": row.get("localidade") or None,
                        "city": row.get("nome_municipio") or None,
                        "state": row.get("sigla_uf") or None,
                        "lat": lat,
                        "lon": lon,
                        "source": source_name,
                    }
        except Exception as exc:
            print(
                "[WARN] zipcode_enrichment_local_source_load_failed "
                f"source={source_name} path={path} error={exc}"
            )
            index = {}

        print(
            "[INFO] zipcode_enrichment_local_source_loaded "
            f"source={source_name} path={path} rows={len(index)}"
        )
        self._local_indexes[source_name] = index
        return index

    def _lookup_brazilguide(
        self,
        *,
        zip_code: str,
    ) -> dict[str, Any] | None:
        dashed_zip = _zip_dash(zip_code)
        if not dashed_zip:
            self.last_metrics["zip_code_brazilguide_skipped_invalid_zip"] += 1
            print(
                "[WARN] zipcode_enrichment_brazilguide_skipped_invalid_zip "
                f"zip_code={zip_code}"
            )
            return None

        url = BRAZILGUIDE_BASE_URL
        self._respect_brazilguide_rate_limit()
        self.last_metrics["zip_code_brazilguide_requests"] += 1
        try:
            response = self.session.get(
                url,
                params={"q": dashed_zip},
                headers={"User-Agent": DEFAULT_USER_AGENT},
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
            response.raise_for_status()
            payload = self._parse_brazilguide_html(response.text, zip_code=dashed_zip)
        except requests.HTTPError as exc:
            status_code = getattr(getattr(exc, "response", None), "status_code", None)
            self.last_metrics["zip_code_brazilguide_failures"] += 1
            print(
                "[WARN] zipcode_enrichment_brazilguide_http_failed "
                f"zip_code={dashed_zip} url={url}?q={dashed_zip} status={status_code}"
            )
            return None
        except Exception as exc:
            self.last_metrics["zip_code_brazilguide_failures"] += 1
            print(
                "[WARN] zipcode_enrichment_brazilguide_failed "
                f"zip_code={dashed_zip} url={url}?q={dashed_zip} error={exc}"
            )
            return None

        if not payload or not payload.get("street"):
            self.last_metrics["zip_code_brazilguide_failures"] += 1
            print(
                "[WARN] zipcode_enrichment_brazilguide_parse_empty "
                f"zip_code={dashed_zip} url={url}?q={dashed_zip}"
            )
            return None

        self.last_metrics["zip_code_brazilguide_successes"] += 1
        return {
            "street": payload.get("street"),
            "neighbourhood": payload.get("neighbourhood"),
            "city": payload.get("city"),
            "state": payload.get("state"),
            "lat": None,
            "lon": None,
            "source": "brazilguide",
        }

    def _parse_brazilguide_html(self, page_html: str, *, zip_code: str) -> dict[str, Any] | None:
        dashed_zip = _zip_dash(zip_code)
        digits = _zip_digits(zip_code)
        paragraphs = re.findall(
            r'<p[^>]*class=["\'][^"\']*text-sm\s+text-gray-500[^"\']*["\'][^>]*>(.*?)</p>',
            page_html,
            flags=re.IGNORECASE | re.DOTALL,
        )
        candidates: list[dict[str, Any]] = []
        for paragraph in paragraphs:
            text = html.unescape(re.sub(r"<[^>]+>", "", paragraph))
            text = re.sub(r"\s+", " ", text).strip()
            if "·" not in text or "/" not in text:
                continue
            parts = [part.strip() for part in text.split("·")]
            if len(parts) < 3:
                continue
            city_state = parts[-1]
            if "/" not in city_state:
                continue
            city, state = [part.strip() for part in city_state.rsplit("/", 1)]
            candidate = {
                "street": parts[0],
                "neighbourhood": parts[1],
                "city": city,
                "state": state,
            }
            candidates.append(candidate)

        if not candidates:
            return None

        page_has_zip = bool(
            (dashed_zip and dashed_zip in page_html)
            or (digits and re.search(rf"\b{re.escape(digits)}\b", re.sub(r"\D+", "", page_html)))
        )
        if page_has_zip or len(candidates) == 1:
            return candidates[0]
        return None

    def _append_brazilguide_success(self, zip_code: str, payload: dict[str, Any]) -> None:
        if not payload.get("street") or self.base_ceps_path is None:
            return
        digits = _zip_digits(zip_code)
        if digits is None:
            return
        centroide = ""
        if payload.get("lat") is not None and payload.get("lon") is not None:
            centroide = f"POINT({payload['lon']} {payload['lat']})"
        row = {
            "cep": digits,
            "logradouro": payload.get("street"),
            "localidade": payload.get("neighbourhood"),
            "id_municipio": "",
            "nome_municipio": payload.get("city"),
            "sigla_uf": payload.get("state"),
            "estabelecimentos": "",
            "centroide": centroide,
        }
        try:
            file_exists = self.base_ceps_path.exists()
            self.base_ceps_path.parent.mkdir(parents=True, exist_ok=True)
            with self.base_ceps_path.open("a", newline="", encoding="utf-8") as file:
                writer = csv.DictWriter(file, fieldnames=list(row.keys()))
                if not file_exists:
                    writer.writeheader()
                writer.writerow(row)
            index = self._local_indexes.get("base_ceps")
            if index is not None:
                index[digits] = {
                    "street": payload.get("street"),
                    "neighbourhood": payload.get("neighbourhood"),
                    "city": payload.get("city"),
                    "state": payload.get("state"),
                    "lat": payload.get("lat"),
                    "lon": payload.get("lon"),
                    "source": "brazilguide",
                }
        except Exception as exc:
            print(f"[WARN] zipcode_enrichment_append_brazilguide_failed zip_code={zip_code} error={exc}")

    def _geocode(
        self,
        *,
        zip_code: str,
        street: str | None,
        city: str | None,
        state: str | None,
        neighbourhood: str | None = None,
    ) -> dict[str, Any] | None:
        candidate_queries = self._build_geocode_queries(
            zip_code=zip_code,
            street=street,
            city=city,
            state=state,
            neighbourhood=neighbourhood,
        )
        if not candidate_queries:
            self.last_metrics["zip_code_geocode_skipped_incomplete_context"] += 1
            return None

        for query in candidate_queries:
            if self._is_geocode_temporarily_blocked():
                if not self._geocode_rate_limit_logged:
                    remaining = max(0.0, self._geocode_blocked_until - time.monotonic())
                    print(
                        "[WARN] zipcode_enrichment_geocode_skipped_rate_limited "
                        f"zip_code={zip_code} "
                        f"cooldown_seconds={remaining:.1f}"
                    )
                    self._geocode_rate_limit_logged = True
                return None

            print(
                "[INFO] zipcode_enrichment_geocode_attempt "
                f"zip_code={zip_code} "
                f"query={query!r}"
            )
            self._respect_geocode_rate_limit()
            try:
                response = self.session.get(
                    "https://nominatim.openstreetmap.org/search",
                    params={
                        "format": "jsonv2",
                        "limit": 1,
                        "countrycodes": "br",
                        "q": query,
                    },
                    headers={"User-Agent": DEFAULT_USER_AGENT},
                    timeout=REQUEST_TIMEOUT_SECONDS,
                )
                response.raise_for_status()
                payload = response.json()
            except requests.HTTPError as exc:
                response = getattr(exc, "response", None)
                if response is not None and response.status_code == 429:
                    self._geocode_blocked_until = time.monotonic() + GEOCODE_BLOCK_COOLDOWN_SECONDS
                    self.last_metrics["zip_code_geocode_rate_limited"] += 1
                    print(
                        "[WARN] zipcode_enrichment_geocode_rate_limited "
                        f"zip_code={zip_code} "
                        f"cooldown_seconds={GEOCODE_BLOCK_COOLDOWN_SECONDS:.0f}"
                    )
                    return None
                continue
            except Exception:
                continue

            if not isinstance(payload, list) or not payload:
                continue

            first_match = payload[0]
            if not isinstance(first_match, dict):
                continue

            lat = _to_float(first_match.get("lat"))
            lon = _to_float(first_match.get("lon"))
            if lat is None and lon is None:
                continue
            self.last_metrics["zip_code_geocode_successes"] += 1
            return {"lat": lat, "lon": lon}
        self.last_metrics["zip_code_geocode_failures"] += 1
        return None

    def _build_geocode_queries(
        self,
        *,
        zip_code: str,
        street: str | None,
        city: str | None,
        state: str | None,
        neighbourhood: str | None,
    ) -> list[str]:
        if any(_is_missing(value) for value in [street, city, state, neighbourhood]):
            return []
        variants = [
            [street, neighbourhood, city, state, zip_code, "Brasil"],
            [street, neighbourhood, city, state, "Brasil"],
        ]
        queries: list[str] = []
        for parts in variants:
            query = ", ".join(str(part).strip() for part in parts if part and str(part).strip())
            if query and query not in queries:
                queries.append(query)
        return queries

    def _respect_geocode_rate_limit(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_geocode_request_at
        if elapsed < GEOCODE_MIN_INTERVAL_SECONDS:
            time.sleep(GEOCODE_MIN_INTERVAL_SECONDS - elapsed)
        self._last_geocode_request_at = time.monotonic()

    def _respect_brazilguide_rate_limit(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_brazilguide_request_at
        if elapsed < BRAZILGUIDE_MIN_INTERVAL_SECONDS:
            time.sleep(BRAZILGUIDE_MIN_INTERVAL_SECONDS - elapsed)
        self._last_brazilguide_request_at = time.monotonic()

    def _is_geocode_temporarily_blocked(self) -> bool:
        return time.monotonic() < self._geocode_blocked_until

    def _load_cache(self) -> dict[str, dict[str, Any]]:
        if self.cache_path is None or not self.cache_path.exists():
            return {}
        try:
            payload = json.loads(self.cache_path.read_text(encoding="utf-8"))
        except Exception:
            return {}
        if not isinstance(payload, dict):
            return {}
        cache: dict[str, dict[str, Any]] = {}
        for zip_code, value in payload.items():
            normalized_zip = _zip_dash(zip_code)
            if normalized_zip is None or not isinstance(value, dict):
                continue
            cache[normalized_zip] = {
                "street": value.get("street"),
                "neighbourhood": value.get("neighbourhood"),
                "city": value.get("city"),
                "state": value.get("state"),
                "lat": _to_float(value.get("lat")),
                "lon": _to_float(value.get("lon")),
                "source": value.get("source"),
                "failed": bool(value.get("failed")),
            }
        return cache

    def _save_cache(self) -> None:
        if self.cache_path is None or not self._dirty:
            return
        try:
            self.cache_path.parent.mkdir(parents=True, exist_ok=True)
            self.cache_path.write_text(
                json.dumps(self._cache, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            self._dirty = False
        except Exception:
            return

    @staticmethod
    def _empty_metrics() -> dict[str, int]:
        return {
            "zip_codes_consulted": 0,
            "zip_code_cache_hits": 0,
            "zip_code_address_filled_count": 0,
            "zip_code_coordinates_filled_count": 0,
            "zip_code_resolution_failures": 0,
            "zip_code_base_ceps_hits": 0,
            "zip_code_cepaberto_hits": 0,
            "zip_code_brazilguide_requests": 0,
            "zip_code_brazilguide_successes": 0,
            "zip_code_brazilguide_failures": 0,
            "zip_code_brazilguide_skipped_invalid_zip": 0,
            "zip_code_negative_cache_hits": 0,
            "zip_code_geocode_skipped_incomplete_context": 0,
            "zip_code_geocode_successes": 0,
            "zip_code_geocode_failures": 0,
            "zip_code_geocode_rate_limited": 0,
        }


def _merge_source(left: Any, right: str) -> str:
    sources = [source for source in [left, right] if source]
    return "+".join(dict.fromkeys(str(source) for source in sources))
