from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from time import perf_counter

import pandas as pd

from scrapers.listings_resume import build_resume_paths, load_resume_state
from scrapers.registry import get_scraper_definitions
from workflow.models import ArtifactRecord, StageResult, ValidationResult
from workflow.paths import normalize_selected_sources
from workflow.stages import Stage


class CollectListingsStage(Stage):
    name = "collect_listings"
    objective = "Coletar os anuncios completos a partir das URLs descobertas no discovery."
    inputs = ["collect_discovery manifest", "scraper_params"]
    block_on_failure = True
    max_parallel_sources = 3

    def run(self, context, input_manifest, logger, stage_options=None):
        if not input_manifest:
            raise ValueError("manifesto da etapa collect_discovery e obrigatorio")
        if input_manifest.get("status") != "success":
            raise ValueError("o manifesto de discovery precisa estar validado com status success")

        verbose = bool((stage_options or {}).get("verbose"))
        selected_sources = normalize_selected_sources((stage_options or {}).get("sources"))
        scrapers = get_scraper_definitions()
        if selected_sources:
            scrapers = [scraper for scraper in scrapers if scraper.source in set(selected_sources)]
        discovery_artifacts = self._map_discovery_artifacts(input_manifest, selected_sources=selected_sources)
        if selected_sources:
            missing_sources = [
                source for source in selected_sources
                if source not in {scraper.source for scraper in scrapers}
            ]
            if missing_sources:
                raise ValueError(f"fontes solicitadas nao configuradas: {missing_sources}")
            missing_artifacts = [source for source in selected_sources if source not in discovery_artifacts]
            if missing_artifacts:
                raise ValueError(f"artefatos de discovery nao encontrados para fontes solicitadas: {missing_artifacts}")
        artifacts: list[ArtifactRecord] = []
        errors: list[str] = []
        source_results: list[dict[str, object]] = []

        with ThreadPoolExecutor(max_workers=min(self.max_parallel_sources, len(scrapers))) as executor:
            futures = {
                executor.submit(
                    self._run_collection,
                    scraper,
                    discovery_artifacts.get(scraper.source),
                    context,
                    logger,
                    verbose,
                ): scraper
                for scraper in scrapers
            }
            for future in as_completed(futures):
                scraper = futures[future]
                result = future.result()
                source_results.append(result)
                logger.info(
                    "collect_listings_end name=%s status=%s output_rows=%s no_op=%s duration_seconds=%.2f",
                    scraper.name,
                    result["status"],
                    result["output_rows"],
                    result["no_op"],
                    result["duration_seconds"],
                )
                artifacts.extend(result.pop("artifacts", []))
                if result["status"] != "success":
                    errors.append(f"{scraper.name}: {result['message']}")

        metrics = {
            "configured_scrapers": len(scrapers),
            "successful_scrapers": sum(1 for item in source_results if item["status"] == "success"),
            "failed_scrapers": sum(1 for item in source_results if item["status"] != "success"),
            "no_op_scrapers": sum(1 for item in source_results if item.get("no_op")),
            "processed_scrapers": sum(1 for item in source_results if not item.get("no_op")),
            "parallel_source_limit": self.max_parallel_sources,
            "selected_sources": selected_sources,
            "source_results": source_results,
            "all_sources_no_op": bool(source_results) and all(item.get("no_op") for item in source_results),
        }
        return artifacts, metrics, errors

    def _map_discovery_artifacts(self, input_manifest, *, selected_sources: list[str] | None = None) -> dict[str, dict[str, object]]:
        discovery_artifacts: dict[str, dict[str, object]] = {}
        allowed_sources = set(selected_sources or [])
        for artifact in input_manifest.get("artifacts", []):
            metadata = artifact.get("metadata", {})
            if artifact.get("format") != "csv":
                continue
            if metadata.get("artifact_role") != "discovery":
                continue
            source = metadata.get("source")
            if allowed_sources and source not in allowed_sources:
                continue
            if source and source not in discovery_artifacts:
                discovery_artifacts[str(source)] = artifact
        return discovery_artifacts

    def _run_collection(self, scraper, discovery_artifact, context, logger, verbose: bool):
        if not discovery_artifact:
            return {
                "name": scraper.name,
                "source": scraper.source,
                "status": "failed",
                "message": "artefato de discovery nao encontrado",
                "input_rows": 0,
                "output_rows": 0,
                "no_op": False,
                "duration_seconds": 0.0,
                "artifacts": [],
            }

        input_path = Path(str(discovery_artifact["path"]))
        output_path = Path(scraper.listings_output_path(context.run_date))
        parquet_path = Path(scraper.listings_parquet_output_path(context.run_date))
        artifacts_root = Path(getattr(context, "artifacts_run_dir", Path("artifacts") / context.run_date))
        resume_paths = build_resume_paths(artifacts_root / self.name / scraper.source)
        resume_state = load_resume_state(resume_paths["state_json"])
        logger.info(
            "collect_listings_start name=%s source=%s input=%s output=%s",
            scraper.name,
            scraper.source,
            input_path,
            output_path,
        )

        started_at = perf_counter()
        status = "success"
        message = "ok"
        stats = {
            "input_rows": int(discovery_artifact.get("rows") or 0),
            "output_rows": 0,
            "no_op": False,
            "resumed": bool(resume_state.get("status") == "in_progress"),
            "skipped_completed": False,
        }
        artifacts: list[ArtifactRecord] = []

        try:
            if stats["input_rows"] <= 0:
                stats["no_op"] = True
            elif resume_state.get("status") == "completed" and output_path.exists():
                stats["output_rows"] = int(resume_state.get("output_rows") or 0)
                if stats["output_rows"] <= 0:
                    stats["output_rows"] = len(pd.read_csv(output_path))
                stats["skipped_completed"] = True
                artifacts.extend(self._build_listing_artifacts(scraper, output_path, parquet_path, stats["output_rows"]))
            else:
                output = scraper.run_collection(
                    input_path=str(input_path),
                    listings_output_path=str(output_path),
                    listings_parquet_output_path=str(parquet_path),
                    resume_dir=str(resume_paths["root"]),
                    verbose=verbose,
                    **scraper.collection_options,
                )
                if output is None:
                    raise RuntimeError(f"scraper {scraper.name} nao gerou artefatos de listings")
                stats["input_rows"] = int(output.get("input_rows", stats["input_rows"]))
                stats["output_rows"] = int(output.get("output_rows", 0))
                stats["no_op"] = bool(output.get("no_op"))
                if not stats["no_op"]:
                    if not output_path.exists():
                        raise FileNotFoundError(f"arquivo final nao encontrado apos scraper: {output_path}")
                    artifacts.extend(self._build_listing_artifacts(scraper, output_path, parquet_path, stats["output_rows"]))
        except Exception as exc:
            status = "failed"
            message = str(exc)
            logger.exception("collect_listings_failed name=%s", scraper.name)

        return {
            "name": scraper.name,
            "source": scraper.source,
            "input_path": str(input_path.resolve()),
            "output_path": str(output_path.resolve()),
            "status": status,
            "message": message,
            "duration_seconds": perf_counter() - started_at,
            "artifacts": artifacts,
            **stats,
        }

    def _build_listing_artifacts(self, scraper, output_path: Path, parquet_path: Path, output_rows: int) -> list[ArtifactRecord]:
        artifacts = [
            ArtifactRecord(
                name=scraper.name,
                path=str(output_path.resolve()),
                format="csv",
                rows=output_rows,
                metadata={
                    "source": scraper.source,
                    "artifact_role": "listings",
                },
            )
        ]
        if parquet_path.exists():
            artifacts.append(
                ArtifactRecord(
                    name=f"{scraper.name}_parquet",
                    path=str(parquet_path.resolve()),
                    format="parquet",
                    rows=len(pd.read_parquet(parquet_path)),
                    metadata={
                        "source": scraper.source,
                        "artifact_role": "listings",
                        "base_artifact_name": scraper.name,
                    },
                )
            )
        return artifacts

    def validate(self, context, input_manifest, result: StageResult, logger, stage_options=None):
        validations: list[ValidationResult] = []
        source_results = result.metrics.get("source_results", [])
        all_no_op = bool(result.metrics.get("all_sources_no_op"))

        validations.append(
            ValidationResult(
                name="all_collection_scrapers_succeeded",
                passed=all(item["status"] == "success" for item in source_results) and bool(source_results),
                message="Todos os scrapers de coleta completa devem concluir com sucesso.",
            )
        )

        for artifact in result.artifacts:
            artifact_path = Path(artifact.path)
            validations.append(
                ValidationResult(
                    name=f"artifact_exists::{artifact.name}",
                    passed=artifact_path.exists(),
                    message=f"Artefato deve existir: {artifact.path}",
                )
            )
            validations.append(
                ValidationResult(
                    name=f"artifact_non_empty::{artifact.name}",
                    passed=bool(artifact.rows and artifact.rows > 0),
                    message=f"Artefato deve conter registros: {artifact.path}",
                )
            )

        if not result.artifacts:
            validations.append(
                ValidationResult(
                    name="artifacts_present_or_all_no_op",
                    passed=all_no_op,
                    message="A coleta completa precisa gerar artefatos, exceto quando todas as fontes estiverem em no-op.",
                )
            )

        return validations
