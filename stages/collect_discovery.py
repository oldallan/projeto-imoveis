from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from time import perf_counter

import pandas as pd

from scrapers.discovery_incremental import find_previous_output
from scrapers.registry import get_scraper_definitions
from workflow.models import ArtifactRecord, StageResult, ValidationResult
from workflow.paths import normalize_selected_sources
from workflow.stages import Stage


class CollectDiscoveryStage(Stage):
    name = "collect_discovery"
    objective = "Executar discovery inicial de anuncios para as plataformas configuradas."
    inputs = ["run_date", "scraper_params"]
    block_on_failure = True
    max_parallel_sources = 3

    def run(self, context, input_manifest, logger, stage_options=None):
        verbose = bool((stage_options or {}).get("verbose"))
        selected_sources = normalize_selected_sources((stage_options or {}).get("sources"))
        scrapers = get_scraper_definitions()
        if selected_sources:
            scrapers = [scraper for scraper in scrapers if scraper.source in set(selected_sources)]
        artifacts: list[ArtifactRecord] = []
        errors: list[str] = []
        source_results: list[dict[str, object]] = []

        with ThreadPoolExecutor(max_workers=min(self.max_parallel_sources, len(scrapers))) as executor:
            futures = {
                executor.submit(self._run_scraper, scraper, context, logger, verbose): scraper
                for scraper in scrapers
            }
            for future in as_completed(futures):
                scraper = futures[future]
                result = future.result()
                source_results.append(result)
                logger.info(
                    "discovery_end name=%s status=%s rows=%s duration_seconds=%.2f",
                    scraper.name,
                    result["status"],
                    result["rows"],
                    result["duration_seconds"],
                )
                artifacts.extend(result.pop("artifacts", []))
                if result["status"] != "success":
                    errors.append(f"{scraper.name}: {result['message']}")

        metrics = {
            "configured_scrapers": len(scrapers),
            "successful_scrapers": sum(1 for item in source_results if item["status"] == "success"),
            "failed_scrapers": sum(1 for item in source_results if item["status"] != "success"),
            "new_links_total": sum(int(item["rows"]) for item in source_results if item["status"] == "success"),
            "parallel_source_limit": self.max_parallel_sources,
            "selected_sources": selected_sources,
            "source_results": source_results,
        }
        return artifacts, metrics, errors

    def _run_scraper(self, scraper, context, logger, verbose: bool):
        output_path = context.raw_dir / scraper.source / scraper.discovery_filename
        parquet_output_path = output_path.with_suffix(".parquet")
        previous_output_path = find_previous_output(
            run_date=context.run_date,
            source=scraper.source,
            filename=scraper.discovery_filename,
            project_root=context.output_root,
        )
        logger.info(
            "discovery_start name=%s source=%s output=%s",
            scraper.name,
            scraper.source,
            output_path,
        )

        started_at = perf_counter()
        status = "success"
        message = "ok"
        row_count = 0
        artifacts: list[ArtifactRecord] = []
        runner_metrics: dict[str, object] = {}

        try:
            returned_payload = scraper.run_discovery(
                output_path=str(output_path),
                parquet_output_path=str(parquet_output_path),
                previous_output_path=str(previous_output_path) if previous_output_path else None,
                verbose=verbose,
                **scraper.discovery_options,
            )
            returned_path = returned_payload
            if isinstance(returned_payload, dict):
                returned_path = returned_payload.get("output_path") or returned_payload.get("path")
                runner_metrics = dict(returned_payload.get("metrics") or {})
            if not returned_path:
                raise RuntimeError(f"scraper {scraper.name} nao retornou arquivo")
            output_path = Path(str(returned_path))
            if not output_path.exists():
                raise FileNotFoundError(f"arquivo nao encontrado apos scraper: {output_path}")
            row_count = len(pd.read_csv(output_path))
            artifacts.append(
                ArtifactRecord(
                    name=scraper.name,
                    path=str(output_path.resolve()),
                    format="csv",
                    rows=row_count,
                    metadata={
                        "source": scraper.source,
                        "artifact_role": "discovery",
                        **scraper.discovery_metadata,
                    },
                )
            )
            parquet_path = output_path.with_suffix(".parquet")
            if parquet_path.exists():
                artifacts.append(
                    ArtifactRecord(
                        name=f"{scraper.name}_parquet",
                        path=str(parquet_path.resolve()),
                        format="parquet",
                        rows=len(pd.read_parquet(parquet_path)),
                        metadata={
                            "source": scraper.source,
                            "artifact_role": "discovery",
                            "base_artifact_name": scraper.name,
                            **scraper.discovery_metadata,
                        },
                    )
                )
        except Exception as exc:
            status = "failed"
            message = str(exc)
            logger.exception("discovery_failed name=%s", scraper.name)

        return {
            "name": scraper.name,
            "source": scraper.source,
            "output_path": str(output_path.resolve()),
            "status": status,
            "rows": row_count,
            "message": message,
            "duration_seconds": perf_counter() - started_at,
            "runner_metrics": runner_metrics,
            "artifacts": artifacts,
        }

    def validate(self, context, input_manifest, result: StageResult, logger, stage_options=None):
        validations: list[ValidationResult] = []
        source_results = result.metrics.get("source_results", [])

        validations.append(
            ValidationResult(
                name="all_discovery_scrapers_succeeded",
                passed=all(item["status"] == "success" for item in source_results) and bool(source_results),
                message="Todos os scrapers de discovery devem concluir com sucesso.",
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
                    passed=bool(artifact.rows and artifact.rows > 0) or artifact.metadata.get("discovery_mode") == "delta",
                    message=f"Artefato deve conter registros: {artifact.path}",
                )
            )

        if not result.artifacts:
            validations.append(
                ValidationResult(
                    name="at_least_one_artifact",
                    passed=False,
                    message="O discovery nao gerou artefatos validos.",
                )
            )

        return validations
