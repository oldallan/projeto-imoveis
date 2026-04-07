from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from itertools import zip_longest
from pathlib import Path
from time import perf_counter

import pandas as pd

from scrapers.registry import get_scraper_definitions
from workflow.models import ArtifactRecord, StageResult, ValidationResult
from workflow.stages import Stage


class CollectGeneralListingsStage(Stage):
    name = "collect_general_listings"
    objective = "Executar scrapers das paginas gerais e gerar a coleta bruta diaria."
    inputs = ["run_date", "scraper_params"]
    block_on_failure = True
    max_parallel_sources = 3

    def run(self, context, input_manifest, logger):
        artifacts: list[ArtifactRecord] = []
        errors: list[str] = []
        source_results: list[dict[str, object]] = []
        scrapers = get_scraper_definitions()
        grouped_scrapers = self._group_scrapers(scrapers)

        for round_index, scraper_batch in enumerate(self._build_rounds(grouped_scrapers), start=1):
            logger.info(
                "scraper_round_start round=%s scrapers=%s",
                round_index,
                ",".join(scraper.name for scraper in scraper_batch),
            )
            with ThreadPoolExecutor(max_workers=min(self.max_parallel_sources, len(scraper_batch))) as executor:
                futures = {
                    executor.submit(self._run_scraper, scraper, context, logger): scraper
                    for scraper in scraper_batch
                }
                for future in as_completed(futures):
                    scraper = futures[future]
                    result = future.result()
                    source_results.append(result)
                    logger.info(
                        "scraper_end name=%s status=%s rows=%s duration_seconds=%.2f",
                        scraper.name,
                        result["status"],
                        result["rows"],
                        result["duration_seconds"],
                    )
                    artifact = result.pop("artifact", None)
                    if artifact is not None:
                        artifacts.append(artifact)
                    if result["status"] != "success":
                        errors.append(f"{scraper.name}: {result['message']}")

        metrics = {
            "configured_scrapers": len(scrapers),
            "successful_scrapers": sum(1 for item in source_results if item["status"] == "success"),
            "failed_scrapers": sum(1 for item in source_results if item["status"] != "success"),
            "parallel_source_limit": self.max_parallel_sources,
            "source_results": source_results,
        }
        return artifacts, metrics, errors

    def _group_scrapers(self, scrapers):
        grouped: dict[str, list] = {}
        for scraper in scrapers:
            group_name = scraper.domain_group or scraper.source
            grouped.setdefault(group_name, []).append(scraper)
        return grouped

    def _build_rounds(self, grouped_scrapers):
        for batch in zip_longest(*grouped_scrapers.values()):
            yield [scraper for scraper in batch if scraper is not None]

    def _run_scraper(self, scraper, context, logger):
        output_path = Path(scraper.output_path(context.run_date))
        logger.info(
            "scraper_start name=%s source=%s domain_group=%s output=%s",
            scraper.name,
            scraper.source,
            scraper.domain_group or scraper.source,
            output_path,
        )

        started_at = perf_counter()
        status = "success"
        message = "ok"
        row_count = 0
        artifact = None

        try:
            returned_path = scraper.runner(output_path=str(output_path), **scraper.params)
            if not returned_path:
                raise RuntimeError(f"scraper {scraper.name} nao retornou arquivo")
            if not output_path.exists():
                raise FileNotFoundError(f"arquivo nao encontrado apos scraper: {output_path}")
            row_count = len(pd.read_csv(output_path))
            artifact = ArtifactRecord(
                name=scraper.name,
                path=str(output_path.resolve()),
                format="csv",
                rows=row_count,
                metadata={"source": scraper.source, "domain_group": scraper.domain_group or scraper.source},
            )
        except Exception as exc:
            status = "failed"
            message = str(exc)
            logger.exception("scraper_failed name=%s", scraper.name)

        return {
            "name": scraper.name,
            "source": scraper.source,
            "domain_group": scraper.domain_group or scraper.source,
            "output_path": str(output_path.resolve()),
            "status": status,
            "rows": row_count,
            "message": message,
            "duration_seconds": perf_counter() - started_at,
            "artifact": artifact,
        }

    def validate(self, context, input_manifest, result: StageResult, logger):
        validations: list[ValidationResult] = []
        source_results = result.metrics.get("source_results", [])

        validations.append(
            ValidationResult(
                name="all_scrapers_succeeded",
                passed=all(item["status"] == "success" for item in source_results) and bool(source_results),
                message="Todos os scrapers configurados devem concluir com sucesso.",
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
                    name="at_least_one_artifact",
                    passed=False,
                    message="A coleta nao gerou artefatos validos.",
                )
            )

        return validations
