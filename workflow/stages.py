from __future__ import annotations

from abc import ABC, abstractmethod
from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from workflow.logging import LoggerWriter, build_stage_logger
from workflow.manifest import read_json, write_stage_manifest
from workflow.models import StageResult, ValidationResult
from workflow.paths import stage_manifest_path


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class Stage(ABC):
    name: str
    objective: str
    inputs: list[str]
    block_on_failure: bool = True

    @abstractmethod
    def run(
        self,
        context,
        input_manifest: dict[str, Any] | None,
        logger,
        stage_options: dict[str, Any] | None = None,
    ) -> tuple[list, dict[str, Any], list[str]]:
        raise NotImplementedError

    @abstractmethod
    def validate(
        self,
        context,
        input_manifest: dict[str, Any] | None,
        result: StageResult,
        logger,
        stage_options: dict[str, Any] | None = None,
    ) -> list[ValidationResult]:
        raise NotImplementedError

    def execute(
        self,
        context,
        input_manifest_path: Path | None = None,
        stage_options: dict[str, Any] | None = None,
    ) -> StageResult:
        selected_sources = (stage_options or {}).get("sources")
        logger, log_path = build_stage_logger(context, self.name, sources=selected_sources)
        manifest_path = stage_manifest_path(context, self.name, sources=selected_sources)
        result = StageResult(
            stage_name=self.name,
            status="running",
            objective=self.objective,
            started_at=utc_now_iso(),
            input_manifest=str(input_manifest_path) if input_manifest_path else None,
            output_manifest=str(manifest_path),
            log_path=str(log_path),
            options=stage_options or {},
        )

        input_manifest = read_json(input_manifest_path) if input_manifest_path else None
        logger.info("stage_start stage=%s run_date=%s", self.name, context.run_date)
        if input_manifest_path:
            logger.info("stage_input_manifest path=%s", input_manifest_path)

        try:
            stdout_writer = LoggerWriter(logger, level=20)
            stderr_writer = LoggerWriter(logger, level=40)
            with redirect_stdout(stdout_writer), redirect_stderr(stderr_writer):
                artifacts, metrics, errors = self.run(context, input_manifest, logger, stage_options=stage_options)
            stdout_writer.flush()
            stderr_writer.flush()
            result.artifacts = artifacts
            result.metrics = metrics
            result.errors.extend(errors)
        except Exception as exc:
            logger.exception("stage_exception stage=%s", self.name)
            result.errors.append(str(exc))

        result.validations = self.validate(context, input_manifest, result, logger, stage_options=stage_options)
        failed_validations = [
            validation
            for validation in result.validations
            if not validation.passed and validation.severity == "error"
        ]

        if result.errors or failed_validations:
            result.status = "failed"
            result.blocked = self.block_on_failure
        else:
            result.status = "success"
            result.blocked = False

        result.finished_at = utc_now_iso()
        logger.info(
            "stage_end stage=%s status=%s blocked=%s artifacts=%s",
            self.name,
            result.status,
            result.blocked,
            len(result.artifacts),
        )
        write_stage_manifest(
            manifest_path,
            result=result,
            context_payload=context.to_dict(),
        )
        return result
