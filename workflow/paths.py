from __future__ import annotations

from datetime import datetime
from pathlib import Path

from workflow.models import PipelineContext


DATE_FORMAT = "%d-%m-%Y"


def default_run_date() -> str:
    return datetime.now().strftime(DATE_FORMAT)


def build_context(run_date: str, project_root: Path | None = None) -> PipelineContext:
    root = (project_root or Path.cwd()).resolve()
    return PipelineContext(
        run_date=run_date,
        project_root=root,
        raw_dir=root / "raw" / run_date,
        processed_dir=root / "processed",
        processed_run_dir=root / "processed" / run_date,
        artifacts_run_dir=root / "artifacts" / run_date,
        logs_run_dir=root / "logs" / run_date,
    )


def stage_manifest_path(context: PipelineContext, stage_name: str) -> Path:
    return context.artifacts_run_dir / stage_name / "manifest.json"


def pipeline_manifest_path(context: PipelineContext) -> Path:
    return context.artifacts_run_dir / "pipeline_run.json"
