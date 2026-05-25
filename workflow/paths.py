from __future__ import annotations

from datetime import datetime
from pathlib import Path

from workflow.models import PipelineContext


DATE_FORMAT = "%d-%m-%Y"


def default_run_date() -> str:
    return datetime.now().strftime(DATE_FORMAT)


def normalize_selected_sources(sources: list[str] | tuple[str, ...] | None) -> list[str]:
    if not sources:
        return []
    normalized: list[str] = []
    seen: set[str] = set()
    for source in sources:
        value = str(source or "").strip().lower()
        if not value or value in seen:
            continue
        normalized.append(value)
        seen.add(value)
    return normalized


def build_source_scope_token(sources: list[str] | tuple[str, ...] | None) -> str | None:
    normalized = normalize_selected_sources(sources)
    if not normalized:
        return None
    return "__".join(sorted(normalized))


def build_scoped_output_dir(base_dir: Path, sources: list[str] | tuple[str, ...] | None) -> Path:
    token = build_source_scope_token(sources)
    if not token:
        return base_dir
    return base_dir / f"sources__{token}"


def build_context(
    run_date: str,
    project_root: Path | None = None,
    output_root: str | Path | None = None,
) -> PipelineContext:
    root = (project_root or Path.cwd()).resolve()
    output = (Path(output_root) if output_root is not None else root).resolve()
    return PipelineContext(
        run_date=run_date,
        project_root=root,
        output_root=output,
        raw_dir=output / "raw" / run_date,
        processed_dir=output / "processed",
        processed_run_dir=output / "processed" / run_date,
        artifacts_run_dir=output / "artifacts" / run_date,
        logs_run_dir=output / "logs" / run_date,
    )


def stage_manifest_path(
    context: PipelineContext,
    stage_name: str,
    sources: list[str] | tuple[str, ...] | None = None,
) -> Path:
    stage_dir = build_scoped_output_dir(context.artifacts_run_dir / stage_name, sources)
    return stage_dir / "manifest.json"


def pipeline_manifest_path(context: PipelineContext) -> Path:
    return context.artifacts_run_dir / "pipeline_run.json"
