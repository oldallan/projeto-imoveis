from __future__ import annotations

from pathlib import Path
from typing import Iterable

from stages import STAGE_SEQUENCE, get_stage
from workflow.manifest import write_json
from workflow.models import StageResult
from workflow.paths import build_context, pipeline_manifest_path, stage_manifest_path


class PipelineRunner:
    def __init__(self, project_root: Path | None = None) -> None:
        self.project_root = project_root or Path.cwd()

    def list_stages(self) -> list[str]:
        return list(STAGE_SEQUENCE)

    def run_stage(
        self,
        stage_name: str,
        run_date: str,
        input_manifest: str | None = None,
    ) -> StageResult:
        context = build_context(run_date, self.project_root)
        stage = get_stage(stage_name)
        manifest_path = Path(input_manifest) if input_manifest else self._default_input_manifest(context, stage_name)
        return stage.execute(context, input_manifest_path=manifest_path)

    def run_all(self, run_date: str, from_stage: str | None = None) -> dict[str, object]:
        context = build_context(run_date, self.project_root)
        selected_stages = self._select_stages(from_stage)
        current_input_manifest: Path | None = None
        if from_stage:
            current_input_manifest = self._default_input_manifest(context, from_stage)

        results: list[dict[str, object]] = []
        pipeline_status = "success"
        blocked_stage: str | None = None

        for stage_name in selected_stages:
            stage = get_stage(stage_name)
            result = stage.execute(context, input_manifest_path=current_input_manifest)
            results.append(result.to_dict())

            if result.status != "success" and result.blocked:
                pipeline_status = "failed"
                blocked_stage = stage_name
                break

            current_input_manifest = Path(result.output_manifest) if result.output_manifest else None

        payload = {
            "context": context.to_dict(),
            "status": pipeline_status,
            "blocked_stage": blocked_stage,
            "results": results,
        }
        write_json(pipeline_manifest_path(context), payload)
        return payload

    def _select_stages(self, from_stage: str | None) -> Iterable[str]:
        if not from_stage:
            return STAGE_SEQUENCE
        if from_stage not in STAGE_SEQUENCE:
            raise ValueError(f"stage desconhecido: {from_stage}")
        return STAGE_SEQUENCE[STAGE_SEQUENCE.index(from_stage):]

    def _default_input_manifest(self, context, stage_name: str) -> Path | None:
        index = STAGE_SEQUENCE.index(stage_name)
        if index == 0:
            return None
        previous_stage = STAGE_SEQUENCE[index - 1]
        manifest_path = stage_manifest_path(context, previous_stage)
        if not manifest_path.exists():
            raise FileNotFoundError(
                f"manifesto de entrada nao encontrado para {stage_name}: {manifest_path}"
            )
        return manifest_path
