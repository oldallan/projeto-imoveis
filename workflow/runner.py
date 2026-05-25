from __future__ import annotations

from pathlib import Path
from typing import Iterable

from stages import STAGE_SEQUENCE, get_stage
from workflow.manifest import read_json, write_json
from workflow.models import StageResult
from workflow.paths import (
    build_context,
    normalize_selected_sources,
    pipeline_manifest_path,
    stage_manifest_path,
)


class PipelineRunner:
    def __init__(self, project_root: Path | None = None) -> None:
        self.project_root = project_root or Path.cwd()

    def list_stages(self) -> list[str]:
        return list(STAGE_SEQUENCE)

    def run_stage(
        self,
        stage_name: str,
        run_date: str,
        output_root: str | Path | None = None,
        input_manifest: str | None = None,
        verbose: bool = False,
        sources: list[str] | None = None,
    ) -> StageResult:
        context = build_context(run_date, self.project_root, output_root=output_root)
        stage = get_stage(stage_name)
        normalized_sources = normalize_selected_sources(sources)
        manifest_path = Path(input_manifest) if input_manifest else self._default_input_manifest(
            context,
            stage_name,
            sources=normalized_sources,
        )
        return stage.execute(
            context,
            input_manifest_path=manifest_path,
            stage_options=self._stage_options(stage_name, verbose, sources=normalized_sources),
        )

    def run_all(
        self,
        run_date: str,
        output_root: str | Path | None = None,
        from_stage: str | None = None,
        verbose: bool = False,
        force_discovery: bool = False,
    ) -> dict[str, object]:
        context = build_context(run_date, self.project_root, output_root=output_root)
        selected_stages = list(self._select_stages(from_stage))
        current_input_manifest: Path | None = None
        if from_stage:
            current_input_manifest = self._default_input_manifest(context, from_stage)

        results: list[dict[str, object]] = []
        pipeline_status = "success"
        blocked_stage: str | None = None

        for stage_name in selected_stages:
            if stage_name == "collect_discovery" and not from_stage and not force_discovery:
                existing_manifest_path = stage_manifest_path(context, "collect_discovery")
                existing_manifest = self._load_reusable_success_manifest(existing_manifest_path)
                if existing_manifest is not None:
                    result_payload = {
                        key: value
                        for key, value in existing_manifest.items()
                        if key != "context"
                    }
                    result_payload.setdefault("stage_name", "collect_discovery")
                    result_payload.setdefault("status", "success")
                    result_payload.setdefault("output_manifest", str(existing_manifest_path))
                    result_payload["skipped"] = True
                    result_payload["skip_reason"] = "existing_success_manifest"
                    results.append(result_payload)
                    current_input_manifest = existing_manifest_path
                    if int((result_payload.get("metrics") or {}).get("new_links_total", 0) or 0) == 0:
                        break
                    continue

            stage = get_stage(stage_name)
            result = stage.execute(
                context,
                input_manifest_path=current_input_manifest,
                stage_options=self._stage_options(stage_name, verbose),
            )
            results.append(result.to_dict())

            if result.status != "success" and result.blocked:
                pipeline_status = "failed"
                blocked_stage = stage_name
                break
            if stage_name == "collect_discovery" and int(result.metrics.get("new_links_total", 0) or 0) == 0:
                break

            current_input_manifest = Path(result.output_manifest) if result.output_manifest else None

        payload = {
            "context": context.to_dict(),
            "status": pipeline_status,
            "blocked_stage": blocked_stage,
            "stop_reason": "no_new_links_after_discovery"
            if pipeline_status == "success"
            and results
            and results[-1]["stage_name"] == "collect_discovery"
            and int(results[-1].get("metrics", {}).get("new_links_total", 0) or 0) == 0
            else None,
            "verbose": verbose,
            "force_discovery": force_discovery,
            "results": results,
        }
        write_json(pipeline_manifest_path(context), payload)
        return payload

    def _load_reusable_success_manifest(self, manifest_path: Path) -> dict[str, object] | None:
        if not manifest_path.exists():
            return None
        try:
            payload = read_json(manifest_path)
        except (OSError, ValueError):
            return None
        if payload.get("status") != "success":
            return None
        return payload

    def _select_stages(
        self,
        from_stage: str | None,
    ) -> Iterable[str]:
        sequence = list(STAGE_SEQUENCE)

        if not from_stage:
            return sequence

        if from_stage not in STAGE_SEQUENCE:
            raise ValueError(f"stage desconhecido: {from_stage}")
        return sequence[sequence.index(from_stage):]

    def _stage_options(
        self,
        stage_name: str,
        verbose: bool = False,
        sources: list[str] | None = None,
    ) -> dict[str, object]:
        options: dict[str, object] = {}
        if stage_name in {"collect_discovery", "collect_listings"}:
            options["verbose"] = verbose
        normalized_sources = normalize_selected_sources(sources)
        if normalized_sources:
            options["sources"] = normalized_sources
        return options

    def _default_input_manifest(self, context, stage_name: str, sources: list[str] | None = None) -> Path | None:
        if stage_name == "collect_discovery":
            return None
        if stage_name == "collect_listings":
            return self._require_manifest(context, "collect_discovery", sources=sources)
        if stage_name == "build_daily_snapshot":
            return self._require_manifest(context, "collect_listings", sources=sources)

        index = STAGE_SEQUENCE.index(stage_name)
        previous_stage = STAGE_SEQUENCE[index - 1]
        return self._require_manifest(context, previous_stage, sources=sources)

    def _require_manifest(self, context, stage_name: str, sources: list[str] | None = None) -> Path:
        manifest_path = stage_manifest_path(context, stage_name, sources=sources)
        if manifest_path.exists():
            return manifest_path
        if sources:
            fallback_manifest_path = stage_manifest_path(context, stage_name)
            if fallback_manifest_path.exists():
                return fallback_manifest_path
        raise FileNotFoundError(
            f"manifesto de entrada nao encontrado para {stage_name}: {manifest_path}"
        )
