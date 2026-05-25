from __future__ import annotations

from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class ArtifactRecord:
    name: str
    path: str
    format: str
    required: bool = True
    rows: int | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class ValidationResult:
    name: str
    passed: bool
    message: str
    severity: str = "error"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class StageResult:
    stage_name: str
    status: str
    objective: str
    started_at: str
    finished_at: str | None = None
    input_manifest: str | None = None
    output_manifest: str | None = None
    log_path: str | None = None
    artifacts: list[ArtifactRecord] = field(default_factory=list)
    metrics: dict[str, Any] = field(default_factory=dict)
    options: dict[str, Any] = field(default_factory=dict)
    validations: list[ValidationResult] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    blocked: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "stage_name": self.stage_name,
            "status": self.status,
            "objective": self.objective,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "input_manifest": self.input_manifest,
            "output_manifest": self.output_manifest,
            "log_path": self.log_path,
            "artifacts": [artifact.to_dict() for artifact in self.artifacts],
            "metrics": self.metrics,
            "options": self.options,
            "validations": [validation.to_dict() for validation in self.validations],
            "errors": self.errors,
            "blocked": self.blocked,
        }


@dataclass
class PipelineContext:
    run_date: str
    project_root: Path
    output_root: Path
    raw_dir: Path
    processed_dir: Path
    processed_run_dir: Path
    artifacts_run_dir: Path
    logs_run_dir: Path

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_date": self.run_date,
            "project_root": str(self.project_root),
            "output_root": str(self.output_root),
            "raw_dir": str(self.raw_dir),
            "processed_dir": str(self.processed_dir),
            "processed_run_dir": str(self.processed_run_dir),
            "artifacts_run_dir": str(self.artifacts_run_dir),
            "logs_run_dir": str(self.logs_run_dir),
        }
