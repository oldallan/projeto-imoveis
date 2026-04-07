from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from workflow.models import StageResult


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def write_stage_manifest(path: Path, result: StageResult, context_payload: dict[str, Any]) -> None:
    payload = {
        "context": context_payload,
        **result.to_dict(),
    }
    write_json(path, payload)
