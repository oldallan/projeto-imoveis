from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

import pandas as pd


def load_csv_records(path: str | Path) -> list[dict[str, Any]]:
    csv_path = Path(path)
    if not csv_path.exists():
        return []
    frame = pd.read_csv(csv_path)
    frame = frame.where(pd.notna(frame), None)
    return frame.to_dict(orient="records")


def save_parquet_records(records: list[Mapping[str, Any]], filename: str | Path) -> None:
    output_path = Path(filename)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    frame = pd.DataFrame(records)
    if not frame.empty:
        frame = frame.replace(to_replace=r"^\s*$", value=None, regex=True)
    frame.to_parquet(output_path, index=False)
