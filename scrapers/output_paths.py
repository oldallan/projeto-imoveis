from datetime import datetime
from pathlib import Path


def build_dated_output_path(source: str, filename: str, run_date: str | None = None) -> str:
    collection_date = run_date or datetime.now().strftime("%d-%m-%Y")
    return str(Path("raw") / collection_date / source / filename)
