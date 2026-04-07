from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from pipelines.historical_store import update_historical_store


def consolidate_data(
    listings_df: pd.DataFrame,
    properties_df: pd.DataFrame,
    link_df: pd.DataFrame,
) -> dict[str, Any]:
    return update_historical_store(listings_df, Path("processed"))
