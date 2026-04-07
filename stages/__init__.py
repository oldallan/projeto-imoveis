from stages.build_daily_snapshot import BuildDailySnapshotStage
from stages.collect_general_listings import CollectGeneralListingsStage
from stages.update_historical_store import UpdateHistoricalStoreStage


STAGE_SEQUENCE = [
    "collect_general_listings",
    "build_daily_snapshot",
    "update_historical_store",
]


_STAGES = {
    "collect_general_listings": CollectGeneralListingsStage(),
    "build_daily_snapshot": BuildDailySnapshotStage(),
    "update_historical_store": UpdateHistoricalStoreStage(),
}


def get_stage(name: str):
    if name not in _STAGES:
        raise ValueError(f"stage desconhecido: {name}")
    return _STAGES[name]
