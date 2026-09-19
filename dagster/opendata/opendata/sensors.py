import json
from datetime import datetime, timezone

import requests
from dagster import (
    AssetSelection,
    RunRequest,
    SensorEvaluationContext,
    SkipReason,
    define_asset_job,
    sensor,
)

from opendata.scripts.opendata_puller import DATASETS, configure_auth, get_remote_rows_updated_at

# Socrata datasets here change at most daily -- no need to poll harder than this.
CHECK_INTERVAL_SECONDS = 6 * 60 * 60


def _make_dataset_job(name: str):
    return define_asset_job(
        f"{name}_job",
        selection=AssetSelection.assets(name).downstream(include_self=True),
    )


def _make_change_sensor(name: str, cfg: dict, job):
    @sensor(name=f"{name}_change_sensor", job=job, minimum_interval_seconds=CHECK_INTERVAL_SECONDS)
    def _sensor(context: SensorEvaluationContext):
        session = requests.Session()
        headers = configure_auth(session)
        remote_watermark = get_remote_rows_updated_at(session, cfg["id"], headers)

        previous = json.loads(context.cursor) if context.cursor else None
        if previous is not None and previous["rows_updated_at"] == remote_watermark:
            return SkipReason(f"[{name}] no new data (rows_updated_at={remote_watermark})")

        # Only lean on --since for datasets with a real per-row date field; the
        # rows_updated_at check above is what tells us to pull at all.
        since = previous["last_pulled_date"] if (previous and cfg["date_field"]) else None
        pulled_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        context.update_cursor(json.dumps({"rows_updated_at": remote_watermark, "last_pulled_date": pulled_date}))

        return RunRequest(
            run_key=f"{name}_{remote_watermark}",
            run_config={"ops": {name: {"config": {"since": since}}}},
        )

    return _sensor


dataset_jobs = {name: _make_dataset_job(name) for name in DATASETS}
change_sensors = [_make_change_sensor(name, cfg, dataset_jobs[name]) for name, cfg in DATASETS.items()]
