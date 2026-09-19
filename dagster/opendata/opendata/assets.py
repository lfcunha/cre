from pathlib import Path

import requests
from dagster import AssetExecutionContext, AssetIn, Config, MetadataValue, asset

from opendata.scripts.lake_sync import get_lake_bucket, sync_to_lake
from opendata.scripts.opendata_puller import DATASETS, configure_auth, pull_dataset


class PullConfig(Config):
    # Populated by the dataset's change_sensor (see sensors.py) from its cursor;
    # left at the default for manual/ad hoc materializations, which always do a
    # full pull -- same behavior as running opendata_puller.py standalone.
    since: str | None = None


def _make_pull_asset(name: str, cfg: dict):
    @asset(name=name, group_name="raw_socrata_pulls", description=cfg["description"])
    def _pull(context: AssetExecutionContext, config: PullConfig) -> str:
        session = requests.Session()
        headers = configure_auth(session)

        since = config.since if cfg["date_field"] else None
        out_path, total_rows = pull_dataset(
            name, cfg, session, headers, since=since, citywide=False, resume=True,
        )

        context.add_output_metadata(
            {
                "row_count": total_rows,
                "since": since or "(full pull)",
                "output_path": MetadataValue.path(str(out_path)),
            }
        )
        return str(out_path)

    return _pull


def _make_lake_sync_asset(name: str, cfg: dict):
    @asset(
        name=f"{name}_lake",
        group_name="lake_sync",
        description=f"Convert {name}'s latest pull to Parquet and sync it to the S3 lake",
        ins={"raw_path": AssetIn(key=name)},
    )
    def _sync(context: AssetExecutionContext, raw_path: str) -> str:
        bucket = get_lake_bucket()
        keys = sync_to_lake(name, cfg, Path(raw_path), bucket)

        context.add_output_metadata({
            "bucket": bucket,
            **{role: MetadataValue.path(key) for role, key in keys.items()},
        })
        return keys.get("incremental") or keys.get("latest")

    return _sync


raw_pull_assets = [_make_pull_asset(name, cfg) for name, cfg in DATASETS.items()]
lake_sync_assets = [_make_lake_sync_asset(name, cfg) for name, cfg in DATASETS.items()]
