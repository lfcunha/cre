from dagster import AssetSelection, Definitions, define_asset_job

from opendata.assets import lake_sync_assets, raw_pull_assets
from opendata.sensors import change_sensors, dataset_jobs

# Convenience for manual/ad hoc "pull everything now" -- the sensors below are
# what drive normal automatic pulls, one per dataset, only when it's changed.
pull_all_job = define_asset_job("pull_all_datasets", selection=AssetSelection.all())

defs = Definitions(
    assets=raw_pull_assets + lake_sync_assets,
    jobs=[pull_all_job, *dataset_jobs.values()],
    sensors=change_sensors,
)
