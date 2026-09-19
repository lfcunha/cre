"""Convert a landed raw pull (.ndjson.zst) to Parquet and sync it to the S3 lake.

Layout in the bucket:
  - Datasets with a per-row `date_field` (acris_legals, acris_master) land
    incremental, new-rows-only files per pull. Each pull's Parquet file is
    independent and additive -- no duplication, so it's fine to just keep
    accumulating them:
        s3://{bucket}/{dataset}/{dataset}_{pulled_at}.parquet

  - Datasets with no `date_field` (acris_parties, acris_references, pluto,
    acris_doc_control_codes) get a *full* re-land every time they change.
    Dumping every one of those as another file in the same "table" directory
    would make a naive `SELECT * FROM directory` over-count massively (every
    historical full snapshot stacked on top of the others). So these get
    written to two places instead:
        s3://{bucket}/{dataset}/latest/{dataset}.parquet   (overwritten each sync --
                                                              this is what analytics
                                                              queries should read)
        s3://{bucket}/{dataset}/archive/{dataset}_{pulled_at}.parquet
                                                             (kept for history/replay)
"""

import os
from pathlib import Path

import boto3
import pyarrow.json as pyarrow_json
import pyarrow.parquet as pq
import zstandard as zstd
from dotenv import find_dotenv, load_dotenv

load_dotenv(find_dotenv(usecwd=True))

PARQUET_CACHE_DIR = Path(__file__).parent / "parquet_cache"


def get_lake_bucket() -> str:
    bucket = os.environ.get("S3_LAKE_BUCKET")
    if not bucket:
        raise RuntimeError(
            "S3_LAKE_BUCKET is not set. Add it to .env (see .env.example) -- it's "
            "the bucket the raw pulls get converted to Parquet and synced into."
        )
    return bucket


def convert_to_parquet(ndjson_zst_path: Path, parquet_path: Path) -> int:
    """Decompresses `ndjson_zst_path` and writes it out as a Parquet file. Returns row count."""
    parquet_path.parent.mkdir(exist_ok=True, parents=True)
    dctx = zstd.ZstdDecompressor()
    with open(ndjson_zst_path, "rb") as compressed:
        with dctx.stream_reader(compressed) as reader:
            table = pyarrow_json.read_json(reader)
    pq.write_table(table, parquet_path, compression="zstd")
    return table.num_rows


def sync_to_lake(name: str, cfg: dict, raw_path: Path, bucket: str) -> dict[str, str]:
    """Converts a completed pull to Parquet and uploads it to the S3 lake.

    Returns the S3 key(s) written to, keyed by role ("incremental", or
    "latest"/"archive" for full-snapshot datasets).
    """
    base_name = raw_path.name.removesuffix(".ndjson.zst")
    parquet_path = PARQUET_CACHE_DIR / f"{base_name}.parquet"
    convert_to_parquet(raw_path, parquet_path)

    s3 = boto3.client("s3")
    keys: dict[str, str] = {}

    if cfg["date_field"]:
        key = f"{name}/{parquet_path.name}"
        s3.upload_file(str(parquet_path), bucket, key)
        keys["incremental"] = key
    else:
        latest_key = f"{name}/latest/{name}.parquet"
        archive_key = f"{name}/archive/{parquet_path.name}"
        s3.upload_file(str(parquet_path), bucket, latest_key)
        s3.upload_file(str(parquet_path), bucket, archive_key)
        keys["latest"] = latest_key
        keys["archive"] = archive_key

    return keys
