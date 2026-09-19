"""
NYC Open Data (Socrata) puller for ACRIS + PLUTO.

Design goals, matching how this will get folded into Dagster later:
  - One generic paginated-pull function shared by every dataset (this becomes
    the guts of a Dagster @asset / @op; each dataset config below becomes an
    asset definition with its own partition/freshness policy).
  - Checkpointed offsets so a killed/rate-limited run can resume instead of
    re-pulling from zero.
  - Raw landing pattern: land exactly what the API returns (as newline-delimited
    JSON) with a pull timestamp, no transformation here. Normalization into
    Postgres tables happens in a separate step, since mixing fetch + transform
    makes retries and schema-drift debugging harder.
  - Manhattan-only filter WHERE THE DATASET SUPPORTS IT. Important ACRIS
    quirk: Master, Parties, and References do NOT carry a borough field —
    only Legals does. So Legals and PLUTO get filtered at fetch time; Master/
    Parties/References get pulled in full (citywide) and get scoped to
    Manhattan downstream in Postgres by joining on document_id against the
    Manhattan document_id set from Legals. Trying to pre-filter Master via a
    giant `document_id IN (...)` clause built from millions of Legals rows
    doesn't work at fetch time (URL/query size limits), so don't do that —
    let the join happen in the database after landing.

Usage:
    python pull_acris_pluto.py                      # pull all datasets (Manhattan-filtered where supported)
    python pull_acris_pluto.py --dataset acris_legals
    python pull_acris_pluto.py --citywide           # disable borough filter on Legals/PLUTO too
    python pull_acris_pluto.py --since 2026-01-01    # only records modified/recorded after this date
    SOCRATA_APP_TOKEN=xxxx python pull_acris_pluto.py

Requires: requests (pip install requests --break-system-packages)
"""

import argparse
import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
import zstandard as zstd
from dotenv import find_dotenv, load_dotenv

load_dotenv(find_dotenv(usecwd=True))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("cre_intel.pull")

BASE_URL = "https://data.cityofnewyork.us/resource/{dataset_id}.json"
METADATA_URL = "https://data.cityofnewyork.us/api/views/{dataset_id}.json"
OUTPUT_DIR = Path(__file__).parent / "raw_data"
CHECKPOINT_DIR = Path(__file__).parent / "checkpoints"
PAGE_SIZE = 50000          # Socrata max is 50k per request without SoQL $limit override issues
REQUEST_TIMEOUT = 60
MAX_RETRIES = 5
RETRY_BACKOFF_SECONDS = 5

# --- Dataset registry -------------------------------------------------------
# `date_field` drives incremental pulls via --since. `borough_field` + code
# drive the Manhattan filter. Set borough_field=None for datasets (like PLUTO)
# where the borough code differs in shape, and handle it in borough_value.

DATASETS = {
    "acris_legals": {
        "id": "8h5j-fqxa",
        "date_field": "good_through_date",  # dataset refresh watermark, not per-doc date -- coarse but only option here
        "borough_field": "borough",
        "borough_value": "1",  # 1 = Manhattan in ACRIS numeric borough codes
        "description": "ACRIS Real Property Legals (BBL <-> document_id linkage; source of truth for Manhattan scoping)",
    },
    "acris_master": {
        "id": "bnx9-e6tj",
        "date_field": "doc_date",   # per-document recording date -- good for real incremental pulls
        "borough_field": None,      # NOTE: Master has no borough field. Pulled citywide; scope to
        "borough_value": None,      # Manhattan downstream by joining document_id against acris_legals.
        "description": "ACRIS Real Property Master (doc_type, doc_date, doc_amount, crfn per document_id)",
    },
    "acris_parties": {
        "id": "636b-3b5g",
        "date_field": None,        # no reliable per-row date field; pull full, re-pull periodically
        "borough_field": None,      # no borough field; scope downstream via document_id join to legals
        "borough_value": None,
        "description": "ACRIS Real Property Parties (grantor/grantee/lender names per document_id)",
    },
    "acris_references": {
        "id": "pwkr-dpni",
        "date_field": None,
        "borough_field": None,      # no borough field; scope downstream via document_id join to legals
        "borough_value": None,
        "description": "ACRIS Real Property References (doc cross-references, e.g. satisfaction -> mortgage)",
    },
    "pluto": {
        "id": "64uk-42ks",
        "date_field": None,      # PLUTO is a point-in-time annual/quarterly snapshot, not incrementally dated
        "borough_field": "borough",
        "borough_value": "MN",  # PLUTO uses 'MN' not '1' -- different code space than ACRIS
        "description": "PLUTO (tax lot land use, building, and lat/long attributes per BBL)",
    },
    "acris_doc_control_codes": {
        "id": "7isb-wh4c",
        "date_field": None,
        "borough_field": None,
        "borough_value": None,
        "description": "ACRIS Document Control Codes (small lookup: doc_type -> human-readable description)",
    },
}


def configure_auth(session: requests.Session) -> dict:
    """Sets up Socrata credentials on `session` and returns any extra headers to send.

    Socrata's current API Keys are a Key ID + Key Secret pair, authenticated via
    HTTP Basic Auth (Key ID as username, Key Secret as password) -- that's the
    primary mechanism now, so it's set directly on the session (applies to every
    request made through it). SOCRATA_APP_TOKEN (a single string sent via the
    X-App-Token header) is kept as a fallback for the older single-token flow.
    """
    key_id = os.environ.get("SOCRATA_KEY_ID")
    key_secret = os.environ.get("SOCRATA_KEY_SECRET")
    if key_id and key_secret:
        session.auth = (key_id, key_secret)
        return {}

    token = os.environ.get("SOCRATA_APP_TOKEN")
    if token:
        return {"X-App-Token": token}

    log.warning(
        "No SOCRATA_KEY_ID/SOCRATA_KEY_SECRET (or legacy SOCRATA_APP_TOKEN) set. "
        "Pulls will work but are capped at a lower throttling limit. Get free "
        "credentials at https://data.cityofnewyork.us/profile/app_tokens"
    )
    return {}


def build_where_clause(cfg: dict, since: str | None, citywide: bool) -> str | None:
    clauses = []
    if not citywide and cfg["borough_field"]:
        clauses.append(f"{cfg['borough_field']}='{cfg['borough_value']}'")
    if since and cfg["date_field"]:
        clauses.append(f"{cfg['date_field']} >= '{since}T00:00:00.000'")
    return " AND ".join(clauses) if clauses else None


def load_checkpoint(dataset_name: str) -> int:
    path = CHECKPOINT_DIR / f"{dataset_name}.offset"
    if path.exists():
        offset = int(path.read_text().strip())
        log.info(f"[{dataset_name}] resuming from checkpointed offset {offset}")
        return offset
    return 0


def save_checkpoint(dataset_name: str, offset: int) -> None:
    CHECKPOINT_DIR.mkdir(exist_ok=True, parents=True)
    (CHECKPOINT_DIR / f"{dataset_name}.offset").write_text(str(offset))


def clear_checkpoint(dataset_name: str) -> None:
    path = CHECKPOINT_DIR / f"{dataset_name}.offset"
    if path.exists():
        path.unlink()


def get_remote_rows_updated_at(session: requests.Session, dataset_id: str, headers: dict) -> int:
    """Dataset-level 'last changed' watermark from Socrata's view metadata endpoint.

    Cheap way to tell whether a dataset has changed at all since our last pull,
    without paging through rows first -- works even for datasets (parties,
    references, doc control codes) that have no reliable per-row date field.
    """
    url = METADATA_URL.format(dataset_id=dataset_id)
    resp = session.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    return int(resp.json()["rowsUpdatedAt"])


def fetch_page(session: requests.Session, dataset_id: str, params: dict, headers: dict) -> list:
    url = BASE_URL.format(dataset_id=dataset_id)
    last_exc = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(url, params=params, headers=headers, timeout=REQUEST_TIMEOUT)
            if resp.status_code == 429:
                wait = RETRY_BACKOFF_SECONDS * attempt
                log.warning(f"Rate limited (429). Waiting {wait}s before retry {attempt}/{MAX_RETRIES}")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as exc:
            last_exc = exc
            wait = RETRY_BACKOFF_SECONDS * attempt
            log.warning(f"Request failed ({exc}). Retry {attempt}/{MAX_RETRIES} in {wait}s")
            time.sleep(wait)
    raise RuntimeError(f"Exhausted {MAX_RETRIES} retries fetching {url} with params={params}") from last_exc


def pull_dataset(name: str, cfg: dict, session: requests.Session, headers: dict,
                  since: str | None, citywide: bool, resume: bool) -> tuple[Path, int]:
    OUTPUT_DIR.mkdir(exist_ok=True, parents=True)
    pulled_at = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_path = OUTPUT_DIR / f"{name}_{pulled_at}.ndjson.zst"
    where = build_where_clause(cfg, since, citywide)

    offset = load_checkpoint(name) if resume else 0
    total_rows = offset
    log.info(f"[{name}] pulling {cfg['description']}  (where={where or 'none'})")

    # Streaming zstd: writes compress page-by-page just like the old plain-text
    # append did, so a killed run still leaves a partial-but-decodable file.
    mode = "ab" if offset else "wb"
    compressor = zstd.ZstdCompressor(level=3)
    with open(out_path, mode) as raw_f, compressor.stream_writer(raw_f) as f:
        while True:
            params = {
                "$limit": PAGE_SIZE,
                "$offset": offset,
                "$order": ":id",   # stable ordering required for reliable pagination
            }
            if where:
                params["$where"] = where

            rows = fetch_page(session, cfg["id"], params, headers)
            if not rows:
                break

            for row in rows:
                f.write((json.dumps(row) + "\n").encode("utf-8"))

            total_rows += len(rows)
            offset += len(rows)
            save_checkpoint(name, offset)
            log.info(f"[{name}] fetched {len(rows)} rows (total {total_rows})")

            if len(rows) < PAGE_SIZE:
                break  # last page

    clear_checkpoint(name)
    log.info(f"[{name}] done: {total_rows} rows -> {out_path}")
    return out_path, total_rows


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dataset", choices=list(DATASETS.keys()), help="Pull only this dataset")
    parser.add_argument("--since", help="Only pull records dated on/after this date (YYYY-MM-DD), where supported")
    parser.add_argument("--citywide", action="store_true", help="Disable Manhattan-only filter")
    parser.add_argument("--no-resume", action="store_true", help="Ignore any existing checkpoint and start fresh")
    args = parser.parse_args()

    session = requests.Session()
    headers = configure_auth(session)

    targets = {args.dataset: DATASETS[args.dataset]} if args.dataset else DATASETS

    results = {}
    for name, cfg in targets.items():
        try:
            path, total_rows = pull_dataset(
                name, cfg, session, headers,
                since=args.since, citywide=args.citywide, resume=not args.no_resume,
            )
            results[name] = f"{total_rows} rows -> {path}"
        except Exception as exc:
            log.error(f"[{name}] FAILED: {exc}")
            results[name] = f"FAILED: {exc}"

    log.info("Summary:")
    for name, result in results.items():
        log.info(f"  {name}: {result}")


if __name__ == "__main__":
    main()
