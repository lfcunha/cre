# cre

NYC commercial real estate intelligence platform. Monorepo: `dagster/` holds the data pipelines, with other services (e.g. a website) expected as sibling top-level folders later.

## Local Postgres setup

```bash
sudo apt update
sudo apt install postgresql postgresql-contrib
```

```bash
sudo service postgresql start    # start
sudo service postgresql status   # check status
sudo service postgresql stop     # stop
```

Connect via `psql`:

```bash
sudo -u postgres psql
```

```sql
-- Set a password for the default 'postgres' user
ALTER USER postgres WITH PASSWORD 'your_secure_password';

-- Create a fresh database for your project
CREATE DATABASE my_project_db;
```

## Python workspace

The repo root is a [uv workspace](pyproject.toml) shared across all Python subprojects (currently just `dagster/opendata`). Run `uv sync` from the repo root any time dependencies change — it installs everything into one shared `.venv` at the root, with each subproject keeping its own `pyproject.toml`/dependency set. A new Python service later just needs its own `pyproject.toml` added to the root's `members` list.

## Environment variables

Copy `.env.example` to `.env` at the repo root and fill in real values (gitignored, never committed):

```bash
cp .env.example .env
```

- `SOCRATA_KEY_ID` / `SOCRATA_KEY_SECRET` — NYC Open Data (Socrata) API Key pair, used via HTTP Basic Auth. Get one at [data.cityofnewyork.us/profile/app_tokens](https://data.cityofnewyork.us/profile/app_tokens).
- `SOCRATA_APP_TOKEN` — legacy fallback (single-token header), only used if the Key ID/Secret pair isn't set.
- `S3_LAKE_BUCKET` — S3 bucket the raw pulls get converted to Parquet and synced into. AWS credentials themselves come from the standard boto3 credential chain, not from `.env`.

## Dagster (`dagster/opendata`)

### Running the dev UI

```bash
cd dagster/opendata
./dev.sh    # or: uv run dagster dev
```

Opens the Dagster UI at http://localhost:3000. This wrapper sets `DAGSTER_HOME` to a persistent local dir (`dagster/opendata/.dagster_home`, gitignored) so run/schedule history survives restarts, instead of the throwaway temp directory `dagster dev` uses by default.

### Pulling data

Two ways to pull, depending on what you need:

**Via Dagster (materialize an asset)** — a manual/ad hoc pull. Always does a real pull (full, or `--since`-filtered where the config provides it) and, on success, converts the raw pull to Parquet and syncs it to the S3 lake.

```bash
cd dagster/opendata
uv run dagster asset materialize --select acris_legals -m opendata.definitions    # one dataset's raw pull
uv run dagster asset materialize --select acris_legals+ -m opendata.definitions   # that pull + its lake sync
uv run dagster asset materialize --select '*' -m opendata.definitions             # everything
```

Or via the UI: Assets → select one or more → Materialize.

**Automatically, via each dataset's sensor** — the normal path in practice. Each dataset has its own `{dataset}_change_sensor`, polling Socrata's dataset-level `rowsUpdatedAt` on an interval and only requesting a run when it's actually changed — no run gets created at all on a no-op check. Sensors are **stopped by default** (Dagster OSS convention); turn them on per-dataset in the UI under Sensors, or `dagster sensor start <name> -m opendata.definitions`.

**Standalone script** — for manual/ad hoc pulls or debugging a single dataset without going through Dagster at all. Same underlying pull logic (auth, zstd compression, checkpoint/resume) as the asset, but no lake sync — that only exists in the Dagster asset wrapper. Always does a real pull regardless of whether the source actually changed, and only lands the raw `.ndjson.zst` file.

```bash
uv run python dagster/opendata/opendata/scripts/opendata_puller.py --dataset pluto
uv run python dagster/opendata/opendata/scripts/opendata_puller.py --dataset acris_master --since 2026-01-01
```

### Pipeline design

- **Datasets**: ACRIS Legals/Master/Parties/References, PLUTO, ACRIS Document Control Codes — all NYC Open Data (Socrata), Manhattan-filtered where the dataset supports a borough field.
- **Raw landing**: newline-delimited JSON, streamed through zstd compression as it's written page-by-page (`scripts/raw_data/`) — a killed pull still leaves a decodable partial file.
- **Change detection**: a Dagster sensor per dataset (`sensors.py`) checks Socrata's dataset-level `rowsUpdatedAt` on an interval, comparing it against a cursor Dagster persists for it (no hand-rolled state file) — only requests a run when the source has actually changed. Datasets with a real per-row date field (`acris_legals`, `acris_master`) additionally pass `--since` (the sensor's last successful pull date, via run config) to fetch only new rows; the rest (`acris_parties`, `acris_references`, `pluto`, doc control codes) re-land in full whenever they've changed. The pull asset itself has no memory of "did this change" — that decision lives entirely in the sensor, so a manual materialize always does a real pull.
- **Lake sync**: each successful pull is converted to Parquet and uploaded to S3. Incremental datasets accumulate one additive Parquet file per pull. Full-snapshot datasets instead write to `{dataset}/latest/{dataset}.parquet` (overwritten each sync — what analytics should query) plus `{dataset}/archive/{dataset}_{timestamp}.parquet` (kept for history).
- **Not yet built**: loading pulls into Postgres. Planned to happen right after each successful pull (not gated on the lake sync), so Postgres stays fresh independent of archival cadence.
