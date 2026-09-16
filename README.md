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

**Via Dagster (materialize an asset)** — the normal path. Checks Socrata's dataset-level watermark first and skips the fetch if nothing's changed, does incremental `--since`-filtered pulls where the dataset supports it, and on success converts the raw pull to Parquet and syncs it to the S3 lake. Also runs automatically on the daily 6am (America/New_York) schedule once `dagster dev`'s daemon is up.

```bash
cd dagster/opendata
uv run dagster asset materialize --select acris_legals -m opendata.definitions    # one dataset's raw pull
uv run dagster asset materialize --select acris_legals+ -m opendata.definitions   # that pull + its lake sync
uv run dagster asset materialize --select '*' -m opendata.definitions             # everything
```

Or via the UI: Assets → select one or more → Materialize.

**Standalone script** — for manual/ad hoc pulls or debugging a single dataset without going through Dagster. Same underlying pull logic (auth, zstd compression, checkpoint/resume), but **no watermark check and no lake sync** — those only exist in the Dagster asset wrapper. A standalone run always does a real pull regardless of whether the source actually changed, and only lands the raw `.ndjson.zst` file.

```bash
uv run python dagster/opendata/opendata/scripts/opendata_puller.py --dataset pluto
uv run python dagster/opendata/opendata/scripts/opendata_puller.py --dataset acris_master --since 2026-01-01
```

### Pipeline design

- **Datasets**: ACRIS Legals/Master/Parties/References, PLUTO, ACRIS Document Control Codes — all NYC Open Data (Socrata), Manhattan-filtered where the dataset supports a borough field.
- **Raw landing**: newline-delimited JSON, streamed through zstd compression as it's written page-by-page (`scripts/raw_data/`) — a killed pull still leaves a decodable partial file.
- **Change detection**: before pulling, each asset checks Socrata's dataset-level `rowsUpdatedAt` against a saved watermark (`scripts/watermarks/`) and skips the fetch entirely if nothing changed. Datasets with a real per-row date field (`acris_legals`, `acris_master`) additionally use `--since` to fetch only new rows; the rest (`acris_parties`, `acris_references`, `pluto`, doc control codes) re-land in full whenever they've changed.
- **Lake sync**: each successful pull is converted to Parquet and uploaded to S3. Incremental datasets accumulate one additive Parquet file per pull. Full-snapshot datasets instead write to `{dataset}/latest/{dataset}.parquet` (overwritten each sync — what analytics should query) plus `{dataset}/archive/{dataset}_{timestamp}.parquet` (kept for history).
- **Not yet built**: loading pulls into Postgres. Planned to happen right after each successful pull (not gated on the lake sync), so Postgres stays fresh independent of archival cadence.
