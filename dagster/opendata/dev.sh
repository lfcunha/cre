#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"
export DAGSTER_HOME="$(pwd)/.dagster_home"
mkdir -p "$DAGSTER_HOME"
exec uv run dagster dev "$@"
