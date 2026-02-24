#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

start_ts=$(python - <<'PY'
import time
print(time.time())
PY
)

echo "[pipeline] root=$ROOT_DIR"

echo "[pipeline] step 1/4 validate sample data"
python -m validators.cli run \
  --input data/sample/market_ohlcv_sample.csv \
  --config configs/validation.yaml \
  --report data/qa/validation_report_pipeline.json

echo "[pipeline] step 2/4 create snapshot"
snapshot_output=$(python -m versioning.cli snapshot \
  --input data/sample/market_ohlcv_sample.csv \
  --dataset market_ohlcv \
  --output-root data/snapshots \
  --schema-version 1.0.0)

echo "$snapshot_output"
snapshot_dir=$(echo "$snapshot_output" | awk -F': ' '/Snapshot created:/ {print $2}' | tail -n 1)
if [[ -z "${snapshot_dir}" ]]; then
  echo "[pipeline] failed to parse snapshot directory from CLI output"
  exit 1
fi

echo "[pipeline] step 3/4 verify snapshot"
python -m versioning.cli verify --snapshot-dir "$snapshot_dir"

echo "[pipeline] step 4/4 materialize features"
python -m features.cli materialize --snapshot-dir "$snapshot_dir" --config configs/features.yaml

end_ts=$(python - <<'PY'
import time
print(time.time())
PY
)

python - <<PY
start_ts = float("$start_ts")
end_ts = float("$end_ts")
print(f"[pipeline] completed in {end_ts - start_ts:.2f}s")
print(f"[pipeline] snapshot_dir=$snapshot_dir")
print("[pipeline] report=data/qa/validation_report_pipeline.json")
PY
