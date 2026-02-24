# Market Data QA + Factor Store

## Live docs (GitHub Pages)

https://h2nryhe.github.io/Market-Data-QA---Factor-Store/


A compact, production-style reference project for **reliable market data pipelines**:

- explicit schema contracts
- composable data-quality validators
- immutable, checksummed snapshots
- deterministic, cacheable factor materialization

The goal of this project is to make market data and derived features behave like **reproducible assets** rather than ad-hoc files.

---

## Project Goal

Market data workflows often fail in quiet ways:

- schema drift
- duplicate bars
- broken time ordering
- missing intervals
- inconsistent backfills
- non-reproducible feature generation

This project provides a minimal but structured pipeline to address those issues by combining:

1. **data contracts** (schema + business rules)
2. **quality checks** (validators with structured reports)
3. **snapshot versioning** (manifests + checksums)
4. **deterministic feature materialization** (factor store style)

---

## Architecture (5-minute view)

```text
CSV sample/raw input
      |
      v
[schemas/*] contract checks (columns/dtypes/rules)
      |
      v
[validators/*] structural + duplicates + temporal + outliers
      |  (JSON report, PASS/WARN/FAIL, non-zero exit on FAIL)
      v
[versioning/*] snapshot -> data.parquet + manifest.json + checksums
      |
      v
[features/*] factor materialization from snapshot only
      |  (deterministic sort + cache key from checksum/config/version)
      v
features.parquet + feature_manifest.json
```

---

## Repository Structure

- `schemas/`: canonical data contract + typed rule/config models
- `validators/`: composable QA checks + structured reporting + CLI
- `versioning/`: snapshot creation + integrity verification CLI
- `features/`: feature registry / cache / materialization CLI
- `configs/`: schema / validator / feature policies
- `tests/`: unit + integration tests for schema, QA, versioning, features
- `ci/sample_pipeline.sh`: local end-to-end smoke run
- `.github/workflows/ci.yml`: CI workflow (if enabled in your fork)

---

## Quickstart

> Tested locally on Python 3.10 and 3.11.  
> Python **3.11+ is recommended** for best toolchain/lint parity.

```bash
# from repo root
python3.11 -m venv .venv
source .venv/bin/activate

python -m pip install --upgrade pip
python -m pip install -e '.[dev]'

# quality gate
ruff check .
black --check .
pytest -q
```

If your machine default is already Python 3.11+, replace `python3.11` with `python`.

---

## End-to-End Demo

Run the local smoke pipeline (validate -> snapshot -> verify -> materialize):

```bash
bash ci/sample_pipeline.sh
```

Expected artifacts include:

- `data/qa/validation_report_pipeline.json`
- `data/snapshots/market_ohlcv/<snapshot_id>/manifest.json`
- `data/features/market_ohlcv/<cache_key>/features.parquet`
- `data/features/market_ohlcv/<cache_key>/feature_manifest.json`

---

## Artifact Layout (after a demo run)

```text
data/
  qa/
    validation_report_pipeline.json
    validation_report.json
    ...
  snapshots/
    market_ohlcv/
      <snapshot_id>/
        data.parquet
        manifest.json
  features/
    market_ohlcv/
      <cache_key>/
        features.parquet
        feature_manifest.json
```

---

## Example Outputs (Operational Signals)

### 1) Lint + Format + Tests

```bash
ruff check .
black --check .
pytest -q
```

Typical result:

- `ruff`: All checks passed
- `black`: files left unchanged
- `pytest`: tests pass (e.g., `16 passed, 1 skipped`)

> Note: a local pandas warning may appear if `bottleneck<1.3.6`; it does **not** affect correctness.

---

### 2) Validation report (QA)

```bash
python -m validators.cli run \
  --input data/sample/market_ohlcv_sample.csv \
  --config configs/validation.yaml \
  --report data/qa/validation_report_manual_check.json
```

Expected signals:

- structured JSON report written to `data/qa/...`
- `overall_status` in `{PASS, WARN, FAIL}`
- per-validator results with:
  - `status`
  - `message`
  - `affected_row_count`
  - `sample_rows`
- non-zero exit code when `overall_status=FAIL`

---

### 3) Snapshot + manifest (reproducibility asset)

```bash
python -m versioning.cli snapshot \
  --input data/sample/market_ohlcv_sample.csv \
  --dataset market_ohlcv \
  --output-root data/snapshots \
  --schema-version 1.0.0
```

Artifacts:

- `data/snapshots/market_ohlcv/<snapshot_id>/data.parquet`
- `data/snapshots/market_ohlcv/<snapshot_id>/manifest.json`

Manifest records:

- dataset + snapshot id + created timestamp
- input path + artifact path
- row / symbol counts + symbol list
- date range
- schema version
- checksums: `artifact_sha256`, `dataset_sha256`

---

### 4) Snapshot verify (integrity check)

```bash
python -m versioning.cli verify \
  --snapshot-dir data/snapshots/market_ohlcv/<snapshot_id>
```

Expected signal:

- verification pass message
- exit code `0`

---

### 5) Factor materialization + cache hit

```bash
python -m features.cli materialize \
  --snapshot-dir data/snapshots/market_ohlcv/<snapshot_id> \
  --config configs/features.yaml
```

Expected signals:

- deterministic output under `data/features/market_ohlcv/<cache_key>/...`
- console output includes `cache_hit=<True|False>` and `cache_key=...`

Run the same command again with the same snapshot + config:

- expected `cache_hit=True`

---

## Validation Examples

### PASS example

```bash
python -m validators.cli run \
  --input data/sample/market_ohlcv_sample.csv \
  --config configs/validation.yaml \
  --report data/qa/validation_report.json
```

Typical summary:

- `overall_status=PASS`
- `structural=PASS`
- `duplicates=PASS`
- `temporal=PASS`
- `outliers=PASS`

### FAIL injection example (duplicate row)

```bash
cp data/sample/market_ohlcv_sample.csv /tmp/market_ohlcv_with_duplicate.csv

# append one duplicate row
tail -n 1 data/sample/market_ohlcv_sample.csv >> /tmp/market_ohlcv_with_duplicate.csv

python -m validators.cli run \
  --input /tmp/market_ohlcv_with_duplicate.csv \
  --config configs/validation.yaml \
  --report /tmp/validation_report_duplicate.json

echo $?  # expected: 1
```

Typical failure signal:

- `overall_status=FAIL`
- `duplicates=FAIL`
- non-zero CLI exit code

---

## Snapshot, Checksum, Manifest

Create and verify an immutable snapshot:

```bash
python -m versioning.cli snapshot \
  --input data/sample/market_ohlcv_sample.csv \
  --dataset market_ohlcv \
  --output-root data/snapshots \
  --schema-version 1.0.0

python -m versioning.cli verify \
  --snapshot-dir data/snapshots/market_ohlcv/<snapshot_id>
```

### Manifest guarantees

The manifest captures both **artifact identity** and **dataset identity**:

- `artifact_sha256`: file-level integrity check
- `dataset_sha256`: deterministic dataset checksum for reproducibility

This separation is useful because file bytes and dataset semantics can drift for different reasons.

### Determinism notes

- dataset-level checksum uses explicit row sorting before canonical row hashing
- canonical row hashing uses stable JSON serialization + SHA256
- feature materialization sorts rows deterministically before computing outputs

---

## Factor Store Example

Materialize factors from a **verified snapshot** (not raw path):

```bash
python -m features.cli materialize \
  --snapshot-dir data/snapshots/market_ohlcv/<snapshot_id> \
  --config configs/features.yaml
```

MVP feature set:

- `ret_1d`
- `ret_5d`
- `vol_20d`
- `mom_20d`
- `dollar_volume`

### Cache-key safety

Cache key composition includes:

- input snapshot checksum
- feature config hash
- feature version map

This prevents accidental reuse of stale artifacts when:

- the upstream snapshot changes
- feature parameters change
- feature implementations are version-bumped

---

## CLI Exit Codes

To make the tools automation-friendly:

- `0`: success (PASS / warn-only run)
- non-zero: validation fail, integrity fail, or CLI/runtime error

This allows CI (or any scheduler/orchestrator) to treat data-quality failures as pipeline failures.

---

## Design Tradeoffs + Limitations

- **Strict contract/validator-first approach** prioritizes safety over permissive coercion.
- Sample data is intentionally small; long-window factors are warmup-null by design.
- No incremental feature backfill planner yet (full materialization on cache miss).
- No orchestration service/scheduler layer yet (currently CLI/CI-driven).
- Temporal checks are basic and not yet fully trading-calendar-aware.

---

## Roadmap

- Add normalization adapters for multiple upstream vendor schemas.
- Add configurable trading-calendar-aware temporal gap logic.
- Add partitioned feature storage and incremental recompute plans.
- Add lineage links from factor artifacts back to validator report IDs.
- Add benchmark suite for validator/feature runtime and memory.
- Add multi-source cross-check validators (vendor A vs vendor B consistency).

---

## Extension Ideas

If you want to grow this into a larger platform, natural next steps include:

- scheduler/orchestrator integration (Airflow/Prefect/Dagster)
- partitioned snapshots and feature tables
- data catalog / metadata service integration
- contract testing for upstream vendor feeds
- historical replay and backfill audit tooling

---

## Local Notes (Optional)

If you see a pandas performance warning during tests related to `bottleneck`, you can silence it with:

```bash
python -m pip install -U bottleneck
```

This is optional and does not affect functional correctness.
