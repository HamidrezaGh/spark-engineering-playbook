# Pre-Deploy Review Checklist

## Code

- Join strategy and shuffle awareness:
  - identify where shuffles happen (`Exchange`) and why
  - confirm join keys and join types are intentional
- Avoid accidental explosions:
  - check for cross joins / missing join conditions
  - check for `collect()` / `toPandas()` / driver-side materialization
- UDF caution:
  - verify any UDFs are necessary; prefer built-in SQL functions when possible
  - validate serialization and Python worker overhead risks
- Output behavior:
  - verify overwrite/merge semantics match idempotency requirements
  - verify partitioning and file sizing expectations for writes

## Data

- Input growth expectations:
  - expected bytes/rows per partition/day
  - whether late/backfill partitions are possible
- Data distribution:
  - identify keys likely to skew (top-key concentration)
  - validate high-cardinality partition columns won’t explode partition counts
- File layout:
  - check file counts and typical file sizes
  - ensure small-file risk is understood and mitigated

## Runtime

- Spark configs:
  - AQE enabled/validated (if used)
  - shuffle partitions choice is reasonable for expected sizes
  - broadcast threshold/strategy is safe for runtime sizes
- Resource sizing:
  - executor cores/memory and memory overhead appropriate for workload (especially PySpark)
  - confirm expected parallelism (tasks vs total cores)
- Operational dependencies:
  - event logs enabled and persisted (for post-mortem debugging)
  - S3/IAM permissions validated for read/write paths

## Rollback

- Have a rollback plan:
  - ability to rerun previous version or restore previous output snapshot
  - clear “stop the bleeding” mitigation (skip partition, disable new feature flag, etc.)
- Validate idempotency:
  - reruns do not duplicate data or corrupt tables
  - backfills can be paused/resumed safely
