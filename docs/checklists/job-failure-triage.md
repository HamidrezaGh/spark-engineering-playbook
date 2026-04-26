# Job Failure Triage Checklist

Status: Draft

## Scope The Failure

- Capture the exact failure mode and where it surfaced:
  - driver vs executor vs cluster provisioning
  - deterministic vs flaky
  - first failure time and last good run
- Identify the blast radius:
  - one job vs many jobs (platform issue)
  - one partition/day vs all partitions (data issue)
  - one table vs many tables (dependency or permissions issue)
- Preserve artifacts:
  - Spark event logs (so you can debug after the cluster is gone)
  - Spark UI screenshots/links for the failed stage
  - YARN/EMR step logs and executor stderr for the failure
  - the SQL physical plan for the failing query/operator

## Inspect Spark Signals

- In Spark UI:
  - **Stages**: find the failed stage, check retries and per-task failures
  - **SQL**: locate the operator feeding the failed stage (`Exchange`, join, aggregation, write)
  - **Executors**: check lost executors, GC time, and skew/hotspots
- Classify the failure:
  - OOM / container killed
  - fetch failure / shuffle corruption
  - write commit failure (permissions, rename/commit protocol issues, throttling)
  - serialization / Python worker issues
  - query planning failures (too many files, too large plan)

## Inspect Data Signals

- Check whether the input changed:
  - row count / bytes for the affected partition(s)
  - file count and file size distribution
  - schema evolution / unexpected nulls
  - key distribution (hot keys / skew)
- Validate table metadata (Iceberg/Hive-like tables):
  - partition stats, file counts, manifest growth (if applicable)
- If failure is localized:
  - isolate the specific partition/day/key that triggers the failure and reproduce on a smaller slice

## Choose Remediation

- Prefer the smallest safe fix that targets the failing operator:
  - If skew: hot-key handling, repartitioning strategy, AQE skew handling (if applicable)
  - If OOM: reduce per-task data (more partitions), reduce row width, avoid unsafe broadcast, fix caching misuse
  - If fetch failure: investigate executor loss/disk pressure, reduce shuffle size, validate shuffle service behavior
  - If write failure: validate permissions/IAM, commit protocol, output paths, and object-store health
- Decide whether to:
  - rerun safely (idempotency/overwrite semantics)
  - backfill later
  - apply a mitigation (skip bad partition) while preparing a full fix

## Prevent Recurrence

- Add a guardrail metric or check:
  - input size, output size, file count, shuffle bytes, spill bytes, top-key concentration, max task duration
- Store explain plans and event logs for critical pipelines.
- Add a pre-deploy review checklist item tied to the root cause.
- If the failure was data-quality driven, add upstream validation and quarantine behavior.
