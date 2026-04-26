# Case Studies

Anonymized production incidents, written up the way a staff engineer would write a post-incident review. Case studies aim for a consistent arc:

- **Situation** — what the job was trying to do and the business context.
- **Symptoms** — what the operator first noticed.
- **Investigation** — what the Spark UI, logs, metrics, and cluster signals showed.
- **Root cause** — why it broke (often more than one compounding issue).
- **What did not work** — false paths that wasted time or money.
- **Fix** — what changed, preferably in the order it was validated.
- **Result** — outcome after the fix, with before/after metrics (often illustrative, not literal customer numbers).
- **Staff-level lesson** — the repeatable platform pattern, not only the local query fix.

These are intentionally generic. No company, dataset, bucket, account, or volume here is a real one; the failure shapes are real, the numbers are illustrative.

## Index

- [`emr-merge-memory-spill.md`](emr-merge-memory-spill.md) — Skewed Iceberg `MERGE` on EMR: widening merge scope, shuffle and spill pressure, why memory bumps stopped working, bounding the merge, and moving SLA-critical shuffle off Spot.
- [`streaming-state-blowup.md`](streaming-state-blowup.md) — A Structured Streaming job whose state store grew to 18 GB and whose batch duration drifted from 6 to 45 seconds over six weeks. The story is about watermarks, state TTL, key cardinality, and detecting slow drift before it becomes an SLA miss.
- [`s3-small-files-regression.md`](s3-small-files-regression.md) — A daily batch job whose runtime tripled over six months due to silent small-file accumulation on the source table. The story is about over-partitioning, failed compaction jobs, producer-side file sizing, and treating file count as a first-class table metric.

## How To Read These

These are not benchmarks or vendor pitches. They are the kind of write-ups a staff engineer would attach to a post-incident review or read during onboarding for a production rotation.

The intended audience is a senior or staff engineer who has seen Spark in production and wants the diagnostic loop, the smallest fix, and the platform-level lesson — not a tutorial. If you are new to Spark, read the relevant book chapter first and come back here.

The order in which to read them depends on what you are working on:

- Working on a large lakehouse `MERGE` or `UPDATE`: `emr-merge-memory-spill.md` first.
- Working on Structured Streaming or any stateful streaming pipeline: `streaming-state-blowup.md` first.
- Working on an Iceberg/Parquet table that is showing scan slowness: `s3-small-files-regression.md` first.
