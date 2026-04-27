# Case Studies

Anonymized production incidents, written in the same structure as a post-incident review. Case studies aim for a consistent arc:

- **Situation** — what the job was trying to do and the business context.
- **Symptoms** — what the operator first noticed.
- **Evidence** — what the Spark UI, event logs, streaming progress, file counts, and YARN/EMR/CloudWatch logs actually showed.
- **Root cause** — why it broke.
- **Fix** — what was changed to resolve it, in the order it was applied.
- **Result** — outcome after the fix, with before/after metrics.
- **Lessons** — the platform/operating insights, not just the local fix.
- **Guardrails added** — what the team built so the same incident is faster to diagnose next time.

These are intentionally generic. No company, dataset, bucket, account, or volume here is a real one; the failure shapes are real, the numbers are illustrative.

## Index

- [`emr-merge-memory-spill.md`](emr-merge-memory-spill.md) — A large Iceberg `MERGE` on EMR that ran for 8+ hours and OOM'd repeatedly. The story is about diagnosing shuffle and spill pressure, resizing without overprovisioning, splitting the merge into bounded batches, and moving SLA-critical shuffle off Spot.
- [`streaming-state-blowup.md`](streaming-state-blowup.md) — A Structured Streaming job whose state store grew to 18 GB and whose batch duration drifted from 6 to 45 seconds over six weeks. The story is about watermarks, state TTL, key cardinality, and detecting slow drift before it becomes an SLA miss.
- [`s3-small-files-regression.md`](s3-small-files-regression.md) — A daily batch job whose runtime tripled over six months due to silent small-file accumulation on the source table. The story is about over-partitioning, failed compaction jobs, producer-side file sizing, and treating file count as a first-class table metric.

## How To Read These

These are not benchmarks or vendor pitches. They are the kind of write-ups you would attach to a post-incident review or read during onboarding for a production rotation.

The intended audience is a senior engineer who has seen Spark in production and wants the diagnostic loop, the smallest fix, and the platform-level lesson — not a tutorial. If you are new to Spark, read the relevant book chapter first and come back here.

The order in which to read them depends on what you are working on:

- Working on a large lakehouse `MERGE` or `UPDATE`: `emr-merge-memory-spill.md` first.
- Working on Structured Streaming or any stateful streaming pipeline: `streaming-state-blowup.md` first.
- Working on an Iceberg/Parquet table that is showing scan slowness: `s3-small-files-regression.md` first.
