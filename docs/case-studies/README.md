# Case Studies

Anonymized production incidents, written up the way a staff engineer would write a post-incident review. Every case study follows the same structure:

- **Problem** — what the job was trying to do and what went wrong.
- **Symptoms** — what the operator first noticed.
- **Evidence** — what the Spark UI, event logs, and YARN/EMR logs actually showed.
- **Root cause** — why it broke.
- **Fix** — what was changed to resolve it.
- **Result** — outcome after the fix.
- **Staff-level lesson** — the platform/operating insight, not just the local fix.

These are intentionally generic. No company, dataset, or volume here is a real one; the shapes are real, the numbers are illustrative.

## Index

- [`emr-merge-memory-spill.md`](emr-merge-memory-spill.md) — A large Iceberg merge on EMR that ran for 8+ hours and OOM'd repeatedly. The story is about diagnosing shuffle and spill pressure, resizing without overprovisioning, and splitting the merge into bounded batches.
