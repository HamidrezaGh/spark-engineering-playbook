# Screenshots and sample outputs

This directory holds **teaching assets** for the handbook: (1) **text** captures from local or sample runs, and (2) **placeholder PNGs** for future **Spark Web UI** and plan screenshots.

## Placeholder images (intentional for now)

The `placeholder-*.png` files are **minimal stand-ins**. They keep Markdown image syntax and layout valid until real screenshots are added. Replace them with real captures from:

- **Local Spark** (e.g. `local[*]`) or
- **Non-production lab / dev** clusters running **synthetic** or **public sample** workloads only

**Do not** add confidential production screens. The goal is to teach **symptoms** (skew, spill, lost executors, plan shape) — not to decorate the repo.

### If you add real screenshots

- **No private company data** — use synthetic tables, public datasets, or clearly fake names.
- **Redact** cluster names, bucket names, table names, account IDs, hostnames, tokens, and user names.
- Prefer **cropped** views that show the metric or column that matters, with a short **caption** explaining what a practitioner should infer.

### Current placeholders

| File | Intended teaching use |
| --- | --- |
| [`placeholder-spark-ui-skewed-stage.png`](placeholder-spark-ui-skewed-stage.png) | Stages or task list: one task much slower than the rest (skew / straggler). |
| [`placeholder-spark-ui-shuffle-spill.png`](placeholder-spark-ui-shuffle-spill.png) | Stage detail: high shuffle read/write and/or spill columns. |
| [`placeholder-explain-physical-plan.png`](placeholder-explain-physical-plan.png) | `EXPLAIN` / SQL tab: `Exchange`, join choice, `AdaptiveSparkPlan`. |
| [`placeholder-spark-ui-executors-failed-tasks.png`](placeholder-spark-ui-executors-failed-tasks.png) | Executors tab: failed tasks, GC imbalance, or lost executors. |
| [`placeholder-yarn-container-log-snippet.png`](placeholder-yarn-container-log-snippet.png) | Log excerpt style (pair with YARN/EMR docs — redact before commit). |

Docs that **reference** these placeholders include:

- [`../../observability/spark-ui-guide.md`](../../observability/spark-ui-guide.md)
- [`../../observability/physical-plans.md`](../../observability/physical-plans.md)
- [`../../book/02-shuffle-and-performance.md`](../../book/02-shuffle-and-performance.md)
- [`../../book/12-production-debugging.md`](../../book/12-production-debugging.md)
- [`../../book/05-data-skew.md`](../../book/05-data-skew.md)
- [`../../troubleshooting/slow-job.md`](../../troubleshooting/slow-job.md)
- [`../../troubleshooting/emr-yarn-failures.md`](../../troubleshooting/emr-yarn-failures.md)
- [`../../troubleshooting/iceberg-merge-issues.md`](../../troubleshooting/iceberg-merge-issues.md)
- [`../../troubleshooting/join-performance.md`](../../troubleshooting/join-performance.md)
- [`../../troubleshooting/skew-and-stragglers.md`](../../troubleshooting/skew-and-stragglers.md)

## Sample text outputs (regeneratable)

These are **labeled** excerpts from the runnable examples; regenerate anytime with the commands in the root [`README.md`](../../../README.md).

| File | Source |
| --- | --- |
| [`explain-formatted-shuffle-output.txt`](explain-formatted-shuffle-output.txt) | Illustrative `EXPLAIN FORMATTED` excerpt (shape matches Spark 3.x; line IDs vary by version). |
| [`skew-detector-output.txt`](skew-detector-output.txt) | Captured from `skew_detector.py` (`--demo` and local CSV). |
| [`file-count-audit-output.txt`](file-count-audit-output.txt) | Captured from `file_count_audit.py --demo`. |
