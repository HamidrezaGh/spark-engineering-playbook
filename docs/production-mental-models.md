# Production mental models (index)

This page is a **map**, not a separate handbook. The mental models in this repository are written
where they are used: mostly at the start of [book](book/README.md) chapters, plus short anchors in
[observability](observability/README.md) and [troubleshooting](troubleshooting/README.md) guides.

**Default production stack** assumed in most content: **Spark on YARN** (often **AWS EMR**), data on
**S3**, sometimes **Iceberg** — see [Documentation layout](README.md).

## The core loop (every incident)

1. **Hypothesis** — name the resource class: CPU, shuffle, memory/spill, skew, or environment.
2. **Evidence** — **Stages** → **SQL** (plan) → **Executors** in the [Spark UI guide](observability/spark-ui-guide.md).
3. **Smallest change** — one knob or one query change; verify in the same UI signals.
4. **Guardrail** — metric, template, or review checklist so the class of issue is easier next time; see
   [Platform patterns and guardrails](book/15-platform-patterns.md).

## Book chapters (mental model sections)

| Topic | Start here |
| --- | --- |
| Jobs, stages, tasks, shuffle boundaries | [Execution model](book/01-execution-model.md) |
| Shuffle cost and stragglers | [Shuffle and performance](book/02-shuffle-and-performance.md) |
| RDD/DF partitions, files, task count | [Partitioning](book/03-partitioning.md) |
| How Spark picks join strategy | [Joins](book/04-joins.md) |
| Skew, hot keys, long tails | [Data skew](book/05-data-skew.md) |
| AQE re-planning at runtime | [Adaptive query execution](book/06-adaptive-query-execution.md) |
| Heap, spill, OOM, PySpark | [Memory management](book/07-memory-management.md) |
| Columnar formats and pruning | [File formats](book/08-file-formats.md) |
| Catalyst and plans | [Spark SQL and Catalyst](book/09-spark-sql-and-catalyst.md) |
| Cache and persistence | [Caching and persistence](book/10-caching-and-persistence.md) |
| EMR, YARN, event logs | [Spark on YARN and EMR](book/11-spark-on-yarn-and-emr.md) |
| Four layers of a Spark incident | [Production debugging](book/12-production-debugging.md) |
| Table formats, MERGE, snapshots | [Iceberg and Spark](book/13-iceberg-and-spark.md) |
| Micro-batches, state, watermarks | [Structured streaming](book/14-structured-streaming.md) |
| Team-wide defaults and metrics | [Platform patterns](book/15-platform-patterns.md) |

Use [concept-map.md](concept-map.md) to jump from a **symptom question** to one chapter and an
[example](../examples/README.md).

## Symptom-first trees

- [Troubleshooting](troubleshooting/README.md) — slow job, join, skew, shuffle-heavy, OOM, small
  files, EMR/YARN, streaming lag, Iceberg MERGE, etc.
- [Field guides](field-guides/README.md) — short entry points for incidents.

## Narrative walkthroughs

- [Case studies](case-studies/README.md) — post-mortem style stories; numbers are illustrative.

## Related

- [Reading physical plans](observability/physical-plans.md) — connect plan nodes to stages and
  fixes.
- [Glossary](glossary.md) — terms with UI / `EXPLAIN` meaning.
