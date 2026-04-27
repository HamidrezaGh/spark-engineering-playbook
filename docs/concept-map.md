# Concept map: questions → chapters and examples

Navigate by **symptom** or **question**. Depth is in the linked chapters, not in this table.

| Question | Read this first | Example / tree |
| --- | --- | --- |
| Why did Spark create a new **stage**? | [Execution model](book/01-execution-model.md) · [Shuffle](book/02-shuffle-and-performance.md) | [explain-shuffle.sql](../examples/sql/01-explain-shuffle.sql) |
| Why is **one task** much slower? | [Data skew](book/05-data-skew.md) · [Observability / UI](observability/spark-ui-guide.md) | [skew-demo](../examples/pyspark/skew-demo/README.md) · [skew tree](troubleshooting/skew-and-stragglers.md) |
| Why is **shuffle** so large? | [Shuffle and performance](book/02-shuffle-and-performance.md) · [Joins](book/04-joins.md) | [shuffle tree](troubleshooting/shuffle-heavy-job.md) |
| Why did the **join** get slow? | [Joins](book/04-joins.md) · [Stats / CBO](book/19-statistics-and-cost-based-optimization.md) | [join-strategies](../examples/sql/join-strategies/README.md) · [join tree](troubleshooting/join-performance.md) |
| Why **OOM** or **spill**? | [Memory](book/07-memory-management.md) | [memory tree](troubleshooting/memory-spill-oom.md) |
| Why so many **small files**? | [Write path](book/17-spark-write-path-and-output-files.md) · [Partitioning](book/03-partitioning.md) | [partitioning-demo](../examples/pyspark/partitioning-demo/README.md) · [small-files tree](troubleshooting/small-files.md) |
| Why **YARN/EMR** failure or **lost executors**? | [EMR + YARN](book/11-spark-on-yarn-and-emr.md) · [Debugging](book/12-production-debugging.md) | [emr-yarn tree](troubleshooting/emr-yarn-failures.md) |
| Why is **Iceberg MERGE** slow or heavy? | [Iceberg](book/13-iceberg-and-spark.md) · [Write path](book/17-spark-write-path-and-output-files.md) | [iceberg-write-path](../examples/sql/iceberg-write-path/README.md) · [merge tree](troubleshooting/iceberg-merge-issues.md) |
| Why is **structured streaming** behind? | [Structured Streaming](book/14-structured-streaming.md) | [streaming-lag](troubleshooting/streaming-lag.md) |
| How do I read a **physical plan**? | [Spark SQL + Catalyst](book/09-spark-sql-and-catalyst.md) | [physical-plans](observability/physical-plans.md) |
| How do I triage a **slow job**? | [Production debugging](book/12-production-debugging.md) | [slow-job tree](troubleshooting/slow-job.md) |
| What do **EMR, S3, and IAM** change? | [Object storage](book/18-object-storage-with-spark.md) · [EMR](book/11-spark-on-yarn-and-emr.md) | [Patterns: merge](patterns/large-iceberg-merge.md) |

**Glossary:** [`glossary.md`](glossary.md) · **Self-check questions:** [`practical-spark-questions.md`](practical-spark-questions.md)
