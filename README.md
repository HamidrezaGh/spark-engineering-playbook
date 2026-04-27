# Spark Engineering Playbook

A **practical Apache Spark engineering** playbook for **understanding, debugging, tuning, and
operating** production Spark workloads (batch and streaming, SQL-first with PySpark where execution
and tooling matter). Depth comes from the content — not from self-labels or question lists.

The examples in this repository assume a common real-world path: **Spark on YARN** (often **AWS
EMR**) and **S3**-backed data, sometimes **Apache Iceberg**. Core Spark behavior is the same in
other deployments; the operational notes call out where **object storage**, **YARN**, and
**lakehouse** formats change the picture.

## Why this exists

In production, Spark work is less about reciting APIs and more about **diagnosing**:

- A stage that was fast **yesterday** and slow **today** (skew, bad stats, layout change, or cluster health).
- **Shuffle**-heavy jobs that dominate cost and **fail** with `FetchFailed` when executors or disks are stressed.
- **Memory** and **spill** patterns that only show up at scale, especially with **Python** and **UDFs**.
- **Write paths** that create **thousands of small files** or unsafe **overwrites** on object stores.
- **Iceberg** (or similar) **MERGE** operations that are correct in SQL but **full scans** in practice.

This playbook exists to make those situations **boring to triage**: read the **physical plan**, read
the **Spark UI**, follow a **decision tree**, and pick the **smallest** verifiable next step.

## What this repo covers

- **Execution model** — actions, jobs, stages, tasks, and why **shuffle** defines boundaries.
- **SQL engine** — Catalyst, `EXPLAIN`, join strategies, **AQE**, and a bit of Tungsten.
- **Performance** — shuffle, **partitioning**, **joins**, **skew**, **broadcast** tradeoffs, **caching** reality.
- **Memory** — spill, OOM, PySpark **overhead**, and what the UI shows before you add heap.
- **Storage** — **Parquet/ORC** behavior, read pruning, and **table design** for cost.
- **Operations** — **EMR/YARN**, log and event log habits, and failure modes you see in the field.
- **Lakehouse** — **Iceberg** snapshots, **MERGE** cost, and maintenance (compaction) as **scheduled** work.
- **Streaming** — checkpoints, watermarks, state, and **sink** back-pressure.
- **Reliability** — idempotency, **correct reruns**, and what “**safe to retry**” means for writes.
- **Platform** — when to standardize (templates, guardrails, **metrics**) instead of re-tuning one job at a time.

## What this repo is *not*

- **Not a shortcut** or flash-card substitute for **running** Spark and reading a few real plans and UIs.
- **Not** tied to a single **vendor** — EMR and S3 are a **bias** from how many teams actually run, not a requirement.
- **Not a replacement** for the **official** [Spark](https://spark.apache.org/docs/latest/) and
  **format** (e.g. Iceberg) **documentation** — this is a complement, a map, and a set of
  checklists.
- **Not a promise** of perfect coverage: chapters vary in depth; **TODO** / in-progress areas are
  called out in the text when they exist.

## How to use this repo

| Goal | Start here |
| --- | --- |
| **Learn internals** in order | [`docs/book/README.md`](docs/book/README.md) — Chapters 1–6, then 7–10, then storage and platform chapters as needed. |
| **Debug a slow or failing job** | [`docs/observability/spark-ui-guide.md`](docs/observability/spark-ui-guide.md) → [`docs/troubleshooting/`](docs/troubleshooting/README.md) (decision trees) → linked book chapters. |
| **Tune** a workload you understand | `EXPLAIN` + UI evidence first, then [`docs/tuning/`](docs/tuning/README.md) and [`docs/configs/`](docs/configs/README.md) for targeted knobs. |
| **Read physical plans** | [`docs/observability/physical-plans.md`](docs/observability/physical-plans.md) and [`examples/sql/01-explain-shuffle.sql`](examples/sql/01-explain-shuffle.sql). |
| **EMR / YARN / S3 / Iceberg** | Book chapters 11, 13, 17, 18; patterns under [`docs/patterns/`](docs/patterns/README.md). |

**Navigate by problem:** [`docs/concept-map.md`](docs/concept-map.md) maps “why is X slow?”
questions to **chapters and examples**.

## Chapter map (book)

| # | Chapter |
| ---: | --- |
| 1 | [Execution model](docs/book/01-execution-model.md) |
| 2 | [Shuffle and performance](docs/book/02-shuffle-and-performance.md) |
| 3 | [Partitioning](docs/book/03-partitioning.md) |
| 4 | [Joins](docs/book/04-joins.md) |
| 5 | [Data skew](docs/book/05-data-skew.md) |
| 6 | [Adaptive query execution (AQE)](docs/book/06-adaptive-query-execution.md) |
| 7 | [Memory management](docs/book/07-memory-management.md) |
| 8 | [File formats](docs/book/08-file-formats.md) |
| 9 | [Spark SQL and Catalyst](docs/book/09-spark-sql-and-catalyst.md) |
| 10 | [Caching and persistence](docs/book/10-caching-and-persistence.md) |
| 11 | [Spark on AWS EMR and YARN](docs/book/11-spark-on-yarn-and-emr.md) |
| 12 | [Production debugging](docs/book/12-production-debugging.md) |
| 13 | [Iceberg and Spark](docs/book/13-iceberg-and-spark.md) |
| 14 | [Structured streaming](docs/book/14-structured-streaming.md) |
| 15 | [Platform patterns and guardrails](docs/book/15-platform-patterns.md) |
| 16 | [Data correctness and idempotency](docs/book/16-data-correctness-and-idempotency.md) |
| 17 | [Write path and output files](docs/book/17-spark-write-path-and-output-files.md) |
| 18 | [S3 and object storage with Spark on EMR](docs/book/18-object-storage-with-spark.md) |
| 19 | [Statistics and CBO](docs/book/19-statistics-and-cost-based-optimization.md) |
| 20 | [Dependency management and packaging](docs/book/20-dependency-management-and-packaging.md) |
| 21 | [Security and governance](docs/book/21-security-and-governance.md) |
| 22 | [Testing and CI/CD](docs/book/22-testing-and-cicd.md) |
| 23 | [Data modeling and table design](docs/book/23-data-modeling-and-table-design.md) |
| 24 | [Incremental processing and backfills](docs/book/24-incremental-processing-and-backfills.md) |
| 25 | [Cluster and workload isolation](docs/book/25-cluster-and-workload-isolation.md) |

**Self-check questions (optional depth):** [`docs/practical-spark-questions.md`](docs/practical-spark-questions.md)

**Glossary:** [`docs/glossary.md`](docs/glossary.md)

## Navigation aids

| Resource | |
| --- | --- |
| **Troubleshooting (decision trees)** | [`docs/troubleshooting/README.md`](docs/troubleshooting/README.md) |
| **Spark UI and plans** | [`docs/observability/README.md`](docs/observability/README.md) |
| **Field guides (short)** | [`docs/field-guides/README.md`](docs/field-guides/README.md) |
| **Checklists** | [`docs/checklists/README.md`](docs/checklists/README.md) |
| **Tuning and configs** | [`docs/tuning/README.md`](docs/tuning/README.md), [`docs/configs/README.md`](docs/configs/README.md) |
| **Case studies** | [`docs/case-studies/README.md`](docs/case-studies/README.md) |
| **Diagrams** | [`diagrams/README.md`](diagrams/README.md) |

## Practical examples (runnable or clearly scoped)

- **EXPLAIN and shuffle** — [`examples/sql/01-explain-shuffle.sql`](examples/sql/01-explain-shuffle.sql)  
  **Sample:** [`docs/assets/screenshots/explain-formatted-shuffle-output.txt`](docs/assets/screenshots/explain-formatted-shuffle-output.txt)
- **Join strategies (broadcast vs sort-merge)** — [`examples/sql/join-strategies/`](examples/sql/join-strategies/README.md)
- **Skew (PySpark)** — [`examples/pyspark/skew-demo/`](examples/pyspark/skew-demo/README.md) and [`examples/pyspark/skew_detector.py`](examples/pyspark/skew_detector.py)  
  **Sample:** [`docs/assets/screenshots/skew-detector-output.txt`](docs/assets/screenshots/skew-detector-output.txt)
- **Partitioning and file count** — [`examples/pyspark/partitioning-demo/`](examples/pyspark/partitioning-demo/README.md) and [`examples/pyspark/inspect_partitions.py`](examples/pyspark/inspect_partitions.py)
- **Small-file audit** — [`examples/pyspark/file_count_audit.py`](examples/pyspark/file_count_audit.py)  
  **Sample:** [`docs/assets/screenshots/file-count-audit-output.txt`](docs/assets/screenshots/file-count-audit-output.txt)
- **Iceberg (templates)** — [`examples/sql/iceberg-write-path/`](examples/sql/iceberg-write-path/README.md) (requires Iceberg-enabled Spark)

### Run the bundled harness

```bash
cd examples/local
./run_examples.sh
```

`./run_examples.sh sql` and `./run_examples.sh pyspark` are supported. See
[`examples/local/README.md`](examples/local/README.md).

## Production troubleshooting (quick)

1. **Find one dominant slow stage** (Spark UI **Stages** tab).
2. **Classify** — skew (long tail) vs even slowness (CPU, shuffle, spill, GC, or environment).
3. **Map to operators** — **SQL** tab; read [`docs/observability/physical-plans.md`](docs/observability/physical-plans.md) if the join or scan is surprising.
4. **Use a tree** — e.g. [`docs/troubleshooting/slow-job.md`](docs/troubleshooting/slow-job.md) or
   [`docs/troubleshooting/skew-and-stragglers.md`](docs/troubleshooting/skew-and-stragglers.md).
5. **One change, one verification run** — avoid multi-knob “tuning” without evidence.

## CI and quality

[`.github/workflows/docs-check.yml`](.github/workflows/docs-check.yml) runs **markdownlint** and
**lychee** on Markdown, plus `compileall` for `examples/` Python and `bash`/`shellcheck` for shell
scripts. See [`CONTRIBUTING.md`](CONTRIBUTING.md) to align with the same checks locally.

## Contribute

If you have a **concrete** production correction or a small runnable example, open an issue or PR
with the exact chapter, claim, and scenario. Vague style-only nits are unlikely to land. See
[`CONTRIBUTING.md`](CONTRIBUTING.md) for structure and quality gates.

## License

[MIT](LICENSE) — see file for text.

Using this material in interviews is a **side effect** of knowing Spark well; this repo is written
to help you **operate and debug** systems, not to optimize a question bank.
