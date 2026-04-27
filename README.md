# Spark Engineering Playbook

[![docs-check](https://github.com/HamidrezaGh/spark-engineering-playbook/actions/workflows/docs-check.yml/badge.svg?branch=main)](https://github.com/HamidrezaGh/spark-engineering-playbook/actions/workflows/docs-check.yml)

A **practical Apache Spark engineering** playbook for **understanding, debugging, tuning, and
operating** production Spark workloads (batch and streaming, SQL-first with PySpark where execution
and tooling matter). The focus is **production debugging** and **operational reliability**: what
breaks, what to look at in the **Spark UI** and **`EXPLAIN`**, and which **next step** is smallest
and verifiable. [Platform patterns and guardrails](docs/book/15-platform-patterns.md) cover how teams
turn one-off fixes into shared defaults. Depth comes from the content, not from self-labels or
canned question lists.

The examples in this repository assume a common real-world path: **Spark on YARN** (often **AWS
EMR**) and **S3**-backed data, sometimes **Apache Iceberg**. Core Spark behavior is the same in
other deployments; the operational notes call out where **object storage**, **YARN**, and
**lakehouse** formats change the picture.

## Why this exists

In production, Spark work is less about reciting APIs and more about **diagnosing** real failures and regressions, for example:

- **Slow jobs** or sudden runtime regressions with no code change.
- **Shuffle-heavy** stages that dominate wall-clock time and **fail** with `FetchFailed` when executors, disks, or the network are stressed.
- **Skewed tasks** and **stragglers** (one task far slower than the rest) pointing at data shape or partition problems.
- **Memory spill** and OOMs that only appear at scale, especially with **Python** and **UDFs**.
- **Inefficient joins** (missing broadcast, bad join order, surprising **sort-merge** on large sides).
- **Small-file** layouts that explode metadata and task count, or **expensive writes** (too many output files, bad commit behavior on object storage).
- **Iceberg** (or similar) **MERGE** and maintenance operations that are correct in SQL but **full-table scans** in practice.
- **EMR / YARN** issues: lost executors, preemption, queue contention, and mis-sized containers.
- **Confusing Spark UI** symptoms: long GC, high scheduler delay, spill columns, uneven executor load.
- **Physical plans** that are hard to read or disagree with your mental model (AQE, missing pushdown, surprise `Exchange`).

This playbook exists to make those situations **boring to triage**: read the **physical plan**, read
the **Spark UI**, follow a **decision tree** or [checklist](docs/checklists/README.md), and pick
the **smallest** verifiable next step. See [Sample UI / plan screenshot placeholders](docs/assets/screenshots/README.md) for where visuals will eventually reinforce the same signals.

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

- **Not a substitute** for **running** Spark and reading a few real plans and UIs on workloads you own or a lab cluster.
- **Not** tied to a single **vendor** — EMR and S3 are a **bias** from how many teams actually run, not a requirement.
- **Not a replacement** for the **official** [Spark](https://spark.apache.org/docs/latest/) and
  **format** (e.g. Iceberg) **documentation** — this is a complement, a map, and a set of
  checklists.
- **Not a promise** of perfect coverage: chapters vary in depth; **TODO** / in-progress areas are
  called out in the text when they exist.

## Who this is for

- **Data and platform engineers** who own Spark jobs in production: reliability, cost, and debug time.
- **Teams on EMR/YARN and S3** (or similar) who need **practical Spark internals** plus **operational reliability** patterns.
- **Anyone** who is comfortable with basic Spark and wants a **debugging workflow**, not a feature tour.

## How to use the repo

| Goal | Start here |
| --- | --- |
| **Mental model map (links to chapter anchors)** | [`docs/production-mental-models.md`](docs/production-mental-models.md) |
| **Learn internals** in order | [`docs/book/README.md`](docs/book/README.md) — Chapters 1–6, then 7–10, then storage and platform chapters as needed. |
| **Debug a slow or failing job** | [`docs/observability/spark-ui-guide.md`](docs/observability/spark-ui-guide.md) → [`docs/troubleshooting/`](docs/troubleshooting/README.md) (decision trees) → linked book chapters. |
| **Tune** a workload you understand | `EXPLAIN` + UI evidence first, then [`docs/tuning/`](docs/tuning/README.md) and [`docs/configs/`](docs/configs/README.md) for targeted knobs. |
| **Read physical plans** | [`docs/observability/physical-plans.md`](docs/observability/physical-plans.md) and [`examples/sql/01-explain-shuffle.sql`](examples/sql/01-explain-shuffle.sql). |
| **EMR / YARN / S3 / Iceberg** | Book chapters 11, 13, 17, 18; patterns under [`docs/patterns/`](docs/patterns/README.md). |

**Navigate by problem:** [`docs/concept-map.md`](docs/concept-map.md) maps “why is X slow?”
questions to **chapters and examples**.

## How the docs are organized

- **[`docs/book/`](docs/book/README.md)** — Long-form chapters: **practical Spark internals**, production failure modes, and how to verify behavior in the UI and plans.
- **[`docs/troubleshooting/`](docs/troubleshooting/README.md)** — **Symptom-first** decision trees (slow job, skew, shuffle-heavy, OOM, small files, streaming lag, EMR/YARN, Iceberg MERGE, etc.); each page ties symptoms → Spark UI / logs → likely causes → next steps.
- **[`docs/checklists/`](docs/checklists/README.md)** — **One-page** lists for pre-deploy review, job design, write path, Iceberg, EMR, streaming, and triage. Use as **engineering reference** in design review or incident handoff.
- **[`docs/observability/`](docs/observability/README.md)** — **Spark UI** and **physical plan** reading guides; connect pixels and plan nodes to code and config changes.
- **[`docs/field-guides/`](docs/field-guides/README.md)** — Shorter **incident** entry points that link into the same material.
- **[`docs/case-studies/`](docs/case-studies/README.md)** — Narrative post-mortem style **production debugging** walkthroughs.
- **[`examples/`](examples/README.md)** — Runnable or clearly scoped SQL/PySpark; see below.

**Optional self-check:** [`docs/practical-spark-questions.md`](docs/practical-spark-questions.md) (concept review, not a study bank). **Glossary:** [`docs/glossary.md`](docs/glossary.md).

## How examples are organized

Examples live under [`examples/`](examples/README.md). They are small, annotated snippets you can run locally (CSV-backed) or adapt for your cluster, paired with book chapters in each README.

| Area | Role |
| --- | --- |
| [`examples/sql/`](examples/sql/README.md) | `EXPLAIN`, join strategies, skew detection, window vs `GROUP BY`, Iceberg **templates** where noted. |
| [`examples/sql/join-strategies/`](examples/sql/join-strategies/README.md) | **Broadcast** vs **sort-merge** side by side. |
| [`examples/pyspark/`](examples/pyspark/README.md) | Inspector scripts: partitions, **skew** detector, file-count **audit**. |
| [`examples/local/`](examples/local/README.md) | **Harness** and sample data: `cd examples/local && ./run_examples.sh` |

Labeled **text** sample outputs (for comparison when you run the tools) are under [`docs/assets/screenshots/`](docs/assets/screenshots/README.md) together with **PNG screenshot placeholders** for future Spark UI and plan images.

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

## Navigation aids (quick links)

| Resource | |
| --- | --- |
| **Troubleshooting (decision trees)** | [`docs/troubleshooting/README.md`](docs/troubleshooting/README.md) |
| **Spark UI and plans** | [`docs/observability/README.md`](docs/observability/README.md) |
| **Field guides (short)** | [`docs/field-guides/README.md`](docs/field-guides/README.md) |
| **Checklists** | [`docs/checklists/README.md`](docs/checklists/README.md) |
| **Tuning and configs** | [`docs/tuning/README.md`](docs/tuning/README.md), [`docs/configs/README.md`](docs/configs/README.md) |
| **Case studies** | [`docs/case-studies/README.md`](docs/case-studies/README.md) |
| **Diagrams** | [`diagrams/README.md`](diagrams/README.md) |
| **Screenshot & sample-output assets** | [`docs/assets/screenshots/README.md`](docs/assets/screenshots/README.md) |

## Practical examples (runnable or clearly scoped)

- **EXPLAIN and shuffle** — [`examples/sql/01-explain-shuffle.sql`](examples/sql/01-explain-shuffle.sql)  
  **Sample text:** [`docs/assets/screenshots/explain-formatted-shuffle-output.txt`](docs/assets/screenshots/explain-formatted-shuffle-output.txt)
- **Join strategies (broadcast vs sort-merge)** — [`examples/sql/join-strategies/`](examples/sql/join-strategies/README.md)
- **Skew (PySpark)** — [`examples/pyspark/skew-demo/`](examples/pyspark/skew-demo/README.md) and [`examples/pyspark/skew_detector.py`](examples/pyspark/skew_detector.py)  
  **Sample text:** [`docs/assets/screenshots/skew-detector-output.txt`](docs/assets/screenshots/skew-detector-output.txt)
- **Partitioning and file count** — [`examples/pyspark/partitioning-demo/`](examples/pyspark/partitioning-demo/README.md) and [`examples/pyspark/inspect_partitions.py`](examples/pyspark/inspect_partitions.py)
- **Small-file audit** — [`examples/pyspark/file_count_audit.py`](examples/pyspark/file_count_audit.py)  
  **Sample text:** [`docs/assets/screenshots/file-count-audit-output.txt`](docs/assets/screenshots/file-count-audit-output.txt)
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

This repo is written to help you **operate, debug, and reason about** production Spark. Optional
**self-check** material is there to test understanding of the same concepts you use in triage and
**design review** — not to optimize a question bank or replace hands-on work with the Spark UI and
`EXPLAIN`.
