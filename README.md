# Spark Engineering Playbook

A production-grade Apache Spark handbook for engineers who want to make better Spark decisions
in real systems, not just pass an interview.

This repository is written from the point of view of someone who runs Spark on AWS EMR with
object-store-backed storage. The emphasis is on production scenarios, debugging workflows, case
studies, design review templates, Spark UI evidence, EXPLAIN-driven examples, operational tradeoffs,
guardrails, and runbooks — not on API walkthroughs. Every chapter focuses on what actually matters
when a job is slow, expensive, fragile, or hard to operate at scale.

## Why this exists

Many Spark resources explain APIs, transformations, and simple tuning flags. Production Spark work
is different: jobs fail because of shuffle pressure, skew, memory spill, small files, unsafe joins,
bad partitioning, object-storage behavior, dependency drift, and missing observability.

This playbook exists to capture practical Spark engineering judgment: how to read the Spark UI,
reason from physical plans, diagnose production failures, choose the smallest safe fix, and turn
incidents into reusable platform patterns.

## What This Is

A practical, opinionated reference covering:

- Spark execution internals: jobs, stages, tasks, shuffle, Catalyst, AQE.
- Spark SQL behavior: physical plans, joins, exchanges, statistics, predicate pushdown.
- Production debugging: Spark UI, event logs, YARN container kills, fetch failures, OOMs.
- Tuning: shuffle partitions, executor sizing, broadcast joins, AQE, memory overhead.
- AWS EMR / YARN operating model: cluster modes, instance fleets, Spot, IAM, S3 layout.
- Lakehouse patterns: Iceberg, incremental processing, idempotent backfills, table maintenance.
- Structured Streaming reliability: triggers, checkpointing, watermarks, exactly-once.
- Reliability and correctness: schema evolution, output commits, small-files control.
- Platform patterns: golden paths, guardrails, observability, cost.

## What This Is Not

- Not an introduction to Spark. We assume you can already write a join and a `groupBy`.
- Not a vendor pitch for Databricks, EMR, or any specific runtime; AWS EMR is the operating bias
  because that's the most common production context the author has run, but the principles apply
  elsewhere.
- Not a config dump. Every tuning knob is paired with a reason, a tradeoff, and a way to validate the
  change in the Spark UI.
- Not generated filler. Each chapter is meant to read like notes from someone who has actually had
  to fix the problem at 2am.

## Who This Is For

- Senior data engineers who want to move from "my job works" to "I understand why it works, why it
  sometimes doesn't, and how to operate it safely."
- Platform and infra engineers who own shared Spark / EMR infrastructure for many teams.
- Tech leads preparing for design reviews, incident response, or cost reviews.
- Engineers who want serious depth on Spark execution and operations—how plans behave under load, how failures show up in production—not reduced to interview trivia.

If you are brand new to Spark, this repo will be too dense. Start with the official Spark
documentation and a short intro course, then come back here.

## Why This Is Different From Typical Spark Notes

Most Spark material optimizes for breadth or interview coverage.
This playbook optimizes for **production decisions**: what to look at in the Spark UI when a stage
is red, how YARN and EMR change failure modes, how object storage and Iceberg interact with commits
and file layout, and how one incident becomes a guardrail for the next forty teams.
It is denser on purpose — it assumes you already ship jobs and need the *why* behind the knobs.

## How To Read This Repo

There are three reasonable entry points depending on your goal.

### 1. Sequential learning path

Read the book chapters in order.

Chapters 1–6 build the execution model, chapters 7–10 cover memory, formats, SQL, and caching,
chapters 11–14 cover EMR/YARN, debugging, Iceberg, and streaming, and chapters 15–25 cover platform
reliability, cost, isolation, and operating concerns.

### 2. Incident-driven path

Start in [`docs/field-guides/`](docs/field-guides/README.md) with the symptom that matches your
incident: slow job, OOM, skew, fetch failures, small files. Each guide points back to the chapter
that explains the underlying behavior.

### 3. Design-review path

Use the patterns and chapters together. For example, before reviewing a new pipeline:

1. Read [`docs/book/04-joins.md`](docs/book/04-joins.md) and
   [`docs/book/05-data-skew.md`](docs/book/05-data-skew.md).
2. Skim [`docs/checklists/pre-deploy-review.md`](docs/checklists/pre-deploy-review.md).
3. Match the workload to a pattern in [`docs/patterns/`](docs/patterns/README.md).

## Recommended Reading Path (By Topic)

| Topic | Primary reading | Diagrams / examples |
| --- | --- | --- |
| Execution model | [`docs/book/01-execution-model.md`](docs/book/01-execution-model.md) | [`diagrams/spark-job-stage-task.md`](diagrams/spark-job-stage-task.md), [`examples/sql/01-explain-shuffle.sql`](examples/sql/01-explain-shuffle.sql) |
| Shuffle and performance | [`docs/book/02-shuffle-and-performance.md`](docs/book/02-shuffle-and-performance.md) | [`diagrams/shuffle-read-write.md`](diagrams/shuffle-read-write.md), [`docs/tuning/shuffle-partitions.md`](docs/tuning/shuffle-partitions.md) |
| Joins | [`docs/book/04-joins.md`](docs/book/04-joins.md) | [`diagrams/broadcast-vs-sort-merge-join.md`](diagrams/broadcast-vs-sort-merge-join.md), [`examples/sql/02-broadcast-vs-sort-merge-join.sql`](examples/sql/02-broadcast-vs-sort-merge-join.sql) |
| Skew | [`docs/book/05-data-skew.md`](docs/book/05-data-skew.md) | [`docs/field-guides/debugging-skew.md`](docs/field-guides/debugging-skew.md), [`examples/sql/03-skew-detection.sql`](examples/sql/03-skew-detection.sql) |
| Memory | [`docs/book/07-memory-management.md`](docs/book/07-memory-management.md) | [`docs/field-guides/debugging-oom.md`](docs/field-guides/debugging-oom.md), [`docs/tuning/memory-overhead.md`](docs/tuning/memory-overhead.md) |
| EMR debugging | [`docs/book/11-spark-on-yarn-and-emr.md`](docs/book/11-spark-on-yarn-and-emr.md), [`docs/book/12-production-debugging.md`](docs/book/12-production-debugging.md) | [`docs/field-guides/debugging-slow-jobs.md`](docs/field-guides/debugging-slow-jobs.md), [`docs/field-guides/spark-ui-reading-guide.md`](docs/field-guides/spark-ui-reading-guide.md) |
| Iceberg write path and merges | [`docs/book/13-iceberg-and-spark.md`](docs/book/13-iceberg-and-spark.md), [`docs/book/17-spark-write-path-and-output-files.md`](docs/book/17-spark-write-path-and-output-files.md) | [`diagrams/iceberg-merge-on-s3.md`](diagrams/iceberg-merge-on-s3.md), [`docs/patterns/large-iceberg-merge.md`](docs/patterns/large-iceberg-merge.md) |
| Case studies | [`docs/case-studies/README.md`](docs/case-studies/README.md) | [`docs/case-studies/emr-merge-memory-spill.md`](docs/case-studies/emr-merge-memory-spill.md) and companion case files |

## Repository Structure

| Area | Path | Purpose |
| --- | --- | --- |
| Book | [`docs/book/`](docs/book/README.md) | Chapter-by-chapter handbook. The main reference. |
| Field guides | [`docs/field-guides/`](docs/field-guides/README.md) | Incident-oriented debugging guides (slow jobs, OOM, skew, small files, Spark UI). |
| Patterns | [`docs/patterns/`](docs/patterns/README.md) | Reusable production architectures (incremental pipelines, idempotent backfills, large merges). |
| Tuning | [`docs/tuning/`](docs/tuning/README.md) | Focused tuning notes for common levers (shuffle partitions, executor sizing, broadcast). |
| Configs | [`docs/configs/`](docs/configs/README.md) | Spark configuration field manual: what each knob does and when to touch it. |
| Checklists | [`docs/checklists/`](docs/checklists/README.md) | Operational checklists for pre-deploy, incident triage, cost, and production readiness. |
| Case studies | [`docs/case-studies/`](docs/case-studies/) | Anonymized production incidents with root cause and fix. |
| Examples | [`examples/`](examples/README.md) | PySpark, Spark SQL, and config examples used by the chapters; see [Runnable examples](#runnable-examples-with-sample-output) below. |
| Diagrams | [`diagrams/`](diagrams/README.md) | Sources for execution, storage, and platform diagrams. |
| Templates | [`docs/templates/`](docs/templates/README.md) | Design review, incident postmortem, production readiness, and cost review templates. |
| Glossary | [`docs/glossary.md`](docs/glossary.md) | Production-oriented Spark vocabulary. |
| Q&A | [`docs/advanced-spark-questions.md`](docs/advanced-spark-questions.md) | The original question list that defines the bar for this handbook. |
| Sample outputs | [`docs/assets/screenshots/`](docs/assets/screenshots/README.md) | Labeled text captures of EXPLAIN shape, skew detector, and file audit (not production screenshots). |

## Try it locally

From the repo root:

```bash
cd examples/local
./run_examples.sh
```

This runs the bundled SQL and PySpark examples against the sample CSVs. You need Spark 3.4+ with
`spark-sql` for SQL mode; PySpark in Python is enough for `./run_examples.sh pyspark` only. See
[`examples/local/README.md`](examples/local/README.md) for prerequisites, modes (`sql`, `pyspark`, `all`), and troubleshooting.

## Runnable examples with sample output

These go beyond documentation-only snippets: you can run them locally and compare your terminal to the labeled samples in [`docs/assets/screenshots/`](docs/assets/screenshots/README.md).

1. **EXPLAIN and shuffle boundaries** — Script:
   [`examples/sql/01-explain-shuffle.sql`](examples/sql/01-explain-shuffle.sql). Annotated plan shape in
   [`docs/assets/screenshots/explain-formatted-shuffle-output.txt`](docs/assets/screenshots/explain-formatted-shuffle-output.txt).
   Run with `spark-sql` after you point `events` at your table, or use
   [`examples/local/run_examples.sh`](examples/local/run_examples.sh) for the bundled CSVs.

2. **Skew detector** — [`examples/pyspark/skew_detector.py`](examples/pyspark/skew_detector.py). Example:
   `python3 examples/pyspark/skew_detector.py --demo` (no input files), or
   `--input examples/local/data/events_sample.csv --format csv --header --key customer_id`.
   Sample output in
   [`docs/assets/screenshots/skew-detector-output.txt`](docs/assets/screenshots/skew-detector-output.txt).

3. **Small-file audit** — [`examples/pyspark/file_count_audit.py`](examples/pyspark/file_count_audit.py). Example:
   `python3 examples/pyspark/file_count_audit.py --demo` (builds a temp multi-file layout and prints a real audit).
   Sample output in
   [`docs/assets/screenshots/file-count-audit-output.txt`](docs/assets/screenshots/file-count-audit-output.txt).

## Suggested Learning Path

For an engineer with one to three years of Spark experience who wants depth beyond day-to-day tuning:

1. **Foundations** — Chapters 1–3: execution model, shuffle, partitioning. Get the mental model right
   before tuning anything.
2. **Query behavior** — Chapters 4–6, 9, 19: joins, skew, AQE, Spark SQL/Catalyst, statistics. Most
   production performance work lives here.
3. **Runtime and memory** — Chapters 7, 10, 12: memory management, caching, production debugging.
   Read alongside the field guides.
4. **Storage layer** — Chapters 8, 13, 17, 18, 23: file formats, Iceberg, write path, S3, table
   design. This is where data platforms succeed or fail.
5. **Operations on EMR** — Chapter 11 plus the configs and checklists. Understand how EMR, YARN, and
   S3 shape Spark behavior.
6. **Platform thinking** — Chapters 15, 16, 20, 21, 22, 24, 25: correctness, packaging, security,
   CI/CD, backfills, isolation, and shared guardrails.
7. **Streaming** — Chapter 14: structured streaming, watermarks, state, exactly-once.

Once those are internalized, use the field guides and case studies to keep the muscle memory fresh.

## Production Bias

Most chapters share a few opinions that are worth stating up front, because they shape the writing:

- **Identify the bottleneck before tuning.** Random `spark.executor.memory` increases are usually a
  sign someone skipped the Spark UI.
- **Treat shuffle as a design event.** If you cannot say where the shuffles are in your job, you
  cannot reason about its cost.
- **Treat the Spark UI as the source of truth.** Logs lie about timing; the UI does not.
- **Treat S3 as durable storage, not local disk.** Listing, throttling, commit semantics, and small
  files are first-class concerns.
- **Treat configs as a contract.** A tuning value without a reason or a way to validate it is
  technical debt.
- **Treat platform problems as platform problems.** If 40 teams all hit the same issue, the answer is
  a guardrail, not 40 fixes.

## Repo Maturity

All book chapters (1–25) exist as real files in [`docs/book/`](docs/book/README.md); depth varies
by topic. Favor the execution stack (1–6), debugging and EMR chapters (11–12), and lakehouse write
paths (13, 17–18) when you need the most battle-tested prose. Shorter chapters are tightening toward
the same bar — if something is still a stub, the Markdown says **TODO** or **In progress** inline.

Automation: [`.github/workflows/docs-check.yml`](.github/workflows/docs-check.yml) runs Markdown
lint (markdownlint), link checks, and lightweight Python and shell validation. For a human pass
before a big docs merge, use
[`docs/checklists/repo-quality-checklist.md`](docs/checklists/repo-quality-checklist.md).

## Contributing

This is currently a single-author, opinionated handbook. If you want to suggest corrections or
production examples, open an issue with:

- The chapter or page you are referencing.
- The specific claim you are challenging or refining.
- A concrete production scenario where the claim fails or could be sharper.

Vague stylistic feedback is unlikely to be incorporated; concrete production experience is welcome.

## Repository Metadata

Suggested GitHub topics for discoverability:

`apache-spark`, `spark-sql`, `aws-emr`, `data-engineering`, `data-platform`, `iceberg`, `yarn`, `s3`,
`performance-tuning`, `distributed-systems`, `structured-streaming`, `lakehouse`.

## License

This repository is released under the [MIT License](LICENSE). The content can be reused and modified
with attribution. The author retains the right to republish or extend this material commercially in
the future; the MIT grant covers others, not the copyright holder.
