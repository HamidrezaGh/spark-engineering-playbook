# Spark Engineering Playbook

A production-grade Apache Spark handbook for engineers who want to make better Spark decisions in real systems, not just pass an interview.

This repository is written from the point of view of a staff-level data platform engineer running Spark on AWS EMR with S3-backed storage. Every chapter focuses on what actually matters when a job is slow, expensive, fragile, or hard to operate at scale.

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
- Staff-level platform thinking: golden paths, guardrails, observability, cost.

## What This Is Not

- Not an introduction to Spark. We assume you can already write a join and a `groupBy`.
- Not a vendor pitch for Databricks, EMR, or any specific runtime; AWS EMR is the operating bias because that's the most common production context the author has run, but the principles apply elsewhere.
- Not a config dump. Every tuning knob is paired with a reason, a tradeoff, and a way to validate the change in the Spark UI.
- Not generated filler. Each chapter is meant to read like notes from someone who has actually had to fix the problem at 2am.

## Who This Is For

- Senior data engineers who want to move from "my job works" to "I understand why it works, why it sometimes doesn't, and how to operate it safely."
- Platform and infra engineers who own shared Spark / EMR infrastructure for many teams.
- Tech leads and staff engineers preparing for design reviews, incident response, or cost reviews.
- Engineers preparing for staff-level Spark interviews who want depth, not just trivia.

If you are brand new to Spark, this repo will be too dense. Start with the official Spark documentation and a short intro course, then come back here.

## How To Read This Repo

There are three reasonable entry points depending on your goal.

### 1. Sequential learning path

Read the book chapters in order. Chapters 1–6 build the execution model, chapters 7–10 cover memory, formats, SQL, and caching, chapters 11–14 cover EMR/YARN, debugging, Iceberg, and streaming, and chapters 15–25 cover staff-level platform topics, reliability, and operating concerns.

### 2. Incident-driven path

Start in [`docs/field-guides/`](docs/field-guides/README.md) with the symptom that matches your incident: slow job, OOM, skew, fetch failures, small files. Each guide points back to the chapter that explains the underlying behavior.

### 3. Design-review path

Use the patterns and chapters together. For example, before reviewing a new pipeline:

1. Read [`docs/book/04-joins.md`](docs/book/04-joins.md) and [`docs/book/05-data-skew.md`](docs/book/05-data-skew.md).
2. Skim [`docs/checklists/pre-deploy-review.md`](docs/checklists/pre-deploy-review.md).
3. Match the workload to a pattern in [`docs/patterns/`](docs/patterns/README.md).

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
| Examples | [`examples/`](examples/README.md) | PySpark, Spark SQL, and config examples used by the chapters. |
| Diagrams | [`diagrams/`](diagrams/README.md) | Sources for execution, storage, and platform diagrams. |
| Glossary | [`docs/glossary.md`](docs/glossary.md) | Production-oriented Spark vocabulary. |
| Q&A | [`docs/advanced-spark-questions.md`](docs/advanced-spark-questions.md) | The original question list that defines the bar for this handbook. |

## Suggested Learning Path

For an engineer with one to three years of Spark experience aiming for staff-level depth:

1. **Foundations** — Chapters 1–3: execution model, shuffle, partitioning. Get the mental model right before tuning anything.
2. **Query behavior** — Chapters 4–6, 9, 19: joins, skew, AQE, Spark SQL/Catalyst, statistics. Most production performance work lives here.
3. **Runtime and memory** — Chapters 7, 10, 12: memory management, caching, production debugging. Read alongside the field guides.
4. **Storage layer** — Chapters 8, 13, 17, 18, 23: file formats, Iceberg, write path, S3, table design. This is where data platforms succeed or fail.
5. **Operations on EMR** — Chapter 11 plus the configs and checklists. Understand how EMR, YARN, and S3 shape Spark behavior.
6. **Platform thinking** — Chapters 15, 16, 20, 21, 22, 24, 25: staff-level engineering, correctness, packaging, security, CI/CD, backfills, isolation.
7. **Streaming** — Chapter 14: structured streaming, watermarks, state, exactly-once.

Once those are internalized, use the field guides and case studies to keep the muscle memory fresh.

## Production Bias

Most chapters share a few opinions that are worth stating up front, because they shape the writing:

- **Identify the bottleneck before tuning.** Random `spark.executor.memory` increases are usually a sign someone skipped the Spark UI.
- **Treat shuffle as a design event.** If you cannot say where the shuffles are in your job, you cannot reason about its cost.
- **Treat the Spark UI as the source of truth.** Logs lie about timing; the UI does not.
- **Treat S3 as durable storage, not local disk.** Listing, throttling, commit semantics, and small files are first-class concerns.
- **Treat configs as a contract.** A tuning value without a reason or a way to validate it is technical debt.
- **Treat platform problems as platform problems.** If 40 teams all hit the same issue, the answer is a guardrail, not 40 fixes.

## Contributing

This is currently a single-author, opinionated handbook. If you want to suggest corrections or production examples, open an issue with:

- The chapter or page you are referencing.
- The specific claim you are challenging or refining.
- A concrete production scenario where the claim fails or could be sharper.

Vague stylistic feedback is unlikely to be incorporated; concrete production experience is welcome.

## License

This repository is released under the [MIT License](LICENSE). The content can be reused and modified with attribution. The author retains the right to republish or extend this material commercially in the future; the MIT grant covers others, not the copyright holder.
