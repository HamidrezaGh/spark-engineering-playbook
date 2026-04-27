# The Spark Engineering Book

This is the main handbook.

Each chapter is written for engineers who are already comfortable with basic Spark and want
**production depth**: how Spark behaves internally, what fails in real systems, how to diagnose
it from the **Spark UI** and **EXPLAIN**, and how to pick **tradeoffs** with evidence.

The chapters can be read in order, but most are also useful as standalone references during incidents or design reviews.

## How To Read A Chapter

Every chapter follows roughly the same shape so you can scan it the same way each time:

- **What You Should Be Able To Answer** — quick self-check before reading.
- **Core Idea** and **Mental Model** — the production model in a paragraph.
- **What Spark Does Internally** — execution detail that drives decisions.
- **Why It Matters In Production** — the operating impact.
- **Common Failure Modes** and **Production Smells** — what to watch for.
- **Tuning And Configuration** — knobs, defaults, and validation strategy.
- **Spark UI Signals** — what to look at to verify the diagnosis.
- **Best Practices** and **Anti-Patterns** — quick decision rules.
- **Example** and **Real Use Case** — how it shows up in real work.

Two reading modes:

| Mode | Use When | What To Scan |
| --- | --- | --- |
| Fast review | Design review, incident, or “what did we decide last time?” | Core idea, mental model, diagrams, production smells, best practices |
| Deep review | Learning the topic or writing production code | Internals, tuning, failure modes, Spark UI signals, example, real use case |

## Reading Lanes

| Lane | Chapters | Main Question |
| --- | --- | --- |
| Execution | 1–6 | How does Spark break work apart and move data? |
| Runtime | 7, 10–12 | Why does the job fail or run slowly? |
| Storage | 8, 13, 17–19, 23 | How does data layout change scan, write, and planning cost? |
| Operations | 11, 14–16, 20–22, 24–25 | How do we run Spark safely on EMR across teams and time? |

## Concept Dependency Map

```text
Execution Model
  |-- Shuffle And Performance
  |     |-- Joins
  |     |-- Data Skew
  |     |-- Adaptive Query Execution
  |
  |-- Partitioning
        |-- Joins
        |-- File Formats And Table Layout
              |-- Iceberg And Spark

Joins + Data Skew + AQE
  -> Production Debugging
  -> Platform Patterns And Guardrails

Iceberg And Spark
  -> Incremental Processing And Backfills
  -> Data Correctness And Idempotency
  -> Platform Patterns And Guardrails
```

## Chapters

### 1. [Execution Model](01-execution-model.md)

- **You'll learn:** how Spark turns code into jobs, stages, and tasks; what creates a stage boundary; what runs on the driver vs executors; how to read these concepts in the Spark UI.
- **Why it matters in production:** every diagnosis starts at the stage level. If you can't pick the slow stage out of a Spark UI in 90 seconds, you can't tune anything.

### 2. [Shuffle And Performance](02-shuffle-and-performance.md)

- **You'll learn:** map-side shuffle write vs reduce-side read, how shuffle shows up in the Spark UI, when to raise or lower `spark.sql.shuffle.partitions`, skew/spill/AQE interactions, EMR/S3 vs local shuffle storage, and what `FetchFailedException` usually means.
- **Why it matters in production:** shuffle dominates cost and failure modes for most non-trivial Spark jobs; confusing shuffle bytes with S3 bytes is a common mis-triage.

### 3. [Partitioning](03-partitioning.md)

- **You'll learn:** the difference between Spark execution partitions and table storage partitions; how partition count drives parallelism, output file count, and skew.
- **Why it matters in production:** wrong partitioning is the most common cause of "too many small files" and "one giant task" incidents.

### 4. [Joins](04-joins.md)

- **You'll learn:** broadcast hash join, sort-merge join, shuffled hash join — when each is chosen, why it sometimes regresses, and how to read join strategy from a physical plan.
- **Why it matters in production:** join strategy regressions silently turn 5-minute jobs into 5-hour jobs.

### 5. [Data Skew](05-data-skew.md)

- **You'll learn:** how skew shows up in the Spark UI, hot-key vs hot-partition skew, salting, hot-key isolation, and AQE skew handling.
- **Why it matters in production:** skew is the most common cause of OOMs and long-tail stages.

### 6. [Adaptive Query Execution](06-adaptive-query-execution.md)

- **You'll learn:** what AQE changes at runtime (coalesce, skew join, dynamic join strategy), what evidence it uses, when to trust it, when to override it.
- **Why it matters in production:** AQE solves many tuning problems automatically — but only if you know what it can and cannot do.

### 7. [Memory Management](07-memory-management.md)

- **You'll learn:** the practical Spark memory model (heap vs overhead vs Python vs off-heap), how spill works, how PySpark changes the memory picture.
- **Why it matters in production:** YARN container kills are usually overhead problems, not heap problems. The fix is rarely "add more memory."

### 8. [File Formats](08-file-formats.md)

- **You'll learn:** Parquet/ORC vs CSV/JSON, predicate pushdown, column pruning, compression tradeoffs, and why row-group/page sizing matters.
- **Why it matters in production:** the file format and layout you write today is the cost of every read for the next year.

### 9. [Spark SQL And Catalyst](09-spark-sql-and-catalyst.md)

- **You'll learn:** what Catalyst can and cannot optimize, why UDFs hurt optimization, how to read logical and physical plans, and how to spot missing pushdown.
- **Why it matters in production:** most "Spark is slow" tickets come down to one missing optimization in the plan.

### 10. [Caching And Persistence](10-caching-and-persistence.md)

- **You'll learn:** when caching actually helps (real reuse), when it just steals memory, storage levels, and how to validate cache value in the Spark UI.
- **Why it matters in production:** caching is the most common premature optimization in Spark code.

### 11. [Spark On AWS EMR And YARN](11-spark-on-yarn-and-emr.md)

- **You'll learn:** client vs cluster mode, EMR cluster shape, instance fleets, Spot risk, how YARN container kills look in practice, and how to persist event logs.
- **Why it matters in production:** EMR release version, queue policy, and Spot strategy are part of your job's runtime contract whether you wrote them down or not.

### 12. [Production Debugging](12-production-debugging.md)

- **You'll learn:** a structured triage workflow — Stages → SQL → Executors → logs — for slow jobs, OOMs, and failures.
- **Why it matters in production:** on shared platforms, you will debug jobs you did not write. A
  repeatable **workflow** (UI → plan → logs) is the difference between minutes and hours.

### 13. [Iceberg And Spark](13-iceberg-and-spark.md)

- **You'll learn:** what Iceberg adds beyond a directory of Parquet (snapshots, metadata, atomic commits), partitioning evolution, table maintenance.
- **Why it matters in production:** lakehouse table formats are now the default. Most "S3 commit horror stories" go away with a real table format.

### 14. [Structured Streaming](14-structured-streaming.md)

- **You'll learn:** micro-batch model, triggers, watermarks, state stores, checkpointing, exactly-once with idempotent sinks.
- **Why it matters in production:** streaming jobs fail differently than batch jobs. Most incidents are checkpoint, watermark, or state size problems.

### 15. [Platform patterns and guardrails](15-platform-patterns.md)

- **You'll learn:** how to turn one-off job fixes into **defaults** — golden paths, guardrails, observability, cost, upgrade strategy, and learning from incidents.
- **Why it matters in production:** at scale, the recurring problem is not “one slow job” but missing **standards** and **evidence** for the next many jobs and teams.

### 16. [Data Correctness And Idempotency](16-data-correctness-and-idempotency.md)

- **You'll learn:** what idempotency means in batch and streaming pipelines, safe reruns, output contracts, and quality gates before publishing.
- **Why it matters in production:** the difference between a recoverable incident and a data corruption incident is whether the pipeline is idempotent.

### 17. [Spark Write Path And Output Files](17-spark-write-path-and-output-files.md)

- **You'll learn:** how Spark writes files (commit protocols, task retries, partition count → file count), and why naive `repartition(N)` before write often makes things worse.
- **Why it matters in production:** small files and bad commits both come from misunderstanding the write path.

### 18. [S3 With Spark On EMR](18-object-storage-with-spark.md)

- **You'll learn:** how S3 differs from HDFS (eventual semantics historically, listing cost, no rename, throttling, request pricing), and how that shapes Spark commit protocols.
- **Why it matters in production:** treating S3 as "free disk" is one of the most expensive cultural mistakes in a data platform.

### 19. [Statistics And Cost-Based Optimization](19-statistics-and-cost-based-optimization.md)

- **You'll learn:** what decisions Spark can make better with stats (broadcast threshold, join order, selectivity), how to populate and maintain stats, and where CBO falls short.
- **Why it matters in production:** missing stats is a silent cause of join strategy regressions.

### 20. [Dependency Management And Packaging](20-dependency-management-and-packaging.md)

- **You'll learn:** why "works on my notebook" fails on executors, how to package PySpark jobs reproducibly, virtualenvs, JARs, and version pinning.
- **Why it matters in production:** dependency drift between driver and executors is one of the most common production-only failure modes.

### 21. [Security And Governance](21-security-and-governance.md)

- **You'll learn:** least-privilege IAM for EMR jobs, KMS, S3 access patterns, secrets handling, and Glue/Lake Formation integration patterns.
- **Why it matters in production:** security failures in data platforms are usually IAM-shaped, not code-shaped.

### 22. [Testing And CI/CD](22-testing-and-cicd.md)

- **You'll learn:** what to test in Spark code (logic, data contracts, operational assumptions), local Spark sessions, golden datasets, and CI patterns for Spark jobs.
- **Why it matters in production:** Spark jobs without tests get patched in production, which is how data correctness incidents start.

### 23. [Data Modeling And Table Design](23-data-modeling-and-table-design.md)

- **You'll learn:** partitioning strategy, clustering/Z-ordering, primary key design, schema evolution, and treating table design as a long-term cost lever.
- **Why it matters in production:** table design decisions outlast the team that made them. They are the highest-leverage performance lever in a data platform.

### 24. [Incremental Processing And Backfills](24-incremental-processing-and-backfills.md)

- **You'll learn:** moving from full reload to incremental merge, watermark/state correctness, idempotent backfills, and how to bound backfill blast radius.
- **Why it matters in production:** full-reload pipelines stop scaling. Incremental pipelines fail in subtler ways. The migration is where most correctness incidents happen.

### 25. [Cluster And Workload Isolation](25-cluster-and-workload-isolation.md)

- **You'll learn:** when to share clusters vs split workloads, YARN queues, EMR fleet design, and policy-level isolation for shared platforms.
- **Why it matters in production:** "shared cluster" incidents are almost always isolation/policy problems, not raw capacity problems.
