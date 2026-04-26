# Diagram — Spark Job, Stage, Task

The fundamental hierarchy in Spark execution. Reading this diagram is the prerequisite for every Spark UI investigation.

## Explanation

A Spark **application** is one running driver process. Inside the application, every action triggers one **job**. Each job is broken into **stages** at shuffle boundaries. Each stage runs many **tasks**, one per partition.

The hierarchy is:

- Application: 1 driver, N executors, N job runs over its lifetime.
- Job: 1 action -> 1 job. A job has one or more stages.
- Stage: a contiguous block of pipelined narrow transformations. Stages are separated by `Exchange` (shuffle) operators.
- Task: one partition's worth of work in one stage, executed on one executor core.

## Spark Job, Stage, and Task Hierarchy

```mermaid
flowchart TB
    App["Spark Application<br/>(1 driver, N executors)"]
    App --> Job1["Job 1<br/>triggered by count()"]
    App --> Job2["Job 2<br/>triggered by write()"]

    Job1 --> S1A["Stage 0<br/>scan + partial agg"]
    Job1 --> S1B["Stage 1<br/>final agg<br/>(after Exchange)"]

    Job2 --> S2A["Stage 2<br/>scan source"]
    Job2 --> S2B["Stage 3<br/>shuffle by join key"]
    Job2 --> S2C["Stage 4<br/>SortMergeJoin + write"]

    S1A --> T1A1["Task 0<br/>partition 0"]
    S1A --> T1A2["Task 1<br/>partition 1"]
    S1A --> T1A3["...<br/>up to N partitions"]

    S1B --> T1B1["Task 0<br/>shuffle partition 0"]
    S1B --> T1B2["Task 1<br/>shuffle partition 1"]
    S1B --> T1B3["...<br/>up to spark.sql.shuffle.partitions"]

    classDef app fill:#1f77b4,stroke:#1f77b4,color:#ffffff
    classDef job fill:#2ca02c,stroke:#2ca02c,color:#ffffff
    classDef stage fill:#ff7f0e,stroke:#ff7f0e,color:#ffffff
    classDef task fill:#d62728,stroke:#d62728,color:#ffffff

    class App app
    class Job1,Job2 job
    class S1A,S1B,S2A,S2B,S2C stage
    class T1A1,T1A2,T1A3,T1B1,T1B2,T1B3 task
```

## How To Use This Diagram In The Relevant Chapter

Use this diagram in [Chapter 1 — Execution Model](../docs/book/01-execution-model.md) when introducing the job/stage/task vocabulary.

When you introduce the diagram, anchor the three layers to their UI surfaces:

- Job = Spark UI **Jobs** tab.
- Stage = Spark UI **Stages** tab.
- Task = the rows in the Stages tab's "Tasks" panel.

Then point at the `Stage 0 -> Stage 1` edge and say: that arrow is the `Exchange`. That arrow is where Spark writes shuffle data to local disk and the next stage reads it back. Most production Spark performance problems live on that arrow.

## Production Interpretation

- A job with one stage is rare and usually means no shuffle — for example, a bare `df.write.parquet(...)` without aggregation or join.
- A job with 8+ stages usually reflects multiple shuffles — joins, group-bys, distinct, window operations. Each is a redistribution event with a real cost.
- The number of tasks per stage tells you the parallelism of that stage. Too few = under-utilized cluster. Too many = scheduler overhead and tiny output files.
- A job that runs many actions over the same DataFrame without caching causes repeated stages. The Jobs tab shows it as multiple jobs running the same lineage. That is usually a bug or a missing `cache()`.

When debugging, the diagnostic is always: pick the slow stage in the Stages tab, look at task distribution within that stage, and form a hypothesis from there. The job-level wall-clock time is the headline; the stage-level metrics are the evidence.
