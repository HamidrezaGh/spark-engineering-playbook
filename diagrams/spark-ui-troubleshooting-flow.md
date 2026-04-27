# Diagram — Spark UI triage flow

A **repeatable** order of tabs so you do not thrash during an incident.

## Explanation

Most slow jobs are **one stage** problems. **Stages** give you **task** shape; **SQL** maps to
**operators**; **Executors** rules out **cluster** health.

## Triage flow

```mermaid
flowchart TB
    A[Open Spark UI] --> B[Stages: sort by duration]
    B --> C{Max task >> median?}
    C -->|yes| S[Skew / file split — profile key or layout]
    C -->|no| D[Read stage metrics: shuffle, spill, GC]
    D --> E[SQL tab: plan vs slow stage]
    E --> F[Executors: lost / driver heap]
    F --> G[One smallest fix + re-run]
```

**See:** [`../docs/observability/spark-ui-guide.md`](../docs/observability/spark-ui-guide.md),
[`../docs/troubleshooting/slow-job.md`](../docs/troubleshooting/slow-job.md)
