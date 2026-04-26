# Spark Config Principles

Status: Draft

## Core Idea

Treat Spark configs as **hypothesis-driven** levers, not random knobs. A config change should be tied to a specific bottleneck you can see in Spark UI, logs, or metrics.

## Rules Of Thumb

- **Start from the slowest/failed stage** and the SQL physical plan, not from executor sizing.
- **Prefer data/plan fixes over config fixes** when the root cause is skew, small files, or a bad join strategy.
- **Validate one change at a time** with before/after Spark UI signals.
- **Record the intent**: what symptom it targets and what metric should move.

## Validation Checklist

For any config change, capture:

- The dominant stage(s) and their key metrics (shuffle, spill, GC, max task time).
- The SQL physical plan for the relevant query.
- Executor health signals (lost executors, GC time, skew hotspots).
- Output shape signals (file count, partition counts) when writing.

## Common Ways Config Changes Backfire

- Increasing parallelism increases **small files**, scheduler overhead, or S3 request costs.
- Increasing memory hides skew and increases GC pauses.
- Forcing broadcast joins causes executor instability when “small” isn’t small.
- “Fixing” shuffle partitions increases total shuffle bytes because upstream got wider.
