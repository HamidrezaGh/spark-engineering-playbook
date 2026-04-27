# Troubleshooting: memory, spill, and OOM

**Problem:** OutOfMemoryError, YARN container killed for memory, heavy spill, or extreme GC time.

## Symptoms

- **Executor** or **driver** OOM in logs; YARN `Container killed` for exceeding memory limits.
- **Spill (memory + disk)** large in Stages or Tasks; runtime grows when spill appears.
- **GC time** a high fraction of task time across many tasks.
- Job succeeds only after **raising memory** (fragile “fix”).

## What to check first

1. **Driver vs executor?** Driver OOM often from `collect()`, huge plans, or massive listings; executor OOM from skew, too few partitions, or huge broadcast.
2. **Spill in UI** — is the job spill-bound? If so, more heap often helps *less* than fewer rows per task or a better plan.
3. **PySpark?** **Memory overhead** and Python worker memory are common — see [`../tuning/memory-overhead.md`](../tuning/memory-overhead.md).
4. **One task vs many?** One bad task → skew. Many tasks with spill → partitions or working set.

## Spark UI signals

| Signal | What it usually means | Where to look |
| --- | --- | --- |
| Spill (memory/disk) | Sort/agg/join working set too large | Stages → Summary metrics, Tasks |
| GC time | Heap pressure, big objects, too little room | Stages, Executors |
| Single task OOM pattern | Skew, broadcast too large, one huge partition | Tasks distribution |
| Driver in Executors | High memory on driver row | `collect`, broadcast planning, file listing |

## Logs and metrics

- YARN: **Diagnostics** for exit code 137, physical memory, container limits.
- Executor log: `OutOfMemoryError`, `Cannot allocate memory`.
- `spark.ui.retainedStages` is not a tuning knob — use event logs for post-hoc analysis.

## Likely causes

- **Too few shuffle partitions** — huge sort/hash per task.
- **Skew** — one task holds the giant partition.
- **Oversized broadcast** — `autoBroadcastJoinThreshold` too high for real row size.
- **Wide rows** or many columns through a hash aggregate or window.
- **Python UDFs** and Python objects — memory outside JVM visibility.
- **Cache** of a large DataFrame with nowhere to go.

## Fix options (prefer order: plan > partitions > memory)

- Remove unnecessary **cache**; narrow **columns** early.
- Increase **shuffle partitions** or let **AQE** coalesce/split as appropriate.
- **Fix skew** (see [skew-and-stragglers](skew-and-stragglers.md)).
- Reduce or **disable broadcast** for tables that are “small” in bytes but wide.
- **Executor memory and overhead** — adjust with evidence, not as first move.
- For PySpark: increase **memory overhead**, reduce Arrow batch size if used.

## Tradeoffs

- Bigger executors: fewer slots per node; avoid one giant heap without GC testing.
- More partitions: less per-task memory but more tasks, scheduling cost, and possible small files at write.
- “Just add memory” can **hide** a bad join or skew until the next data growth.

## Example final diagnosis

*Symptoms:* Intermittent executor kill on a sort-merge stage. **UI:** high **spill**, max task ≫ median. **Cause:** one partition from skewed `join` key. **Fix:** salt + AQE; spill dropped without raising executor memory. **Lesson:** spill was a symptom, not a heap-sizing problem.

## Prevention checklist

- [ ] Pre-deploy: expected row width and partition count for heavy operators.
- [ ] PySpark jobs: overhead documented in platform defaults.
- [ ] No `coalesce(1)` before heavy operators without explicit review.
- [ ] Broadcast size limits aligned with real compressed size, not just row count.

**See also:** [`../book/07-memory-management.md`](../book/07-memory-management.md), [`../field-guides/debugging-oom.md`](../field-guides/debugging-oom.md).
