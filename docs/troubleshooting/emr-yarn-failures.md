# Troubleshooting: EMR and YARN failures

**Problem:** ApplicationMaster failure, lost executors, container killed, or Spark errors that mention YARN, preemption, or node loss.

## Symptoms

- **`ExecutorLostFailure`**, `FetchFailedException` during shuffle.
- YARN: **Container killed** for exceeding physical memory, **preempted** (Spot), **disk** failure, **unhealthy** node.
- **Cluster** step or EMR step **FAILED**; driver in cluster mode lost with application.
- Intermittent failures that correlate with **Spot** task fleet or **heavy shuffle**.

## What to check first

1. **YARN ResourceManager / NodeManager** logs; EMR: step stderr, **container** diagnostics.
2. **Spot vs On-Demand** — shuffle-heavy or SLA jobs on Spot = higher `FetchFailed` risk.
3. **Memory limits** — container **physical** limit vs Spark **executor** + **overhead** settings.
4. **Disk** — `/tmp`, YARN `local` dirs, or EBS full → shuffle and container loss.

## Spark UI signals

- **Executors** lost during a **shuffle** stage (not a single straggler).
- **Failed tasks** with retries across **many** executors — environmental, not one bad key.
- **GC** or **spill** not the main pattern — if shuffle stage fails with executor churn, look at the cluster.

## Logs and metrics

- `yarn logs -applicationId` or EMR log aggregation; search **Exit code**, **Preempted**, **Disk**, **SIGKILL**.
- CloudWatch: instance **StatusCheckFailed**, EBS stutter, autoscaling detaching task nodes.
- Spark **Event log** for `ExecutorRemoved` with reason.

## Likely causes

- **Spot interruption** during shuffle.
- **Memory** mis-sizing vs YARN `yarn.scheduler.maximum-allocation-mb` and Spark overhead.
- **Disk** full on executor — shuffle blocks, local dir exhausted.
- **Network** / security group / DNS blips in the VPC.
- **Driver in client mode** on an unstable host (notebook) — not YARN’s fault but looks similar.

## Fix options

- **Move driver** to cluster mode for production; use stable primary for client.
- **Reduce shuffle exposure on Spot** — on-demand for shuffle-heavy steps, or fewer Spot executors.
- **Right-size** `spark.executor.memory` + `memoryOverhead` to fit **YARN** container.
- **Increase** disk or clean **local** dirs; use instance types with **EBS** bandwidth for shuffle-heavy work.
- **Speculation** and **retries** help transient loss but mask chronic hardware issues.
- **EMR** release: ensure consistent Hadoop/YARN/Spark for known bugs (check release notes).

## Tradeoffs

- On-Demand only: more cost, fewer interruptions.
- **Dynamic allocation** off vs on: stability vs cost — document platform default.
- **Larger** containers: fewer per node, less parallelism for small jobs.

## Example final diagnosis

*Symptoms:* Nightly ETL fails ~20% of runs with `FetchFailed` on stage 4. **Cluster:** 80% Spot task nodes. **YARN:** “Preempted” in NM logs. **Fix:** run shuffle-heavy **join** step on on-demand **core** group only; Spot for scan-only **prep** job. **Result:** failure rate to ~0%.

## Prevention checklist

- [ ] Policy for **Spot** on **shuffle** vs **scan-only** workloads.
- [ ] Container = executor + overhead validated against YARN max.
- [ ] Disk and `/tmp` monitoring on worker instances.
- [ ] Event log + YARN log retention for post-mortems

**See also:** [`../book/11-spark-on-yarn-and-emr.md`](../book/11-spark-on-yarn-and-emr.md), [`../book/12-production-debugging.md`](../book/12-production-debugging.md), [`../checklists/spark-emr-checklist.md`](../checklists/spark-emr-checklist.md).
