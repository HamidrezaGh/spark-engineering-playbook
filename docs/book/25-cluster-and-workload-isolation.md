# Cluster And Workload Isolation

Status: First Draft
Level: Staff
Covers: EMR workload separation, YARN queues, managed scaling, core nodes, task nodes, Spot, guardrails, cost attribution

## Core Idea

Workload isolation prevents one Spark job, team, or workload type from degrading the platform for everyone else. A shared Spark platform needs scheduling policy, resource quotas, guardrails, and cost attribution.

## Key Takeaways

- **Shared EMR clusters need workload isolation**, not only more capacity.
- **YARN queues and dynamic allocation bounds prevent runaway jobs**.
- **Spot task nodes reduce cost but increase executor-loss risk**.
- **Cost attribution changes behavior only when ownership is visible**.

## Mental Model

Interactive queries, batch ETL, streaming jobs, ad hoc exploration, and backfills have different reliability and latency needs. They should not all compete blindly for the same unrestricted resources.

```text
Shared Spark platform
  |-- prod-critical queue
  |     -> strict SLA
  |
  |-- prod-batch queue
  |
  |-- streaming reserved capacity
  |
  |-- backfill capped queue
  |     -> executor and runtime caps
  |
  |-- adhoc low-priority queue
        -> low priority and limits
```

| Workload | Needs | Isolation Control |
| --- | --- | --- |
| Streaming | Stable latency | Reserved capacity, careful autoscaling |
| Production batch | Predictable completion | Queue capacity and retries |
| Backfill | Large temporary capacity | Capped queue and off-peak windows |
| Ad hoc | Flexibility | Low priority and runtime limits |

## Platform Responsibilities

The platform should define:

- Queues or namespaces by workload type and priority.
- Executor, memory, core, and runtime limits.
- Autoscaling policies.
- Streaming resource reservations.
- Backfill isolation.
- Cost attribution by team or application.
- Alerts for abusive or runaway jobs.
- EMR managed scaling bounds.
- Rules for core nodes vs task nodes.
- Rules for Spot task node usage.
- S3 file-count and request-pressure guardrails.

## Why It Matters In Production

Without isolation, one large backfill can starve streaming jobs, one bad join can fill shuffle disks, and one team can consume most of the cluster budget.

## Common Failure Modes

- Shared cluster saturation.
- Streaming jobs miss SLAs because batch jobs consume resources.
- Interactive users wait behind long ETL jobs.
- Autoscaling reacts too slowly for latency-sensitive jobs.
- No team owns cost spikes.
- Large shuffle jobs fill local disks.

## Configuration And Controls

On EMR with YARN, queues and capacity/fair scheduling control resource allocation. EMR managed scaling controls cluster elasticity within configured bounds. Core nodes and task nodes should be treated differently: core nodes are more stable and may hold HDFS roles if used; task nodes are better for elastic compute and Spot capacity.

Spark-level guardrails include max executors, dynamic allocation bounds, memory limits, runtime limits, and job admission checks.

EMR isolation controls:

| Control | Use It For | Risk If Missing |
| --- | --- | --- |
| YARN queues | Team/workload fairness | One job starves the cluster |
| Dynamic allocation bounds | Elasticity with limits | Runaway executor allocation |
| Managed scaling limits | Cluster cost and capacity control | Surprise cost or under-capacity |
| Task node Spot policy | Cheap elastic compute | Executor loss and fetch failures |
| Core node minimums | Stable cluster baseline | Critical jobs lose capacity |

## Operating Signals

Monitor:

- Queue utilization.
- Pending applications.
- Executor allocation time.
- Streaming batch latency.
- Cluster CPU and memory utilization.
- Shuffle disk usage.
- Cost by team/application.
- Jobs exceeding guardrail thresholds.
- EMR managed scaling events.
- Spot interruption-related executor loss.
- S3 request errors and throttling during high-concurrency jobs.

## Best Practices

- Separate production, backfill, streaming, and exploration workloads.
- Reserve capacity for latency-sensitive pipelines.
- Enforce per-team limits.
- Use cost attribution to change behavior.
- Provide approved executor profiles.
- Review large backfills before execution.
- Use Spot primarily for interruption-tolerant task capacity, not as the only capacity for critical SLAs.
- Cap backfill queues and schedule them outside peak production windows.
- Keep production event logs and YARN logs available after transient clusters terminate.

## Anti-Patterns

- One shared queue for every workload.
- Unlimited dynamic allocation.
- Running massive backfills during business-critical windows.
- Optimizing cluster utilization while missing streaming SLAs.
- Measuring cost only at cluster level with no owner.
- Letting ad hoc notebooks use the same queue and limits as production jobs.
- Scaling task nodes aggressively without considering S3 request pressure.

## Example

A platform can define separate queues:

- `prod-critical`: reserved capacity, strict deployment controls.
- `prod-batch`: scheduled ETL.
- `backfill`: capped resources and off-peak windows.
- `adhoc`: lower priority and runtime limits.

## Interview-Style Questions Covered

- How do you separate interactive, batch, streaming, and backfill workloads?
- What is fair scheduling in Spark?
- How do queues work in YARN?
- How do EMR managed scaling, YARN queues, core nodes, and task nodes affect workload isolation?
- How do you prevent one team's Spark job from starving others?
- How do you set guardrails for executor count, memory, cores, runtime, and shuffle size?
- How do you design per-team cost attribution?
- How do you isolate high-priority pipelines from exploratory workloads?
- How do you manage autoscaling without hurting streaming or latency-sensitive jobs?
- How do you design cluster policies for a shared Spark platform?

## Real Use Case

A year-end backfill launches with unrestricted dynamic allocation and consumes most of the shared EMR cluster. Streaming jobs fall behind. The staff-level fix creates a capped backfill queue, reserves capacity for streaming, adds runtime and executor guardrails, and publishes cost by application owner.
