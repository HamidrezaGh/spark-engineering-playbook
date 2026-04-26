# Spark On AWS EMR And YARN

Status: First Draft
Level: Senior to Staff
Covers: EMR, YARN, client mode, cluster mode, ApplicationMaster, containers, executor sizing, S3, logs, CloudWatch

## Core Idea

On AWS EMR, Spark commonly runs on YARN. YARN manages containers for the driver, ApplicationMaster, and executors, while EMR provides the cluster lifecycle, release version, EC2 instance layout, bootstrap actions, application configuration, step orchestration, and integrations with S3, IAM, CloudWatch, and CloudWatch Logs.

The production question is not only "does the Spark code work?" It is "does the Spark application fit this EMR release, instance fleet, YARN queue, S3 layout, IAM role, logging setup, and cost model?"

## Mental Model

In client mode, the driver runs where `spark-submit` is launched, such as an edge node, notebook host, or gateway. In cluster mode, the driver runs inside the cluster in an application container.

Client mode is convenient for interactive work but fragile for long production jobs. If the notebook or gateway loses connectivity, the driver may fail. Cluster mode is usually better for scheduled production jobs.

| Deploy Mode | Driver Location | Best For | Main Risk |
| --- | --- | --- | --- |
| Client | Submit host, notebook, gateway | Interactive debugging | Driver tied to client availability |
| Cluster | Cluster-managed container | Scheduled production jobs | Harder interactive debugging |

```text
spark-submit
  -> YARN ResourceManager
  -> ApplicationMaster
      |-- driver
      |-- executor container 1
      |-- executor container 2
      |-- executor container 3

driver
  -> schedules tasks on executor containers
```

EMR also adds a cluster-level shape:

```text
EMR cluster
  |-- primary node
  |     |-- cluster coordination, YARN ResourceManager, EMR services
  |
  |-- core nodes
  |     |-- YARN NodeManagers
  |     |-- executors
  |     |-- HDFS capacity if used
  |
  |-- task nodes
        |-- YARN NodeManagers
        |-- executors
        |-- good for elastic/Spot compute
        |-- not durable storage nodes
```

| EMR Concept | Production Meaning |
| --- | --- |
| EMR release | Pins Spark, Hadoop, Hive, Python, Java, and connector compatibility |
| Primary node | Cluster coordination and control-plane services |
| Core nodes | Compute plus durable HDFS role if HDFS is used |
| Task nodes | Elastic compute; often good for Spot but can be lost |
| EMR step | Cluster-managed unit of work, often wrapping `spark-submit` |
| Instance profile | IAM permissions used by EC2 nodes to access S3 and AWS services |

## What Spark Does Internally

During `spark-submit` on EMR, Spark packages configuration and dependencies, contacts YARN, starts an ApplicationMaster, allocates executor containers on EMR nodes, launches executors, and schedules tasks.

YARN containers enforce memory and CPU limits. If executors exceed container memory, YARN can kill them even when Spark does not report a clean JVM OOM.

EMR-specific runtime behavior to understand:

- EMR steps give the cluster a durable way to run production jobs and capture step status.
- Notebook-submitted jobs are useful for development but often run in client mode and inherit notebook lifecycle risks.
- Bootstrap actions and EMR configurations define cluster-level dependencies and Spark defaults.
- S3 is the usual durable storage layer; executor local disks are temporary working space for shuffle, spill, and caches.
- Spot task nodes can disappear; Spark can retry lost work, but shuffle-heavy jobs may suffer fetch failures and recomputation.
- EMR release versions matter because connector artifacts must match Spark and Scala versions.

## Why It Matters In Production

The same Spark code can behave differently depending on EMR release, deploy mode, queue capacity, executor sizing, dependency distribution, S3 layout, IAM permissions, and YARN container limits.

Executor cores affect how many tasks run concurrently per executor. Executor instances affect total cluster parallelism. Too many cores per executor can increase memory contention; too few can create overhead and poor utilization.

## Common Failure Modes

- Notebook client mode driver dies mid-job.
- YARN kills containers for memory overhead violations.
- Executors wait for resources because the queue is full.
- Dependencies exist on the driver but not executors.
- Logs are scattered across YARN containers.
- EMR step fails because the cluster role lacks S3 read/write permissions.
- Job works on one EMR release but fails after an upgrade because a connector jar no longer matches Spark/Scala.
- Spot task node loss causes executor loss and shuffle fetch failures.
- S3 listing, throttling, or small files make the cluster look underutilized.
- Bootstrap action changes create environment drift between clusters.

## Tuning And Configuration

On EMR, executor sizing depends on instance type, YARN overhead, daemon processes, workload memory needs, and desired parallelism. Avoid using all node memory for executors; leave room for OS and cluster services.

Practical sizing questions:

- How many cores per executor?
- How many executors per node?
- How much heap per executor?
- How much memory overhead?
- How many total concurrent tasks?
- Is the workload CPU-bound, memory-bound, IO-bound, or shuffle-heavy?

EMR sizing checklist:

| Question | Why It Matters |
| --- | --- |
| What instance family is used? | Compute, memory, network, and disk differ heavily |
| Are task nodes Spot? | Expect executor loss and retry pressure |
| How much local disk is available? | Shuffle and spill need local disk, even with S3 storage |
| Is dynamic allocation enabled? | Helps elastic workloads but needs sensible min/max limits |
| Are jobs sharing one YARN queue? | One large job can starve others |
| Is the workload S3-heavy? | More executors may increase S3 request pressure |

Common EMR tuning levers:

- `spark.executor.instances`, `spark.dynamicAllocation.enabled`, and dynamic allocation min/max bounds.
- `spark.executor.cores` to control concurrent tasks per executor.
- `spark.executor.memory` and `spark.executor.memoryOverhead`, especially for PySpark.
- `spark.sql.shuffle.partitions` and AQE settings for shuffle-heavy jobs.
- `spark.local.dir` capacity and disk layout for spill-heavy workloads.
- YARN queue capacity and application limits for shared clusters.

## Spark UI Signals

Use:

- Spark UI for stages, executors, SQL plans, and task failures.
- YARN ResourceManager for application state and queue pressure.
- YARN aggregated logs for container kill reasons.
- EMR steps and cluster logs for bootstrap, dependency, and environment issues.
- CloudWatch metrics for cluster CPU, memory pressure proxies, HDFS if used, and instance health.
- CloudWatch Logs or S3 log archive for persistent driver, executor, step, bootstrap, and YARN logs.
- S3 metrics and request errors when scan planning or writes are slow despite low CPU.

## Best Practices

- Use cluster mode for scheduled production jobs.
- Size executors based on workload and instance shape.
- Keep dependency packaging reproducible.
- Use YARN queues for workload isolation.
- Preserve event logs for post-mortem debugging.
- Pin and document the EMR release for production jobs.
- Treat S3 file count and layout as part of Spark performance.
- Use EMR steps or a scheduler that submits durable `spark-submit` jobs for production.
- Prefer task nodes for elastic compute and keep critical durable services off volatile capacity.
- Store Spark event logs and YARN logs in S3 for after-the-fact debugging.
- Keep IAM policies least-privilege but explicit for required S3 prefixes, Glue catalog access, KMS keys, and CloudWatch logging.

## Anti-Patterns

- Running critical batch jobs from an unstable notebook session.
- Maximizing executor cores without considering memory per task.
- Ignoring YARN kill messages and only reading Python stack traces.
- Using one shared queue with no guardrails.
- Upgrading EMR release without validating Spark, Scala, Python, Java, and connector versions.
- Treating S3 as if it were local HDFS.
- Using Spot task nodes for shuffle-heavy SLA-critical jobs without retry and capacity planning.
- Debugging only from EMR step status without opening Spark UI, YARN logs, and executor logs.

## Example

```bash
spark-submit \
  --deploy-mode cluster \
  --master yarn \
  --executor-memory 8g \
  --executor-cores 4 \
  --conf spark.executor.memoryOverhead=2g \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=s3://my-emr-logs/spark-event-logs/ \
  jobs/daily_orders.py
```

These values are placeholders. A production config must fit the EMR instance type, workload shape, and queue capacity.

## Interview-Style Questions Covered

- Difference between Spark client mode and cluster mode?
- Where does the driver run in client mode?
- Where does the driver run in cluster mode?
- Why can client mode fail from a notebook?
- What happens during `spark-submit`?
- What are ApplicationMaster and containers in YARN?
- How do executor cores affect parallelism?
- How do executor instances affect parallelism?
- How do you size executors on EMR?
- How do you debug failed Spark jobs from YARN logs?
- What is the difference between EMR steps, `spark-submit`, and notebook-submitted jobs?
- How do EMR release versions affect Spark, Hadoop, Python, Java, and connector compatibility?
- How do core, task, and primary nodes affect Spark behavior on EMR?
- When would you use Spot task nodes, and what failure modes should you expect?

## Real Use Case

A nightly EMR job fails only when launched from a notebook. The actual transformation is fine; the driver is running in client mode on the notebook host and dies when the session disconnects. Moving the job to `spark-submit --deploy-mode cluster`, storing event logs, and using a production YARN queue makes the run independent of the notebook lifecycle.
