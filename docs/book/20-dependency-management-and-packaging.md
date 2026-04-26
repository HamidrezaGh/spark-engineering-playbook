# Dependency Management And Packaging

Status: First Draft
Level: Senior to Staff
Covers: application packaging, classpath, Python dependencies, connector versions, reproducible builds

## Core Idea

Spark applications on EMR run across driver and executor processes on cluster nodes. Dependencies must be available in the right place, with versions compatible with the EMR release, Spark version, Scala binary version, Java version, Python version, and any connectors.

## Key Takeaways

- **Driver dependencies and executor dependencies are different failure surfaces**.
- **EMR release compatibility controls Spark, Scala, Python, Java, and connector versions**.
- **Notebook state is not a production artifact**.
- **Reproducible packaging prevents local-success/cluster-failure surprises**.

## Mental Model

The driver classpath affects code that runs in the driver: planning, catalog access, job orchestration, and some client-side logic. The executor classpath affects task execution. PySpark also has Python environments on driver and executors.

```text
Driver process
  - SparkSession, catalog access, planning, submission logic
  - needs driver-side jars and Python packages

Executor processes
  - task execution, UDFs, connector reads/writes
  - need executor-side jars and Python packages
```

| Failure | Usually Means | Check |
| --- | --- | --- |
| `ClassNotFoundException` | Missing jar | Submit packages and executor classpath |
| `NoSuchMethodError` | Binary version conflict | Spark/Scala/connector versions |
| `ModuleNotFoundError` | Python package missing on executors | `--py-files`, archive, image |
| Works locally only | Environment drift | CI artifact vs notebook state |

## What Spark Does Internally

During submission, Spark distributes application jars, Python files, archives, and configuration based on submit options and cluster manager behavior. Connectors such as Iceberg, Delta, Kafka, and JDBC drivers must match Spark, Scala, Java, and Python versions where relevant.

## Why It Matters In Production

Dependency issues often appear only in production because dev and cluster environments differ. A notebook may have a Python package installed locally while executors do not. A connector jar may work with one Spark/Scala version and fail with another.

## Common Failure Modes

- `ClassNotFoundException` for missing connector jars.
- `NoSuchMethodError` from binary-incompatible jar versions.
- Python `ModuleNotFoundError` on executors.
- Driver can access a dependency but executors cannot.
- Local test passes but EMR/YARN job fails.
- Secrets or environment config accidentally packaged into artifacts.

## Configuration And Deployment

Use reproducible packaging:

- Lock Python dependencies.
- Pin connector versions to the EMR release's Spark and Scala versions.
- Build application artifacts in CI.
- Avoid manual cluster mutations.
- Separate code config from environment secrets.
- Test packaging in an environment close to production.
- Treat bootstrap actions and custom AMIs as versioned infrastructure, not manual fixes.
- Record the EMR release and installed applications for every production job.

## Operating Signals

Check:

- Driver logs.
- Executor logs.
- Spark submit arguments.
- Resolved jars.
- Python environment paths.
- Cluster image/bootstrap configuration.

## Best Practices

- Keep a compatibility matrix for EMR release, Spark, Scala, Java, Python, Hadoop AWS libraries, and connectors.
- Use build artifacts rather than notebook state for production jobs.
- Validate dependencies in integration tests.
- Keep deployment configuration reviewable.
- Prefer minimal dependency sets.

## Anti-Patterns

- Installing packages manually on one EMR cluster and calling it production.
- Depending on notebook-local packages.
- Mixing connector versions across jobs.
- Adding broad shaded jars without checking conflicts.
- Storing secrets in packaged config files.

## Example

```bash
spark-submit \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.x.x \
  --py-files dist/my_job.zip \
  jobs/load_table.py
```

The Spark and Scala suffix must match the runtime. The exact Iceberg version should be pinned by the platform's compatibility matrix.

## Interview-Style Questions Covered

- How do you package a Spark application for production?
- What causes dependency conflicts in Spark jobs?
- What is the difference between driver classpath and executor classpath?
- How do Python dependencies get distributed to executors?
- Why can a job work locally but fail on EMR/YARN?
- How do Java, Scala, Python, and Spark version compatibility issues show up?
- How do you manage third-party connectors such as Iceberg, Delta, Kafka, or JDBC drivers?
- How do you make Spark builds reproducible?
- How do you handle secrets and environment-specific configuration during deployment?
- How do you debug `ClassNotFoundException`, `NoSuchMethodError`, or Python module import failures?

## Real Use Case

A job works in a notebook but fails on EMR executors with `ModuleNotFoundError`. The package was installed only on the notebook host. The production fix is to build a versioned Python artifact, distribute it with `--py-files` or an environment archive, and test the submitted artifact in CI against the target EMR release.
