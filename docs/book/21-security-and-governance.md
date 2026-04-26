# Security And Governance


## What You Should Be Able To Answer

After this chapter, you should be able to answer (quickly, from memory or by skimming this page):

- What “job identity” means on EMR and how least-privilege should look in practice.
- Where Spark security failures commonly happen (S3/Glue/KMS permissions, secrets, logs).
- How secrets leak in Spark systems (configs, logs, notebooks, closures) and how to prevent it.
- What production governance should standardize (auditability, separation of envs, defaults).
- What to check first when a job fails with permissions errors (which identity, which policy, which prefix/key).

## Core Idea

Spark security on EMR is the combination of compute identity, S3 and Glue permissions, secret handling, KMS encryption, network controls, table governance, and auditability. A production EMR platform should make secure behavior the default.

## Key Takeaways

- **Every Spark job has an identity**, and that identity must be least-privilege.
- **S3, Glue, KMS, and logs are all security surfaces**.
- **Secrets can leak through configs, logs, closures, and notebooks**.
- **Dev, staging, and production permissions should be separated by default**.

## Mental Model

Every Spark job has an identity. That identity reads sources, writes targets, accesses catalogs, decrypts data, and emits logs. Security failures happen when identity, permissions, or data handling are broader than the job requires.

```text
Spark job identity
  |-- catalog permissions
  |     -> tables and views
  |
  |-- S3 permissions
  |     -> data files
  |
  |-- secret manager
  |     -> external systems
  |
  |-- logs and metrics
        -> audit trail
```

| Surface | Control | Failure Mode |
| --- | --- | --- |
| Storage | IAM role / instance profile | Overbroad S3 read/write/delete |
| Catalog | Table/row/column permissions | Bypassed governance |
| Logs | Redaction and logging policy | Secret or PII leak |
| Environment | Separate roles | Dev accesses prod data |

## What Spark Does Internally

Spark runs code on drivers and executors. Secrets and credentials can appear in environment variables, Spark configs, logs, task closures, or application code if not handled carefully. Executors need access to data and services, so permissions must account for distributed execution.

## Why It Matters In Production

Spark jobs often process sensitive data at large scale. One bad job can expose PII, write unauthorized data, or leak credentials into logs.

## Common Failure Modes

- Secrets passed as plain Spark config and printed in logs.
- Overbroad IAM roles or EMR EC2 instance profiles.
- Executors lack permissions that the driver has.
- PII written to debug output or staging tables.
- No audit trail for table reads/writes.
- Dev credentials used in production.

## Configuration And Controls

Use:

- Least-privilege job identities.
- Secret managers instead of hardcoded secrets.
- Table-level, column-level, and row-level controls where required.
- Encryption at rest and in transit.
- Network restrictions for sensitive systems.
- Separate dev, staging, and production permissions.
- EMR security configurations for encryption and authentication where required.
- KMS key policies aligned with S3 data and log buckets.
- VPC endpoints and network controls for private S3, Glue, and CloudWatch access.

## Operating Signals

Track:

- Which job identity read or wrote each table.
- Access denied events.
- Secret access events.
- PII table usage.
- Cross-environment access attempts.
- Unexpected writes to production datasets.

## Best Practices

- Treat logs as data exposure surfaces.
- Mask or avoid logging sensitive values.
- Use separate IAM roles or cluster templates for read, write, admin, and maintenance jobs where your EMR setup supports it.
- Enforce permissions in catalogs and storage.
- Audit access to gold and sensitive datasets.

## Anti-Patterns

- Sharing one admin role across all Spark jobs.
- Embedding database passwords in notebooks.
- Writing PII to temporary public paths.
- Granting direct object-store access that bypasses table governance.
- Assuming driver-only credential checks are enough.
- Letting dev clusters use production S3 prefixes by default.

## Example

A production EMR job that writes `gold.customer_profile` should run on a cluster or role configuration with read access to approved silver S3 prefixes and Glue catalog objects, write access only to its target table, KMS permissions for required buckets, no broad delete permission outside its table, and no ability to read unrelated PII datasets.

## Interview-Style Questions Covered

- How do Spark jobs access data securely?
- How do IAM roles, instance profiles, and EMR security configuration affect Spark jobs?
- How do you prevent secrets from leaking into Spark logs?
- How do you enforce table-level, row-level, and column-level access control?
- How do you handle PII or sensitive data in Spark pipelines?
- How do you audit who read or wrote a dataset?
- How do encryption at rest and encryption in transit affect Spark architecture?
- How do you design secure access for shared EMR/YARN environments?
- How do you separate developer, staging, and production permissions?
- How do you design governance for bronze, silver, and gold datasets?

## Real Use Case

A team copies production customer data into a dev bucket for debugging. A staff-level platform prevents this by separating environment roles, enforcing table permissions, masking sensitive columns in lower environments, and auditing every read from PII-classified tables.
