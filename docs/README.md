# Documentation

This directory contains the public Spark Engineering Playbook.

The default production environment is AWS EMR with Spark on YARN and S3-backed data lakes. Generic Spark concepts still apply elsewhere, but examples and operational guidance prefer EMR, S3, IAM, and CloudWatch.

## Layout

- [`book/`](book/README.md) — the main chapter-by-chapter handbook.
- [`field-guides/`](field-guides/README.md) — incident-oriented debugging guides (slow jobs, OOM, skew, small files, Spark UI).
- [`patterns/`](patterns/README.md) — reusable Spark and lakehouse architectural patterns.
- [`tuning/`](tuning/README.md) — focused tuning references for common levers.
- [`configs/`](configs/README.md) — Spark configuration field manual.
- [`checklists/`](checklists/README.md) — operating checklists for pre-deploy, triage, cost, and production readiness.
- [`case-studies/`](case-studies/README.md) — anonymized post-incident reviews.
- [`templates/`](templates/) — authoring templates for new chapters, patterns, and field guides.
- [`glossary.md`](glossary.md) — shared production-oriented terminology.
- [`advanced-spark-questions.md`](advanced-spark-questions.md) — the roadmap of questions the handbook should answer.
