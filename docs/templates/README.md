# Templates

Reusable templates for the kinds of documents **platform and data** teams actually produce: design
reviews, incident postmortems, production readiness checks, cost reviews, and content-authoring
scaffolds.

Use these when you start a new document, not as decoration. The questions in each template exist because at least one production incident has been caused by skipping them.

## Review templates

These are the four templates a Spark platform team uses regularly. Read [Chapter 15 — Platform
patterns and guardrails](../book/15-platform-patterns.md) for context on why each one exists.

| Template | When To Use |
| --- | --- |
| [`spark-design-review.md`](spark-design-review.md) | Before launching a new Spark job in production, or significantly changing an existing job's shape (volume, partitioning, output destination). |
| [`spark-incident-review.md`](spark-incident-review.md) | After any production Spark incident — slow runtime, OOM, fetch failures, output corruption, SLA miss. |
| [`spark-production-readiness-review.md`](spark-production-readiness-review.md) | Before promoting a job from staging to production, or before adopting an existing job into a managed on-call rotation. |
| [`spark-cost-review.md`](spark-cost-review.md) | Quarterly for SLA-critical jobs, opportunistically when a job's cost has surprised the team. |

## Authoring Templates

These are the templates used for new content in this repo. They mirror the structure expected by [`AUTHORING_GUIDE.md`](../../AUTHORING_GUIDE.md).

| Template | Used For |
| --- | --- |
| [`book-chapter-template.md`](book-chapter-template.md) | New book chapters in `docs/book/`. |
| [`field-guide-template.md`](field-guide-template.md) | New incident-oriented field guides in `docs/field-guides/`. |
| [`pattern-template.md`](pattern-template.md) | New patterns in `docs/patterns/`. |
| [`checklist-template.md`](checklist-template.md) | New checklists in `docs/checklists/`. |
| [`tuning-note-template.md`](tuning-note-template.md) | New tuning notes in `docs/tuning/`. |

## How To Use A Template

1. Copy the template into the appropriate directory.
2. Replace placeholder text with concrete answers.
3. Delete sections that are genuinely not applicable; do not leave them blank or write "N/A" without a reason.
4. Treat reviewer sign-off as a real gate; it is the most useful check on whether the work is done.
