# Spark Incident Review Template

Use this template after any production incident involving a Spark job — slow runtime, OOM, fetch failures, output corruption, SLA miss. The goal is a written record that the next on-call engineer can read in 10 minutes and understand what happened, why, and what changed because of it.

This is not a blame document. It is a learning artifact. If a section says "the on-call engineer increased memory three times before opening the Spark UI," that is information for the platform, not a critique of the engineer.

## Incident Identity

- **Incident date / time**:
- **Job name**:
- **Application id (Spark) and EMR cluster id (or equivalent)**:
- **Severity** (SEV-1, SEV-2, SEV-3):
- **Duration of impact**:
- **Detection method** (alert, downstream complaint, customer report):
- **Linked ticket / postmortem doc**:

## What Changed?

The first triage question. Anything in this list is a candidate cause.

- Did the application code change since the last good run? (link to commit / PR)
- Did configuration change? (`spark.conf` diff)
- Did the cluster shape change? (instance fleet, EMR release, Databricks runtime)
- Did dependencies change? (JARs, Python packages, EMR bootstraps)
- Did upstream input change? (volume, schema, file count, partition layout)
- Did downstream consumers change? (new query patterns, new freshness requirement)
- Did the schedule change? (new collisions on the cluster)
- Was there a platform event? (Spot reclamation wave, S3 throttling, network disruption)

## What Failed?

- What was the user-visible symptom?
- What error or condition triggered the alert?
- What was the wall-clock impact (runtime regression, full failure, partial failure)?
- Did the job retry? Did the retry succeed? At what cost?

## Where Did Time Go?

Open the Spark UI (or the persisted event log via Spark History Server). For each significant stage:

- Stage id and operator (e.g., "Stage 14, SortMergeJoin")
- Stage duration:
- Stage shuffle write/read bytes:
- Stage spill (memory + disk):
- Stage task count:
- Median task duration:
- Max task duration:
- Max-to-median ratio:

The stage at the top of this list is the one to focus on. If the slowest stage is 95% of total runtime, almost nothing else matters.

## Slowest Stage

- What operator feeds the slowest stage?
- What does the physical plan show (excerpt of `EXPLAIN FORMATTED` for that operator)?
- Did AQE intervene? (look for `isFinalPlan=true` and any `isSkewedJoin` annotations)
- What is the input data shape feeding this stage?

## Largest Shuffle

- Which shuffle has the largest write bytes? Read bytes?
- What is the shuffle key?
- Was AQE coalescing or skew handling active?
- Did the shuffle size change vs the last good run?

## Spill Evidence

- Which tasks spilled? How many? How much?
- Was spill concentrated on a few tasks (skew signature) or distributed (working-set-too-large signature)?
- Did spill correlate with executor losses?

## Skew Evidence

- Top key on the shuffle key (with row count and share of total)
- Max-to-median ratio for the slowest stage
- Was AQE skew join handling expected to fire? Did it?
- Is the skew shape new, or has it been growing slowly?

## Driver vs Executor Issue

- Did the driver fail, or did executors fail?
- For driver: heap, plan size, listing scope, `collect()`?
- For executor: container kill (memory), task failure (logic), fetch failure (network/storage)?
- Was the cluster healthy? Any node losses, instance terminations, network anomalies?

## Data Shape Change

- Compared to the last good run:
  - Input row count diff:
  - Input bytes diff:
  - Input file count diff:
  - Top-key concentration diff:
  - Schema diff:
  - Late-arriving partition or partition-bound change:

## Root Cause

A single concise paragraph. If you cannot write the root cause in two or three sentences, the investigation is not finished.

## Smallest Safe Fix

- What was changed to resolve the immediate incident?
- Why is this the smallest possible change?
- What evidence demonstrates the fix worked?
- What was reverted that did not help?

## Guardrail Added

- What metric, alert, runbook update, or platform change came out of this incident?
- Where does it live? (dashboard, alert rule, runbook page, config default)
- Who owns it going forward?
- How would this guardrail have caught the incident earlier?

## Lessons For The Platform

Local fixes are not the most valuable output of an incident review. Platform-level lessons are.

- Could other jobs hit the same shape? Which ones?
- Should the platform's golden-path templates change?
- Should the design review template add a question?
- Should a config default change cluster-wide?
- Should the on-call runbook change?

## Action Items

| Item | Owner | Due | Status |
| --- | --- | --- | --- |
| Add metric for X | | | |
| Update runbook section | | | |
| Roll cluster-wide config change | | | |
| Audit similar jobs for the same shape | | | |
| Schedule follow-up review | | | |

## Sign-Off

- Incident commander:
- Reviewing engineer:
- Date of review:
