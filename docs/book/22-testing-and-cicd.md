# Testing And CI/CD

Status: First Draft
Level: Senior to Staff
Covers: local tests, integration tests, representative datasets, deterministic tests, streaming tests, deployment

## Core Idea

Spark testing should prove transformation correctness, data contract behavior, deployment packaging, and production safety. The goal is not to unit test Spark itself; it is to test the business logic and operational assumptions around Spark jobs.

## Key Takeaways

- **Test business logic and data contracts**, not Spark internals.
- **Representative small data beats perfect toy data**.
- **Local Spark tests do not prove EMR/S3/Glue behavior**.
- **The same artifact should move from dev to staging to production**.

## Mental Model

Use different test layers:

- Unit tests for pure transformation logic on small DataFrames.
- Contract tests for schemas, required columns, uniqueness, and null rules.
- Integration tests for Glue/catalogs, table formats, S3, and connectors.
- Regression tests for known edge cases such as skew, late data, duplicates, and schema evolution.
- Deployment tests for packaging and runtime compatibility.

```text
Spark testing pyramid

          deployment smoke tests
        integration tests
      contract / data quality tests
    local Spark transformation tests
  pure function and config tests
```

| Test Type | Catches | Should Stay |
| --- | --- | --- |
| Local DataFrame test | Transformation logic | Fast and deterministic |
| Contract test | Schema/key/null violations | Close to publish boundary |
| Integration test | Catalog/storage/connector behavior | Production-like |
| Deployment smoke test | Packaging/runtime mismatch | Minimal but real |

## What Spark Does Internally

Local Spark tests run with local executors and small data. They catch many logic bugs but do not fully represent distributed shuffle, S3 behavior, EMR permissions, executor packaging, or production data volume.

Streaming tests need deterministic input, controlled triggers, checkpoint isolation, and assertions over output/state.

## Why It Matters In Production

Spark jobs often fail in production because tests only used happy-path toy data. Real pipelines need representative small datasets that include nulls, duplicates, late records, out-of-order events, schema changes, hot keys, and empty inputs.

## Common Failure Modes

- Tests pass locally but dependencies fail on executors.
- Non-deterministic tests due to unordered DataFrame comparisons.
- No test covers duplicate keys before merge.
- Streaming tests reuse checkpoints and leak state.
- CI becomes too slow because every test starts a heavy Spark session.

## Test Strategy

Make tests deterministic:

- Compare sorted outputs when order matters.
- Use explicit schemas.
- Avoid current timestamps unless injected.
- Isolate temp directories and checkpoints.
- Use small but representative fixtures.

Keep local tests fast, and move storage/catalog/table-format behavior to integration tests.

## CI/CD Signals

CI should report:

- Unit and integration test results.
- Packaging validation.
- Static checks.
- Schema contract checks.
- Example query plans for critical jobs where useful.
- Deployment artifact version.

## Best Practices

- Write transformations as testable functions.
- Use explicit input and expected-output fixtures.
- Test empty, duplicate, late, null-heavy, and skewed data.
- Validate output schema and key constraints.
- Promote through dev, staging, and production with the same artifact.
- Keep rollback strategy documented.

## Anti-Patterns

- Testing only with one perfect input file.
- Comparing DataFrames without handling ordering.
- Running production jobs from mutable notebooks.
- Skipping integration tests for table writes.
- Treating CI success as proof of production data correctness.

## Example

```python
def build_customer_daily(events):
    return events.groupBy("customer_id", "event_date").count()
```

This function can be tested with a local Spark session using fixtures that include duplicate events, null customer IDs, and multiple dates.

## Interview-Style Questions Covered

- How do you unit test Spark transformations?
- What should be tested with a local Spark session vs an integration environment?
- How do you create small but representative test datasets?
- How do you test skew, late data, schema evolution, duplicate input, and null-heavy data?
- How do you make Spark tests deterministic?
- How do you test streaming queries?
- How do you validate query plans or output file counts in tests?
- How do you run Spark tests in CI without making them slow or flaky?
- How do you promote Spark jobs from dev to staging to production?
- What should a Spark deployment rollback strategy look like?

## Real Use Case

A customer merge job passes tests but corrupts production because duplicate customer IDs in the source create multiple matched rows. The fix is to add duplicate-key fixtures, a data quality gate before merge, an integration test against the table format, and deployment promotion using the same packaged artifact.
