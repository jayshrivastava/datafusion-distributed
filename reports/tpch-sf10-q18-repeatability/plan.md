# TPC-H Q18: Dynamic Filtering On Versus Off

## Question

Can enabling dynamic filtering produce a repeatable local SF10 latency win
with identical results and plan metrics explaining the reduced work?

Test Q18 first because an earlier local run showed a benefit. Do not compare
B against A: both sides use the same B/global-hash implementation, and only
the master dynamic-filter switch changes.

## Fixed Conditions

- Existing SF10 Parquet dataset; no data regeneration or upload.
- Release build, four localhost workers, four partitions/threads per worker.
- Parquet row pushdown and filter reordering enabled in both cases.
- Ordinary row-group pruning enabled, LZ4, broadcast joins, static planning.
- No injected post-scan filtering: its fallback is inactive with pushdown on.
- Default join filter thresholds; no query-specific optimizer tuning.
- Exact repository Q18 SQL; no changes to make its predicate more selective.
- Warm filesystem cache. No intentional cache eviction between runs.
- No simultaneous benchmark, test, or compilation started by this agent.

The only changed setting is:

```text
datafusion.optimizer.enable_dynamic_filter_pushdown = false / true
```

## Schedule And Acceptance

Three fresh coordinator/worker sessions. Each has one excluded warmup per
case followed by 20 measured pairs, alternating/reversing execution order.
Reverse the initial order in the second session. Total: 60 measurements
per case and six excluded warmups.

Predeclared criteria for a strong local result:

- Mean speedup at least 1.20x in every session.
- At least 90% of measured pairs favor filtering enabled.
- The paired bootstrap 95% interval for the pooled mean ratio stays above
  1.10x. Resample pairs within sessions and retain all observations.
- Exact sorted result values match across every execution and the saved
  earlier SF10 reference.
- Plan metrics show useful filtering and reduced downstream work; receiving
  updates or a faster timer alone does not establish a filter benefit.

Retain every plan, raw metric, result, and timing. Report failed criteria
instead of removing inconvenient samples. If Q18 does not meet these
criteria, report that outcome before evaluating another candidate.

Execution timing excludes planning, plan rendering, and file writes.
Planning time is captured separately. No claim is made about SF100 until
it is tested with the same isolation of settings.
