# Exhaustive local dynamic-filter sweep

## Scope

Run every repository query in TPC-H SF10 (except Q18), TPC-DS SF10,
and full ClickBench (partitions 0 through 99, with Date32 EventDate).
This is 21 + 99 + 43 = 163 query IDs, or 978 matrix entries.
Query errors and timeouts remain explicit coverage gaps, not silent exclusions.

## Matrix

| Case | Partitioned-join merge | Dynamic filters | Parquet pushdown/reordering |
| --- | --- | --- | --- |
| Control-off | A | Off | Both off |
| Control-on | A | Off | Both on |
| A-off | Existing OR merge | On | Both off |
| A-on | Existing OR merge | On | Both on |
| B-off | Global hash CASE merge | On | Both off |
| B-on | Global hash CASE merge | On | Both on |

Compare A-off/B-off against Control-off, and A-on/B-on against Control-on.
Parquet settings must be held fixed when attributing a change to dynamic
filtering. B confirmations also use a same-B-binary control to remove binary
and session drift as explanations.

A is commit c78f9e124875c6bb25cb63ed16df75af700eb34d. B adds only the
four-file global-hash patch frozen for the remote experiment. Neither build
contains the injected post-scan FilterExec experiment. Existing natural
FilterExec consumers remain enabled. No user worktree changes are reverted.
Both builds retain the committed AND merge for incremental sort/aggregate
filters. A/B changes only the partitioned-join CASE representation, not that
merge policy. A local-only filter benefit does not establish a benefit from
remote propagation or the global-hash change.

## Execution

- Release builds, four separate localhost gRPC workers, four target partitions
  and four Tokio worker threads per worker. Coordinator uses two threads.
- Static task planning, maximum four tasks per stage, broadcast joins enabled,
  union child isolation enabled, LZ4 transport compression.
- Ordinary Parquet statistics pruning stays enabled in every case.
- Metrics and completed dynamic-filter reporting enabled in every case.
- Reuse local TPC-H SF10. Generate TPC-DS SF10 with 16 files per table.
  Store both TPC-DS and full ClickBench on instance storage.
- No cache eviction: this measures repeated local, warm-cache workloads.
- Do not overlap timed queries with data generation, compilation, or another
  benchmark. Run each query/case serially.
- Screen with one excluded warmup and three measured executions per entry.
  Balance case ordering across queries and repetitions.
- Save SQL, effective settings, executed plans with metrics, raw numeric
  metrics, result values, errors, binary hashes, and source provenance.
- Measure physical planning separately. Query latency covers execution and
  draining the result stream, not display rewriting or artifact generation.

The numeric JSON export uses `MetricValue::as_usize`, which does not preserve
composite `PruningMetrics` or `Ratio` values. Their zero placeholders must not
be interpreted as measured zeros. Rendered plans preserve these metrics;
use those for statistics/page-index pruning. Ordinary counts, byte counts,
and timings in the numeric export are unaffected.

## Confirmation And Ranking

Screen all queries before final ranking. Re-run promising queries using
20 measurements of the chosen enabled configuration and each disabled
control, split across two fresh-worker sessions with reversed initial order.
Pair by session and iteration. Include slower samples and report variance.
The matching-mode comparison isolates dynamic filtering; the other disabled
control checks whether changing Parquet settings alone would be faster.

A qualified win requires:

1. Correct results against the disabled control. Flag any nondeterministic
   LIMIT ties or floating-point differences for explicit inspection.
2. Faster mean and median, with a paired bootstrap confidence interval whose
   lower bound is above 1.0. Report the interval and pair win rate.
3. Visible dynamic predicates and metrics demonstrating reduced scan output,
   rows/row groups processed, shuffle volume, or downstream operator work.
   Updates received alone are not proof of useful filtering.
4. Matching Parquet settings and no injected post-scan filter.

Rank up to ten distinct queries by confirmed matched-control speedup, using
their best qualifying A/B and Parquet configuration. Report fewer than ten
if the evidence does not support ten. Distinguish local-only from remote
filter effects; the master switch affects both.

For the headline list, additionally require a confidence interval above 1.0
against the faster of the two disabled-filter configurations. Show gains that
hold only against the matching Parquet mode separately. Report empty-result
queries explicitly; prefer a nonempty query for a presentation example.

An exhaustive screen and selected-query wins do not establish a whole-suite
performance improvement. Keep suite totals and regressions visible too.
