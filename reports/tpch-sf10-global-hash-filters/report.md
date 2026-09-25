# TPC-H SF10: Global Hash Dynamic Filters

Uncommitted experiment. See [setup and routing safeguards](README.md).

The comparison isolates the coordinator merge change. Both binaries include
the earlier post-scan filter experiment. Four localhost workers each use
four target partitions and four worker threads.

Ten measured executions per entry, in A/B/B/A blocks of five. Each block
also has one warmup per entry. All 288 executions return identical results
across configurations and binaries, and match the previous benchmark.

## Conclusion

- q17 benefits from both cheaper evaluation and more selective predicates:
  about 1.7x faster than the old merge, with 44% fewer forwarded scan rows
  in dynamic-only mode. It is still slower than filtering disabled.
- q18 improves by 1.30x in dynamic-only mode and 1.36x with all options on.
  All-on is also 1.54x faster than its own all-off control.
- q20 regresses by 1.27x in dynamic-only mode and 1.05x with all options on.
  Its predicates filter too little to repay their evaluation cost.
- No dynamic row groups were pruned; scanned bytes were unchanged.
- q15 has no partitioned join and performs no global merges.

## Latency

Means in milliseconds. Speedup is baseline / candidate, not all-off / on.

| Query | Configuration | Baseline | Global hash | Speedup |
| --- | --- | ---: | ---: | ---: |
| q17 | all_off | 1784.9 | 1777.5 | 1.00x |
| q17 | dynamic_only | 3413.0 | 2004.7 | 1.70x |
| q17 | all_on | 3347.6 | 2009.2 | 1.67x |
| q18 | all_off | 2039.2 | 2044.2 | 1.00x |
| q18 | dynamic_only | 1810.2 | 1387.8 | 1.30x |
| q18 | all_on | 1811.0 | 1328.1 | 1.36x |
| q20 | all_off | 526.2 | 533.5 | 0.99x |
| q20 | dynamic_only | 992.7 | 1260.1 | 0.79x |
| q20 | all_on | 786.0 | 828.4 | 0.95x |
| q15 | all_off | 449.5 | 450.2 | 1.00x |
| q15 | dynamic_only | 480.0 | 459.2 | 1.05x |
| q15 | all_on | 615.4 | 594.8 | 1.03x |

## Filtering

Rows forwarded after scans and immediate post-scan filters, in millions.
Includes repeated table scans; downstream filters are not subtracted.

| Query | Configuration | Baseline M | Global hash M |
| --- | --- | ---: | ---: |
| q17 | dynamic_only | 47.556 | 26.610 |
| q17 | all_on | 59.427 | 41.076 |
| q18 | dynamic_only | 80.105 | 79.191 |
| q18 | all_on | 85.556 | 78.613 |
| q20 | dynamic_only | 69.964 | 69.865 |
| q20 | all_on | 17.116 | 17.085 |
| q15 | dynamic_only | 120.072 | 120.072 |
| q15 | all_on | 4.631 | 4.631 |

## Filter Compute

Summed operator compute milliseconds across tasks, not query wall time.
Dynamic-only uses post-scan compute; all-on uses Parquet pushdown evaluation.

| Query | Configuration | Baseline ms | Global hash ms |
| --- | --- | ---: | ---: |
| q17 | dynamic_only | 23074.2 | 11584.7 |
| q17 | all_on | 18343.4 | 9649.4 |
| q18 | dynamic_only | 10260.9 | 5253.2 |
| q18 | all_on | 9118.7 | 4888.9 |
| q20 | dynamic_only | 4207.4 | 6808.0 |
| q20 | all_on | 985.0 | 1696.8 |
| q15 | dynamic_only | 114.3 | 110.0 |
| q15 | all_on | 116.3 | 114.2 |

## Routing and Row Groups

Mean global merges and dynamically pruned row groups per query execution.

| Query | Configuration | Global merges | Baseline RG | Global hash RG |
| --- | --- | ---: | ---: | ---: |
| q17 | dynamic_only | 2.0 | 0.0 | 0.0 |
| q17 | all_on | 2.0 | 0.0 | 0.0 |
| q18 | dynamic_only | 3.0 | 0.0 | 0.0 |
| q18 | all_on | 3.0 | 0.0 | 0.0 |
| q20 | dynamic_only | 3.0 | 0.0 | 0.0 |
| q20 | all_on | 3.0 | 0.0 | 0.0 |
| q15 | dynamic_only | 0.0 | 0.0 | 0.0 |
| q15 | all_on | 0.0 | 0.0 | 0.0 |

## Why Q17 Improves

This is more than eliminating repeated hash evaluation. The completed q17
predicate has fifteen IN-list branches and one bounds-only branch.
DataFusion's default per-partition IN-list limit is 150 distinct values;
one build partition exceeds it. Its serialized predicate retains broad
min/max bounds instead of an exact membership test.

That partition belongs to task 1, local partition 3, global partition 7.
With the old OR merge, its bounds also admit rows from global buckets 3,
11, and 15, because all four buckets have the same local remainder.

```text
Before: OR together each task's CASE(hash % 4)

global bucket:       3       7       11      15
local remainder:     3       3        3       3
task 1's broad F3:   can admit rows from all four buckets

After: one CASE(hash % 16)

global bucket:       3       7       11      15
selected predicate: exact   broad   exact   exact
                    IN      bounds  IN      IN
```

The global CASE evaluates only the owning bucket's predicate, retaining
the exact IN-list filters for buckets 3, 11, and 15. This proves a source
of improved predicate selectivity independently of update-arrival timing.
The actual row counts still depend on when scans receive the update.

See `predicate-routing.json` for the branch classification extracted from
the first measured q17 dynamic-only plans for both binaries.

## Other Queries

- **q18:** Global routing reduces filter evaluation cost. Its selective
  order-key filters reduce rows entering the remaining joins and shuffles.
  More rows are filtered in these runs, but timing alone can explain part
  of that difference; it is not proof of stronger final predicates.
- **q20:** Most large-scan predicates are broad bounds. The extra filtering
  is small, while filter compute increases. A plausible explanation is
  that a 16-branch CASE costs more than an OR that often short-circuits on
  an already-permissive four-branch CASE. DataFusion supports that OR
  short-circuit, but no function-level CPU profile was collected here.
- **q15:** Both joins use CollectLeft. No global hash merge occurs, and
  the post-scan supplier-key predicate remains nonselective. Timing changes
  here are a control for run-to-run variation, not a benefit of this PoC.

Even after the improvement over the previous implementation, compare the
candidate's dynamic-enabled latency against its own all-off latency before
claiming that dynamic filtering speeds up a query. Improving a slow filter
does not necessarily make it cheaper than omitting that filter altogether.

## Validation and Limits

- The final full dynamic-filter integration run passed all 23 tests.
- Three new SQL cases verify global CASE routing, simplified empty-partition
  predicates, and Parquet row pushdown, comparing results with filters off.
- Clippy passed for the library and dynamic-filter integration target with
  `-D warnings`. Formatting and `git diff --check` passed.
- A union snapshot intermittently reports `empty` instead of a completed
  predicate. It failed once in the full suite, then passed on rerun; an
  isolated repeat failed on run 9 after eight passes. SQL result comparison
  passed before each snapshot failure. The assertion was not weakened or
  rebaselined, and this timing flake remains unresolved.
- These four SF10 queries plus the integration suite are not the entire
  TPC-H/TPC-DS correctness matrix. No whole-suite performance claim is made.

The experiment demonstrates a useful optimization for q17/q18, not a
universal win. Retain an OR fallback for unsupported routing, and consider
predicate shape/selectivity before making global CASE routing unconditional.

## Plans With Metrics

Median-near measured executions. Every execution plan is retained.

| Query | Configuration | Baseline | Global hash |
| --- | --- | --- | --- |
| q17 | all_off | [plan][a-q17-all_off] | [plan][b-q17-all_off] |
| q17 | dynamic_only | [plan][a-q17-dynamic_only] | [plan][b-q17-dynamic_only] |
| q17 | all_on | [plan][a-q17-all_on] | [plan][b-q17-all_on] |
| q18 | all_off | [plan][a-q18-all_off] | [plan][b-q18-all_off] |
| q18 | dynamic_only | [plan][a-q18-dynamic_only] | [plan][b-q18-dynamic_only] |
| q18 | all_on | [plan][a-q18-all_on] | [plan][b-q18-all_on] |
| q20 | all_off | [plan][a-q20-all_off] | [plan][b-q20-all_off] |
| q20 | dynamic_only | [plan][a-q20-dynamic_only] | [plan][b-q20-dynamic_only] |
| q20 | all_on | [plan][a-q20-all_on] | [plan][b-q20-all_on] |
| q15 | all_off | [plan][a-q15-all_off] | [plan][b-q15-all_off] |
| q15 | dynamic_only | [plan][a-q15-dynamic_only] | [plan][b-q15-dynamic_only] |
| q15 | all_on | [plan][a-q15-all_on] | [plan][b-q15-all_on] |

[a-q17-all_off]: baseline-2/plans/q17-all_off-01.txt
[b-q17-all_off]: candidate-1/plans/q17-all_off-05.txt
[a-q17-dynamic_only]: baseline-2/plans/q17-dynamic_only-04.txt
[b-q17-dynamic_only]: candidate-1/plans/q17-dynamic_only-03.txt
[a-q17-all_on]: baseline-2/plans/q17-all_on-03.txt
[b-q17-all_on]: candidate-1/plans/q17-all_on-03.txt
[a-q18-all_off]: baseline-2/plans/q18-all_off-02.txt
[b-q18-all_off]: candidate-1/plans/q18-all_off-05.txt
[a-q18-dynamic_only]: baseline-2/plans/q18-dynamic_only-01.txt
[b-q18-dynamic_only]: candidate-2/plans/q18-dynamic_only-05.txt
[a-q18-all_on]: baseline-2/plans/q18-all_on-03.txt
[b-q18-all_on]: candidate-1/plans/q18-all_on-01.txt
[a-q20-all_off]: baseline-1/plans/q20-all_off-04.txt
[b-q20-all_off]: candidate-2/plans/q20-all_off-04.txt
[a-q20-dynamic_only]: baseline-1/plans/q20-dynamic_only-03.txt
[b-q20-dynamic_only]: candidate-1/plans/q20-dynamic_only-04.txt
[a-q20-all_on]: baseline-2/plans/q20-all_on-04.txt
[b-q20-all_on]: candidate-2/plans/q20-all_on-04.txt
[a-q15-all_off]: baseline-1/plans/q15-all_off-01.txt
[b-q15-all_off]: candidate-2/plans/q15-all_off-01.txt
[a-q15-dynamic_only]: baseline-2/plans/q15-dynamic_only-02.txt
[b-q15-dynamic_only]: candidate-2/plans/q15-dynamic_only-05.txt
[a-q15-all_on]: baseline-1/plans/q15-all_on-02.txt
[b-q15-all_on]: candidate-1/plans/q15-all_on-05.txt

## Raw Data

- [Summary JSON](summary.json), including block means and shuffle bytes.
- [Baseline block 1](baseline-1/samples.json) and [block 2](baseline-2/samples.json).
- [Candidate block 1](candidate-1/samples.json) and [block 2](candidate-2/samples.json).
- [Reproduction driver](run-abba.mjs) and [analysis script](summarize.mjs).

These selected queries cannot establish a full-suite performance improvement.
