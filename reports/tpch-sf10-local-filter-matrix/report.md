# TPC-H SF10 Local Dynamic-Filtering Matrix

Source: `js/6-apply-merged-filters`, commit `c0e9548765d419503e0decc7ea9faa0bf7c48403`.

## Findings

These runs do not show a general speedup from enabling dynamic filtering.
Q18 with all three options enabled is the only mean-latency improvement.

- **Dynamic filtering alone:** every enabled case receives remote updates,
  but none prunes any row groups. Scan output rows are identical to all-off,
  and bytes scanned are essentially identical. Mean latency is 1-8% worse.
- **Q17:** all-on is 1.83x slower despite removing 62.4 million rows at scans.
  Scan predicate evaluation totals 18.9 seconds across tasks/partitions, not
  wall-clock time. The runtime predicates contain partition-routing CASEs and
  large IN lists. Reduced join/shuffle work does not offset scan-filter cost.
- **Q18:** all-on is 1.11x faster, from 2023.8 to 1824.4 ms. The orders scan
  shrinks from 15 million to 2.89 million rows, and the second lineitem scan
  from 59.99 million to 33.87 million rows on average. The initial lineitem
  aggregation scan remains unfiltered. This is row filtering without dynamic
  row-group pruning, and the reduced join inputs are visible in the metrics.
- **Q20:** all-on is 1.43x slower. Most scan-row savings are associated with
  the existing date/name predicates being evaluated inside Parquet. The
  partsupp scan only shrinks from 8 million to 7.997 million rows, so its
  remote join filters provide little selectivity in this run.
- **Q15:** all-on is 1.41x slower. Its remote scan predicate is
  `l_suppkey >= 1 AND l_suppkey <= 100000 AND true`, covering the entire
  supplier domain. Both lineitem scans emit 2,265,714 rows with pushdown,
  reflecting the static three-month date restriction, not a selective remote
  supplier filter. The local revenue filter reduces the outer join input
  from 100,000 rows to one, but only after the revenue aggregations.

All numbers above use the ten measured iterations, excluding warm-ups.
Result rows match the all-off baseline in every iteration: q17=1, q18=624,
q20=1804, and q15=1. Full result values are retained in `samples.json`.

This matrix does not isolate the effect of reordering from Parquet pushdown,
or remote filtering from local filtering. It therefore cannot attribute all
of the all-on change to one of those mechanisms.

## Setup

- Release build; four separate localhost gRPC workers.
- Four Tokio threads and four target partitions per worker.
- Separate coordinator with two Tokio threads; all processes share 16 ARM cores.
- One unmeasured warm-up per case; ten measured iterations per case.
- Configuration order rotates and reverses between iterations.
- Static task planning, broadcast joins enabled, LZ4 transport compression.
- Distributed metrics and completed dynamic-filter collection enabled in every case.
- Ordinary Parquet statistics pruning remains enabled in every case.
- Execution time measures collecting the query output, including execution-time
  coordination/reporting. SQL/physical planning, view setup/teardown, plan
  formatting, result comparison, and artifact writes are outside this timer.
- Warm-cache benchmark: no OS cache eviction between runs.
- Every result is checked against the all-off warm-up for that query, with
  rows sorted and exact per-cell string comparison.

Data: `/instance_storage/bits_cache/datafusion-benchmark-data/tpch/sf10`.

Generated SF10 locally, 16 files per scalable table, 6.12 GiB total.
No local SF100 Parquet dataset was found or generated. Existing remote
benchmark metadata/results and correctness fixtures were preserved.

## Configurations

| Case | Dynamic filtering | Parquet pushdown | Filter reordering |
| --- | --- | --- | --- |
| all_off | false | false | false |
| dynamic_only | true | false | false |
| all_on | true | true | true |

Keys:

- `datafusion.optimizer.enable_dynamic_filter_pushdown`
- `datafusion.execution.parquet.pushdown_filters`
- `datafusion.execution.parquet.reorder_filters`

## Latencies

Milliseconds, ten measured runs. Speedup is all-off mean / case mean;
values below 1 mean a slowdown. Each plan link is an executed plan with
per-task metrics and reported runtime dynamic filters from a median-near run.

| Query | Case | Mean | Median | Min | Max | Speedup | Plan |
| --- | --- | ---: | ---: | ---: | ---: | ---: | --- |
| q17 | all_off | 1902.4 | 1747.5 | 1683.2 | 2387.3 | 1.00x | [plan](plans/q17-all_off.txt) |
| q17 | dynamic_only | 2045.4 | 1911.0 | 1844.7 | 2546.1 | 0.93x | [plan](plans/q17-dynamic_only.txt) |
| q17 | all_on | 3486.5 | 3429.3 | 3192.9 | 3825.4 | 0.55x | [plan](plans/q17-all_on.txt) |
| q18 | all_off | 2023.8 | 2016.4 | 1960.5 | 2096.7 | 1.00x | [plan](plans/q18-all_off.txt) |
| q18 | dynamic_only | 2052.8 | 2054.9 | 1992.3 | 2115.9 | 0.99x | [plan](plans/q18-dynamic_only.txt) |
| q18 | all_on | 1824.4 | 1807.1 | 1560.3 | 2173.3 | 1.11x | [plan](plans/q18-all_on.txt) |
| q20 | all_off | 528.9 | 515.0 | 484.0 | 615.4 | 1.00x | [plan](plans/q20-all_off.txt) |
| q20 | dynamic_only | 554.6 | 542.8 | 497.1 | 613.0 | 0.95x | [plan](plans/q20-dynamic_only.txt) |
| q20 | all_on | 757.4 | 762.9 | 696.2 | 821.9 | 0.70x | [plan](plans/q20-all_on.txt) |
| q15 | all_off | 431.7 | 427.3 | 410.3 | 475.0 | 1.00x | [plan](plans/q15-all_off.txt) |
| q15 | dynamic_only | 458.0 | 459.7 | 418.2 | 500.7 | 0.94x | [plan](plans/q15-dynamic_only.txt) |
| q15 | all_on | 610.5 | 607.8 | 553.9 | 660.0 | 0.71x | [plan](plans/q15-all_on.txt) |

## Scan and Filter Metrics

Means over the same ten measured iterations. Scan totals sum distributed leaf
scan metrics, including repeated scans of a table. Pushdown-pruned rows include both
static and dynamic predicates; they are not solely remote-filter savings.
Dynamic updates received are coordinator counts, not counts of rows saved.

| Query | Case | Updates | Dynamic RG pruned | Scan output rows | Pushdown rows pruned | MiB scanned |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| q17 | all_off | 0.0 | 0.0 | 121972104.0 | 0.0 | 1524.2 |
| q17 | dynamic_only | 13.0 | 0.0 | 121972104.0 | 0.0 | 1524.2 |
| q17 | all_on | 12.9 | 0.0 | 59582493.9 | 62389610.1 | 1524.2 |
| q18 | all_off | 0.0 | 0.0 | 136472104.0 | 0.0 | 1170.4 |
| q18 | dynamic_only | 19.9 | 0.0 | 136472104.0 | 0.0 | 1170.4 |
| q18 | all_on | 20.0 | 0.0 | 98248908.0 | 38223196.0 | 1170.4 |
| q20 | all_off | 0.0 | 0.0 | 70086077.0 | 0.0 | 944.0 |
| q20 | dynamic_only | 19.4 | 0.0 | 70086077.0 | 0.0 | 944.0 |
| q20 | all_on | 20.9 | 0.0 | 17119520.3 | 52966556.7 | 944.0 |
| q15 | all_off | 0.0 | 0.0 | 120072104.0 | 0.0 | 1525.2 |
| q15 | dynamic_only | 6.5 | 0.0 | 120072104.0 | 0.0 | 1525.2 |
| q15 | all_on | 6.2 | 0.0 | 4631428.0 | 115440676.0 | 1525.2 |

## Interpretation Limits

- Dynamic filtering toggles both local and remote filtering, not remote alone.
- The all-on comparison also changes static-predicate row pushdown and ordering.
- Timings are local shared-host observations, not remote-cluster predictions.
- A final filter in a plan does not prove it arrived early enough to save work.
- This four-query subset does not establish a whole-suite speedup.

## Artifacts and Reproduction

- [Raw timings, results, and per-node metrics](samples.json)
- [Machine-readable summary](summary.json)
- [Source, machine, and dataset metadata](metadata.json)
- [Runner output](run.log)
- [Runner source used for this run](tpch-filter-matrix.rs)
- `plans/` retains every warm-up and measured iteration, plus 12 representative plans.

```bash
cd /home/bits/datafusion-distributed
cargo build -p datafusion-distributed-benchmarks \
  --bin tpch-filter-matrix --release --locked
target/release/tpch-filter-matrix \
  --data /instance_storage/bits_cache/datafusion-benchmark-data/tpch/sf10 \
  --output /path/to/new-results --iterations 10
```
