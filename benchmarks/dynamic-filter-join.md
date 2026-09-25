# Selective Partitioned Join: Local Benchmark

## Result

Remote dynamic filtering helps this **wide, highly selective synthetic join**
when Parquet row-level pushdown is enabled. Across ten measured iterations,
execution averaged **2,452 ms without dynamic filtering versus 1,435 ms with
it: 1.71x faster, or 41.5% less execution time**.
An independent ten-run confirmation averaged **2,354 ms off versus 1,370 ms
on: 1.72x faster**.

The four-measure version did **not** improve. Widening the workload to sixteen
measures made the avoided decoding and shuffle work outweigh filtering costs.
This is a workload-specific demonstration, not a TPC-H or general performance
claim. No library implementation was changed for this benchmark.

## Setup

- Local machine: 16 physical ARM Neoverse-N1 cores, approximately 61 GiB RAM.
- Library revision: `7caf1ba`, on `js/6-apply-merged-filters`.
- Four separate gRPC worker processes, four Tokio execution threads and four
  target partitions each. The separate coordinator uses two Tokio threads.
  Threads are not CPU-pinned; all processes share the machine and page cache.
- Release build, static planning, four tasks per distributed stage, no broadcast
  joins. Both single-partition hash-join thresholds are zero.
- One warm-up per configuration, followed by ten measured runs per
  configuration. Configuration order rotates and reverses between rounds.
- Execution timing covers `collect`, excluding planning, startup, data
  generation, validation, and post-execution plan reconstruction. Planning
  times are retained separately in `samples.json`.
- Metrics collection and completed dynamic-filter reporting are enabled in
  every configuration. Filter reordering is **false throughout**.
- Every run validates the count and every sum against an independent oracle
  computed directly while generating the records.

## Workload

The dimension has 1,048,576 unique keys. A Boolean `selected` column identifies
1,024 contiguous keys, or 0.0977%. The fact table has 33,554,432 rows, with keys
permuted throughout each file, and either four or sixteen integer measures.
Every key appears 32 times, so the join produces exactly **32,768 rows**.

The query does not contain a fact-key range predicate. Only the dimension's
Boolean filter reveals which keys survive; remote dynamic filtering supplies
the fact-side restriction at runtime.

```sql
SELECT COUNT(*) AS n,
       SUM(f.m0), SUM(f.m1), SUM(f.m2), SUM(f.m3),
       SUM(f.m4), SUM(f.m5), SUM(f.m6), SUM(f.m7),
       SUM(f.m8), SUM(f.m9), SUM(f.m10), SUM(f.m11),
       SUM(f.m12), SUM(f.m13), SUM(f.m14), SUM(f.m15)
FROM dim d
JOIN fact f ON d.k = f.k
WHERE d.selected;
```

There are four dimension files and 32 fact files, written with Snappy
compression and 65,536-row row groups. The scan's reported Parquet bytes are
approximately 1.39 GB for four measures and 4.80 GB for sixteen measures.

```text
Coordinator: final COUNT and SUMs
  |
  +-- Stage 3: four partitioned hash-join tasks, four partitions each
        | build                         | probe
        NetworkShuffle                 NetworkShuffle
        |                              |
        Stage 1: four dimension        Stage 2: four fact scan tasks
        scan/filter tasks              with dynamic-filter consumers

Stage 3 producers --> coordinator: OR completed producer predicates
                      |
                      +--> Stage 2: apply merged filter during scanning
```

The producer and scan consumer are in different stages. This exercises remote
partitioned-join filtering, **not** the recent AND change for TopK/MIN/MAX.

## Wide-Table Results

Ten measured runs per row; one warm-up excluded. All times are milliseconds.

| Dynamic filters | Parquet pushdown | Mean | Median | Min | Max |
|---|---|---:|---:|---:|---:|
| Off | Off | 2,517 | 2,499 | 2,387 | 2,653 |
| On | Off | 2,543 | 2,533 | 2,446 | 2,698 |
| Off | On | 2,452 | 2,452 | 2,348 | 2,522 |
| On | On | 1,435 | 1,431 | 1,344 | 1,561 |

The clean comparison is the last two rows: only dynamic filtering changes.
The first two show that dynamic filtering without row pushdown did not help.

Average fact-side counters for the last two rows:

| Counter | Dynamic off | Dynamic on |
|---|---:|---:|
| Scan output / join probe rows | 33,554,432 | 3,377,563 |
| Parquet pushdown rows pruned | 0 | 30,176,869 |
| Probe shuffle bytes transferred | 2,642,845,818 | 263,818,105 |
| Parquet bytes scanned | 4,804,463,030 | 4,804,463,030 |
| Runtime dynamic row groups pruned | 0 | 0 |
| Coordinator filter updates received | 0 | 6.7 |

### Confirmation

The final runner was rebuilt with additional topology checks and run with
fresh worker processes against the same dataset. Ten measured runs per case:

| Dynamic filters | Parquet pushdown | Mean ms |
|---|---|---:|
| Off | Off | 2,366 |
| On | Off | 2,396 |
| Off | On | 2,354 |
| On | On | 1,370 |

In this run, filtering reduced average probe rows to 2,383,202 and probe
shuffle traffic to 186 MB, versus 33,554,432 rows and 2.64 GB without filtering.
Different arrival timing changes how much scanning occurs before the filter
is available. Both complete matrices are retained; the headline uses the first
run, not a selection of the fastest iterations.

### Why It Helps

- The remote predicate removes about **89.9% of probe rows before shuffling**.
  Completed predicates are visible on the fact scans in the saved plans.
- Shuffle traffic decreases by **90.0%**. Parquet pushdown also allows late
  materialization of the sixteen measure columns for surviving rows.
- This is **not** a row-group-pruning or reduced-file-read result. The data is
  deliberately unclustered by join key, and bytes scanned are unchanged.

The summed row-predicate evaluation time is approximately 6.68 seconds across
parallel scans, not wall time. Filtering is substantial work even in the case
that wins overall. Scanning starts immediately; no sleep, barrier, or artificial
probe delay is used to give filters time to arrive.

## Attempts That Did Not Help

The initial four-measure workload used the same row count, keys and selection.

| Dynamic filters | Parquet pushdown | Mean ms |
|---|---|---:|
| Off | Off | 817 |
| On | Off | 857 |
| Off | On | 803 |
| On | On | 855 |

Although row pushdown pruned 28.96 million rows and reduced shuffle traffic
from 748 MB to 100 MB, predicate CPU cost offset the savings for the narrower
rows. The merged predicate contains an OR of producer-specific hash-routing
CASE expressions. Evaluating it cost approximately 6.07 seconds of accumulated
predicate-evaluation time across parallel scans.

A second ten-run matrix set
`datafusion.optimizer.hash_join_inlist_pushdown_max_distinct_values=0`.
This used remote bounds instead of serialized IN-list membership tests, but
did not remove the routing CASE expressions or produce a clear improvement:
the matched pushdown comparison averaged **829 ms off versus 861 ms on**.

The promising optimization target is therefore the cost of evaluating merged
partition-routing predicates, not assuming that fewer rows always means faster
execution. This experiment did not change that implementation.

## Reproduce

Run from the repository root. The binary starts and stops its own four workers,
chooses unused ports, and generates the dataset on its first run.

```bash
cargo run -p datafusion-distributed-benchmarks \
  --bin dynamic-filter-join --release -- \
  --measures 16 --iterations 10 \
  --data target/dynamic-filter-join/wide-data \
  --output target/dynamic-filter-join/wide-results
```

For the narrower workload, use `--measures 4` with a different data directory.
To repeat the bounds-only experiment, also pass `--inlist-max-values 0`.
Existing data is reused only if its generation parameters match; incomplete
or incompatible directories are rejected rather than overwritten.

## Artifacts

Paths below are relative to the repository root. Raw files live under ignored
`target/`, so `cargo clean` can remove them. The report and runner remain in
`benchmarks/`.

- `target/dynamic-filter-join/results/`: four-measure default matrix.
- `target/dynamic-filter-join/bounds-results/`: four-measure bounds-only matrix.
- `target/dynamic-filter-join/wide-results/`: sixteen-measure default matrix.
- `target/dynamic-filter-join/wide-confirmation/`: independent wide-table repeat.

Each result directory contains `samples.json` with exact timings and selected
numeric metrics, `query.sql`, worker logs, and a full executed plan with metrics
and reconstructed filter predicates for every iteration. Iteration `00` is
the warm-up. New runs also write `settings.txt`.

Example plans:
[wide dynamic off](../target/dynamic-filter-join/wide-results/off_pushdown-08.txt),
[wide dynamic on](../target/dynamic-filter-join/wide-results/on_pushdown-03.txt).

## Validation

All 160 measured executions across the four matrices passed the generated
count-and-sums oracle, as did their warm-ups. The final runner checks for
exactly one partitioned hash join, two direct shuffle inputs, and nonzero
remote update counts whenever dynamic filtering is enabled.

`cargo fmt --all -- --check` and the following scoped lint check pass:

```bash
cargo clippy -p datafusion-distributed-benchmarks \
  --bin dynamic-filter-join -- -D warnings
```
