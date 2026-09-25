# Local Dynamic-Filter Findings

Completed September 25, 2026. This is a local distributed benchmark, not an
SF100 or remote-cluster result.

## Bottom Line

**There are repeatable, metric-backed wins without injected FilterExecs.**
Every repository TPC-H SF10 query except Q18, every TPC-DS SF10 query, and
all 43 ClickBench queries were screened in all six configurations. The
shortlist then received 20 measurements per enabled configuration and per
disabled control, across two fresh-worker sessions.

- **Best remote-filter example: TPC-DS Q80.** The selected B configuration
  improves 1,796 to 638 ms, or **2.82x**. A independently improves 1,742 to
  640 ms, or **2.72x**. Its useful scan filters cross shuffle boundaries;
  the global-hash PoC is not needed. See [A's plan][ds80a-on].
- **Smaller SQL example with a partitioned producer: TPC-DS Q37.** A improves
  801 to 212 ms with matching settings. Against the faster disabled-filter
  configuration, the gain is **1.70x**, not 3.78x. Both local and remote
  filtering contribute, so this is not a remote-only ablation.
- **Largest local-only example: TPC-DS Q21.** B improves 592 to 136 ms,
  or **4.34x**; A independently achieves **4.10x**. Local date filters let
  Parquet skip inventory data even with row pushdown and reordering off.

Q80 is my first choice for demonstrating remote propagation. Q37 is easier
to explain as SQL, but its large local inventory reduction must be disclosed.
None of these results establishes that the same gain will hold at SF100.

## Top 20 Queries

This combines the 17 confirmed queries with the three strongest remaining
result-matching, metric-backed screening candidates. Rows are ranked by
reported speedup against the matching Parquet control, taking one
configuration per query and preferring confirmation over screening data.

**Rows marked `*` have only three screening measurements per case.** They
are candidates, not confirmed wins; `Screen` means no confirmation interval
is available. All other rows have 20 measurements per case across two
fresh-worker sessions. This is not a 20-run ranking of every screened query.

`A` uses the existing partitioned-join OR merge. `B` uses the global-hash
CASE PoC. Both have dynamic filtering enabled. The `off`/`on` suffix means
Parquet row pushdown and filter reordering are both off/on.

`DF off` uses the same Parquet settings as `DF on`. Confirmed comparisons
also use the same binary; the B screening candidates reuse A's disabled
controls. `Best off` compares against the faster of the two disabled-filter
controls. CI is the paired 95% bootstrap interval for matching-mode speedup.

`Remote DF` indicates a dynamic filter reaching a consumer across a stage
boundary in the enabled query. `Local DF` indicates a consumer sharing its
producer's task. Both may be `Yes`. These describe filter presence, not a
separate attribution of the measured gain to each kind of filter.

Producer types are `Join` (`HashJoinExec`) and `TopK` (`SortExec` with a
fetch limit); `-` means no corresponding relationship. A sort or aggregate
merely appearing in the plan does not count: its filter must reach a
consumer. None of these 20 queries has a MIN/MAX aggregate filter producer.

| # | Query | Case | Remote DF | Remote producers | Local DF | Local producers | DF off ms | DF on ms | Gain | 95% CI | Best off |
| ---: | --- | --- | --- | --- | --- | --- | ---: | ---: | ---: | --- | ---: |
| 1 | TPC-DS Q21 | B-off | No | - | Yes | Join | 591.8 | 136.4 | 4.34x | 4.14-4.55 | 4.34x |
| 2 | TPC-DS Q37 | A-on | Yes | Join, TopK | Yes | Join | 801.3 | 211.9 | 3.78x | 3.70-3.89 | 1.70x |
| 3 | TPC-DS Q80 | B-on | Yes | Join | No | - | 1,796.1 | 637.7 | 2.82x | 2.74-2.90 | 2.82x |
| 4 | TPC-DS Q39 | B-off | No | - | Yes | Join | 1,257.9 | 481.5 | 2.61x | 2.55-2.69 | 2.61x |
| 5 | TPC-DS Q27 | A-on | Yes | Join, TopK | No | - | 2,437.6 | 1,012.0 | 2.41x | 2.35-2.47 | 2.40x |
| 6 | TPC-DS Q25 | A-on | Yes | Join | No | - | 1,364.3 | 575.6 | 2.37x | 2.22-2.53 | 2.35x |
| 7 | TPC-DS Q82 | A-on | Yes | Join, TopK | Yes | Join | 852.6 | 377.5 | 2.26x | 2.19-2.33 | 1.05x |
| 8 | TPC-DS Q17 | B-on | Yes | Join | No | - | 1,165.6 | 569.1 | 2.05x | 1.93-2.16 | 2.04x |
| 9 | ClickBench Q23 | A-on | No | - | Yes | TopK | 2,904.8 | 1,437.3 | 2.02x | 1.94-2.12 | 2.02x |
| 10 | TPC-DS Q26 | B-on | Yes | Join, TopK | No | - | 617.4 | 305.8 | 2.02x | 1.98-2.06 | 1.99x |
| 11 | TPC-DS Q98 | B-on | No | - | Yes | Join | 465.2 | 236.2 | 1.97x | 1.82-2.12 | 1.85x |
| 12 | TPC-DS Q18 | A-off | Yes | Join | Yes | Join | 809.5 | 416.2 | 1.94x | 1.89-2.00 | 1.94x |
| 13 | TPC-DS Q66 | A-on | No | - | Yes | Join | 420.6 | 232.5 | 1.81x | 1.71-1.90 | 1.81x |
| 14 | TPC-DS Q29* | B-on | Yes | Join | No | - | 1,169.7 | 692.9 | 1.69x | Screen | 1.69x |
| 15 | TPC-DS Q24* | B-on | Yes | Join | No | - | 1,628.0 | 974.2 | 1.67x | Screen | 1.67x |
| 16 | TPC-DS Q20 | B-off | No | - | Yes | Join | 285.5 | 172.4 | 1.66x | 1.55-1.78 | 1.66x |
| 17 | TPC-DS Q49* | A-on | Yes | Join | No | - | 928.5 | 562.7 | 1.65x | Screen | 1.40x |
| 18 | TPC-DS Q40 | B-on | Yes | Join | No | - | 504.1 | 372.2 | 1.35x | 1.17-1.60 | 1.33x |
| 19 | ClickBench Q26 | B-off | No | - | Yes | TopK | 237.3 | 184.8 | 1.28x | 1.24-1.34 | 1.28x |
| 20 | TPC-H Q7 | A-on | Yes | Join | No | - | 1,478.5 | 1,179.5 | 1.25x | 1.23-1.28 | 1.19x |

Q37 and Q82 have remote TopK consumers whose predicates remain `true` in
the saved plans. They are included as relationships, not credited with the
measured pruning; the useful predicates in those examples come from joins.

The first ten won **20/20 matching-mode pairs**, had faster means in both
sessions, matched disabled-control results, and showed actual work
reduction. Their intervals against the best disabled control also exclude
1.0. Two cautions:

- **Q82 is a weak practical win:** its best-disabled interval is only
  **1.004-1.091x**. It meets the predefined rule, but is not a strong demo.
- **Q17 returns zero rows in every configuration.** Its work reduction is
  real, but a nonempty example is preferable for a presentation.

Selecting B here does not establish that B beats A. The comparison being
qualified is dynamic filtering versus disabled filtering. In particular,
Q21, Q39, and Q80 report no global-hash merges even when running B.

## Plan Evidence

Unless explicitly described as a single-task example, counters below are
means across the corresponding table entry: 20 measured runs for confirmed
queries, three for the starred screening candidates. M and GB/MB use
decimal units. Join and network counters sum operator work across the plan,
not unique source rows. Operator timings are summed elapsed measurements,
not query wall time.

The linked plans are unmodified, median-latency examples with per-task
metrics. [evidence.json](evidence.json) records the confirmed plans' original
paths and per-operator means; [summary.json](summary.json) retains screening
and confirmation timings and metrics. Final displayed predicates alone are
not evidence that they arrived early enough to help; the row and byte
counters are.

### 1. TPC-DS Q21: Local Inventory Pruning

Plans: [disabled][ds21-off], [enabled][ds21-on].

- CollectLeft joins and the inventory consumer share a stage. Inventory
  output drops **133.11M -> 4.80M rows**, with scanned bytes
  **458.14 -> 21.30 MB**. The date predicate is visible at the scan.
- In the enabled plan's inventory task 0, statistics retain **4 of 33 row
  groups**, versus all 33 disabled; page-index pruning retains **1.20M of
  4.19M rows**. This works with Parquet row pushdown off.
- There are **zero remote updates**. A's independent **4.10x** confirmation
  shows that this is not a benefit requiring the global-hash change.

### 2. TPC-DS Q37: Local And Remote Join Filters

Plans: [disabled][ds37-off], [enabled][ds37-on].

- Stage 3's local item/date filters cut inventory output from
  **50.66M rows to 74**. This accounts for substantial local savings.
- Stage 5's partitioned LeftSemi join sends its item-key filter to the
  `catalog_sales` scan in stage 4, across a shuffle. That scan emits
  **14.40M -> 4.05M rows** on average, with **10.35M row-pushdown rejects**.
- Network bytes fall **20.25 -> 5.64 MB**; the query averages 10.8 remote
  updates and still returns the same two rows. Against the best disabled
  baseline, **360.6 -> 211.9 ms** is the appropriate practical comparison.

### 3. TPC-DS Q80: Remote Filters Before Three Shuffles

Plans: [disabled][ds80-off], [enabled][ds80-on].

- CollectLeft date/dimension joins in stages **7, 14, and 21** send filters
  to the sales scans in stages **6, 13, and 20**. Those consumers are remote
  across shuffles. The partitioned Right joins are not the source of the
  useful sold-date predicates; the inner joins above them are.
- Store, catalog, and web sales output respectively drops
  **28.80M -> 0.592M**, **14.40M -> 0.321M**, and **7.20M -> 0.228M rows**.
  Summed join inputs fall **108.19M -> 9.64M rows**.
- Network bytes fall **1.022 GB -> 87.75 MB**, with 79.4 remote updates per
  query. A also confirms the gain at **2.72x**; B records **zero**
  global-hash merges. The result has 100 rows in both configurations.

One branch of Q80 illustrates the mechanism. Tree edges show plan children;
the arrows show filter messages. Query data flows upward through the tree.

```text
Stage 7: CollectLeft date join --produced filter--> Coordinator
  `-- Partitioned Right join                           |
      +-- Shuffle: returns (stage 5)                   |
      `-- Shuffle: sales (stage 6)                     |
          `-- Parquet scan <-----applied filter--------+
```

### 4. TPC-DS Q39: Local Filters On Both Inventory Branches

Plans: [disabled][ds39-off], [enabled][ds39-on].

- The two inventory scans each start at **133.11M output rows**. Local
  date predicates reduce them to **2.240M and 2.376M**, respectively.
- Their combined scanned bytes drop **916.27 -> 29.33 MB**. Summed join
  inputs across the query drop **799.55M -> 14.74M rows**.
- Network output stays **4.97M rows** and there are zero remote updates.
  This is local scan/join savings, not reduced shuffle traffic or a remote
  propagation win. Parquet row pushdown and reordering are off.

### 5. TPC-DS Q27: Remote Filtering Of Repeated Fact Scans

Plans: [disabled][ds27-off], [enabled][ds27-on].

- The three UNION branches scan `store_sales` separately. Date, store,
  and demographic join predicates reach those scans across shuffles;
  their combined output falls **86.40M -> 5.37M rows**.
- Network bytes fall **1.213 GB -> 80.81 MB**, and summed join inputs
  fall **89.20M -> 6.91M rows**. The existing A implementation suffices.
- Total scanned bytes do **not** improve: **4.56 -> 4.57 GB**. This is
  evidence of useful row filtering and downstream work reduction even
  without an I/O win. No post-scan FilterExec was injected.

### 6. TPC-DS Q25: Remote Filters Across Multiple Boundaries

Plans: [disabled][ds25-off], [enabled][ds25-on].

- Stage 10's date filters reach `catalog_sales`, `store_returns`, and
  `store_sales` below up to two shuffle boundaries. Store-sales output
  drops **28.80M -> 0.253M rows**; returns drop **2.88M -> 0.259M**.
- Across all scans, output falls **46.18M -> 4.86M rows**. Network bytes
  fall **739.57 -> 71.81 MB**, and join inputs fall **48.58M -> 5.19M**.
- Summed join compute time falls **3.66 -> 0.50 seconds**. The query
  returns the same single row, with 45.1 remote updates per execution.

### 7. TPC-DS Q82: Real Filtering, Modest Best-Baseline Gain

Plans: [disabled][ds82-off], [enabled][ds82-on].

- Local item/date filters reduce inventory output **50.66M -> 230 rows**.
  A remote partitioned LeftSemi filter reduces stage 4's `store_sales`
  output **28.80M -> 1.36M rows**.
- Network bytes fall **62.20 -> 2.92 MB**, but summed row-predicate
  evaluation time rises **0.144 -> 1.771 seconds**. Reduced rows are not
  free; the final predicate contains the merged remote item-key filters.
- The best disabled configuration takes **395.2 ms**, versus **377.5 ms**
  enabled. Do not present the matching-mode **2.26x** as the advantage
  over an otherwise well-configured baseline.

### 8. TPC-DS Q17: Remote Filtering With An Empty Result

Plans: [disabled][ds17-off], [enabled][ds17-on].

- Remote date and partitioned-join filters reach all three fact scans.
  B's `% 16` hash CASE predicates are visible in the consumer plans;
  the registry records two global-hash merges per execution.
- Scan output falls **46.18M -> 5.35M rows**; network bytes fall
  **639.75 -> 71.24 MB**; join inputs fall **48.58M -> 5.72M rows**.
- All enabled and disabled executions return **zero rows**. Keep the
  measured win, but use Q80 or Q37 for a nonempty example. These results
  do not independently establish that the global-hash change is necessary.

### 9. ClickBench Q23: Local TopK Filtering

Plans: [disabled][cb23-off], [enabled][cb23-on].

- A worker-local SortExec(TopK) supplies an `EventTime` bound to its
  Parquet scan, alongside the static `URL LIKE '%google%'` predicate.
  There are **zero remote updates**.
- Scanned bytes fall **38.13 -> 11.21 GB**, and scan output falls
  **15,911 -> 4,947 rows**. Summed sort compute falls **519 -> 123 ms**;
  the same ten result rows are returned.
- In the representative enabled plan, task 0's page-index counter retains
  **3.14M of 16.00M rows**. Earlier pruning explains why total row-pushdown
  rejects can decrease while the query becomes faster.

### 10. TPC-DS Q26: Remote Catalog-Sales Filtering

Plans: [disabled][ds26-off], [enabled][ds26-on].

- The stage 6 joins send date/demographic predicates to stage 5's
  `catalog_sales` scan across a shuffle. B's global hash CASE is present,
  with one global-hash merge per execution.
- Catalog-sales output falls **14.40M -> 2.86M rows**, and its scanned
  bytes fall **789.87 -> 209.33 MB**. Network bytes fall
  **219.72 -> 47.02 MB**.
- The displayed TopK filter on `item` does not reduce that scan's output:
  it remains **102,000 rows**. The useful evidence is the fact-scan and
  shuffle reduction, not simply the presence of a dynamic predicate.

## Evidence For Entries 11-20

The seven additional confirmed queries also passed the matching-mode and
best-disabled criteria. The three starred candidates have matching results
and reduced work in the screen, but still need fresh-worker confirmation.

- **11. TPC-DS Q98:** local store-sales filters reduce total scan output
  **28.83M -> 0.109M rows** and join inputs **37.51M -> 0.280M**.
  There are no remote updates. Plans: [disabled][ds98-off], [enabled][ds98-on].
- **12. TPC-DS Q18:** remote filters reach the customer/catalog-sales
  scans; a local join filters the item scan. Scanned bytes fall
  **1.092 GB -> 297.44 MB**, and network bytes **285.06 -> 69.56 MB**.
  Plans: [disabled][ds18-off], [enabled][ds18-on].
- **13. TPC-DS Q66:** local date/time/dimension filters reach both sales
  scans. Total scan output falls **21.66M -> 0.223M rows** and join inputs
  **49.29M -> 0.892M**. There are no remote updates.
  Plans: [disabled][ds66-off], [enabled][ds66-on].
- **14. TPC-DS Q29*:** remote filters reach all three fact scans. Total
  scan output falls **46.18M -> 10.98M rows** and network bytes
  **639.68 -> 112.34 MB** in the three-run screen. Results match at three
  rows. Plans: [disabled][ds29-off], [enabled][ds29-on].
- **15. TPC-DS Q24*:** remote filters reach both store-sales branches.
  Total scan output falls **64.96M -> 11.59M rows** and network bytes
  **899.40 -> 139.63 MB** in the screen. Scanned bytes do not improve;
  this is downstream work reduction. Results match at seven rows.
  Plans: [disabled][ds24-off], [enabled][ds24-on].
- **16. TPC-DS Q20:** local date/item filters reduce scanned bytes
  **286.16 -> 39.11 MB** and join inputs **18.82M -> 0.953M rows**, with
  Parquet row pushdown off. There are no remote updates.
  Plans: [disabled][ds20-off], [enabled][ds20-on].
- **17. TPC-DS Q49*:** remote filters reach the web/catalog/store sales
  scans. Total scan output falls **15.24M -> 0.535M rows** and network
  bytes **195.84 -> 7.58 MB** in the screen. Results match at 41 rows.
  Plans: [disabled][ds49-off], [enabled][ds49-on].
- **18. TPC-DS Q40:** the remote catalog-sales filter reduces total scan
  output **15.85M -> 9.01M rows** and network bytes **170.75 -> 96.42 MB**.
  Plans: [disabled][ds40-off], [enabled][ds40-on].
- **19. ClickBench Q26:** a local TopK filter reduces scanned bytes
  **1.622 GB -> 755.99 MB** and scan output **99.57M -> 38.58M rows**.
  There are no remote updates. Plans: [disabled][cb26-off], [enabled][cb26-on].
- **20. TPC-H Q7:** remote join filters cut network bytes
  **839.68 -> 394.14 MB** and join inputs **90.98M -> 39.35M rows**.
  Total scan output improves only **34.83M -> 34.31M rows**; the larger
  reduction occurs downstream. Plans: [disabled][h7-off], [enabled][h7-on].

TPC-DS Q18 is not the excluded TPC-H Q18. Q40 is notably variable: it won
16/20 pairs and its matching-mode interval is 1.17-1.60x. Its screening
gain was larger; the confirmed number above includes the slower runs.

## Whole-Suite Context

Selected winners do not imply that enabling everything helps every query.
Below are sums of the three-run screening means, in seconds, using only
the same completed, result-matching queries across all six cases.

| Suite | Queries | Ctrl-off | A-off | B-off | Ctrl-on | A-on | B-on |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| TPC-H | 21 | 17.17 | 17.69 | 18.34 | 18.32 | 23.02 | 24.87 |
| TPC-DS | 98 | 86.20 | 85.62 | 85.56 | 88.44 | 77.87 | 76.54 |
| ClickBench | 35 | 38.24 | 38.35 | 38.50 | 36.75 | 35.16 | 35.25 |

TPC-H regresses overall, particularly with Parquet row pushdown enabled.
TPC-DS has a broader positive result with row pushdown enabled. ClickBench's
comparable subset improves only modestly overall. These are screening
totals, not independently confirmed whole-suite claims. See the complete
[six-case matrix](matrix.md), including regressions and excluded queries.

## Coverage And Limits

- **163 queries, 978 configurations attempted:** 21 TPC-H, 99 TPC-DS,
  and 43 ClickBench. There are **2,928 measured screening executions**.
- **976 configurations completed.** Both disabled TPC-DS Q72 controls
  timed out at 180 seconds during warmup. Enabled Q72 runs completed, but
  without a completed control result there is no qualified speedup.
- Eight ClickBench queries produced different row sets, including between
  repeated disabled controls: **Q17, Q24, Q31, Q32, Q38, Q39, Q40, Q41**.
  Their LIMIT/OFFSET ordering is absent or nonunique. This is consistent
  with nondeterministic selection, not by itself a dynamic-filter bug.
  No tie-aware equivalence was established; they are excluded from wins
  and comparable-suite totals. All raw results remain available.
- Confirmation covered **19 enabled configurations across 17 queries**,
  with **1,140 measured executions** plus 114 excluded warmups. All 38
  process blocks exited successfully, with no query or display errors.
- Across the 1,254 confirmation results including warmups, 1,190 matched
  exactly and 64 matched with the predefined floating-point tolerance.
  Validation compares typed row multisets, not output order. Non-float
  values are exact; floats allow relative tolerance `1e-12`.
- Screening blocks `screen-tpcds-q72-A` and `screen-tpcds-q95-B` were
  recovered from complete artifacts after interrupted orchestration.
  Their process exit status was not observed and is recorded as unknown
  in the manifest. No confirmation block needed this recovery.

The machine has 16 Neoverse-N1 cores and about 61.4 GiB RAM. Runs used
release binaries, four separate localhost gRPC workers, four target
partitions and four Tokio threads per worker, and two coordinator threads.
Static planning, broadcast joins, union isolation, LZ4 transport, metrics,
and normal Parquet statistics pruning were enabled throughout.

TPC-H SF10 was reused. TPC-DS SF10 was generated with 16 files per table.
ClickBench contains all 100 partitions and 99,997,497 rows, with Date32
`EventDate`; its generated uncompressed layout need not match the remote
benchmark dataset's encoding. This measures warm local workloads, without
cache eviction or simultaneous builds, data preparation, or benchmarks.

Timing covers execution and draining results; planning, result formatting,
plan rendering, and artifact writes are outside the latency measurement.
Each confirmation has two fresh-worker sessions of ten measurements plus
one warmup per case, with reversed initial ordering in the second session.
Intervals use 20,000 paired bootstrap resamples stratified by session.
These are per-comparison intervals, not multiplicity-adjusted guarantees.

The JSON metric exporter does not preserve composite `PruningMetrics` or
`Ratio` values. Their zero placeholders are not evidence of zero pruning.
The native text plans preserve statistics and page-index counters. Also,
`row_groups_pruned_dynamic_filter=0` does not exclude pruning through
statistics or page indexes, as Q21 and ClickBench Q23 demonstrate.

See [plan.md](plan.md) for settings and qualification rules,
[confirmation-selection.md](confirmation-selection.md) for the fixed
shortlist, and [manifest.json](manifest.json) for blocks, binary hashes,
and source provenance. A is `c78f9e124875c6bb25cb63ed16df75af700eb34d`;
B adds the frozen [global-hash patch](B.patch). Neither includes the
injected post-scan FilterExec experiment. Both retain the existing AND
merge for incremental sort/aggregate filters.

## Reproduce Q80

These commands use the saved A release binary and existing SF10 dataset.
They create fresh workers and new output directories, without overwriting
the recorded confirmation samples. Each invocation writes raw results and
executed plans with per-task metrics.

```bash
REPORT=/home/bits/datafusion-distributed/reports/local-dynamic-filter-sweep
DATA=/instance_storage/bits_cache/datafusion-benchmark-data/tpcds/sf10
RUN=$(date -u +%Y%m%dT%H%M%S)

"$REPORT/runner-A" --suite tpcds --queries q80 --data "$DATA" \
  --output "$REPORT/runs/repeat-q80-$RUN-1" \
  --cases Control-on,A-on,Control-off --iterations 10

"$REPORT/runner-A" --suite tpcds --queries q80 --data "$DATA" \
  --output "$REPORT/runs/repeat-q80-$RUN-2" \
  --cases Control-on,A-on,Control-off --iterations 10 --reverse-order
```

Replace `q80` with `q37` for the smaller SQL example. These standalone
reruns do not automatically change the saved manifest or findings.

[ds21-off]: runs/selected/tpcds-q21-B-off-control.txt
[ds21-on]: runs/selected/tpcds-q21-B-off-enabled.txt
[ds37-off]: runs/selected/tpcds-q37-A-on-control.txt
[ds37-on]: runs/selected/tpcds-q37-A-on-enabled.txt
[ds80-off]: runs/selected/tpcds-q80-B-on-control.txt
[ds80-on]: runs/selected/tpcds-q80-B-on-enabled.txt
[ds80a-on]: runs/selected/tpcds-q80-A-on-enabled.txt
[ds39-off]: runs/selected/tpcds-q39-B-off-control.txt
[ds39-on]: runs/selected/tpcds-q39-B-off-enabled.txt
[ds27-off]: runs/selected/tpcds-q27-A-on-control.txt
[ds27-on]: runs/selected/tpcds-q27-A-on-enabled.txt
[ds25-off]: runs/selected/tpcds-q25-A-on-control.txt
[ds25-on]: runs/selected/tpcds-q25-A-on-enabled.txt
[ds82-off]: runs/selected/tpcds-q82-A-on-control.txt
[ds82-on]: runs/selected/tpcds-q82-A-on-enabled.txt
[ds17-off]: runs/selected/tpcds-q17-B-on-control.txt
[ds17-on]: runs/selected/tpcds-q17-B-on-enabled.txt
[cb23-off]: runs/selected/clickbench-q23-A-on-control.txt
[cb23-on]: runs/selected/clickbench-q23-A-on-enabled.txt
[ds26-off]: runs/selected/tpcds-q26-B-on-control.txt
[ds26-on]: runs/selected/tpcds-q26-B-on-enabled.txt
[ds98-off]: runs/selected/tpcds-q98-B-on-control.txt
[ds98-on]: runs/selected/tpcds-q98-B-on-enabled.txt
[ds18-off]: runs/selected/tpcds-q18-A-off-control.txt
[ds18-on]: runs/selected/tpcds-q18-A-off-enabled.txt
[ds66-off]: runs/selected/tpcds-q66-A-on-control.txt
[ds66-on]: runs/selected/tpcds-q66-A-on-enabled.txt
[ds29-off]: runs/screen-tpcds-q29-A/plans/q29-Control-on-03-s0.txt
[ds29-on]: runs/screen-tpcds-q29-B/plans/q29-B-on-02-s0.txt
[ds24-off]: runs/screen-tpcds-q24-A/plans/q24-Control-on-01-s0.txt
[ds24-on]: runs/screen-tpcds-q24-B/plans/q24-B-on-02-s0.txt
[ds20-off]: runs/selected/tpcds-q20-B-off-control.txt
[ds20-on]: runs/selected/tpcds-q20-B-off-enabled.txt
[ds49-off]: runs/screen-tpcds-q49-A/plans/q49-Control-on-01-s0.txt
[ds49-on]: runs/screen-tpcds-q49-A/plans/q49-A-on-03-s0.txt
[ds40-off]: runs/selected/tpcds-q40-B-on-control.txt
[ds40-on]: runs/selected/tpcds-q40-B-on-enabled.txt
[cb26-off]: runs/selected/clickbench-q26-B-off-control.txt
[cb26-on]: runs/selected/clickbench-q26-B-off-enabled.txt
[h7-off]: runs/selected/tpch-q7-A-on-control.txt
[h7-on]: runs/selected/tpch-q7-A-on-enabled.txt
