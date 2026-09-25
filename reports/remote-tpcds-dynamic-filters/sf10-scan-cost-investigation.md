# Why Some Local Dynamic-Filter Wins Disappear Remotely

Date: 2026-09-25. This investigation uses the completed SF10 measurements;
it adds no benchmark runs. SF100 remains paused.

## Conclusion

The strongest working theory is that dynamic filtering trades downstream
join and shuffle work for expensive predicate preparation and evaluation
at the scans. When filtering does not avoid reading Parquet bytes, those
new costs can outweigh the saved work on the larger remote cluster.

Q27 is the clearest example: its filters remove most output rows, but
read volume stays unchanged. Predicate evaluation and file-opening time
increase substantially, and both remote measurement blocks get slower.
Q25 shows a similar pattern. This is not simply an absence of filtering
or a small improvement hidden by measurement noise.

The metrics identify likely costs, not a proven critical-path breakdown.
A worker CPU profile is needed to establish which functions dominate.

## Comparison And Measurement Rules

- `Control-on` disables dynamic filtering and enables both Parquet row
  pushdown and filter reordering. `A-on` enables all three features.
- `Control-off` disables all three; `A-off` enables only dynamic
  filtering. Statistics and page-index pruning remain enabled.
- A uses the original partitioned-join OR merge. B uses the global-hash
  CASE experiment. Neither inserts post-scan FilterExecs.
- Remote values below are arithmetic means of ten measured executions,
  split across two five-run blocks. Warmups are excluded.
- Operator times are summed across tasks and partitions. They are not
  query wall time, do not necessarily represent CPU time, and cannot be
  added or subtracted to reconstruct the critical path.
- Predicate-evaluation time includes all Parquet row predicates, not
  exclusively dynamic predicates. Adding filters can also change which
  rows reach the other predicates.
- Byte units are decimal. Remote counters are approximate because they
  were parsed from rounded plan displays.

The [main report](sf10-report.md) documents the complete six-case matrix,
environment, measurement schedule, and limitations.

## Q27: Effective Row Filtering, No I/O Reduction

Comparing `Control-on` with `A-on`:

| Metric | Dynamic filters off | On |
| --- | ---: | ---: |
| Query latency | 2,180 ms | 2,393 ms |
| Scan output rows | 86.79 M | 5.70 M |
| Bytes read | 4.56 GB | 4.57 GB |
| Network transfer | 1.52 GB | 140 MB |
| Predicate evaluation, summed | 40 ms | 4,851 ms |
| Join computation, summed | 959 ms | 392 ms |
| File opening, summed | 192 ms | 3,459 ms |
| Metadata loading, summed | 46 ms | 47 ms |
| File-opening count | 966 | 966 |
| Tasks | 96 | 96 |

Plans: [disabled][q27-control], [enabled][q27-a]. The linked plans are
representative executions; table values average all ten executions.

### What The Metrics Establish

The filters work: scans emit about 93% fewer rows, and network transfer
falls about 91%. However, scans still read approximately 4.57 GB.
Filtering avoids downstream processing, not the underlying read volume.

Parquet predicate evaluation increases by about 4.81 seconds of summed
operator time. Join computation falls by about 0.57 seconds. These are
not comparable as serial wall-time contributions, but they establish a
substantial shift in where the query performs work.

The two blocks agree on the direction of the latency change:

| Measurement pass | Control-on | A-on |
| --- | ---: | ---: |
| First A pass | 2,429 ms | 2,520 ms |
| Second A pass | 1,930 ms | 2,267 ms |

Locally, this same configuration improved from 2,438 to 1,012 ms,
or 2.41x. Enabled predicate evaluation totaled 1,178 ms locally versus
4,851 ms remotely, despite similar scan-output selectivity.
See the [local/remote comparison](local-vs-remote.md#q27-a-on).

### What May Explain The Extra Cost

The local plan has 40 tasks; the remote plan has 96. Local workers use
four target partitions each, versus fifteen remotely. More producer
tasks and partitions can make merged OR/CASE predicates larger and
increase repeated predicate preparation. Different CPUs and batching
also affect evaluation cost. These factors were not isolated.

File opening is a second suspect: its summed time rises from 192 to
3,459 ms while metadata loading and opening count remain essentially
unchanged. That is consistent with extra predicate preparation or
scheduling delays during opening, rather than merely slower footer
reads. It does not prove that preparation consumed 3.27 seconds of CPU.

The effect is also present without row pushdown: Q27 `A-off` has about
3,191 ms of opening time versus 197 ms in `Control-off`, while scan
output remains 92.69 M rows in both. Therefore, investigating only the
row-predicate evaluator would miss another likely source of overhead.

## Source Evidence For The Theory

The frozen benchmark uses DataFusion 55.0.0. Its Parquet implementation
contains the following relevant work:

- In `datafusion-datasource-parquet-55.0.0/src/opener/mod.rs`,
  `MetadataLoadedParquetOpen::prepare_filters` adapts predicates to the
  file schema, simplifies them, and builds file-specific statistics and
  page-pruning predicates. The rewrite/simplification starts near line
  1042. `build_stream` also constructs row filters when pushdown is on.
- In `datafusion-datasource-parquet-55.0.0/src/row_filter.rs`,
  `DatafusionArrowPredicate::evaluate`, near line 148, times expression
  evaluation, Boolean-array conversion, and matched/pruned row counting.
  This timer does not include reading predicate columns from storage.
- In that same file, `build_row_filter` orders candidates by estimated
  required column bytes when reordering is enabled. It does not directly
  rank the computational complexity of a merged OR/CASE expression.
- `datafusion-datasource-55.0.0/src/file_stream/metrics.rs` defines opening
  as elapsed time from opening a file until its stream becomes ready.
  The metric includes waiting and must not be interpreted as CPU time.

These code paths make predicate preparation and evaluation concrete
profiling targets. They do not prove that expression size, reordering,
or a particular function caused the observed regression.

## Q25: A Similar Pattern

Comparing `Control-on` with `A-on`:

| Metric | Dynamic filters off | On |
| --- | ---: | ---: |
| Query latency | 1,520 ms | 1,722 ms |
| Scan output rows | 46.18 M | 2.11 M |
| Bytes read | 1.03 GB | 789 MB |
| Network transfer | 1.05 GB | 121 MB |
| Predicate evaluation, summed | 0.8 ms | 1,339 ms |
| Join computation, summed | 4,677 ms | 1,144 ms |
| File opening, summed | 118 ms | 2,141 ms |
| Metadata loading, summed | 35 ms | 37 ms |
| File-opening count | 621 | 621 |

Plans: [disabled][q25-control], [enabled][q25-a].

There is useful pruning, including some read-volume reduction, but the
query still reads about 789 MB. Both enabled blocks lose to their
same-pass controls. Locally, this case improved 2.37x. Repeated opening
work and predicate evaluation are again plausible costs that offset
downstream savings; the exact critical path remains unmeasured.

## Q26: Contrast With A Repeatable Win

With row pushdown and reordering off, Q26 `A-off` improves from
1,054 to 823 ms, or 1.28x, against `Control-off`.

| Metric | Dynamic filters off | On |
| --- | ---: | ---: |
| Total bytes read | 813 MB | 220 MB |
| Catalog-sales output | 14.40 M rows | 3.20 M rows |
| Page-index rows pruned | 0 | 11.20 M |
| Network transfer | 294 MB | 86 MB |

Plans: [disabled][q26-control], [enabled][q26-a].

Both enabled blocks beat both disabled blocks. Here, remote dynamic
filters avoid substantial read volume through page pruning without
paying row-predicate evaluation cost. Opening time still increases,
so that increase alone does not imply a regression.

This supports the tradeoff: avoiding expensive reads can matter more
than discarding already-read rows before a highly parallel join.
It does not establish that every page-pruning query will improve, or
that row pushdown is generally undesirable.

## Not Every Inconclusive Result Has The Same Cause

Q27 `A-on` is slower in both blocks; its `A-off` result is inconclusive.
The report's representative-case label must not be applied to every
configuration of a query.

Q25 `A-off` has a 1.09x mean gain, but changes direction between passes:
1,430 to 1,477 ms in the first, and 1,743 to 1,422 ms in the second.
There is work reduction, but baseline drift prevents a repeated claim.
Q98's `Control-off` block means similarly move from 575 to 903 ms.

Q80 remains less explained: `A-on` cuts scan output from 55.47 M to
6.14 M rows and network transfer from 1.35 GB to 128 MB, yet latency
changes from 1,307 to 1,377 ms. It still reads 1.63 GB. Its metrics show
that saved work did not reduce end-to-end latency; they do not identify
whether scan startup, remaining I/O, scheduling, or reporting dominates.
B records no global-hash merges for Q80, so its result should not be
blamed on the larger CASE experiment.

## Follow-Up To Test The Theory

No additional runs or profiles have been started for this report.

1. Profile Q27 workers with dynamic filtering off/on, keeping Parquet
   settings fixed. Separate per-file predicate adaptation/simplification,
   pruning-predicate construction, and row-predicate evaluation.
2. Capture merged predicate size and producer count. The current remote
   endpoint does not rewrite completed filter snapshots into its plan
   display, so printed `DynamicFilter [ empty ]` is not a reliable
   description of the predicate that executed.
3. Vary producer fanout on the same cluster, with a matched disabled
   control for each setting. Compare predicate costs as well as scan
   selectivity; changing partitions also changes ordinary join execution.
4. Alternate enabled/disabled runs within fresh-worker sessions and
   measure execution separately from planning and report rendering.
   Correlate worker timings to locate the actual critical path.

## Artifacts And Scope

The [numeric summary](summary.json) contains every measured sample and
parsed plan node. Opening and metadata figures here were calculated by
summing the corresponding `DataSourceExec` metrics in each sample, then
averaging those totals across its ten measured runs. File-opening counts
can include separate ranges and must not be read as unique physical files.

See the [full matrix](matrix.md), [main report](sf10-report.md), and
[local comparison](local-vs-remote.md) for supporting results. All 384
local SF10 files match the remote dataset's checksums; dataset identity
is documented in [data-comparison.json](data-comparison.json).

Hardware, storage, fanout, and timing boundaries differ between local
and remote environments. This is a diagnosis of selected queries, not
a whole-suite performance claim or an isolated hardware experiment.
Remote checks compare result row counts, not complete result values.

[q27-control]: plans/tpcds-sf10-q27-Control-on.txt
[q27-a]: plans/tpcds-sf10-q27-A-on.txt
[q25-control]: plans/tpcds-sf10-q25-Control-on.txt
[q25-a]: plans/tpcds-sf10-q25-A-on.txt
[q26-control]: plans/tpcds-sf10-q26-Control-off.txt
[q26-a]: plans/tpcds-sf10-q26-A-off.txt
