# Remote Global-Hash Dynamic Filter Benchmark

Current execution status and partial results are in [report.md](report.md).

## Sources

- A: commit `c78f9e124875c6bb25cb63ed16df75af700eb34d`.
- B: A plus the existing global-hash predicate merge experiment. Only
  `dynamic_filter_registry.rs`, `coordinator/mod.rs`,
  `query_coordinator.rs`, and `partitioned_dynamic_filter.rs` differ.
- Neither build includes the injected post-scan FilterExec experiment.
- Both retain the same existing incremental sort/aggregate merge behavior.
- Source snapshots, patch hashes, binary hashes, and deployment identities
  are recorded with the raw artifacts. The dirty checkout is not modified.

## Workloads And Hardware

- All 22 TPC-H SF100 queries, q1-q22.
- ClickBench is deferred until TPC-H is complete. Skip it if TPC-H provides
  promising, repeatable gains supported by plan metrics. Otherwise consider
  all 43 ClickBench queries, q0-q42, using `0-100-date32`.
- Existing S3 datasets; no dataset generation or upload.
- 12 workers on distinct c5n.4xlarge nodes, each with 15 CPU and 38 GiB.
- Release x86_64 binaries. Verify automatic target partitions before timing.
- Keep other defaults: LZ4, broadcast joins, static planning, metric and
  dynamic-filter collection enabled, partial reduction disabled.
- No other benchmark may share our worker nodes. After the pass-2
  interruption, the user approved continuing alongside the bot on separate
  nodes. Record the other pods at each checkpoint. Shared S3/network or
  cluster-level effects remain a caveat despite node isolation.

## Matrix

| Case | Build | Dynamic filters | Row pushdown | Reordering |
| --- | --- | --- | --- | --- |
| Control-off | A | Off | Off | Off |
| Control-on | A | Off | On | On |
| A-off | A | On | Off | Off |
| A-on | A | On | On | On |
| B-off | B | On | Off | Off |
| B-on | B | On | On | On |

The master dynamic-filter option controls local and remote filters together.
Parquet row-group pruning retains its default in every case. Disabling row
pushdown is not the same as disabling row-group pruning.

The three matrix options are:

```text
datafusion.optimizer.enable_dynamic_filter_pushdown
datafusion.execution.parquet.pushdown_filters
datafusion.execution.parquet.reorder_filters
```

Every block checks these unchanged settings before running the query:

```text
datafusion.execution.target_partitions = 15
datafusion.execution.parquet.pruning = true
distributed.compression = lz4
distributed.broadcast_joins = true
distributed.dynamic_task_count = false
distributed.partial_reduce = false
distributed.max_tasks_per_stage = 0
distributed.collect_metrics = true
distributed.collect_dynamic_filters = true
```

Worker binary SHA-256 identities:

```text
A: 5614af1613be69b227ed41e7340c933d1e95499768653b98925bbc9c883e1b88
B: 8b0c04dad33f9c439553a0c188504f3babb25c839c49f4ab1b65d4bcccf4d3e2
```

## Schedule

Run build passes A, B, B, A. Each applicable query/case gets one excluded
warmup and five measured executions per pass: two warmups and ten measured
executions total. Rotate case order between passes.

After the interruption in pass 2, prioritize Q17 and Q18 in the remaining
passes. Their per-query configuration order and sample counts are unchanged.
Already validated blocks are retained and skipped on resume.

TPC-H total: 1,320 measured executions and 264 excluded warmups.
Including ClickBench would total 3,900 measured executions and 780 warmups.
Set all three options before recreating tables, because providers retain Parquet
options. Checkpoint each completed query/case. Stop on errors or invalid
configuration, worker restarts, or inconsistent result row counts.

The client caches the unchanged S3 table listing. Table providers are still
recreated for every case, and the six query executions use the benchmark
tool's DataFusionRunner and result format. Only server elapsed time is used.
The stock server starts its timer before physical planning and stops after
execution, metric-plan rewriting, plan rendering, and task counting. These
are not execution-only latencies; the timing boundary is identical in A/B.

## Evidence

Save every executed plan with per-task metrics, elapsed time, task count,
and output row count. Summarize mean, median, spread, and suite totals.
Compare A/B under matching Parquet settings and against matching controls.
Report scan rows/bytes, row-group and row pruning, filter evaluation,
shuffle traffic, spills, tasks, and successful global-hash merges.

For the top queries benefiting from dynamic filters, include a side-by-side
metric table against the matching no-filter control, plus A/B where useful.
Link each table to the underlying plans. Explain which metrics support the
speedup, and explicitly identify unexplained or timing-sensitive differences.

Stock binaries are used for timed runs. If final predicate text is needed,
capture it separately using diagnostic binaries and exclude those timings.
Result row counts are a sanity check, not full value-equivalence validation.

Operational scripts and raw artifacts are under the tools repository at
`.data/remote-global-hash-filters/`. Reports remain in this directory.
