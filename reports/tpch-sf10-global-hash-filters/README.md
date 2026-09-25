# Global Hash Dynamic-Filter Experiment

Uncommitted PoC on `js/6-apply-merged-filters`.

## Change

The existing merge ORs task-local predicates, each routing on `hash % P`.
The PoC uses one `CASE hash % (P * T)`, with the local branch number offset
by `task_index * P`. It changes the merged remote predicate only.

The coordinator traces join keys through projections, filters, and grouped
aggregates to a remote `NetworkShuffleExec`. It does not infer global task
ownership from a local partition count alone. Unsupported paths retain the
existing OR merge. Either a build-side proof of key ownership or a probe-side
proof of which task will test a key is sufficient. Isolated union children use
their effective task context.
Merging still waits for all producer tasks to complete and their stage to seal.

For a four-task stage with four partitions per task:

```text
task 0 local 0..3 -> global  0..3
task 1 local 0..3 -> global  4..7
task 2 local 0..3 -> global  8..11
task 3 local 0..3 -> global 12..15
```

Task-local predicates can be simplified when a task has only one nonempty
partition. Those predicates are retained within their owning task's branches.
Normal local CASE expressions are flattened into the global CASE. Unknown
or null-preserving expression shapes are retained inside the global branches.

`dynamic_filter_global_hash_merges` counts successful global merges.

## Comparison

- Baseline: saved release binary from the post-scan-filter experiment.
- Candidate: the same source plus this global-hash merge PoC.
- Both include the experimental post-scan filters and corrected plan display.
- Four localhost worker processes, four threads/target partitions each.
- Existing SF10 Parquet data, static task planning, broadcast joins enabled.
- q17, q18, q20, plus q15 as a CollectLeft-only control.
- Each query runs all-off, dynamic-only, and all-on configurations.
- A/B/B/A blocks: five measured iterations per case per block, plus warmup.
- Ten measured samples per case and binary, two warmups per case and binary.
- Configuration order rotates/reverses within each block.
- No compiler/test workload is run concurrently with timed measurements.
- Every execution retains its executed plan with metrics and exact result rows.

The all-on case enables dynamic filters, Parquet row pushdown, and filter
reordering. Dynamic-only enables dynamic filters and the post-scan fallback,
but not Parquet row pushdown or reordering. Ordinary statistics pruning stays
enabled in all configurations. This is a warm-filesystem-cache local test,
not a network-isolated or full-suite performance claim.

The baseline source and artifacts remain in
`../tpch-sf10-post-scan-filters/`. The SF10 dataset remains at
`/instance_storage/bits_cache/datafusion-benchmark-data/tpch/sf10`.
