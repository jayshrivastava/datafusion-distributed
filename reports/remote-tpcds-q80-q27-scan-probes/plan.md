# Q80 and Q27: scan CPU, reads, and shuffle buffering

## Question

Remote dynamic filters reduce scan output substantially, but do not reproduce
the local latency gains. The preceding experiment found late scan/shuffle
output and little reduction in critical `store_sales` read volume. Its
wall-clock timers could not distinguish CPU work from I/O and scheduling.

This follow-up measures those mechanisms. It does not change filtering,
coalescing, scheduling, data, or query logic.

## Matrix

| Setting | Value |
| --- | --- |
| Dataset | Existing TPC-DS SF10 Parquet in S3 |
| Queries | Q80 and Q27 |
| Workers | 12 dedicated c5n.4xlarge nodes |
| Per worker | 15 CPU, 38 GiB memory, 15 target partitions |
| Control | Dynamic filtering disabled |
| A | Dynamic filtering enabled; original partitioned-join OR merging |
| Both cases | Parquet row pushdown and filter reordering enabled |
| Other settings | Existing bot defaults, verified from the SQL session |
| Sessions | Two fresh-pod sessions; reversed query order |
| Warmups | One discarded run per query/case/session |
| Measured runs | Ten alternating off/on pairs per query/session |
| Total | 80 measured runs, 20 per query/case |

Both cases use the same instrumented release binary. No B/fine-hash variant,
injected post-scan FilterExec, SF100 run, or new dataset generation is included.
Compression remains LZ4; broadcast joins remain enabled; adaptive task counts
and partial reduction remain disabled. Batch size remains 8192.

## Probes

The previous task, coordinator, and request-phase timings are retained.
Additional probes are isolated patches to the pinned DataFusion 55.0.0
physical-plan and Parquet crates. All dependency versions stay unchanged.

- `probe_prepare_filters_{wall,cpu}`: per-file predicate adaptation,
  simplification, and pruning-predicate preparation.
- `probe_build_reader_{wall,cpu}`: synchronous reader construction, including
  page pruning and row-filter construction.
- `probe_row_filter_cpu`: CPU used evaluating Parquet row predicates.
  Compare with existing `row_pushdown_eval_time`, which measures wall time.
- `probe_decode_{wall,cpu}`: synchronous decoder/record-batch-reader calls.
  Includes nested row-filter evaluation; these are not additive categories.
- `probe_scan_poll_{wall,cpu}`: time inside the push-decoder stream's polls.
  Includes decoder and predicate work, but excludes await gaps and downstream
  backpressure. It overlaps the narrower CPU probes.
- `probe_range_read_{calls,ranges,wall,cpu}` and
  `probe_data_bytes_requested`: decoder data requests, excluding metadata.
  Wall time spans the await; CPU counts only the future's synchronous polls.
- `probe_first_batch_read_{calls,wall}`: subset of decoder data requests before
  the file stream emits its first nonempty output batch, including all reads
  for a stream that emits no rows.
- `probe_scan_first_batch_wall`, `probe_scan_streams`, and
  `probe_scan_output_streams`: first-output delay and its denominators.
- `probe_coalesce_{regular,eof}_{rows,batches}`: repartition output produced
  during normal input versus only when the last input sender finishes.
- `probe_coalesce_first_emit_wall`, `probe_coalesce_output_buckets`, and
  `probe_coalesce_first_emit_at_eof`: first-input-to-first-output delay per
  nonempty output bucket and whether that first output awaited EOF.

CPU probes use the OS thread-CPU clock. Guards cannot cross an await; futures
are timed separately on every poll. Background HTTP/TLS/runtime task CPU is
not included. No per-row or per-shuffle-fragment CPU clock calls are added.

Read calls are logical `get_byte_ranges` calls, not physical HTTP requests:
the object store can coalesce ranges, issue retries, or fetch ranges in
parallel. Awaited wall time includes I/O and scheduling delays. Do not label
wall-minus-poll-CPU as pure network latency.

All times in plans aggregate overlapping tasks/partitions. Use critical task
completion timestamps and request execution time for the latency argument;
do not sum aggregate timers into a critical-path decomposition.

## Validation and Evidence

- SQL integration check: probe metrics are nonzero and reach the coordinator.
- Formatting and scoped lint checks on the isolated build.
- Smoke off/on runs: expected result row counts, probe presence, and remote
  update application only in the enabled case.
- Every measured run retains a complete plan with per-task metrics.
- Record binary/source/dependency hashes, effective settings, dataset
  inventory, node shape, pod identities, and restart counts.
- Row counts are sanity checks, not full result-equivalence verification.

The final report will distinguish observed mechanisms from remaining
uncertainty and compare the two fresh sessions for reproducibility. Absolute
latencies from differently instrumented builds are not a controlled A/B.

No commits or pushes. Restore a fresh 12-worker deployment after measurement;
do not change foundation infrastructure or datasets.
