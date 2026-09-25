# Remote TPC-DS Dynamic-Filter Matrix

This is a separate follow-up to the local sweep. Results here must not be
mixed into the local measurements.

## Queries And Cases

Take the first ten TPC-DS queries in the local ranked table, excluding
ClickBench: Q21, Q37, Q80, Q39, Q27, Q25, Q82, Q17, Q26, and Q98.
Run each at SF10 and SF100. This is 120 query/configuration entries.

| Case | Build | Dynamic filters | Parquet row pushdown | Reordering |
| --- | --- | --- | --- | --- |
| Control-off | A | Off | Off | Off |
| Control-on | A | Off | On | On |
| A-off | A | On | Off | Off |
| A-on | A | On | On | On |
| B-off | B | On | Off | Off |
| B-on | B | On | On | On |

The dynamic-filter switch controls local and remote filtering together.
Normal Parquet statistics and page-index pruning retain their defaults.
Set all three switches before recreating table providers for every case.
Verify the effective settings, rather than relying on requested options.

## Sources And Deployment

- A: release build of `c78f9e124875c6bb25cb63ed16df75af700eb34d`.
- B: the same commit plus the frozen global-hash CASE experiment from the
  local sweep. All four changed source files and Cargo.lock match the
  previously published remote binaries' source snapshots.
- Neither build injects post-scan FilterExecs. Both retain the same
  incremental sort/aggregate filter merging behavior.
- Reuse the content-addressed remote artifacts after verifying local
  SHA-256 identities. Do not build from the dirty user checkout.
- Twelve workers on distinct nodes. Record the selected node type, CPU,
  memory, actual target partitions, pod identities, and restart counters.
  The existing dedicated deployment is 12 c5n.4xlarge nodes, with 15 CPU
  and 38 GiB per worker; tool defaults are c5n.2xlarge, 7 CPU, and 17 GiB.
  Worker-shape selection is recorded in the manifest before deployment.
- Leave compression, broadcast joins, static planning, task sizing, and
  memory defaults unchanged. Collect metrics and dynamic-filter reports.
- No foundation changes or teardown. Never share a measured worker node
  with another active benchmark deployment.

## Dataset Preparation

S3 inspection found only TPC-DS SF1. Generate SF10 and SF100 directly into
their empty final S3 prefixes with the existing release `dfbench` generator
and 16 Parquet files per table, matching the local SF10 layout.

Dataset generation is a separate command, not a side effect of benchmarking.
Wait for the generator's existing `_SUCCESS` marker and record the object
inventory before reading a dataset. Never overwrite an existing nonempty
prefix or create a local SF100 copy. Do not overlap generation and timing.

## Schedule And Validation

Use A, B, B, A passes with fresh worker pods before each pass. Each applicable
query/case has one excluded warmup and five measured executions per pass:
ten measured executions and two warmups per entry. Reverse and rotate case
order between passes. Total: 1,200 measured runs and 240 excluded warmups.

The original schedule interleaved both datasets within each pass. After
pass 1 completed both datasets and pass 2 completed SF10, the user asked
to finish SF10 first. The runner stopped between datasets, before any
SF100 B measurement, and resumed with an SF10-only schedule. This retains
the A/B/B/A order and all measured samples for SF10. Finish its 600 measured
executions and report, then pause for review before resuming SF100.

The manifest preserves two operational error entries from this deliberate
handoff. They do not represent failed or interrupted query measurements.
An earlier SSO expiry also stopped between datasets, after pass 1 SF10.

Use the remote tool's DataFusionRunner, dataset discovery, port forwarding,
and result format. Checkpoint every iteration and completed query/case.
Stop on query errors, unexpected configuration, worker restarts, changed
pod identities within a pass, or inconsistent result row counts. Preserve
partial runs and logs; do not average failed or interrupted samples.

The stock remote endpoint reports elapsed time, row count, task count, and
the executed plan. Its timer includes physical planning and plan rendering;
this differs from the local sweep's execution-only timing. Row counts are
a sanity check, not full result-value equivalence validation.

The frozen remote worker calls `rewrite_distributed_plan_with_metrics`,
but does not call `rewrite_distributed_plan_with_dynamic_filters` before
rendering. Consumers can therefore show `DynamicFilter [ empty ]` even
when their runtime filters pruned data. Preserve these returned plans
unchanged and base work-reduction evidence on scan, join, network, and
update counters. Do not treat `empty` as proof that no update was applied.
No worker-display change is being mixed into this performance comparison.

## Reporting

Store everything under this report directory, using a `raw/` symlink into
instance storage for larger artifacts. Operational scripts live in the
dev-tools repository's ignored `.data/remote-tpcds-dynamic-filters/` folder.

Report separate SF10 and SF100 matrices with arithmetic mean, median,
spread, sample count, and failures. Rank dynamic-filter gains against the
matching disabled control, not primarily A versus B. Also compare against
the faster disabled-control Parquet configuration.

Save every executed plan with metrics, plus a representative plan for each
entry. For candidate wins, compare scan output, scanned bytes, statistics
and page pruning, row-pushdown rejects, predicate evaluation, join inputs,
network traffic, spills, tasks, and remote/global-hash update counters.
Classify local/remote producers from the remote plans themselves: stage
placement can differ from the local sweep and between scale factors.

Keep provisional/noisy gains distinct from consistent, metric-supported
improvements. Record all latencies, plan paths, source hashes, settings,
dataset inventory, and deployment checks so the matrix can be reproduced.

For the local-versus-remote SF10 comparison, keep the locally selected case
fixed instead of comparing different winning configurations. All 384 local
Parquet files match the saved S3 object ETags, including multipart ETags;
see `data-comparison.json`. Differences in hardware, partition counts,
storage, and timer scope remain confounders, not individually isolated
causes of a changed speedup.

Before inspecting timings, define a repeated win as at least a 10% mean
improvement, both enabled block means below either disabled block mean,
a within-pass bootstrap lower bound above 1.0, and a visible dynamic
consumer with at least 5% less scan, join, or network work. Bootstrap each
five-run block independently and retain equal block weights. This interval
does not measure day-to-day AWS variability or correct for selection among
multiple queries/configurations.

A's disabled controls are in the same passes as A. B is bracketed by the
two A control passes, not a separate B-disabled measurement. Keep that
limitation explicit when attributing B's gains to dynamic filtering.
