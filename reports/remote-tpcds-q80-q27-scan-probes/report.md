# Q80: Why The Remote Gain Disappears

TPC-DS SF10, 2026-09-26. This is the consolidated investigation, including
the new 12-worker scan probes and the earlier
[four-worker cross-check](#four-worker-cross-check).

## TLDR

**The filters work, but they turn the critical fact scan into a more
serialized read pipeline without reducing its data bytes.** Q80 goes
from one to five decoder read calls per output-producing file stream.
All these calls happen before that stream's first output batch. The
filtered shuffle then emits all its rows only at EOF.

Measured scan-loop CPU actually falls, so extra scan CPU alone does not
explain the regression. Dependent remote reads are the leading hypothesis,
not an established root cause: read-await time includes scheduling, shuffle
buffering also changes, and no controlled prefetch ablation has been run.

## Results

Same instrumented release binary in both cases. Twenty measurements per
configuration, across two fresh-pod sessions with alternating off/on
order: **40 measured Q80 runs**, plus excluded warmups and smoke runs.
Parquet row pushdown and filter reordering remain on in both cases; only
dynamic filtering is toggled.

| Query | Execution: DF off | Execution: DF on | Change |
| --- | ---: | ---: | ---: |
| Q80 | 839 ms | 981 ms | 17.0% slower |

Both sessions reproduce the execution regression. The paired 95% interval
for execution speedup, defined as off/on, is **0.797-0.920x**. This
describes variation within these two sessions, not arbitrary machines
or days.

First-result latency also worsens: **803 -> 943 ms**. The regression is
not just final reporting.

The full endpoint totals look deceptively similar: **1,550 -> 1,536 ms**.
Enabled plans render about 161 ms faster, masking slower execution. The
tiny endpoint improvement is not evidence of faster query execution.

See [measurements.md](measurements.md) for every phase, session means,
confidence intervals, and links to representative executed plans.

## Critical Scan Evidence

These are means over all 20 runs per case, restricted to `store_sales`.
CPU and await times below are **summed across concurrent streams**, not
additive query wall-time components.
Displayed plan counters are rounded; bytes use decimal GB.

| Metric | DF off | DF on |
| --- | ---: | ---: |
| Output rows | 28.800 M | 0.592 M |
| Decoder data bytes requested | 1.058 GB | 1.058 GB |
| Output-producing file streams | 32 | 32 |
| Decoder read calls | 32 | 160 |
| Read calls per output stream | 1 | 5 |
| Requested byte ranges | 224 | 8,890 |
| Data-read await time, summed | 4.782 s | 9.023 s |
| Scan-poll CPU, summed | 2.801 s | 2.432 s |
| Row-predicate CPU, summed | ~0 s | 0.368 s |
| Mean file-stream first-batch delay | 151 ms | 339 ms |
| Slowest fact-task EOS, mean | 771 ms | 906 ms |

`probe_first_batch_read_calls` equals `probe_range_read_calls` in these
scans: the added read phases all precede first output. Data-read calls
are logical `get_byte_ranges` calls, **not HTTP request counts**. The
object store can coalesce ranges and issue several requests concurrently.

The decoder awaits each requested set before continuing. Row pushdown
introduces dependencies between fetching predicate columns, evaluating
selections, and fetching subsequent columns:

```text
DF off:
  request needed column ranges together
    -> await data -> decode -> output batches

DF on:
  read predicate columns -> evaluate selection
    -> read next columns -> evaluate selection
      -> ... -> read projected columns -> output batches
  [five observed read phases per output-producing stream, on average]
```

Filtering removes 97.9% of the fact-scan output rows, but there is no
corresponding data-byte saving. This scan reports zero page and dynamic
row-group pruning. Total `bytes_scanned`, including other
reads such as metadata, increases slightly rather than decreasing.

The critical-path timing agrees with this diagnosis. Q80's fact branch
finishes about **136 ms later**, while its first result arrives **140 ms
later**. These are differences of run means, not a complete causal time
decomposition or proof that I/O alone caused the regression.

## Shuffle Evidence

The new coalescer counters directly confirm the buffering behavior.
The store-sales stage has 12 tasks hashing into 180 output partitions per task,
giving 2,160 producer/output buckets. Batch size remains 8,192 rows.

| Fact stage | First output at EOF: off | First output at EOF: on |
| --- | ---: | ---: |
| Q80, Stage 6 | 0 / 2,160 buckets | 2,160 / 2,160 buckets |

With filtering, `probe_coalesce_regular_rows=0`: every fact-scan shuffle
row is emitted by the last-sender EOF flush. Without filtering, about
22.1 M rows leave these coalescers before EOF.

This establishes lost streaming overlap, **not an increase in the
coalescer's own holding-time metric**. For example, Q80's mean interval
from first coalescer input to first output falls from 141 ms to 83 ms.
Its input is already arriving later. The scan/read pipeline and the EOF
barrier must be considered together; their aggregate timers cannot simply
be added.

## Where Filtering Helps

- This is not a scan-loop CPU explosion: measured scan-poll CPU falls
  13%, despite row-predicate evaluation. Predicate preparation plus
  reader construction rises only from 5 ms to 66 ms of summed CPU.
- Useful I/O savings occur on a different branch: `catalog_sales` reads
  571 -> 52 MB and prunes 13.76 M rows through the page index. Its enabled
  branch finishes around 630 ms, before `store_sales` at 906 ms.
- Remote updates are applied successfully, averaging 128 applications.
  Reported network bytes fall 1.353 -> 0.128 GB, but the critical fact
  branch still requests the same data in more dependent read phases.

Plans: [off](plans/12w-q80-Control-on.txt),
[on](plans/12w-q80-A-on.txt). Start at **Stage 6** for the critical scan
and its repartition metrics.

## Why Local Differs

The [local confirmation](../local-dynamic-filter-sweep/findings.md)
used warm local files, four localhost gRPC workers, and four execution
threads/partitions per worker on one 16-core ARM machine. It measured
execution without plan rendering. A improved Q80 from **1,742 -> 640 ms**,
or **2.72x**.

The best-supported explanation is that the tradeoff changes: warm local
reads make additional read phases relatively cheap, while avoiding rows
reduces substantial join/shuffle work on the smaller CPU allocation.
On S3, those phases introduce dependent waits without reducing the
critical scan's data volume. Scan output and downstream processing start
later.

This is an inference connecting the measured remote mechanism to the
known local setup. We did **not** rerun the local benchmark with these
probes, nor isolate storage, CPU architecture, and parallelism separately.
The earlier four-worker remote test also retained 15 vCPU per worker;
it was not a hardware match for the 16-core local test.

## Four-Worker Cross-Check

The earlier experiment also tested four remote workers with four target
partitions each. Each pod still had 15 vCPU and 38 GiB, and read the same
SF10 data from S3. It did not reproduce the local machine's CPU allocation
or warm local storage. Parquet row pushdown and reordering stayed on;
only dynamic filtering was toggled.

These are execution-only means from 20 measurements per case across two
fresh-pod sessions, separate from the 40 new Q80 scan-probe runs above:

| Query | DF off | DF on | Change |
| --- | ---: | ---: | ---: |
| Q80 | 1,325.4 ms | 1,510.3 ms | 14.0% slower |

Filtering still reduced output and network traffic:

| Metric | DF off -> on |
| --- | ---: |
| All scan output rows | 55.47 -> 6.14 M |
| All scan bytes | 2.142 -> 1.626 GB |
| Network bytes | 1,022.0 -> 86.3 MB |
| Last store-sales task output EOS | 1,316 -> 1,496 ms |

Q80's store-sales stage finished about 180 ms later, close to its 185 ms
execution regression. Lowering the worker and partition counts therefore
did not restore the local gain, despite substantial row and network
reductions.

This earlier build had task and coordinator timings, but not the new
CPU, decoder read-phase, or coalescer probes. Compare off/on within each
experiment; absolute four-worker versus new 12-worker timings are not a
controlled scaling comparison.

Representative executed plans: [off](plans/4w-q80-Control-on.txt) /
[on](plans/4w-q80-A-on.txt).
The earlier [manifest](raw/prior-timings/manifest.json),
[summary](raw/prior-timings/summary.json), and
[instrumentation patch](raw/prior-timings/instrumentation.patch) are
archived under `raw/prior-timings/`. Its `raw/` link retains every original
run artifact. The old report is archived there for provenance, rather
than maintained as a second report.

## Scope And Reproduction

- Existing 12 dedicated c5n.4xlarge workers: 15 vCPU and 38 GiB per pod,
  15 target partitions each. Existing TPC-DS SF10 S3 dataset, unchanged.
- Dynamic filtering is the only setting toggled. Parquet row pushdown
  and reordering are on in both cases. LZ4, broadcast joins, static task
  planning, union isolation, and all other checked settings are retained.
- A only: no finer-hash B changes or injected post-scan FilterExec.
  Source is `c78f9e124875c6bb25cb63ed16df75af700eb34d`, DataFusion 55.0.0.
  Instrumentation uses isolated copies of pinned dependencies; no
  dependency versions changed and `~/datafusion` was not modified.
- CPU clocks measure synchronous thread work in annotated scopes, not
  process-wide CPU or background HTTP/TLS tasks. Await wall time includes
  I/O and scheduling waits. Decoder, predicate, and scan-poll CPU overlap.
  These probes do not assign every millisecond of query latency to a cause.
- The same probes are present in both cases, but their overhead has not
  been independently calibrated. Compare these cases internally rather
  than treating absolute timings across instrumented builds as an A/B.
- All measured runs returned the expected 100 rows; row counts are not
  full result-equivalence checks. Metric collection's 14 integration
  tests, the remote partitioned-filter test, formatting, and scoped
  Clippy passed. No query errors or worker restarts occurred in either
  measured session.

[manifest.json](manifest.json) records settings, binary hash, dataset
inventory, stable pod identities, and every sample.
[summary.json](summary.json) contains aggregate and per-stage evidence.
The Q80 subset includes **40 measured plans**, four warmups, and two smoke
plans in `raw/plans/`. Four representative Q80 plans are linked above,
covering the new 12-worker runs and the earlier four-worker cross-check.
The original full experiment, including other query results, is retained
unchanged in the measurements, manifests, summaries, and raw artifacts;
this explanation focuses only on Q80.

The [plan and metric definitions](plan.md), [dependency patches](patches/),
[dependency provenance](dependency-provenance.json),
[library instrumentation patch](instrumentation.patch), and
[harness sources](harness/) preserve the experiment. Large raw artifacts
live on instance storage through the report's `raw` symlink.

The deployment was left healthy at 12 workers with fresh default sessions.
No commits or pushes were made. A useful next ablation would vary column
prefetch/read batching and shuffle batch size independently while keeping
the predicates unchanged.

## Timeline Summary

OFF/ON refers to dynamic filtering; Parquet row pushdown and reordering
remain enabled. The execution times are measured averages, but the bars
are schematic, not an actual trace or a to-scale breakdown. Operators
can overlap in execution.

```text
Q80: schematic timeline, not an actual trace
R = read/wait    F = filter    J = downstream shuffle/join work

LOCAL: warm files
time ------------------------------------------------------>

OFF  |RR|JJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJJ|  1,742 ms
 ON  |RFRFRFRFR|JJJJJJ|                                640 ms
      ^ cheap reads
               ^ much less downstream work


REMOTE: S3
time ------------------------------------------------------>

OFF  |RRRRRRRRRRRRRRRR|JJJJJJJJJJ|                      839 ms
 ON  |RRRRRFRRRRRFRRRRRFRRRRRFRRRRR|JJ|                 981 ms
      ^ each read depends on the previous filter
                                    ^ less work,
                                      but starts later
```

**Same critical scan bytes, fewer output rows.** Locally, downstream
savings win; remotely, the evidence points to extra dependent read waits
outweighing those savings.

The five remote read phases are measured. The local timeline illustrates
the proposed explanation; local read phases were not measured with the
same probes. Read waits include I/O and scheduling, not just network
latency, and a read phase is not necessarily one HTTP request.
