# TPC-H Q18 SF10: Repeatability Result

**All predeclared acceptance criteria passed.**

Dynamic filtering off: **2024.1 ms**; on: **1398.4 ms**.
Mean speedup: **1.45x**, with 56/60 paired executions faster.
Paired bootstrap 95% interval: **1.38-1.51x**.

Same B/global-hash binary on both sides. Parquet row pushdown and
reordering stay enabled; only dynamic filtering is toggled. No injected
post-scan filter is active. All 126 executions, including warmups, return
the same 624 rows and exact values as the saved SF10 reference.

## Independent Worker Sessions

| Session | Off mean ms | On mean ms | Speedup | Faster pairs |
| --- | ---: | ---: | ---: | ---: |
| round-1 | 2020.9 | 1430.3 | 1.41x | 19/20 |
| round-2 | 2036.1 | 1322.9 | 1.54x | 20/20 |
| round-3 | 2015.4 | 1442.1 | 1.40x | 17/20 |

Each session starts fresh worker processes and excludes one warmup per
case. Each measured round contains both settings; order is balanced.

## Latency Spread

| Case | n | Mean ms | Median ms | P90 ms | SD ms | Min ms | Max ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Off | 60 | 2024.1 | 2020.1 | 2060.9 | 35.9 | 1949.9 | 2158.8 |
| On | 60 | 1398.4 | 1294.5 | 1788.3 | 263.1 | 1196.9 | 2327.8 |

## Plan Evidence

Means across the same measured executions, from numeric metrics rather
than rounded display text. Operator times sum concurrent work, not wall time.

| Metric | DF off | DF on |
| --- | ---: | ---: |
| Scan output, million rows | 136.5 | 89.1 |
| Scanned MB | 1227.2 | 1227.2 |
| Parquet rows pruned, millions | 0.0 | 47.3 |
| Dynamic row groups pruned | 0.0 | 0.0 |
| Parquet evaluation, summed ms | 0.0 | 4437.7 |
| Network output, million rows | 106.5 | 45.6 |
| Transferred MB | 836.9 | 240.8 |
| Join compute, summed ms | 13066.0 | 4776.2 |
| Spilled MB | 0.0 | 0.0 |
| Remote filter updates | 0.0 | 20.1 |
| Global-hash merges | 0.0 | 3.0 |

Representative executed plans: [off](round-3/plans/q18-dynamic_off-11.txt),
[on](round-2/plans/q18-dynamic_on-18.txt). Each is closest to its case's pooled median.

Interpretation is in [findings.md](findings.md).

## Reproduce

Build from the recorded commit plus saved patch/untracked sources, or use
the saved release binary. The runner starts and stops its four workers.

```sh
./runner \
  --data /instance_storage/bits_cache/datafusion-benchmark-data/tpch/sf10 \
  --output /tmp/q18-repeat-new \
  --queries q18 --paired-dynamic-filtering --iterations 20
```

Repeat with new output directories and `--reverse-order` on alternate
sessions. All settings, result values, per-execution plans, source hashes,
and timing samples are retained here. Do not overwrite earlier sessions.

## Limits

- One selected query and one machine; this is not a suite-wide claim.
- Warm local SF10 files, not S3 or a cold-cache test.
- Four workers share 16 ARM cores; this is not twelve separate machines.
- Planning/rendering are outside the execution timer; planning is recorded.
- Local and remote dynamic filtering share the master switch.
- Confidence intervals resample pairs within sessions. They do not model
  variability across machines or arbitrary long-term system drift.
- No new SF100 claim: an earlier remote run did not reproduce the gain.

[Predeclared plan](plan.md), [raw summary](summary.json),
[provenance](manifest.json), [harness](source/benchmarks/src/bin/tpch-filter-matrix.rs).
