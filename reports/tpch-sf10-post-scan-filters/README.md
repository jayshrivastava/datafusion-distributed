# Post-Scan Dynamic Filter Experiment

This experiment starts from the SF10 matrix in
`../tpch-sf10-local-filter-matrix/` and keeps those results unchanged.

The uncommitted changes in `js/6-apply-merged-filters` do two things:

- Show completed dynamic predicates on intermediate `FilterExec` nodes,
  separately for each task. Preserve the original plan and metric positions.
- Add a row-level `FilterExec` above Parquet scans when dynamic predicates
  were accepted only for pruning, with Parquet row pushdown disabled.

The second change is experimental. It trades predicate evaluation and batch
filtering costs for potentially less shuffle and downstream processing.
It does not avoid decoding rows or reading pages by itself.

## Scope

The fallback reuses dynamic-expression state and projects column references
into the scan output schema. It copies only top-level dynamic conjuncts,
not arbitrary descendants of OR expressions. Predicates needing columns
removed by projection are skipped, as are scans with an embedded limit.
Parquet scans already doing row pushdown are left unchanged.

## Matrix

- Queries: q17, q18, q20, q15; existing SF10 data.
- Four localhost worker processes, four threads and target partitions each.
- Release mode; one warm-up and ten measured runs per case.
- Cases: all three flags off; dynamic filtering only; all three flags on.
- The other flags are Parquet pushdown and filter reordering.
- Every result is compared with the all-off result.
- Every run retains its executed plan and metrics.

The old release binary is retained as `baseline-runner` for reproducibility.
The runner also records dynamic post-scan filter input and output rows;
scan output alone no longer describes the rows sent downstream.
