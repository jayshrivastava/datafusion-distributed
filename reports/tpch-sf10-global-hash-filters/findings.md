## Why Q17 Improves

This is more than eliminating repeated hash evaluation. The completed q17
predicate has fifteen IN-list branches and one bounds-only branch.
DataFusion's default per-partition IN-list limit is 150 distinct values;
one build partition exceeds it. Its serialized predicate retains broad
min/max bounds instead of an exact membership test.

That partition belongs to task 1, local partition 3, global partition 7.
With the old OR merge, its bounds also admit rows from global buckets 3,
11, and 15, because all four buckets have the same local remainder.

```text
Before: OR together each task's CASE(hash % 4)

global bucket:       3       7       11      15
local remainder:     3       3        3       3
task 1's broad F3:   can admit rows from all four buckets

After: one CASE(hash % 16)

global bucket:       3       7       11      15
selected predicate: exact   broad   exact   exact
                    IN      bounds  IN      IN
```

The global CASE evaluates only the owning bucket's predicate, retaining
the exact IN-list filters for buckets 3, 11, and 15. This proves a source
of improved predicate selectivity independently of update-arrival timing.
The actual row counts still depend on when scans receive the update.

See `predicate-routing.json` for the branch classification extracted from
the first measured q17 dynamic-only plans for both binaries.

## Other Queries

- **q18:** Global routing reduces filter evaluation cost. Its selective
  order-key filters reduce rows entering the remaining joins and shuffles.
  More rows are filtered in these runs, but timing alone can explain part
  of that difference; it is not proof of stronger final predicates.
- **q20:** Most large-scan predicates are broad bounds. The extra filtering
  is small, while filter compute increases. A plausible explanation is
  that a 16-branch CASE costs more than an OR that often short-circuits on
  an already-permissive four-branch CASE. DataFusion supports that OR
  short-circuit, but no function-level CPU profile was collected here.
- **q15:** Both joins use CollectLeft. No global hash merge occurs, and
  the post-scan supplier-key predicate remains nonselective. Timing changes
  here are a control for run-to-run variation, not a benefit of this PoC.

Even after the improvement over the previous implementation, compare the
candidate's dynamic-enabled latency against its own all-off latency before
claiming that dynamic filtering speeds up a query. Improving a slow filter
does not necessarily make it cheaper than omitting that filter altogether.

## Validation and Limits

- The final full dynamic-filter integration run passed all 23 tests.
- Three new SQL cases verify global CASE routing, simplified empty-partition
  predicates, and Parquet row pushdown, comparing results with filters off.
- Clippy passed for the library and dynamic-filter integration target with
  `-D warnings`. Formatting and `git diff --check` passed.
- A union snapshot intermittently reports `empty` instead of a completed
  predicate. It failed once in the full suite, then passed on rerun; an
  isolated repeat failed on run 9 after eight passes. SQL result comparison
  passed before each snapshot failure. The assertion was not weakened or
  rebaselined, and this timing flake remains unresolved.
- These four SF10 queries plus the integration suite are not the entire
  TPC-H/TPC-DS correctness matrix. No whole-suite performance claim is made.

The experiment demonstrates a useful optimization for q17/q18, not a
universal win. Retain an OR fallback for unsupported routing, and consider
predicate shape/selectivity before making global CASE routing unconditional.
