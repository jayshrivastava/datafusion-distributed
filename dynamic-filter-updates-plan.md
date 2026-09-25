# Incremental Dynamic Filter Updates: Plan and Findings

Status: investigation and implementation plan only. No implementation changes or
new test runs have been made for this work. This document records the latest
decisions, superseding the earlier proposal to wait for every sort/aggregate
producer before forwarding an update.

## Scope and Branches

Implement the TODO in `DynamicFilterMergeMode` for incremental SortExec and
AggregateExec filters, using the existing producer reporting, merging, forwarding,
and worker application infrastructure.

Current stack at the time of inspection:

| Branch | Commit | Responsibility |
| --- | --- | --- |
| `main` | `e2f04db` | Producer discovery and reporting already merged |
| `js/4-merge-dynamic-filters` | `172cecb` | Coordinator merge state and policies |
| `js/5-forward-dynamic-filters` | `12d9331` | Coordinator-to-worker delivery |
| `js/6-apply-merged-filters` | `58d4645` | Worker application and SQL coverage |

Implement bottom-up and restack the children. Preserve the existing one-commit-
per-branch arrangement when preparing the changes for publication. Do not push
as part of this planning work.

Rough production-code estimate: 150-220 handwritten lines changed, excluding
tests, snapshots, generated protobuf code, and potential helper deletion. This
is a scope estimate, not a measured diff or performance claim.

## Settled Design Decisions

- For one expression ID, OR the latest predicate from each producer that has
  reported. Replace each producer's previous predicate; do not OR the entire
  history of updates.
- Valid TopK and MIN/MAX bounds can be forwarded immediately. Do not gate these
  incremental updates on stage sealing or reports from all producer tasks.
- Keep stage sealing and complete-producer coverage for partitioned hash joins
  and the conservative fallback mode. Missing join producers may have matching
  keys that cannot safely be excluded.
- Preserve the CollectLeft fast path: one complete replicated build-side filter
  is sufficient.
- Separate readiness to forward from final completion. An incremental merged
  filter is complete only after all relevant producer stages are sealed and
  every registered producer has completed. Never infer global completion from
  just the producers that have reported so far.
- Suppress unchanged predicate/completion pairs. A completion-only transition
  still needs delivery.
- Do not add an envelope generation field or stale-update rejection protocol.
  Producer observations are ordered; order coordinator merge-and-enqueue work
  under the registry lock as well.
- Preserve existing observation coalescing. "Forward updates" does not promise
  delivery of every intermediate producer generation.

OR-ing an additional producer's looser bound can weaken the merged predicate.
That is safe for independently valid TopK/MIN/MAX bounds: rows discarded by an
earlier bound were already known not to affect the result. Do not assume merged
selectivity increases monotonically just because message generations do.

## js/4: Merge State and Policies

Primary code: [dynamic_filter_registry.rs](src/coordinator/dynamic_filter_registry.rs).

- Add an incremental mode for SortExec and eligible AggregateExec producers.
  Preserve the two completed-only join modes and conservative fallback.
- Retain each producer's latest dynamic-filter snapshot, including its inner
  predicate and completion state. Keep tasks, rather than just stages, as the
  producer identities because distributed unions can produce different task
  subtrees.
- For incremental mode, merge after the first accepted report and recompute on
  subsequent changed reports. Unregistered/unreported producers do not block
  forwarding; reject reports from unknown tasks as today.
- Keep deterministic task ordering when constructing the existing flat OR.
- Store the merged result as a full dynamic-filter expression, preserving the
  original-coordinate metadata and expression ID. Replace its inner predicate
  with the merged predicate and derive its completion flag from the merge mode.
- Readiness does not need sealing, but completion can change when a stage is
  sealed; recompute and forward that completion-only transition when appropriate.
- Keep the potential upstream aggregate NULL-bound issue below separate from
  the stage-sealing decision. Do not silently fix DataFusion or upgrade the
  dependency as part of this work.

## js/5: Protocol-Agnostic Message and Delivery

Use the same public expression representation as `ProducedDynamicFilter`:

```rust
pub struct ApplyDynamicFilter {
    pub expression_id: u64,
    pub expression: MaybeEncoded<Arc<dyn PhysicalExpr>>,
}
```

The expression is the full dynamic filter, not just its current inner predicate.
Completion already lives inside that expression. Do not add separate
`is_complete` or generation fields to this message.

`MaybeEncoded::Encoded` contains bytes. With the existing expression codec,
those bytes serialize a `PhysicalExprNode` containing the `DynamicFilter`
variant, not a bare `PhysicalDynamicFilterNode`. Generated protobuf types remain
internal instead of becoming part of the public transport abstraction.

- Serialize each changed merged snapshot once and retain it as
  `MaybeEncoded::Encoded` for delivery. Do not share a mutable merged expression
  with consumers through the local transport.
- Reset the existing per-expression delivery markers when its merged state
  changes. Send each changed snapshot to remote consumers; send the latest
  snapshot to late-registering consumer channels.
- Continue excluding consumer tasks that also contain the local producer.
- Merge, update delivery bookkeeping, and enqueue under the registry mutex.
  Unbounded-channel enqueue is synchronous; do not perform network awaits under
  the lock. The current implementation collects deliveries under the lock but
  sends after unlocking, which would allow repeated updates to overtake each
  other if left unchanged.
- Rename the gRPC payload to `expression_proto`, preserving its field number,
  and regenerate protobuf code. It now contains the full expression wrapper.
- Pass the task context into outgoing expression encoding so both `Encoded`
  and `Decoded` variants work. Wrap received bytes as `MaybeEncoded::Encoded`.
- If an optimization update cannot be encoded, skip that update without
  interrupting work-unit delivery. Retain existing fail-open behavior for closed
  task channels and invalid runtime updates.

These are revisions to the new, unmerged ApplyDynamicFilter API. No broader
public protocol redesign or dependency upgrade is part of the plan.

## js/6: Apply Repeated Updates and Simplify Discovery

Primary code: [impl_coordinator_channel.rs](src/worker/impl_coordinator_channel.rs)
and [discovery.rs](src/dynamic_filtering/discovery.rs).

- Remove the worker's once-per-expression application guard.
- Use the existing `MaybeEncoded::to_proto` helper to inspect either payload
  representation, validate the expression ID and dynamic-filter variant, and
  obtain the inner predicate and completion state. DataFusion 55's synchronous
  `is_complete()` accessor is crate-private; no new upstream API is required.
- Decode the inner predicate with the existing codec and consumer schema. Apply
  it through `dynamic_filter_update_target` so producer-coordinate predicates
  are not remapped twice or incorrectly shared between aliased consumers.
- Call `mark_complete()` only when the received merged state is complete.
- Preserve best-effort handling of unknown IDs and decode/update failures.
- Update the existing remote sort/aggregate snapshots to show populated leaf
  filters using stable expression labels.

### Additional Simplification Requested

Consider removing `discover_runtime_dynamic_filter_consumers` rather than
maintaining another dedicated discovery helper.

Current findings:

- `discover_dynamic_filter_consumers` already excludes producer occurrences,
  separates anchors from real consumers, retains the input schema, and
  deduplicates real consumers by expression ID.
- `discover_runtime_dynamic_filter_consumers` repeats most of that traversal,
  returning a vector of every consumer occurrence for each expression ID.
- The worker application code currently takes only the first consumer in that
  vector, relying on shared dynamic-filter state for the other occurrences.

Preferred simplification: use
`discover_dynamic_filter_consumers(&plan)?.consumers`, collect it into a map
from expression ID to a single `DiscoveredDynamicFilter`, and remove the
runtime-only traversal and unnecessary vectors. Confirm the existing shared-
state invariant and column remapping with repeated/aliased consumers before
removing the helper. Keep anchors excluded. Do not introduce another wrapper
helper or broaden the public API just to support this refactor.

## DataFusion Findings

Inspected `/home/bits/datafusion` at `cb1480485` and the pinned DataFusion 55.0.0
sources used by this repository. Upstream main and the pinned release are not
identical; implementation should follow the pinned API.

### TopK: One Producer's Bound Can Be Globally Safe

[TopK::update_filter](../datafusion/datafusion/physical-plan/src/topk/mod.rs)
only publishes a threshold once the heap contains K rows. The shared threshold
may be tightened by any local emitter; it does not wait for all emitters to
produce a bound.

```text
ORDER BY x DESC LIMIT 3

Task A retains: 100, 90, 80
Published bound: x > 80

Task B has not reported:
  95 passes
  75 can safely be discarded because A already retains three better rows
```

Therefore a valid TopK report can be forwarded before other producer tasks
register or report. Missing producers do not need to contribute `true`.

TopK completion is a separate matter: the pinned implementation tracks local
emitters and can mark the local filter complete. The old TODO wording about
filters that "do not complete" should not be treated as an API guarantee.

### MIN/MAX: An Observed Value Is Already a Useful Bound

[AggregateStream](../datafusion/datafusion/physical-plan/src/aggregates/aggregate_stream.rs)
constructs `x < current_min` or `x > current_max` and shares bounds across local
partitions. A task that has already accumulated `MIN(x) = 10` can safely prune
future values `>= 10` globally: that retained value still contributes to the
final aggregate.

[AggregateExec](../datafusion/datafusion/physical-plan/src/aggregates/mod.rs)
limits this optimization to partial aggregates without grouping and supported
MIN/MAX expressions. This reasoning does not extend to arbitrary aggregates.

### Potential Core Bug: Missing Bounds in Multi-Aggregate Filters

This is a code-derived concern, not a reproduced failure. The predicate builder
skips an accumulator when its current bound is NULL, even if other accumulators
already have bounds. Both inspected versions have this behavior.

```text
Query: SELECT MIN(x), MIN(y) FROM input

First batch: (10, NULL)
Possible published predicate: x < 10

Later batch: (20, 5)
The predicate rejects the row, potentially losing the correct MIN(y) = 5.
```

The same situation can occur within one partition in one DataFusion process;
distribution is not necessary. If reproduced, it is a DataFusion core
correctness bug. Conservatively, an aggregate without a usable bound must not
be silently omitted in a way that prunes rows needed by that aggregate.

This is not a justification for stage-sealing barriers on otherwise valid
TopK/MIN/MAX bounds. Reproduce it separately before claiming unconditional
safety for every multi-aggregate predicate. Do not describe it as confirmed or
fixed. Upstream main also rejects unsupported aggregate arguments more strictly
than pinned 55, another reason not to generalize from the node type alone.

## Validation Plan

- Extend existing small registry tests: first incremental report before sealing,
  another producer registering later, replacement instead of historical OR,
  duplicate suppression, completion-only transitions, and unchanged join rules.
- Extend routing coverage: repeated updates in order, late consumers receiving
  the latest state, and local producer/consumer tasks not receiving remote
  overwrites.
- Cover encoded and decoded ApplyDynamicFilter payloads and gRPC roundtrips,
  including preservation of the expression ID and internal completion state.
- Extend the existing remote SQL TopK and MIN/MAX tests, covering both sort
  directions and aggregate bounds. Reuse the enabled-versus-disabled result
  comparison and populated-leaf plan snapshots.
- Add a small SQL-built worker-channel integration test for two incomplete
  updates followed by completion. Use the public channel API, bounded condition
  waits, and existing fixtures; do not add an observer framework or verbose
  manually constructed operator chains.
- Exercise repeated/aliased consumers to validate discovery-helper removal and
  the existing original-coordinate update-target behavior.
- Keep the proposed upstream NULL-bound reproduction separate from assertions
  that incremental distributed forwarding itself is incorrect.
- Run cancellation/channel-cleanup coverage so incomplete filters cannot keep a
  finished query alive.

Suggested checks, starting with the narrowest relevant ones:

```bash
cargo test --lib dynamic_filter_registry
cargo test --lib protocol::grpc
cargo test --features integration --test dynamic_filtering
cargo test --features integration --test stateful_data_cleanup
cargo test --doc
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
```

No new test result or performance claim is implied by this document. Prefer
existing SQL fixtures and helpers; keep test additions proportional to the
behavior change.
