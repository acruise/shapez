# shapez TODO

Status snapshot, immediate next steps, and the agreed analyzer design. See `JSON_TILES_ROADMAP.md` for the longer-horizon plan.

## Status

Crates landed:

- `shapez` — core types and path DSL. ShapeNode (Type | Variant | Tuple | Absent) layered over `_meta::ValueType`. PathStep (Field | Index) and PathPattern (adds AnyField | AnyIndex) with Display/FromStr/round-trip. Stats with observation_count + exemplar reservoir (no sketches yet). Assertion model (Assertion, AssertionRef, AssertionTarget, AssertionSet). ShapeException + AssertionViolation + ExceptionSink trait. ExceptionSession + ClusterKey + ExceptionEvent + ShapeDelta + CloseReason. `low_entropy_prefix` typed as `PathPattern` (the canonical / map-collapsed form). Analyzer trait stubbed in `ingest.rs`, no logic.

- `shapez-json` — `lower(&serde_json::Value) -> meta_types::value::Value`. Numbers split into i64/u64/f64. Objects lower to `Value::Map` with `MapKey::String`. 11 unit tests + 1 integration test against the corpus pass.

- `shapez-gen` — `Corpus::load(dir)` reads `manifest.json` and JSON Schema files; `Generator::new(corpus, seed)` is `Iterator<Item = serde_json::Value>`. Internal `Shape` model parses a subset of JSON Schema (oneOf, anyOf, enum, const, format, prefixItems, additionalProperties + propertyNames). Six atomic corpus entries cover scalar root, record stable, record optional, map UUID keys, tuple heterogeneous, polymorphic array discriminated. 4 integration tests pass.

Docs in `shapez/`: `JSON_TILES_ROADMAP.md`, this file.

## Immediate next steps

In rough order. Each unblocks the next.

1. Define `JsonEventSink` trait in `shapez/src/ingest.rs`. SAX-style with explicit document boundaries, scalar callbacks (null/bool/int/uint/float/string), and array/object begin/key/end. Strings as `&str` at the boundary. This is the analyzer's primary input contract; the `Analyzer` trait shrinks to "anything implementing JsonEventSink."

2. Rework `shapez-json` to provide an `EventDriver` that walks `serde_json::Value` and drives any `JsonEventSink`. Keep `lower()` as the exception-exemplar path.

3. Implement the analyzer (see design below). End goal: ingest a stream of values from the `shapez-gen` corpus and produce a `ShapeNode` tree that matches the schema each value was instantiated from (round-trip property test).

4. Add per-step sketches: HyperLogLog for value cardinality, HyperLogLog for object key cardinality, Space-Saving for top-K values. Probably the `hyperloglogplus` crate; Space-Saving hand-rolled (small).

5. Implement the record-vs-map decision (using key HLL + record-view overflow), tuple-vs-bag decision (using positional purity vs bag entropy).

6. Implement assertion evaluation and exception emission. Wire ExceptionSink into the analyzer.

7. Add ExceptionSession cache with simple LRU policy. Defer 2Q / W-TinyLFU to later.

After all that we are at roadmap phase 1 plus a chunk of phase 2 stat collection. The cluster-aware ROI advisor (Phase 4.5) comes after.

## Analyzer design

### Input contract

The analyzer implements `JsonEventSink`. Adapters drive events; the analyzer never sees a parser-specific type. Document boundaries are explicit (`document_begin(ordinal)` / `document_end`) so the analyzer can finalize per-doc state. Scalars are reported with their concrete type (i64/u64/f64 split, not a generic Number). Arrays and objects use begin/end pairs; object keys arrive via `object_key(key)` between begin and end, alternating with their values.

### Internal state

The analyzer maintains:

- A path stack. Each frame is either an array context (current position counter, total length, child position frames) or an object context (current key, set of keys-seen-this-doc, per-key state). Document root is a special frame.

- A shape tree being constructed. Each node is a `ShapeNode { kind, stats }`. Nodes are addressed by the path traversed to reach them. The tree grows lazily: paths not yet observed have no node.

- Per-node stats: observation_count (number of documents in which this path appeared), exemplar reservoir, plus the sketches added in step 4.

- Per-object-step dual view: `record_view: HashMap<String, ChildState>` capped at K keys (drop the view on overflow), plus a `map_view: MapStats` that aggregates regardless. The map_view holds a key HLL and per-value-shape stats over all keys.

- Per-array-step dual view: `bag_view: BagStats` (per-type histogram of elements) plus `positional_view: Vec<PositionStats>` capped at K positions.

- Per-array-step and per-object-step size distribution: a DDsketch over per-occurrence element count (arrays) or key count (objects). Yields min / max / mode / p50 / p95 etc. without storing every length. Feeds tuple-vs-bag detection and downstream costing.

- Per-compound-step subtree cluster view: a Space-Saving sketch over the subtree shape signatures of elements (arrays) or values (objects). Each element/value gets hashed, looked up, count incremented; evictions when at cap. A rolling eviction counter feeds a chaos threshold — if churn exceeds K * cap, set `cluster_view_alive = false` and fall back to bag_view permanently for this path. While alive, the reported element shape becomes a Variant whose arms come from the top-K clusters (plus an optional "other" arm if any eviction happened). This subsumes Variant emergence at element positions and handles the polymorphic-array-with-discriminator case automatically. The cluster cache lives at the compound step's path, so it accumulates both within-document (across the many elements of one array) and across-document (across many traversals of the same path).

- Per-variant tracking elsewhere (leaves where the same path holds different scalar types across docs): same Space-Saving mechanism at smaller scale, or just a small `HashMap<ValueType, count>` since the arm count is typically tiny. Open implementation choice for non-compound variant sites (see below).

### Path stack lifecycle

Pseudocode for the event handlers:

```
document_begin(ordinal):
    reset per-doc state (e.g., keys-seen-this-doc on each object frame)
    record doc_ordinal for later use in exemplars

document_end:
    apply doc-level finalization (e.g., "this key was absent in this doc" for
    every record-view key not seen)

scalar(type, value):
    observe at current path: increment observation_count, sample value into
    HLL / top-K, capture exemplar if reservoir says so

array_begin:
    push array frame with position = 0
    observe at current path that an Array was seen

array_end:
    pop array frame
    propagate length stats to bag_view, finalize positional_view

object_begin:
    push object frame
    observe at current path that an Object was seen

object_key(key):
    set current key on top object frame
    extend path with Field(key)
    update record_view (if alive) and map_view's key HLL

object_end:
    pop object frame
    record absent-field facts for record-view keys not seen this object instance
```

### Variant emergence (open implementation choice)

For non-compound paths (leaf positions where the same path holds different scalar types across docs). Compound positions are covered by the cluster view; their Variants emerge from there automatically.

Two reasonable strategies. Pick one before implementing.

Option A: lazy wrap. Each path starts as Absent. First observation creates a leaf of the observed type. On subsequent observation of a different type, wrap the existing node in a Variant and add a new arm. Pro: minimal allocation in the homogeneous case (the dominant case). Con: more mutation states to track.

Option B: always-variant. Each path is a Variant from the start; arms grow as types appear. At report time, if exactly one arm and it is not Null, unwrap. Pro: uniform code. Con: extra allocation in the homogeneous case.

I lean A. Variant emergence at leaves is rare enough that paying for it on demand beats paying for it always.

### Record-vs-map decision

Deferred to report time. During ingest, both views accumulate. At finalization or query, the decision uses:

- record_view alive? If overflowed and dropped, must be map.
- key HLL relative to document count. High cardinality (e.g., >= 0.5 * doc_count) is strong map signal.
- value-shape stability across all keys. Tight ShapeNode equivalence across keys is map signal.
- key value distribution. Top-K keys matching UUID / hash / ISO-timestamp / integer-string patterns is map signal.

Output: either the record_view's per-key children become Field steps in the shape tree, or the map_view's aggregate becomes a single `AnyField` step with the uniform value-shape underneath. Either way, the canonical form goes into a PathPattern.

### Tuple-vs-bag decision

Same dual-view, same report-time choice. Heuristic ingredients: arity distribution (tight mode = tuple), small-arity prior, per-position vs pooled entropy, cross-position type diversity. Trailing-optional handled by positional_view storing `Absent` for unobserved positions at indices i < seen_max.

### Assertion evaluation

Two flavors of falsification, both producing structured exceptions:

- Per-doc: leaf or subtree at a specific path violates a typed assertion. Detected at the moment of observation. Emit `ShapeException` immediately.

- Aggregate: an assertion like `assert: record` is violated when key-cardinality crosses the threshold. Detected at the moment the next observation pushes the count past the line. Emit once, with the tipping doc_ordinal in the violation mode.

Aggregate falsification short-circuits silently after first emission — the analyzer keeps accumulating stats but does not re-emit.

### Exception emission

The analyzer takes an `&mut dyn ExceptionSink` (or owns one) and calls `emit` per violation. The session-aware sink (with cluster key, LRU cache, delta protocol) is the eventual production sink; tests use simpler sinks (CountingSink, MemoryRingSink).

### Sampling-driven analysis

Inspired by BTRBlocks, which found that sampling ~5% of scalar values yields ~98% of the optimal compression/encoding choice. The same principle applies to shape inference: a small sample drives nearly-optimal analyzer decisions, and most documents barely need to be touched.

Three-tier processing per document:

1. **Sampled (~5%).** Full simdjson tape walk. Update shape tree, cluster sketches, HLLs, top-K samples. Evaluate assertions. Capture exemplars on violation.

2. **Asserted-but-not-sampled (~95% when assertions are configured).** Walk the tape, but skip subtrees not covered by assertions using simdjson's O(1) container-end offset. At assertion paths: evaluate, emit exception on violation. Increment per-path counters. No sketch updates.

3. **Non-asserted documents.** Increment the doc counter. Skip the document.

**Implementation:**

- Sampling is systematic by default (every Kth document). Switch to randomized if stream ordering shows correlations that bias the systematic pick.
- Sample rate auto-tunes against tile size to hit ~50 samples per tile. Default 5%; tile size 1024 -> ~51 sampled.
- HLL cardinality from samples is treated as "cardinality of this sample" — relative magnitudes carry the signal even without absolute calibration. Use a sampling-aware estimator (Chao, jackknife) at finalization if absolute estimates matter downstream.

**Adaptive sample rate.** Default rate is a starting point, not a fixed setting. Tune the rate based on the surprise signal from each closed tile.

Surprise signals to feed the controller (all measured per sampled doc):

- Cluster sketch eviction rate at any step (something new displaced something old)
- New variant arms introduced (strict mismatches that did not match any existing arm)
- Aggregate chaos threshold crossings across all steps
- Top-K shape signature churn at the document root (new dominant shapes emerging)

Simple control law to start:

```
surprise = (evictions + new_arms + chaos_crossings) / sampled_docs_in_tile

if surprise > high_band:
    next_rate = min(current_rate * 1.5, ceiling)   # bump up
elif surprise < low_band:
    next_rate = max(current_rate * 0.8, floor)     # decay back
else:
    next_rate = current_rate                        # leave it
```

With `floor = 1-2%`, `ceiling = 10-15%`, `high_band` and `low_band` tuned against the corpus. The ceiling is set by diminishing-returns economics (above ~150 samples per tile of 1024, additional samples buy negligible sketch convergence), not by cost ceiling. The floor exists because sketches need enough refresh rate to track even slow shape drift across many tiles.

Important edge case: **chaos saturation.** If a step's cluster view has hit the chaos threshold (given up on clustering), sampling more will not help — the data is not clustering at any rate. The controller should detect "high surprise AND high chaos saturation" and stop increasing the rate; the analyzer accepts that the stream is inherently diverse and reports it as such. Otherwise sample rate can run away on streams that will never settle.

The adaptive controller runs independently within each progressive indexing tier (see below). Each tier has its own default, floor, and ceiling.

**Why this replaces an earlier PIC-tier design.** A well-written branchy event handler is fast enough. PIC specialization promised "make the analyzer almost free" by per-step dispatch tricks, but the honest assessment is that event-driven analysis is fundamentally branch-rich (per-event dispatch + strictness check + sketch update + stats increment), and PIC tricks do not change that. Sampling avoids the work on 95% of documents entirely, which is a far larger speedup than any per-event specialization can deliver. The cluster sketches still matter — they drive promotion advice and downstream optimization — they are just maintained against the sampled subset.

### Strictness rule for cluster sketch tier-ups

Defines what causes a cluster sketch at a step to add a new arm vs just updating stats on an existing arm. Applies to events observed during sampled-doc traversal.

Default policy: **type changes are strict, presence and cardinality variation are soft.**

Strict (causes new arm):
- Scalar type change at a leaf (i64 -> string, etc.)
- Never-before-seen field at an object step (shape extension)
- Subtree signature at an element position matches no known arm

Soft (updates stats on existing arm):
- null at an expected-non-null position or vice versa (update saw-null / saw-value counters)
- Known-optional field absent
- Known field present this doc but missing last K docs
- Array length outside historic range (update length stats)
- Object key count outside historic range
- Tuple-shaped element with different arity than historic mode

Operator override is on the roadmap (per-path or per-shape strictness profiles); defaults should be invariant unless a deployment opts out.

### Epoch model: dictionary stability decoupled from drift

A unifying frame, borrowed from sliding-window / delta compression. The pattern is: within an epoch, the active "dictionary" (schema, promoted columns, frequent itemsets, whatever the local cache is) is **fixed and pure**. At epoch boundary, the dictionary updates based on accumulated observation, producing a better-informed dictionary for the next epoch.

The defining properties:

- **Within-epoch self-containment.** Anything readable in an epoch is decodable using only that epoch's frozen dictionary. A reader of one tile needs only that tile's header. No cross-epoch lookups required for correctness.
- **Cross-epoch monotonic improvement.** The accumulator (cluster sketches in our case) carries evidence forward. Each epoch's dictionary is a snapshot of the accumulator at that boundary. The next dictionary will be no less informed than the current one, modulo intentional decay or eviction.
- **Drift independence.** Epoch boundaries are arbitrary relative to data drift. Drift mid-epoch costs you a suboptimal dictionary for the rest of that epoch; the next boundary catches up. No need to detect drift precisely or online.

This pattern appears in many guises and we should expect to see it everywhere: tiles, partition reordering, residual-JSON dictionaries if we ever add one, statistics rollups for query optimization, progressive indexing tiers (next section). They are all the same loop at different granularities.

### Two-tier sketch model: tile-local accumulator, global dictionary

For each statistic (cluster sketch, HLL, top-K values, DDsketch), maintain **two instances**: a tile-local one that accumulates during the tile, and a global one that persists across tiles. At tile close, the tile-local is used to produce that tile's frozen dictionary (schema, promoted columns, presence bitmaps) and is then merged into the global sketch with LRU-style replacement when the global sketch has bounded capacity (matching JSON Tiles' 256 frequency counters / 64 HLLs).

```
during tile:
    tile_local_sketch.update(observation)
    global_sketch is read-only this epoch

at tile close:
    tile_schema := freeze_from(tile_local_sketch)        // per-tile dictionary
    global_sketch.merge(tile_local_sketch)               // updates the global dictionary
    tile_local_sketch.reset()
```

Why two tiers rather than one global sketch:

- **JSON Tiles parity for storage.** Each tile's promoted-column schema is built from that tile's own contents, making tiles independently scannable. Pure JSON-Tiles-style storage layer.
- **Long-lived shape memory.** The global sketch keeps cross-tile evidence so future tiles' promotion decisions can refer to historic prevalence, not just one tile's view.
- **Trivial multi-thread ingest.** Each worker thread builds its own tile-local sketch against a read-only snapshot of the global. Merge happens at tile close under a brief lock. No mid-tile coordination.

The same two-tier structure applies to per-step cluster sketches, per-leaf HLL value cardinality, top-K value samples, and DDsketches over length and key-count distributions. One implementation pattern, many use sites.

### Progressive / tiered indexing

Borrowed from Lake Superior: pay for deep analysis as data proves it will stick around, not on first contact. Three indexing tiers, each gated by age or durability signals from the storage layer:

- **Tier 0 (ingest fresh, RAM-bound).** Minimum work per document: count, evaluate assertions if configured, route to storage. Optionally sample at very low rate (e.g., 1%) for crude shape signal. No promotion decisions made. Goal: get out of RAM fast.

- **Tier 1 (warm tile, recently committed).** Full sampling-driven analysis at the default 5% rate. Cluster sketches, shape tree, promotion plan, tile schema. Triggered when a tile completes ingest and stabilizes. Goal: produce a good-enough columnar layout for typical streaming-query access.

- **Tier 2 (aged file, durable).** Higher sample rate or full scan. Discover latent regularities that did not manifest in the Tier 1 sample. Refine the schema and possibly re-promote columns to better encodings. Triggered when a file has survived past a duration threshold (e.g., one hour) and/or accumulated query hits past a count threshold. Goal: optimize for repeated query access; the cost is justified by the file's proven longevity.

Each tier is an epoch in its own right. Tier 0's dictionary is "nothing promoted, all residual JSONB." Tier 1's dictionary is the sampling-driven promotion plan. Tier 2's dictionary is the refined plan. Across tiers, the dictionary improves monotonically because the underlying data is fixed by the time Tier 1 or 2 runs over it.

This composes with the two-tier sketch model directly:

- Tier 0: no sketches.
- Tier 1: tile-local and global sketches built via 5% sampling.
- Tier 2: rebuild sketches from a fuller scan, replacing the Tier 1 sketches for that file.

Operator policy controls promotion between tiers (age threshold, hit-count threshold, storage budget). The analyzer exposes "give me Tier N analysis on this input" as a callable operation; the tier-transition decision lives upstream in the storage system.

### Finalization

`analyzer.finish() -> ShapeNode` consumes the analyzer and produces the final shape tree with record-vs-map and tuple-vs-bag decisions applied. Stats remain attached to each node. A separate API can return the raw dual-view state if the caller wants to apply different decision policies later.

## Backlog (further out)

- Sketches: HLL value cardinality, HLL object-key cardinality, Space-Saving top-K leaf values, Space-Saving subtree-signature top-K (used by both per-step cluster views and document-level cluster identification — same machinery, different scope). DDsketch for array length and object key count distributions.

- Subtree shape signature: a stable canonical hash of an inferred ValueType-equivalent subtree. Required for the cluster sketches above. Order-canonicalized for objects (sorted by field name) and for Variant arms (sorted by inner hash).

- Cluster-aware ROI advisor: per-tile shape-signature distribution, per-cluster promotion plan, cumulative ROI cutoff, layout recommendation. See `JSON_TILES_ROADMAP.md` (planned phase 4.5).

- Tile boundaries: a `TileBuilder` that closes every N documents and emits a `TileReport`. Default N = 2048.

- Frequent itemset mining (FPGrowth) over canonical path-sets within each tile.

- Cross-tile reordering at partition granularity, with the open question of how it composes with notochord's subject-keying.

- Storage layer integration: PromotionPlan -> notochord buffer module columns. Residual binary JSON format choice (own JSONB vs CBOR/MessagePack).

- Optimizer statistics aggregated tile -> relation with LRU-style replacement.

- Query path: predicate push-down, cast rewriting, tile skipping.

- More corpus entries to round out the atomic decision-dimension coverage matrix. Currently 6 of ~22 dimensions covered; missing: record_nullable, leaf_int_or_float, leaf_iso_timestamp (standalone), enum_string, recursive_tree, deeply_nested_record, empty_containers, map_slug_keys, array_polymorphic_bag, scalar_root_string, array_root_uniform, tuple_trailing_optional. Plus 4-6 compound schemas.

- `shapez-simdjson`: tape-driven `JsonEventSink` adapter using the `simd-json` crate. Lazy-reparse strategy for capturing exemplar `Value`s on the exception path.

- Other feedstock adapters: Iceberg variant column, Spark 4 dataframe variant. Each is a `JsonEventSink` adapter; the analyzer does not change.
