# From shapez to JSON Tiles: a Bridge Plan

This is the gap analysis from where shapez sits today to the vision described by Durner, Leis, and Neumann's "JSON Tiles" (SIGMOD 2021, doi:10.1145/3448016.3452809). The paper's contribution is a per-tile schema inference, columnar promotion of frequent paths, residual binary JSON for the long tail, and optimizer statistics, all integrated into the Umbra RDBMS. The plan below treats JSON Tiles as a north star and asks what each intermediate phase buys us.

## What shapez has today

A path DSL, a ShapeNode type language layered over `_meta::ValueType` (with Variant, Tuple, Absent as shape-language-only meta-nodes), an exception model with sink trait and session-aware delta protocol, a JSON Schema corpus and value-stream generator for testing. No analyzer logic yet: the types are scaffolded, ingestion is unimplemented.

## What JSON Tiles has that shapez does not

- Tile boundaries (2^10 to 2^12 documents) as the unit of inference. shapez inference is currently stream-shaped, no batch boundary.
- Frequent itemset mining (FPGrowth) over (path, primitive-type) pairs within each tile. The output is "which path sets co-occur in this tile above the extraction threshold."
- Columnar materialization of frequent paths, residual binary JSON for outliers and infrequent paths. Heart of the speedup.
- Per-tile header recording extracted paths, types, presence bitmap, null bitmap, plus a bloom filter of all observed paths (for tile-skipping).
- Cross-tile reordering at the partition level (8 tiles in the paper) to consolidate shape clusters when insertion order is hostile.
- Optimizer statistics: 256 frequency counters and 64 HyperLogLog sketches per relation, aggregated from tiles with an LRU-style replacement policy.
- Query-side support: access-expression push-down into scan, cast rewriting, tile skipping under null-safe predicates, auto-detection of numeric and date/time strings.

## What shapez has that JSON Tiles does not

These are the places shapez could lead, not follow.

- Explicit Variant nodes. JSON Tiles handles multi-type paths by promoting the dominant type and dropping the rest into JSONB. shapez models the variant directly with arm statistics, which preserves the information for downstream consumers and matches data that is genuinely polymorphic.
- Record-vs-map decision. JSON Tiles treats all object keys as path components, full stop. shapez's dual-view ingest plus key-cardinality heuristic explicitly recognizes objects-keyed-by-data and collapses the per-key fanout into a single wildcard step, continuing inference leafward into the uniform value shape. The canonical form uses `PathPattern` with `AnyField` at the collapsed step, so a document location like `.flags.<uuid>.enabled` reports as `.flags.*.enabled`.
- Tuple-vs-bag decision for arrays. JSON Tiles materializes "the first x elements" of arrays whose size varies, a half-solution that conflates tuples with prefixes of bags. shapez's positional-vs-bag dual statistics target this directly.
- User-provided assertions with structured falsification reporting. JSON Tiles is unsupervised; shapez supports declared expectations as a first-class concern with a structured exception stream.

## Phased bridge

Each phase is a useful endpoint on its own. The progression is roughly cumulative cost order, not strict dependency order.

### Phase 1: Real analyzer logic

Walk a `meta_types::value::Value`, build a `ShapeNode` tree, accumulate per-node stats (count, first/last doc ordinal, exemplar reservoir). No tile boundaries yet, no promotion, no storage. The output is just an inferred shape tree at end-of-stream. Validates the shape language end-to-end against the curated corpus.

Dependencies: nothing new. This is what the shapez scaffold is waiting for.

### Phase 2: Tile boundaries

Add a `TileBuilder` that consumes N values then emits a `TileReport`: the inferred ShapeNode plus per-path frequency, null rate, presence bitmap, and a bloom filter of all paths observed in the tile. Tile size configurable, default 2048 to match the paper's sweet spot.

This is where shapez starts to look operationally similar to JSON Tiles. The TileReport is the "header" in the paper's language. Path observations still include the shapez-specific record-vs-map and tuple-vs-bag annotations.

Dependencies: phase 1.

### Phase 3: Frequency-based promotion candidates

Per-tile, identify (path, type) pairs whose observation count exceeds an extraction threshold (e.g., 60%). Output is a `PromotionPlan` listing which paths a downstream storage layer should materialize. shapez does not itself promote anything; it just produces the plan.

This is the smallest useful production artifact: even without columnar storage, a "what should we hoist into columns" recommendation is consumable by query designers, schema generators, ETL pipeline tools.

Dependencies: phase 2.

### Phase 4: Frequent itemset mining

Implement FPGrowth (or an equivalent) over the tile's observed path sets to find frequent co-occurring path itemsets. The PromotionPlan upgrades from "individual paths above threshold" to "maximum itemsets above threshold," which is what produces good clustering when documents have multiple distinct shapes.

Bound the mining work by a budget parameter as the paper does (equation 1, k chosen to cap subset count at u).

Dependencies: phase 3.

### Phase 5: Cross-tile reordering

Group K tiles (default 8) into a partition. Mine frequent itemsets at partition granularity with a relaxed threshold (threshold / partition_size). Match each tuple to its best itemset, redistribute tuples between tiles in the partition to consolidate shape clusters, re-mine each tile.

This is mechanically expensive but the gain is large on streams with poor spatial locality. Concurrency story: each partition reorders independently; readers see a tile only when fully assembled.

Open question: how does reordering compose with a host system's partition keys (e.g. subject-keyed buffers)? Tile reordering across partition keys is presumably wrong when key identity is load-bearing in the host's domain. The right composition is probably "tile within a key" rather than "tile across keys." Worth deciding before this phase rather than during.

Dependencies: phase 4.

### Phase 6: Storage layer integration

The PromotionPlan from phase 3 or 4 starts driving actual columnar promotion in a host storage layer. Promoted (path, type) pairs become typed columns following the host's column abstractions. Residual JSON stays in a binary JSON column. The same plan also targets Spark / Iceberg shredded-variant emit: promoted paths become typed sub-columns of the variant, residual stays in the variant blob.

Connections worth pulling:

- `meta_types::ValueType` becomes the column type directly. No conversion.
- The host's existing pluggable physical layouts (dictionary-coded, byte-buffer, reference, delta — whatever the embedding system provides) are the right home for promoted columns. shapez does not need to know about them.
- Hosts with predicate bitmaps gain a direct parallel to JSON Tiles' presence bitmaps. Shared infrastructure where it lines up.

A binary JSON format is needed for the residual on the column-store side. JSON Tiles defines its own (paper section 5) optimized for log(n) object lookups and forward-iterable nested structures. A host could adopt CBOR or MessagePack to skip rolling its own, accepting some performance tax on residual access; or invest in a JSONB-like format if residual access is hot. Decide based on residual frequency in target workloads. The variant-export path uses the Spark/Iceberg variant binary spec instead and has no such choice.

Dependencies: phase 4, plus the relevant emitter (host buffer adapter or variant writer).

### Phase 7: Optimizer statistics

Per-tile HyperLogLog sketches and frequency counters for promoted paths. Aggregate to the relation level with an LRU-style replacement policy (the paper proposes 256 frequency counters and 64 sketches as a memory bound). These propagate to whatever query planner downstream consumes the data, including host buffer-level operations and external optimizers (Substrait, for instance).

Dependencies: phase 6, plus a planner / consumer that benefits from the stats. Without a consumer, the stats are dormant infrastructure.

### Phase 8: Query path

Access-expression push-down into the scan, cast rewriting, tile skipping under null-safe predicates. These are the speedup-realization phases. shapez does not own this work directly; it lives in whatever query engine consumes the buffer (the host system, plus external integrations).

Dependencies: phases 6 and 7.

## Where the shapez differences land

shapez carries strictly more shape information than JSON Tiles needs. Promoting that information through the pipeline does not change the JSON Tiles algorithm proper, it just enriches the artifacts.

- A path that shapez has decided is a Map step (high-cardinality keys, uniform value shape) collapses the key fanout to a wildcard and continues inference leafward. Concrete document locations like `.flags.<uuid_1>.enabled`, `.flags.<uuid_2>.enabled`, ... all canonicalize to the single PathPattern `.flags.*.enabled` and accumulate observations into one bucket. The itemset miner sees one frequent path instead of k singletons, and the storage layer can promote `enabled` to a column even though it lives behind a map step. This is materially stronger than JSON Tiles' rule of treating the map as opaque JSONB: we still get columnar promotion under the wildcard, with the key recoverable per row via a sidecar key column or composite key encoding.
- A path that shapez has decided is a Tuple is materialized as a fixed-arity row of typed columns (one per position) rather than a single array column. Position-i becomes its own promoted column. Tuples with trailing optionals materialize the optional positions as nullable columns.
- A Variant path with N arms materializes as N typed columns where each row populates exactly one. The other arms are null in that row. This is materially better than JSON Tiles' "pick the dominant type, drop the rest into JSONB" rule for genuinely polymorphic paths.
- Assertion violations flow into the existing exception stream regardless of phase. The structured falsification report is orthogonal to promotion decisions and can ship from phase 1 forward.

## What to do first

Phase 1 unblocks every later phase and validates the shape-language design against real input. Phase 2 turns shapez into a recognizable JSON Tiles ancestor without committing to storage changes. The decision points worth surfacing before phase 5 are (a) how reordering composes with the host system's partition keys, and (b) whether to adopt JSON Tiles' own JSONB or an off-the-shelf binary JSON for the residual storage on the column-store path (the variant-export path is governed by the Spark/Iceberg spec instead). Both are deferred-decision-friendly until phase 5/6.
