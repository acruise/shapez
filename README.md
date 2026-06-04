# shapez

Structural shape inference over streams of semi-structured values. shapez observes a stream of `meta_types::value::Value` documents and produces an inferred `ShapeNode` tree -- a description of the data's recurring structure -- plus a stream of `ShapeException`s when user-declared assertions about that structure are falsified.

It is a schema-inference and columnar-promotion-advice layer for downstream analytical stores. The north star is the JSON Tiles approach (Durner, Leis, Neumann, SIGMOD 2021): infer per-tile schemas, promote frequent paths into columns, keep the long tail in residual binary JSON. shapez aims to be a JSON-Tiles ancestor that carries strictly more shape information -- explicit variants, record-vs-map decisions, tuple-vs-bag decisions, and user assertions -- through the same pipeline. See `shapez/JSON_TILES_ROADMAP.md` for the phased bridge plan and `shapez/TODO.md` for current status and the analyzer design.

## Status

Scaffolding and type language are landed; analyzer logic is not yet implemented. The types compile and round-trip, the JSON adapter and corpus generator work, but `Analyzer` in `ingest.rs` is a stub. Phase 1 (real analyzer logic) is the immediate next step. Treat this README as design intent, not a description of working inference.

## The three crates

shapez is split into the analyzer and two satellites so the analyzer never depends on any particular input format or on test machinery.

- `shapez` -- the core. Shape type language (`ShapeNode`), path DSL (`Path` / `PathPattern`), per-node statistics, the assertion model, the exception stream, and the session-aware exception clustering protocol. Input is always `meta_types::value::Value`; the analyzer is format-agnostic by construction.

- `shapez-json` -- the JSON feedstock adapter. `lower(&serde_json::Value) -> Value` maps JSON into the analyzer's input type, splitting numbers into i64 / u64 / f64 and lowering objects to maps. This is the bridge any JSON source crosses to reach the analyzer.

- `shapez-gen` -- a curated JSON Schema corpus plus a value-stream generator. `Corpus::load(dir)` reads a manifest and its schemas; `Generator::new(corpus, seed)` is an `Iterator<Item = serde_json::Value>` emitting conforming documents. The corpus is organized around analyzer decision dimensions (scalar root, stable record, optional record, map with UUID keys, heterogeneous tuple, discriminated polymorphic array, ...) so each schema exercises one inference choice. This is the test feedstock: generate from a known schema, run the analyzer, assert the inferred shape matches.

## Core design ideas

### Shape language layered over ValueType

`ShapeNode` wraps the closed `ValueType` set from `_meta` and adds three shape-language-only meta-nodes: `Variant` (a path holds different shapes across documents), `Tuple` (a fixed-arity positional array), and `Absent` (a path observed to be missing). Reusing `ValueType` means a promoted column's type is the shape node's type directly, with no conversion at the storage boundary. `Tuple` is produced only at report time; during ingest, arrays are stored as `Type(Array)` with positional statistics kept on the side.

### Paths are pure location; shape decisions live in the tree

A `Path` is syntactic location only -- a sequence of `Field` / `Index` steps. Whether an object step is a record field or a high-cardinality map key is a property of the inferred shape at that position, not of the path. `PathPattern` adds `AnyField` / `AnyIndex` for two jobs: matching assertions to ingest sites, and the canonical map-collapsed form. A location like `.flags.<uuid>.enabled` canonicalizes to `.flags.*.enabled`, so all the per-key fanout accumulates into one bucket.

### Dual-view ingest, decisions deferred to report time

The hard structural choices -- record vs map, tuple vs bag -- are not made during ingest. Both views accumulate simultaneously (a capped per-key/per-position record/positional view alongside an aggregate map/bag view), and the decision is made at finalization using key cardinality, value-shape stability, arity distribution, and key-pattern heuristics. This lets the same observations support different decision policies and avoids committing early on ambiguous data.

### Sampling-driven analysis

Inspired by BTRBlocks' finding that sampling ~5% of values yields ~98% of the optimal encoding choice, shapez does full structural analysis on a small sampled subset and minimal work on the rest. Three tiers per document: sampled documents get a full walk with sketch and shape-tree updates; asserted-but-unsampled documents get assertion evaluation only (skipping irrelevant subtrees); non-asserted documents just bump a counter. The sample rate auto-tunes toward roughly 50 samples per tile and adapts up or down based on a surprise signal (sketch evictions, new variant arms, chaos crossings), with a guard against runaway rates on streams that never settle.

### Epochs and two-tier sketches

Inference runs in epochs (tiles). Within an epoch the active dictionary -- schema, promoted columns -- is frozen and pure, so any tile is decodable from its own header with no cross-tile lookups. At epoch boundaries the dictionary updates from accumulated evidence, improving monotonically because the underlying data is fixed by analysis time. Each statistic is kept in two instances: a tile-local accumulator that produces the tile's frozen dictionary, and a bounded global sketch that carries cross-tile memory with LRU-style replacement. This gives JSON-Tiles-style independently-scannable tiles plus long-lived shape memory, and makes multi-threaded ingest trivial (per-worker tile-local sketches over a read-only global snapshot, merged under a brief lock at tile close).

### Assertions and the exception stream

shapez is supervised where JSON Tiles is not. An `Assertion` declares an expectation at a `PathPattern` -- this path is a record, that one is a map, this leaf is an i64 -- with a tolerance for violating documents. Falsification comes in two flavors: per-document (a leaf or subtree violates a typed assertion, detected at observation) and aggregate (a structural assertion like "this is a record" is broken when key cardinality crosses a threshold, detected at the tipping document). Violations flow as `ShapeException`s through an `ExceptionSink`. Recurring exceptions cluster by `ClusterKey` (assertion refs plus low-entropy path prefix) into an `ExceptionSession` with an open / delta / close protocol, so a storm of similar violations collapses into one session plus compact deltas rather than a flood of identical reports.

## Intended use cases

- Schema discovery for semi-structured streams. Point shapez at a stream of JSON (or any feedstock with an adapter) and get back an inferred shape tree, including which object steps are really data-keyed maps and which arrays are really tuples -- distinctions a naive path-counting inferencer misses.

- Columnar promotion advice for downstream stores. The end goal: produce a `PromotionPlan` saying which `(path, type)` pairs a tile should hoist into typed columns, with the long tail left in residual binary JSON. The same plan also drives Spark / Iceberg shredded-variant emit: promoted paths become typed sub-columns of the variant, the rest stays in the variant blob. Map-collapsed wildcards still promote leafward (`.flags.*.enabled` becomes a column behind a map step), tuples promote per position, and variants promote to one nullable column per arm -- all stronger than JSON Tiles' "dominant type wins, rest to JSONB" rule.

- Data-contract monitoring. Declare assertions about expected structure and receive a structured, de-duplicated exception stream when live data violates them, with exemplar back-pointers into the input for triage. This is usable from phase 1, independent of any columnar storage.

- Optimizer statistics. Per-tile HyperLogLog sketches and frequency counters aggregated to the relation level, feeding query planners (including external consumers such as Substrait).

- Drift detection over time. Because epochs decouple dictionary stability from data drift, comparing successive tile schemas surfaces structural drift without needing precise online drift detection.

## Where to read next

- `shapez/DESIGN.md` -- domain model, goals, non-goals, design tenets, and the stable wire-level contracts.
- `shapez/TODO.md` -- current status, immediate next steps, and the full analyzer design (input contract, path-stack lifecycle, variant emergence, record-vs-map and tuple-vs-bag heuristics, sampling, epochs, progressive tiered indexing, finalization).
- `shapez/JSON_TILES_ROADMAP.md` -- the gap analysis against JSON Tiles and the eight-phase bridge plan, including where shapez intentionally leads rather than follows.
