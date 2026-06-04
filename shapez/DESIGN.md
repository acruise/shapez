# shapez — design

A focused reference for what shapez is, what it commits to, and what it deliberately leaves to others. For implementation status and the analyzer walkthrough see `TODO.md`; for the longer-horizon storage roadmap see `JSON_TILES_ROADMAP.md`.

## Purpose

Given a stream of semi-structured documents, shapez infers the recurring **shape** of the data — the structure operations and storage layers should expect — and surfaces structured **falsifications** when declared expectations are broken. It is a supervised, sampling-driven, tile-oriented shape inference engine. It is not a query engine, not a storage engine, and not a schema validator.

The motivating consumer is an analytical store ingesting semi-structured input: shapez tells it which paths to hoist into typed columns, which structural calls to commit to (record vs map, tuple vs bag), and where the residual JSON tail begins. The same advice also drives Spark / Iceberg shredded-variant emit.

## Domain model

The objects shapez deals with and how they relate. Names match the Rust types.

- **Value** — one document from the input stream, typed as `meta_types::value::Value`. The analyzer never sees a parser-specific type; adapters lower their feedstock into `JsonEventSink` events.
- **Path** / **PathPattern** — *pure syntactic location.* `Path` is a sequence of `Field` / `Index` steps; `PathPattern` extends with `AnyField` / `AnyIndex` for matching assertions to ingest sites and for the canonical map-collapsed form (`.flags.*.enabled`). Whether a step is a record field or a map key is *not* part of the path — it is part of the shape at that position.
- **ShapeNode** — the inferred shape tree. Leaves wrap the closed `ValueType` set from `_meta`; meta-nodes (`Variant`, `Tuple`, `Absent`) and rich compounds (`Array { element }`, `Record { fields }`, `Map { key, value }`) carry shape-language information that flat `ValueType` cannot express.
- **Stats** — per-node observation count, doc-ordinal range, exemplar reservoir, and (planned) bounded sketches: HLL value/key cardinality, Space-Saving top-K leaf values, DDsketch length/key-count distributions.
- **Cluster** — Space-Saving top-K equivalence class over subtree signatures observed at a compound position. Drives variant emergence at array elements (and eventually map values).
- **Assertion** — declared expectation: at this `PathPattern`, expect *record* / *map* / *tuple* / *bag* / *type T* / *variant arm S*, with a tolerance for violating documents.
- **ShapeException** — one falsified observation. Carries the doc ordinal, full path, low-entropy path prefix, observed value (via the exemplar adapter), observed shape, and the violations that triggered it.
- **ExceptionSession** — recurring exceptions clustered by `ClusterKey` (assertion refs + low-entropy path prefix) into one open/delta/close stream, so a storm of similar violations collapses into a session plus compact deltas.
- **Tile** / **Epoch** — bounded chunk of input. Within an epoch the active dictionary (schema, promoted columns) is frozen and pure; at epoch boundaries the dictionary updates from accumulated evidence. Tiles are independently scannable from their own headers.

## Goals

Things shapez commits to delivering.

- **Structural inference from a population of values.** Decide record vs map per object position, tuple vs bag per array position, and surface variants where the same path holds genuinely different shapes.
- **Sampling-driven analysis.** Full structural work on a small sampled subset (BTRBlocks-style), light or zero work on the rest. Sample rate auto-tunes against a surprise signal with a chaos guard.
- **Bounded, persistent state.** All cross-tile memory is sketch-shaped (HLL, Space-Saving, DDsketch) with explicit caps; no unbounded growth on adversarial input.
- **Supervised falsification.** Declared assertions produce a structured exception stream — not log lines — with deduplicated sessions for repeated patterns.
- **Format-agnostic boundary.** The analyzer sees `JsonEventSink` events, never a parser-specific type. New feedstocks (simd-json tape, Iceberg variant, Spark variant) are adapters, not analyzer changes.
- **Independently-scannable tiles.** Anything readable in a tile is decodable from that tile's frozen dictionary. No cross-tile correctness lookups.
- **Shredding advice.** Emit a `PromotionPlan`: which `(path, type)` pairs should be hoisted to typed columns (or shredded variant sub-columns), which paths stay in the residual variant/JSONB tail, and how variants partition into one nullable arm per cluster. The plan targets both columnar buffer storage and Spark/Iceberg shredded variant equally; shape inference is the same, only the emit format differs.
- **Stable wire-level contracts.** `JsonEventSink`, the `ShapeNode` tree, `ShapeException`, and the session open/delta/close protocol are committed surfaces; everything else (sketch choices, decision thresholds, sample rate control) is implementation.

## Non-goals

Things shapez deliberately does not do, even when adjacent.

- **Not a query engine.** We feed promotion plans and statistics to one; we do not execute queries.
- **Not a storage engine.** We advise on column promotion and residual layout; we do not write the bytes. shredded-variant emit, Parquet writes, and JSONB encoding are downstream of the `PromotionPlan`, not part of shapez.
- **Not a declarative schema validator.** Assertions are a supervision layer for inferred shape, not a JSON-Schema replacement. We refuse to grow into one.
- **Not a single-document inferencer.** A shape is a property of a *population* of documents; one document tells you almost nothing.
- **Not exact-count statistics.** Cardinalities and frequencies are sketch-approximated. Consumers that need exact counts must aggregate elsewhere.
- **Not online drift detection.** Epochs decouple dictionary stability from drift — comparing successive tile schemas surfaces structural drift without an online detector.
- **Not strict heterogeneity preservation.** Clustering deliberately folds rare subtree shapes into a residual; the head of the distribution is the schema.
- **Not a general-purpose semi-structured database.** shapez is one component a host system pulls in; it is not the system itself.

## Design tenets

The load-bearing decisions. Each is a commitment the rest of the design rests on.

1. **Paths are location; shape decisions live in the tree.** A `Path` does not encode "is this a map key." That decision is at the shape node for that position, and changes do not invalidate paths.
2. **Dual-view ingest, decisions deferred to report time.** Both record-and-map (and tuple-and-bag) views accumulate during ingest; the call is made at finalization using cardinality, value-shape stability, arity distribution, and key-pattern signals. This avoids early commitment on ambiguous data.
3. **Option A lazy variant emergence at leaves.** Paths start as `Absent`. First scalar observation creates the leaf; observation of a different type wraps it in a `Variant`. Pays the allocation cost only when needed.
4. **Cluster sketches surface variants at compound positions.** Per-element subtree signatures feed a per-step Space-Saving sketch. Top-K become Variant arms at finalization; an eviction-rate chaos signal can mark a position as inherently diverse and stop clustering.
5. **Sampling beats per-event specialization.** Sampling 5% of documents at full fidelity buys ~98% of the optimal sketch state for far less cost than per-event handler tricks could deliver.
6. **Two-tier sketches: tile-local + bounded global.** Each statistic is kept twice — a tile-local accumulator that produces the tile's frozen dictionary, and a bounded global sketch that carries cross-tile memory with LRU-style replacement. JSON-Tiles parity for storage; long-lived memory across tiles.
7. **Epochs make drift cheap.** Dictionary stability is decoupled from data drift by construction. A mid-epoch drift costs you a suboptimal dictionary for the rest of that epoch; the next boundary catches up. No online detector required.
8. **Falsifications are structured streams, not strings.** A `ShapeException` is a typed value carrying the exemplar back-pointer, the violation set, and a cluster key. Recurring violations collapse into sessions so consumers see signal, not volume.

## Stable contracts

What downstream code can build against and expect to keep working.

- **`shapez::ingest::JsonEventSink`** — the input protocol. Document boundaries are explicit; scalars carry their concrete type (i64/u64/f64 split); strings cross as `&str`. Adapters drive events; the analyzer accumulates.
- **`shapez::ingest::Analyzer`** — `JsonEventSink` plus `finish(self) -> ShapeNode`.
- **`shapez::node::ShapeNode`** — the inferred shape language. `Type(ValueType)` covers the cases ValueType expresses; rich compounds and meta-nodes carry the rest. Storage layers consuming `Type(ValueType::X)` need no conversion at the boundary.
- **`shapez::exceptions::{ShapeException, ExceptionSink}`** — the falsification stream and the trait through which it flows. The session-aware sink is the production form; testing sinks are simpler.
- **`shapez::session::{ExceptionSession, ExceptionEvent, ShapeDelta, CloseReason}`** — the open/delta/close protocol for clustered exceptions. Wire format for exception consumers.

Everything else — choice of HLL implementation, Space-Saving cap, sample-rate control law, decision thresholds, finalizer strategy — is implementation and may change without affecting consumers that respect the contracts above.

## Type alignment with Spark / Iceberg Variant

The Spark VARIANT spec (and Iceberg's adoption of it) defines the binary representation downstream consumers will read. `_meta::ValueType` is a strict superset of the Variant type set, so shapez can describe any shape a Variant can carry. The asymmetric pieces matter for the shredding-advice emitter, not for the analyzer.

Direct correspondence:

| Spark/Iceberg Variant | `_meta::ValueType` |
|---|---|
| `null` | `Null` |
| `boolean` | `Bool` |
| `int8` / `int16` / `int32` / `int64` | `I8` / `I16` / `I32` / `I64` |
| `float` / `double` | `F32` / `F64` |
| `decimal4` / `decimal8` / `decimal16` | `Decimal { precision, scale }` (storage type follows precision) |
| `date` | `Date` |
| `timestamp_micros` / `timestamp_nanos` (with TZ) | `Timestamp { precision: Micros\|Nanos, timezone: UtcOffset }` |
| `timestamp_ntz_micros` / `timestamp_ntz_nanos` | `Timestamp { precision: Micros\|Nanos, timezone: None }` |
| `string` (short and long forms) | `String` |
| `binary` | `Blob` |
| `array` | `Array { element_type, elements_nullable }` |
| `object` | `Struct { fields }` |

`_meta` extras that Variant lacks (the emitter must shred to a compatible carrier):

- `U8..U64` — Variant has no unsigned ints. Promote to the next-wider signed int; `U64` shreds to `String` or `decimal8` to avoid overflow. Operator policy chooses.
- `Uuid` — shred as fixed-size `binary` (16 bytes) or `string`. Default `string` for human readability; `binary` when a column-typed `uuid` carrier is available.
- `Ipv4` / `Ipv6` — shred as `string` (presentation form) or fixed `binary` (4 / 16 bytes).
- `Clob` — shred as `string`.
- `Enum { values }` — shred as `string` (or `int32` if a per-column dictionary is available downstream).
- `EntityRef` — shred to its `key_type` (typically `string` or `uuid`); the `target_type_id` becomes a column-level annotation, not a per-value field.

Map shape: Spark Variant does not have a distinct map type — high-cardinality maps must serialize as objects with arbitrary keys. The shapez `Map` decision still drives a *shredding* plan that promotes the map-value shape uniformly, even though the wire format remains a Variant object.

The reverse direction (Variant → shapez) is trivial: every Variant primitive lifts to its `ValueType` counterpart, every Variant container drives the same `JsonEventSink` events as JSON would.

This table is what the shredding-advice emitter consults. The analyzer itself is unaware of the Variant target; it only produces shapes and a `PromotionPlan`.

## Where this fits in a host system

- **Ingest path.** The host routes documents into shapez (sampling-aware), receives per-tile schema and per-path promotion advice in return.
- **Columnar buffer storage.** The `PromotionPlan` becomes typed columns; the residual goes to JSONB. Tile self-containment means the buffer reads each tile from its header.
- **Iceberg / Spark Variant export.** The same `PromotionPlan` drives shredded-variant emit: promoted paths become typed sub-columns of the variant, the rest stays in the variant blob. shapez does not write the binary; it tells the writer where the seams go.
- **Optimizer statistics.** Per-tile sketches aggregate to relation-level statistics; the query planner consumes them (Substrait or internal).
- **Data contracts.** Operator-declared assertions on shape produce the exception stream that feeds alerting and audit. shapez is the single source of truth for "what shape did the data actually have."

The cluster-aware ROI advisor (planned phase 4.5) is where shapez stops describing shape and starts costing layout decisions; that is the seam where shapez ends and the downstream layout engine (buffer writer or Variant writer) begins.
