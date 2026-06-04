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
2. **Dual-view ingest, decisions deferred to report time.** Both record-and-map (and tuple-and-bag) views accumulate during ingest; the call is made at finalization using cardinality, value-shape stability, arity distribution, and key-pattern signals. This avoids early commitment on ambiguous data. Because the dual view is per-position, wildcard injection composes recursively — see *Wildcard injection at intermediate steps* below.
3. **Option A lazy variant emergence at leaves.** Paths start as `Absent`. First scalar observation creates the leaf; observation of a different type wraps it in a `Variant`. Pays the allocation cost only when needed.
4. **Cluster sketches surface variants at compound positions.** Per-element subtree signatures feed a per-step Space-Saving sketch. Top-K become Variant arms at finalization; an eviction-rate chaos signal can mark a position as inherently diverse and stop clustering.
5. **Sampling beats per-event specialization.** Sampling 5% of documents at full fidelity buys ~98% of the optimal sketch state for far less cost than per-event handler tricks could deliver.
6. **Two-tier sketches: tile-local + bounded global.** Each statistic is kept twice — a tile-local accumulator that produces the tile's frozen dictionary, and a bounded global sketch that carries cross-tile memory with LRU-style replacement. JSON-Tiles parity for storage; long-lived memory across tiles.
7. **Epochs make drift cheap.** Dictionary stability is decoupled from data drift by construction. A mid-epoch drift costs you a suboptimal dictionary for the rest of that epoch; the next boundary catches up. No online detector required.
8. **Falsifications are structured streams, not strings.** A `ShapeException` is a typed value carrying the exemplar back-pointer, the violation set, and a cluster key. Recurring violations collapse into sessions so consumers see signal, not volume.

## Wildcard injection at intermediate steps

A standout property of shapez — and one we don't see emphasized in most adjacent work — is that *any* intermediate path step with extreme cardinality can be promoted to a wildcard, and that wildcard injection composes recursively. This is the mechanism that lets the analyzer do real structural work *under* high-cardinality maps, where the conventional move is to give up and dump the subtree into JSONB.

### The problem

Real semi-structured streams routinely have data-keyed maps somewhere in the middle of the path:

```json
{
  "flags": {
    "550e8400-e29b-41d4-a716-446655440000": { "enabled": true,  "rollout_pct": 50 },
    "6f9619ff-8b86-d011-b42d-00c04fc964ff": { "enabled": false }
  }
}
```

A naive inferencer sees thousands of distinct fields at `.flags.<uuid>` and concludes "this is unstructured." Every leaf under the map step collapses into a JSONB tail. The recurring leaf shape (here: `{enabled: bool, rollout_pct?: i64}`) is lost — even though every UUID's value follows the same template.

### The mechanism

Each object position in the shape tree carries a **dual view**:

- A bounded **record view** holding the first K distinct keys observed, each with its own per-key child accumulator.
- A **map_value** aggregate: a single child node that accumulates *all* observed values regardless of which key they came in under.

When the record view overflows at level N — more distinct keys than the cap K — that level commits to a map decision and every subsequent observation routes through `map_value`. Crucially, `map_value` is a full node with its own dual view, its own scalar arms, its own array/object accumulators. So the same drop-and-route logic fires independently at level N+1 if cardinality is also extreme there. The canonical `PathPattern` accumulates a wildcard step per overflow:

```
.flags.*.enabled                 — one level injected
.users.*.events.*.kind           — two levels injected
.metric.*.tags.*.value           — two levels injected
```

There is no special multi-level case in the implementation. The recursion at every level uses the same code path — the dual view is per-position, not per-tree, and the cascade is an emergent property.

### Pooling is what makes it work

Per-key data at a high-cardinality step is *sparse by construction*. If a stream has 10 000 distinct UUID keys over 1000 documents, each key appears in only a handful of documents — far too few to support per-key shape inference. A per-key analysis would have nothing to work with.

But `map_value` aggregates across all keys. After the overflow, every UUID's payload pools into the same accumulator, and the leaf-level statistics there are dense even when each individual key's history is sparse. That is the difference between "this map is opaque" and "this map's values are records with two stable fields, one nullable."

The same argument runs at each recursive level. With two stacked wildcards (e.g. `.users.*.events.*`), 1 000 docs × 50 outer keys × 10 inner keys × 1 leaf = 500 000 leaf observations all pooled into one accumulator — even though the average per-pair sees only a single event.

### Storage and shredding consequence

The canonical wildcard path is still a *promotable* path. `.flags.*.enabled` becomes a typed column behind a map step in a column-store host (with a sidecar key column or composite-key encoding recovering the UUID per row), or a shredded sub-column of a Spark/Iceberg variant object. Whether the host materializes the map natively as `Map<String, Struct{...}>` or as a typed sub-column projected out of the variant blob, the advice from shapez is the same: this leaf is worth typed storage.

This is materially stronger than the "high-cardinality map = opaque JSONB" rule from JSON Tiles and most variant-shredding tooling we've encountered. Promotion advice survives an arbitrary number of intermediate wildcards; map steps stop being a termination condition for structural analysis.

### Limits, honestly stated

- Detection is heuristic. The current rule is record-view-overflow at a fixed cap (K=64) plus a cardinality-vs-mean-keys ratio. False negatives at small data sizes are possible; planned HLL key-cardinality sketches will improve this without changing the wildcard mechanism itself.
- Pre-drop per-key children do not automatically pool back into `map_value` at finalization. If the drop happens late and per-key data dominates by sample count, the map-view's leaf signal is thinner than ideal. A future refinement merges retained per-key accumulators into the map view at finish-time.
- "Map of maps" is handled at any depth, but if the same path holds *both* a record-shaped object in some docs and a map-shaped object in others, the dual view collapses both into one position; variant emergence at the object/map boundary is not yet automatic.

These are refinement issues, not gaps in the core mechanism.

### Why we keep underlining this

Most schema-inference and variant-shredding work we've surveyed treats high-cardinality intermediate steps as a termination condition for structural analysis. We deliberately do not, and the design follows through end to end: dual view at every object position, wildcard preservation in `PathPattern`, leafward analysis under `map_value`, and shredding advice that includes the wildcard steps in its promotion plan. The mechanism is small, but the consequences ripple through the entire pipeline — from inference cost (pooled observations converge fast under hot wildcards) to storage layout (typed leaf columns behind map steps) to query planning (predicate pushdown survives the wildcard).

## Stable contracts

What downstream code can build against and expect to keep working.

- **`shapez::ingest::JsonEventSink`** — the input protocol. Document boundaries are explicit; scalars carry their concrete type (i64/u64/f64 split); strings cross as `&str`. Adapters drive events; the analyzer accumulates.
- **`shapez::ingest::Analyzer`** — `JsonEventSink` plus `finish(self) -> ShapeNode`.
- **`shapez::node::ShapeNode`** — the inferred shape language. `Type(ValueType)` covers the cases ValueType expresses; rich compounds and meta-nodes carry the rest. Storage layers consuming `Type(ValueType::X)` need no conversion at the boundary.
- **`shapez::exceptions::{ShapeException, ExceptionSink}`** — the falsification stream and the trait through which it flows. The session-aware sink is the production form; testing sinks are simpler.
- **`shapez::session::{ExceptionSession, ExceptionEvent, ShapeDelta, CloseReason}`** — the open/delta/close protocol for clustered exceptions. Wire format for exception consumers.

Everything else — choice of HLL implementation, Space-Saving cap, sample-rate control law, decision thresholds, finalizer strategy — is implementation and may change without affecting consumers that respect the contracts above.

## Cost, briefly

If this seems expensive: yes it is, tweak the sample rate if you want.

The per-document work is non-trivial in the honest sense — every observed value walks an arena lookup, several `BTreeMap` operations, a `Sig` allocation, a Space-Saving update, a string-format detect, and a numeric / string stats update. On a sampled document we are doing real CPU. The design accepts that cost because of how rare sampled documents are: at the default 5% sample rate, 95% of documents pay a single counter bump and get out, and the full pipeline runs on the remaining 5% (~50 documents per 1024-doc tile, per the BTRBlocks finding that this buys ~98% of the optimal sketch state).

That sample-rate dial is the primary cost lever; everything else is secondary. The knobs you have, roughly in order of how much they move the needle:

- **Sample rate.** Defaults to 5%, auto-tunes against a surprise signal, with operator-settable floor (~1%) and ceiling (~15%). Halving it ~halves the analyzer's CPU footprint at the cost of slower sketch convergence on drift.
- **`record_view` cap (K=64), `positional_view` cap (K=32), cluster cap (K=16), skeleton cap (K=16).** These bound per-node memory and per-event sketch work. Reducing them trades signal fidelity at the long tail of the distribution for lower cap-driven overhead.
- **Tier-based processing.** Tier 0 (fresh in RAM) can skip shapez entirely or run at 1%; Tier 1 (warm tile) is where the 5% lives; Tier 2 (aged file) can crank to a fuller scan if storage budget warrants. The cost split is operator policy.
- **Format-detection and skeleton work** runs only on sampled documents and only at observed string positions. Disable via a (planned) profile flag if your data is known not to benefit; the savings are modest compared to sample-rate changes.

What we don't currently do that would lower cost further: simd-json tape walks (planned `shapez-simdjson`), per-tier specialization beyond sampling rate, and adaptive cap tuning. Phase 1 prioritizes correctness on the corpus over absolute throughput; the simdjson adapter and the tile / epoch model are the next two big steps once Phase 1 lands.

## Driving the analyzer

shapez has two operating modes, sharing the analyzer:

**Streaming (the default).** A long-lived `StreamingAnalyzer` consumes events as documents arrive. The caller drives values one at a time via the `JsonEventSink` interface (a Kafka consumer in a service binary, a webhook handler, a tail-following log adapter — whatever the host wants) and periodically inspects the in-progress state via `analyzer.summary()` / `analyzer.report()` for monitoring or alerting. `analyzer.finish()` is typically never called: the stream doesn't end and consuming the analyzer would discard the accumulated state. shapez doesn't own the consumer lifecycle, only the analysis state.

**Batch.** A bounded source — a directory of files, a Kafka offset range, a SQL query, a time-windowed set of S3 objects — drives docs into a fresh analyzer that runs to completion. `analyze(source, policy) -> AnalysisOutcome` is the single entry point. Common use cases:

- First-time analysis of an existing dataset ("infer the shape of last month's events").
- Operator-triggered re-analysis: same data, different policy. Higher sample rate, larger record_view cap, a new format detector. Operators often call this *retraining*; mechanically it's just another batch run with `policy.reason` populated for the audit log.
- Ad-hoc investigations: scoped slice + tight policy + concrete answer.

The rest of this section is batch specifics — sources, time predicates, policy provenance, bailout. The streaming path is unchanged across all of it; both modes share the analyzer and its policy, and a batch run is just "a streaming run over a bounded source that calls `finish()` at the end."

### Sources and the `DocumentSource` trait

A `DocumentSource` produces a stream of `serde_json::Value` documents and drives them into a `StreamingAnalyzer`. Sources are pluggable so backend dependencies live in their own crates rather than in shapez core.

Implemented now:

- **`JsonlDir`** — local directory of `*.jsonl` files, recursive walk, deterministic order, closure-based path filter (callers plug in `regex::Regex`, glob match, suffix check, or whatever). Zero new dependencies.

Planned, with the dependency cost called out so adopting any of them is a deliberate choice:

- **Cloud / network object store.** A wrapper around `object_store` (or `opendal`) covering local FS + S3 + GCS + Azure under one async API. Pulls in `tokio`. Lives in a sibling crate like `shapez-objstore`. The `analyze` entry point doesn't change — it's the same trait — but the source impl is async-internally and blocks at the `drive` boundary.
- **Columnar files (Parquet / ORC).** A source that opens a Parquet or ORC file (local or via the cloud crate), pulls one column by path, and emits each value as a `serde_json::Value`. Depends on `arrow-rs` / `parquet`. Useful for "I want to analyze just `.user.email` from this file." Compile-time cost is significant; lives in a `shapez-columnar` sibling crate behind a feature flag.
- **Kafka topic.** A `rdkafka`-based source that reads a topic between operator-specified offset bounds, decodes each message's value, and emits it as a `serde_json::Value`. Lives in `shapez-kafka`. Kafka is a batch source just as much as a streaming one — multi-day retention on a topic means an analyzer can look at "the last 48 hours" by configuring the offset range, and a retrain can replay an old window for as long as the data is still on the brokers. The configuration surface is wider than the file-based sources because Kafka values aren't always JSON (see *Value decoding and path prefix* below).
- **SQL query.** A source that opens a database connection, runs an operator-supplied query, and emits each row as a `serde_json::Value::Object`. Depends on `sqlx` (or `postgres` + `mysql` for sync). Useful for analyzing a host's metadata schema or a slow-changing dimension. Lives in `shapez-sql`.

All five follow the same shape: implement `DocumentSource::drive(sink) -> Result<docs, ReplayError>` and `describe() -> String`. The `analyze` function is dependency-blind — it sees only the trait.

### Value decoding and path prefix

Two cross-cutting concerns surface as soon as sources stop being plain JSON files:

**Value decoding.** Kafka, Parquet, and SQL all deliver values whose on-the-wire encoding is not JSON. The source impl owns the decode step and produces a `serde_json::Value` (or whatever in-memory model the host adapter wants — eventually the JsonEventSink interface accepts events directly without any intermediate Value, but for the v1 sources we go through Value). Each source's constructor takes the decoding config it needs:

- **Kafka**: a `value_format` enum — JSON (just `serde_json::from_slice`), Avro (schema from a pinned `.avsc` file or a Confluent Schema Registry URL + subject), or Protobuf (a compiled `.proto` plus the message name). Plus a `key_format` for the key half of the record when the operator wants to look at keys too.
- **Parquet / ORC**: column type from the file's schema; no operator config needed for the decode itself.
- **SQL**: column-to-JSON-type mapping driven by the database's metadata, but the operator can override (e.g., declaring a varchar column should be parsed as JSON).

**Path prefix.** Once decoded, the structural payload often lives in a subtree of the value, not at the root. CloudEvents wrap their domain payload in `.data`; Debezium CDC events have `.payload.before`, `.payload.after`, `.payload.source`; Kafka topics from internal services often look like `{headers: {...}, body: <the actual thing>}`. The operator wants to point shapez at the actual thing.

Every `DocumentSource` exposes `.at(path: shapez::path::Path)` for this. The path uses shapez's existing `Path` syntax (`.payload`, `.event.data`, `.items[0]`). At drive time, the source decodes the message, walks into the subtree, and feeds *that* to the analyzer. Documents whose path doesn't resolve are silently skipped — there's no value in feeding the analyzer documents that don't have the structure we're investigating.

The `.at()` builder is uniform across sources; the implementation belongs on each source because it composes with the source's own decode step. The audit trail (`AnalysisOutcome.source`) includes the path so a replay knows where to look.

### Beyond JSON: any machine-readable syntax

JSON is the first feedstock shapez has wired up, but the analyzer doesn't care about wire format. Anything with a machine-readable syntax that lifts cleanly to a JSON-isomorphic event stream qualifies as a feedstock — the `JsonEventSink` trait names the **event vocabulary** (null / bool / i64 / u64 / f64 / string + array + object + key), not a requirement that the input be JSON.

Concrete near-neighbors worth naming:

- **Protobuf.** Messages decode to event streams trivially; the interesting case is wide `oneof` clauses, which are exactly the variant emergence the cluster sketch is built for. A proto with `oneof event { ViewEvent view = 1; PurchaseEvent purchase = 2; SystemMetric metric = 3; ... }` and 47 arms is the polymorphic-array case in disguise — shapez should produce a Variant of the arms each present in the actual traffic, ignoring the long tail of arms the .proto declares but the data never uses. The adapter needs the compiled descriptor; the analyzer does the rest.
- **CSV / TSV / fixed-width.** No real schema in the data — header rows are advisory and types are conventional. This is where shapez earns its keep most loudly: each row becomes an object event, the column header gives the field name, and the analyzer figures out the actual types, ranges, format families, length distributions, and which columns are functionally nullable. A "varchar(255)" promise becomes "97% are UUIDs in this dataset, the other 3% are emails — neither is text-like."
- **Avro.** The schema is explicit, so leaf typing isn't a discovery problem. But Avro unions and nested records still benefit from variant clustering and record-vs-map detection at decode time: the schema tells you the *possible* shape, the analyzer tells you the *actual* one observed in this dataset. Useful for "this union has 12 arms but only 3 show up in practice."
- **Apache Arrow / columnar in general.** Decoding to events is mostly mechanical row-major iteration over the typed columns. The analyzer's contribution is the same as above — surfacing variant-arm reality, format detection on string columns, range hints on numeric columns.
- **XML.** Possible but awkward, and we won't pretend otherwise. Mixed content (text interleaved with elements), attributes-vs-elements ambiguity, and recursive content models don't lift to JSON's event vocabulary without an opinionated mapping. Doable if someone really wants it; the adapter ends up making choices the data alone can't justify.

What unifies these: each format has a machine-readable syntax that constrains values *in principle*, but the actual shape distribution in any particular dataset is much narrower than the syntax permits. The syntax bounds the universe; shapez characterizes the population. That's true whether the constraint is JSON Schema, a `.proto` file, an Avro schema document, a CSV header, or implicit ("trust the column names"). The analyzer's job is the same in every case.

We aren't committing to adapters for any of these now. Each lives in its own sibling crate when the time comes, on the same `DocumentSource` contract as the JSON adapters — decode events, drive the analyzer, return an `AnalysisOutcome`. The trait shape doesn't change; only the decoder does.

### Lookback outside the time window (planned)

The `TimePredicate` describes the **data window** the operator cares about. It does *not* describe the metadata the source needs to read in order to materialize values inside that window. Sources may legitimately have to look outside the predicate's bounds — sometimes far outside — to find:

- **Avro / Protobuf schemas via Schema Registry**: a Kafka message at offset O decoded with schema-id S requires fetching S, which was registered whenever it was registered. Not in the data window.
- **Iceberg / Delta manifests and transaction logs**: figuring out which data files belong to a snapshot in the window means reading the manifest list and possibly older snapshots' chain back to the table's current state.
- **Parquet row-group dictionaries**: the dictionary page sits at the start of its row group and must be read to decode any value in the group. Usually within the same file, but the read is unavoidable.
- **Compacted Kafka topics**: the "current" value for a key may live at an offset arbitrarily older than the lookback window. Compaction guarantees the latest survives; it doesn't put it inside any particular range.
- **Debezium snapshot baselines**: CDC change events make sense only relative to the snapshot that established the initial state. Analyzing CDC requires the snapshot even if it predates the window.
- **External schema documents**: an OpenAPI spec, a JSON Schema file, a `.proto` definition — all may live in a different store and a different time entirely.

Each source handles this internally — it's not a user-visible API concern. The `TimePredicate` constrains the *data* the analyzer sees; the metadata fetch is whatever the backend needs to honor that promise. Source impls should document their own metadata-read behavior so operators understand what their batch run is actually touching.

A pathological case to guard against: a metadata-fetch that effectively reads the whole table when the operator asked for "the last hour." Sources should fail loudly (or at least surface a warning in `AnalysisOutcome`) when the metadata read substantially exceeds the data read, so the operator knows their tight scope didn't translate into a tight workload.

### `AnalyzerPolicy` and provenance

Every run carries an `AnalyzerPolicy` bundling the cost knobs from the *Cost, briefly* section: record_view / positional / cluster caps, planned sample rate, format detector list, assertion set, plus an audit `reason` string. The outcome (`AnalysisOutcome`) stamps the policy back into the result alongside the source description, so any run is reproducible by *"same source description + same policy + same data."*

The `reason` field is conventionally empty for routine streaming ingest and populated for batch runs where the operator wants to record *why* this run happened ("investigation: missing-field alert on .user.email", "weekly re-analysis with new format detector"). It's a string, not an enum, so any audit semantics live in whatever audit log consumes it — shapez doesn't try to enforce a vocabulary.

### Bailing out when the chaos gets too spicy

A run can grind through pathologically wild input forever before the operator's question gets answered. If every observation is evicting something out of the cluster sketch, the data isn't clustering at any rate and there's no point continuing. The analyzer surfaces a stop signal, and sources are expected to honor it.

The mechanism:

- `AnalyzerPolicy` carries three opt-in bailout knobs: `max_eviction_rate` (e.g., 0.5 = fail if half the docs are evicting cluster entries), `max_docs` (hard cap for "show me what you've got after N docs"), and `min_docs_before_bail` (warmup gate, default 100 — sketches need samples before their rate is meaningful).
- `StreamingAnalyzer::should_bail()` returns `Some(reason)` when any threshold trips. Cheap O(1) — total cluster evictions are maintained incrementally as `SpaceSaving::observe` returns whether it evicted.
- `DocumentSource::drive` implementations are expected to poll `sink.should_bail()` after every doc and exit the loop cleanly when it returns `Some`. The `analyze` function then checks once more and stamps `bailed: Option<String>` on the outcome.

This applies equally to a first-time pass and to a re-analysis with adjusted policy — chaos is chaos, and the operator might just as well want a fast fail on the initial pass through a new data source as on a follow-up over old data.

### Time predicates across sources

Run scope is rarely "every byte ever stored." Operators want windows — "the last 24 hours," "October 2023," "between when we deployed v2.1 and now." The machinery exposes a uniform `TimePredicate { start, end }` that each source interprets in whatever way its backend supports:

- **`JsonlDir`** filters by file modification time. JSONL has no row-level metadata so the granularity is per-file; `.within(predicate)` excludes files whose `mtime` falls outside the window.
- **Cloud / object_store** sources filter by object last-modified header — server-side where the API allows, client-side otherwise.
- **Parquet / ORC** sources filter by file modtime *and*, if the schema includes a designated time column, push the predicate into the column scan as a min/max stat filter.
- **SQL** sources synthesize a `WHERE` clause on an operator-designated time column.

The trait stays uniform (`source.within(predicate)`) but the semantics vary because the backends do. Documenting how each source interprets the predicate is part of the source's contract — the `analyze` function itself remains backend-blind.

A future refinement allows the predicate to designate a JSON-path-and-parser pair (e.g. *"the value at `.ts` parsed as epoch millis"*) so content timestamps can drive the filter for sources whose backend metadata is absent or lying. Until that lands, file / object / row metadata is the only honest time signal.

### Retention and replay

A batch run needs the data it's going to analyze. Two regimes:

**Ephemeral.** The streaming side already passed the data; documents are gone. The only source available is the per-tile state the analyzer kept — sketches, residual JSON tail, per-key samples that survived eviction. A batch run in this regime is *constrained*: you can apply new format detectors to retained strings, re-run record-vs-map at a different cap, recompute percentiles with new bucket choices — but you cannot recover information about documents you never sampled in the first place.

**Replayable.** The source is still around. Kafka topics with multi-day retention, Iceberg or Delta tables in a data lake, archival JSONB columns in a warehouse — all support full replay. The batch run reads from one of the durable `DocumentSource` impls at any sample rate including 100%, runs format detectors over the original strings, and discovers patterns the streaming pass would have skipped. Strictly more powerful, strictly costlier.

Both regimes use the same `analyze` entry point and produce the same `AnalysisOutcome` shape. The host arranges source access; shapez sees only the trait.

A consequence: the per-tile residual format choice (the JSONB / CBOR / MessagePack discussion in `JSON_TILES_ROADMAP.md`) is load-bearing for ephemeral batch runs. If the residual is lossy, ephemeral re-analysis is correspondingly less useful.

### Composition with the epoch model

When the storage layer uses the two-tier sketch + per-tile frozen-dictionary design:

- Each tile carries a self-contained dictionary in its header. A batch run with adjusted policy produces a **new dictionary version** for the affected tile. Readers using the old dictionary continue to work — the previous version doesn't get clobbered, it just stops being the default.
- The **global sketch** for the relation may need rebuilding once enough tiles are re-analyzed. This is the same merge operation as initial ingest, just sourced from already-stored tile-local sketches.
- The **cross-epoch monotonic improvement** property still holds: an adjusted-policy run is no less informed than the original because the underlying data is the same and the policy is strictly different (often strictly richer).

### Non-goals for batch runs

- **Not real-time correction.** Batch is for offline analysis. Real-time enforcement is what the assertion / exception stream is for — a separate feature on the streaming path.
- **Not automatic.** The system surfaces signals that *suggest* a batch re-run might help (high cluster-eviction rates, dense assertion violations, suspicious skeleton patterns), but never triggers one. The decision to spend cycles is the operator's.
- **Not retroactive on already-shredded storage.** A batch run produces new shape advice and a new promotion plan. Whether downstream storage layers act on it — re-shred columns, rewrite variant blobs, rebuild indexes — is the host's choice and policy.
- **Not a substitute for assertions.** Assertions enforce expectations in real time on every streaming document. Batch is for *discovering* expectations, not for enforcing them.

### Why this matters for the wire contracts

The `ShapeNode` tree and `PromotionPlan` should be designed from day one to carry their **policy provenance**: the sample rate, cap settings, format detector list, and assertion set under which they were produced. Any batch run — first-time or follow-up — is then expressible as *"give me the same scope, these knobs, this reason"* and reproducibility is built in. Without this, a re-analysis is "looks like that one over there but slightly different," which is exactly the failure mode that makes drift impossible to track in conventional schema-inference tools.

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

## A tenuous connection to grammar induction

Worth naming out loud because the field is interesting and almost never enters the schema-inference conversation: what shapez does is, by another name, *grammar induction* over the language of JSON value trees. We don't talk about it that way because the audiences barely overlap — grammar induction lives in formal-language theory and computational linguistics, while schema inference lives in databases and data engineering — but the structural moves are recognizable to anyone who has read the GI literature.

### The mapping, made explicit

| shapez | grammar induction |
|---|---|
| `ShapeNode` tree | schema grammar; each node is a non-terminal |
| `Variant { arms }` at a position | a disjunctive production at that non-terminal |
| Wildcard injection (`.foo.*`, `.[*]`) | introducing a regular non-terminal that ranges over an unbounded vocabulary at a position |
| Space-Saving cluster sketch at array elements | frequency-based merging of element-language non-terminals (cf. Stolcke–Omohundro 1994 Bayesian state merging) |
| `record_view` overflow → drop to `map_value` | merging per-symbol productions into a single recursive production once cardinality exceeds an MDL-style threshold |
| `StringFormat` detectors (UUID, ISO timestamp, …) | terminals tagged with regular sub-grammars; format detection is a lexer over the value alphabet |
| `_punct`-style skeleton sketch | learning a regular template at a position whose terminals have been collapsed |
| Sampling-driven analysis (BTRBlocks 5%) | approximate inference: trade exact recognition for cheap convergence on the head of the distribution |
| Per-tile dictionary + cross-tile global sketch | piecewise grammar induction with shared symbol table; tile-local productions inherit from a global vocabulary |

Several mechanisms shapez treats as engineering choices (when to merge per-key children into a wildcard, when to spawn a new variant arm, when to keep a noise cluster vs evict) are exactly the *core* algorithmic decisions in GI, where they go by names like *state merging*, *minimum description length*, *Bayesian prior on grammar size*. The shapez heuristics are crude where the GI literature has full formalisms — and we know it.

### What we deliberately don't inherit from GI

- **No soundness ambition.** A grammar induced by an academic GI algorithm is meant to *recognize* the target language. Our shape tree is meant to *describe the head of the observed distribution* and drive promotion advice. We deliberately fold rare arms into a residual via cluster eviction; a sound GI algorithm would refuse to.
- **No PAC/MDL formalism.** Our thresholds (K=64 for record_view, K=16 for cluster, mode_share ≥ 0.9 for tuple) are operator-tunable engineering knobs, not derivations from a description-length objective. We pay for the simplicity in occasional false negatives at small data sizes.
- **No CFG expressiveness yet.** Our shape grammar is *regular* in the formal-language sense: no non-terminal references itself, so genuinely recursive structures (binary trees, nested ASTs, JSON-LD graphs) get unrolled into a finite shape and lose their recursive nature. Adding a recursive `Ref(NodeId)` shape kind is the obvious extension; it's deliberately out of scope for Phase 1.

### What the two sides could learn from each other

**What GI could offer us:**

- Principled merge / split criteria. The record-vs-map and variant-vs-merge decisions could be reformulated as MDL trade-offs (one nullable struct field vs N variant arms) and choose the description-length winner automatically. This would replace the K=64 cap with a data-driven boundary.
- A vocabulary for talking about the global vs tile-local sketch interaction. Shared-symbol-table GI maps well onto our two-tier sketch design and has a richer body of theory behind it.
- A framework for proving the soundness of the wildcard cascade: under what input distributions does the per-level dual view converge to the right call?

**What we could offer GI:**

- A worked example of approximate GI at industrial scale, where exact inference is structurally infeasible and the storage-layout-driven sampling is a feature rather than a compromise.
- The "head of the distribution is the schema, the tail is the residual" framing as a first-class design tenet, complete with shredded-variant storage as its downstream consumer. Most GI work treats outliers as adversarial; we treat them as compressible.
- A type-rich input model (already-parsed `meta_types::value::Value` trees with `ValueType` tags) that lifts most of the lexer / terminal problem off the GI algorithm's plate.

The connection is genuine even if the vocabulary doesn't translate cleanly across the disciplinary boundary. We don't claim to be doing PAC-learnable grammar induction, but a future version of shapez that imports MDL scoring and a recursive `Ref` shape kind would not be a different system — it would be the same system, more crisply justified.

## One person's syntax, another's semantics

A guiding observation that's worth naming: as data moves through layers, what was *structure* at one level becomes *meaning* at the next, and what was meaning compresses back down into structure. shapez sits on one of these layer boundaries by design, and the framing shows up in nearly every decision the analyzer makes.

At the wire layer, JSON bytes are pure syntax — `[52, 50]` carries no meaning. Parsing produces `Value::I64(42)`, a semantic value with a type. One layer in, one increment of meaning.

At the value layer, the 36-character string `550e8400-e29b-41d4-a716-446655440000` is still just syntax: a particular sequence of hex digits and hyphens. Once `detect_format` runs over it, `StringFormat::Uuid` is the new semantic — *meaning created from pattern*. The skeleton sketch does the same trick for unrecognized strings: `ORD-9999-NNNNNN` becomes `A-9-9`, which is syntax to the sketch and semantics to the operator reading the report.

At the shape layer, object keys are themselves syntax — character sequences identifying child positions. But the analyzer's record-vs-map decision treats their *cardinality* as semantically load-bearing: high cardinality says "these are data, not metadata," a meaning that didn't exist at the key-string level. The promotion-advice layer then takes that newly-created meaning and renders it back as syntax: a wildcard `.flags.*.enabled` in a `PathPattern`, which the storage layer reads as "a column-promotable path behind a map step."

And meaning is destroyed at layer transitions too. A `Uuid` semantic that shapez detected at the leaf may become a `Blob` column at the storage layer, losing the format hint. A `Variant{post, like, follow}` at the cluster sketch becomes "the dominant arm plus a residual blob" once the shredder commits to a layout. The typing information was real; the writer dropped it on purpose for a different objective.

What shapez does, fundamentally, is promote *structural recurrence in observed values* into *semantic shape categories* (record, map, variant, format, range), then re-emit those categories as syntax (column names, types, wildcard paths) for whatever consumes the `PromotionPlan`. The principle isn't shapez-specific — every layer in a data system does this, often without naming it. Naming it makes it easier to notice when a layer is doing the work it claims to do, and when it's quietly dropping signal on the floor.

### Recursive syntax shifts (TODO)

The layer transitions don't stop being interesting just because we've crossed one. Real systems happily embed one syntax inside another — a CSV column whose values are JSON blobs, a JSON field whose value is a CSV-encoded substring, a log line that's free text up to the first `{` and JSON from there to the end, URL query strings as key/value soup inside a string column, base64-wrapped binary that's itself a protobuf, JSON-in-string-in-JSON-in-string nested several layers deep because every system on the path "didn't want to deal with structured data right now." Chocolate, peanut butter.

shapez today stops at the outer layer's string boundary: a string is a string, gets a `StringFormat` classification, a length sketch, possibly a `_punct` skeleton. The next step, not built yet, is to *recurse* — when a leaf string consistently parses as a known machine-readable syntax (JSON, CSV, URL-encoded, …), the analyzer could treat the inner structure as if it were directly observed and produce a sub-shape at that position. The user-visible result: a column declared `varchar` that's actually 92% JSON objects with two recurring shapes gets reported as `Variant{ String, Json::Variant{...} }` rather than just `String`, and the inner promotion plan flows through alongside the outer one.

Sketch of the requirements:

- **Cheap detection at the leaf.** A string starting with `{` or `[` and parsing successfully is JSON-ish; one with consistent comma counts per line is CSV-ish. Detection has to be conservative — the cost of a false positive is wasted recursion on data that wasn't meant to be structured.
- **Per-position opt-in.** Recursing on every string is too expensive and noisy. Operators (and the cluster sketch's eviction-rate signal) tell us where the recursion is likely worth it.
- **Honest layering in the report.** Inner shapes nest cleanly into the outer shape tree; the path syntax extends so `.payload@json.user.id` is addressable as a thing.
- **Bounded depth.** Recursion has a configurable cap. Beyond it, treat the inner string as opaque rather than risk turtles all the way down.
- **Format-detection alignment.** This is the same logic as `detect_format` at a higher level — every layer up the stack, `StringFormat::Other` → "this looks like syntax X" is the same kind of promotion the analyzer already does for UUID / timestamp / etc.

Real-world value scales with how badly the systems on the path mistreated their data. In well-typed pipelines, this feature does nothing. In log-aggregator–shaped pipelines, it does most of the work.

### Bitstream-level syntax discovery (long-horizon TODO)

The recursive-syntax-shift story above assumes we have a *hypothesis* to test — "this string looks like JSON." A deeper version doesn't assume any hypothesis at all. Sketch of a design that's been bouncing around for a while:

1. **Start with a bitstream**, no a priori knowledge of syntax, framing, encoding, or even whether the input is text.
2. **Build streaming frequency tables** of single characters, bigrams, and *maybe* trigrams. Bounded-memory; only the head of the distribution matters.
3. **Recognize characteristic distribution patterns** as fingerprints of candidate syntaxes. `}` + `,` + `nu` + `,[` is almost certainly JSON; `<` + `</` + `="` looks like XML/HTML; lots of commas and consistent newline cadence suggests CSV; high-entropy printable ASCII with `=` and `&` is URL-encoded; etc. Each candidate is a hypothesis, not a commitment.
4. **Fork into multiple speculative parsers** for the candidate syntaxes. Each one runs against the same stream; any number of modules that have not yet declared bankruptcy (parse failure, internal contradiction, runaway error rate) proceed in parallel. The winner is whichever module is still standing after a confidence threshold is reached.
5. **Each syntax module exposes its child structure** — JSON's object fields and array elements, CSV's columns, XML's child elements, etc. — and the analyzer recurses into each child position the same way it does today on known-format data.
6. **If chaos at any leaf gets too high** under the chosen parser — high cluster eviction rate, runaway format diversity, length distribution that doesn't make sense for the inferred type — *recurse back* into character/bigram analysis on that descendant. It will often turn out that a column-the-parent-called-a-string is itself a structured payload in some other syntax, invisible to the parent's parser.

This composes with the existing analyzer rather than replacing it. The character/bigram tables are a sketch like any other; the candidate-parser machinery is a layer above the `DocumentSource` trait that produces events from already-decoded values; the chaos-driven descent is the same `should_bail`-style signal already wired into the analyzer.

It's also the principle from *one person's syntax, another's semantics* taken to its logical extreme. We don't just promote known syntactic patterns into semantic shape categories — we *discover the syntactic boundaries themselves* from the data, then promote. Meaning is created at boundaries we infer, not just at boundaries the caller declared.

Scope-honesty: this is the kind of feature that runs for a year before it's reliable, and the design will look different by the time it lands. Captured here so the framing — bitstream → ngram fingerprints → speculative parallel parsing → recursive descent into chaos-shaped leaves — survives between sessions of actually working on it.

## Where this fits in a host system

- **Ingest path.** The host routes documents into shapez (sampling-aware), receives per-tile schema and per-path promotion advice in return.
- **Columnar buffer storage.** The `PromotionPlan` becomes typed columns; the residual goes to JSONB. Tile self-containment means the buffer reads each tile from its header.
- **Iceberg / Spark Variant export.** The same `PromotionPlan` drives shredded-variant emit: promoted paths become typed sub-columns of the variant, the rest stays in the variant blob. shapez does not write the binary; it tells the writer where the seams go.
- **Optimizer statistics.** Per-tile sketches aggregate to relation-level statistics; the query planner consumes them (Substrait or internal).
- **Data contracts.** Operator-declared assertions on shape produce the exception stream that feeds alerting and audit. shapez is the single source of truth for "what shape did the data actually have."

The cluster-aware ROI advisor (planned phase 4.5) is where shapez stops describing shape and starts costing layout decisions; that is the seam where shapez ends and the downstream layout engine (buffer writer or Variant writer) begins.
