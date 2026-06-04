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

A property of batch mode worth calling out explicitly: **it makes design iteration cheap.** Streaming pipelines force caution about every change because production is on the same code path; batch lets you sit down, try five dumb things in ten minutes, pick whichever one is less dumb, and keep iterating. The cost of being wrong is *"wasted ten minutes"* instead of *"broke the live pipeline."*

Put differently: **the distinguishing property isn't speed but control.** Streaming is reactive — events arrive at their own cadence, the rhythm is set by the data feed, and you live with the hurry-up-and-wait that follows. Batch is imperative — the caller says *"go analyze this,"* the analyzer does it, the caller decides what to ask next.

Both volume and tempo turn out to be orthogonal to the distinction: batch can absolutely chew through a petabyte in minutes (nobody calls that slow), and streams can be infinite-in-principle but trivially sparse in practice (a heartbeat event per hour, a single error log line per day — the events come when they come). Likewise batches can be a single record handed to `analyze` from a test fixture. The axis that actually defines the choice is **who initiates each unit of work** — the data source for streaming, the caller for batch — and that's independent of how much data flows and how fast. Design work needs that initiative on the caller's side: the decision-maker (operator at a terminal, tournament-scoring function picking among candidates, LLM agent doing design iteration) sets the rhythm, not the data feed. You can't iterate against a feed whose tempo is dictated by a Kafka producer somewhere; you can iterate against something you've captured and can interrogate repeatedly at whatever cadence the decision-maker prefers.

The committed `samples/` corpus plus the `samples_regression.rs` invariants exist for exactly this reason — load-bearing decisions (record-vs-map thresholds, cluster cap, sample rate floor, format detector ordering) get iterated against a fixed fixture and decided by reading the diff, not argued about from first principles. The hypothesis is wrong far more often than first-principles reasoning suggests, and the fixture is patient. Every committed analyzer-behavior change in this repo so far has run through that loop at least once; the convention is worth preserving.

A related operational property: **when streaming is already running and you want a different policy, the answer often isn't to redeploy.** Cross your fingers and wait until the streaming pipeline saturates — let the sketches converge under the existing policy, accept that they're producing a coarse-but-real answer, then run batch analysis against the accumulated tile state with the new policy whenever a refined view is worth the cycles. The batch pass produces the new artifacts (`ShapeNode` tree, `PromotionPlan`, compiled matcher); the streaming pipeline keeps doing its old job, which is fine because what it's producing isn't *wrong*, just less precise than the new policy could make it. The analytical policy decision gets decoupled from the streaming deployment decision — operators can change their mind about analysis without touching the live ingest path. That asymmetry is what production operators want and what most schema-inference tooling doesn't offer.

A third operational property, this one on the streaming side: **nothing prevents running multiple parallel analyzers on the same production stream**, each with different match criteria, different target fields, different policy coefficients. The `JsonEventSink` trait broadcasts events; every analyzer in a fan-out receives the same input independently, maintains its own state, produces its own output, contends for nothing. Shadow analyzers run experimental policies alongside the primary (output goes to a diff display rather than production storage, until a shadow earns promotion). Targeted analyzers each focus on a different `.at()` projection of the same events — one looking at `.events.user_actions`, another at `.events.system_metrics`, another at the envelope. Differential sensitivity testing spins up N copies with varied caps to see how much the inferred shape actually depends on those choices. Per-analyzer cost stacks linearly with N, but each instance is already cheap by design (sampling, bounded sketches), and *"are these two policies meaningfully different?"* answered by an hour of parallel real traffic beats a coordinated deploy-then-observe cycle every time. The fan-out pattern is what the event-vocabulary input contract was designed to support, even if we only call it out once we have enough policy machinery to make the parallel configurations meaningfully distinct.

### Tournament-style policy lifecycle (TODO)

A recurring pattern in this project's headcanon, connected to the JSON Tiles ancestry: **keep the boring stuff running, spin up the neat ideas, promote them to boring if they turn out to be good or let them die if they can't compete.** Three tiers of analyzer policy coexist:

- **Boring (production)**: the policy driving `PromotionPlan` output, emitting the active Quamina matcher, appearing in audit logs as the *"this is what we believe about the data"* answer. Conservative, well-understood, slow to change.
- **Spinning up (experimental)**: candidate policies — new format detector, bigger cluster cap, tweaked sample rate, alternative time-extractor — running as shadow analyzers (parallel against the live stream) and/or back-tested against the `samples/` corpus and accumulated tile state. Output goes to a comparison surface, never directly to production storage.
- **Retired**: experimental policies whose results couldn't justify the change, or former-boring policies that got outperformed by a challenger. Kept around for audit and historical reproducibility; never running.

The three operational properties named above are exactly the mechanism: streaming fan-out runs shadows on real traffic; batch on `samples/` produces controlled comparisons; deployment-decoupling means the boring stays put through the entire evaluation. **Promotion is a config change, not a code change.**

The promotion criterion is the interesting design question — *coverage* (more of the long tail classified into named categories), *compression* (more bytes shredded into typed columns, less in the residual), *stability* (less churn across re-analyses with varied seeds), *resource cost* (CPU per record, weighted as a constraint rather than a free dimension). Scoring is operator policy; shapez produces the inputs and trusts the host to combine them per deployment.

**This is analogous to back-testing in quant finance**, and the parallels are exact enough to be worth naming: candidate strategies (policies) are back-tested against historical data (`samples/`, accumulated tile state) and forward-tested via paper-trade mode (shadow analyzers on live traffic) before being promoted to live trading (the boring tier). Strategy retirement is the symmetric move when an old policy gets outperformed. The discipline quant teams have built around tournament-style strategy management is directly applicable here — performance attribution (*why* did the new policy win on this slice?), regime detection (*the boring is fine for steady state but a recent challenger handles the burst case better*), even alpha decay (*the once-clever format detector is now part of the boring baseline and no longer differentiates*). The *"never just one strategy in production"* mindset is exactly what makes the three operational properties above worth having.

Not built yet; flagged here because this is the natural endgame for the operational design, and the right shape for the eventual policy-registry, promotion-criterion-API, and tournament-scoring surface. None of the pieces are exotic in isolation — config registry, shadow output capture, scoring functions — but the discipline of treating analyzer policies as a portfolio worth managing rather than a single deployment decision is what makes the whole machine *actually* keep learning.

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

### Path expression DSLs (TODO)

Several API slots take a path expression — `.at(path)` for sub-document projection, `.with_time_field(path, parser)` for time extraction, future similar slots for assertion targets and promotion-plan addressing. shapez has its own `Path` DSL (`shapez::path::Path`) used internally for `PathPattern` matching: small, ASCII, wildcard-aware (`.flags.*.enabled`), already round-tripping cleanly between `Display` and `FromStr`. It's the canonical wire-level type and stays that way.

The dialect is also unfamiliar. Operators who don't read shapez's docs first won't know that `.flags.*.enabled` is the canonical form, and there's no reason they should — there's an industry-standard path language for JSON-shaped data already, and it's called **JSONPath**.

The plan:

- **Accept both as input.** Operator-facing builders (`.at`, `.with_time_field`, future assertion locators) take an `impl Into<Path>` or an `impl TryInto<Path>` and parse either shapez native (`.event.data`) or JSONPath (`$.event.data`) into the same internal `Path`. The dialect is autodetected from the first character (`.` vs `$`), or the operator picks explicitly via separate constructors.
- **Stay native for output.** Reports, audit strings, and the inferred `PathPattern` always render in shapez's DSL — that's what the rest of the system uses, and we shouldn't pretend the input syntax is the wire format. If the operator supplied JSONPath, the audit trail records both forms so a replay reproduces what they typed and the system reasons about what they meant.
- **Constrained JSONPath subset.** JSONPath has features (filters `[?(@.x > 5)]`, recursive descent `..`, slicing `[1:5]`, unions `[a,b]`) that don't map to a single-location selector. v1 accepts only the parts that round-trip to a `Path`: dotted field access, bracket field access (`['foo bar']` for quoted names), array indexing (`[0]`), and the root marker (`$`). Wildcards (`*`) get parsed to shapez's `AnyField` / `AnyIndex` so `PathPattern` round-tripping works. Anything richer than that raises a parse error — better a clear "we don't support filters yet" than a half-implementation that silently does the wrong thing.

The cost is small — one parser per dialect, both targeting the same AST — and the value is large: an operator who already knows JSONPath can be productive without first learning that shapez's paths start with `.` and not `$`. The path DSL is the user-facing surface; the internal `Path` is the wire-level type. Two dialects feeding one representation is the right shape.

### Multi-path automaton traversal (TODO)

A natural generalization once we have several path-shaped slots evaluating against the same record — `.at()` projection, `.with_time_field()` extraction, planned assertion targets, planned `FromSiblingField` policy lookups, planned promotion-plan addresses — is to *compile them all into a single multi-path automaton* and drive them in a single traversal of the input.

The shape:

- Each `Path` / `PathPattern` compiles to its own NFA, with states for each step (`Field`, `Index`, `AnyField`, `AnyIndex`).
- The N automata advance in lockstep over the event stream or DOM traversal — at each event, every still-live automaton either takes a transition (the event matched its current expected step), stays in its current state, or fails.
- When an automaton reaches its accepting state, the bound callback fires with the matched value.
- When an automaton fails its next transition, **the per-expression config decides what happens**:
  - **Precondition**: the whole traversal aborts. The record is rejected — *"this required path didn't resolve, the record fails its prerequisites."* `.at()` and required `.with_time_field()` both behave this way today: a record where `.payload` doesn't exist is silently skipped.
  - **Prune**: just that expression drops out; the other automata continue. `FromSiblingField`'s tz hint, optional assertion targets, and any "if you can find this, use it" policy belong here.

This is the classical streaming-XPath / streaming-JSONPath compiled-query-plan technique (Yfilter, XPush, twig-pattern matching from the early-2000s XML literature) applied to shapez's specific set of address slots. It composes naturally with both architectural choices from the *materialize vs reorder* discussion below: run the unified automaton over a materialized DOM in a single visit, or use the automaton's per-step demand to tell an event-driven parser which events to deliver next.

**Read the production-scale precedents before reinventing.** AWS's [`event-ruler`](https://github.com/aws/event-ruler) (Java) is the canonical industrial system — used internally at AWS for years (CloudWatch Events / EventBridge) before open-sourcing, designed to match millions of incoming events per second against millions of registered patterns. Tim Bray's [`quamina`](https://github.com/timbray/quamina) (Go) is the successor in spirit, smaller scope, current and active, and probably the easiest entry point for understanding the technique. Both compile N JSON-shaped patterns into a single NFA, both handle wildcards and prefix matches cleanly, and both have already worked through the engineering questions (state explosion, pattern addition/removal at runtime, observability under high cardinality) that we'd otherwise rediscover the hard way. Neither is a drop-in for shapez — they're event-routers, not analyzer-driver compilers, and the precondition-vs-prune semantics aren't quite the same as their "did any rule match" output — but the data structures and the production knowledge they encode are exactly what an in-tree implementation should crib from.

Implementation notes for whoever picks this up:

- The shapez `Path` and `PathPattern` types already give us the AST; the missing pieces are the compile-to-NFA step and the multi-automaton driver. Wildcards (`AnyField`, `AnyIndex`) push the cumulative state set into proper N-ary territory, but in practice the branching is bounded by the actual record shape.
- The precondition / prune flag is per-expression, not per-source. Two policies on the same source can have different behaviors when their path doesn't resolve, and the operator picks at the slot where the path is named.
- The right place to surface the distinction in user-facing config is alongside the path itself — `.at(path).required()` vs `.at(path).optional()`, or an explicit `OnMissing::{Skip, Prune}` enum hung off each path-taking builder.

Not built yet; the case for it gets stronger as more path-shaped slots show up. With one or two, independent walks are cheap. With four or five, a unified driver starts to matter — for correctness as much as throughput, because *"did this record satisfy every required precondition"* is best answered by a single decision point rather than scattered across per-slot checks that might evaluate in different orders.

### Compiling inferred categories to Quamina matchers (TODO)

A complementary integration path between shapez and Quamina worth naming: once an analysis run reaches high confidence that the categories it sees are stable — cluster sketches not churning, variant arms holding their distribution, format detectors not flipping — the categories shapez identified can be **compiled into Quamina patterns and handed off** for real-time classification. shapez is slow and thorough; Quamina is fast and deterministic; the compile step is the bridge.

The pipeline:

1. **Analyze.** Run shapez over a representative sample until confidence is high enough — bounded eviction rate, stable variant-arm distributions, format detectors not flipping. The natural snapshot point is an epoch boundary; the dictionary is frozen there by design.
2. **Annotate, optionally human-in-the-loop.** shapez sees structural categories — *"variant arm at `.event` whose record signature is `{type=purchase, order:{...}}`"* — but their *meaning* is operational. The operator (UI, annotation file, workflow tool) names each one: `purchase_event`, `view_event`, `system_metric`, `auth_failure`. Names are the bridge from structural shape to semantic category — the *one person's syntax, another's semantics* tenet doing its job at the human/machine boundary. shapez can autogenerate placeholder names (`category_001`, `arm_3`) so the unannotated pipeline still runs.
3. **Compile.** Each named category becomes a Quamina pattern. The compilation is mechanical for the common cases: shapez's per-category structural signature (cluster `Sig`, variant arm, discriminator field value) maps to Quamina's JSON pattern language. Field-existence, value-equality, prefix-match all fall out naturally; numeric ranges and the more sophisticated predicates compose too. The output is a Quamina program — *the "black box"* — that takes an event in and returns the set of matching category names.
4. **Deploy.** The Quamina matcher is a frozen artifact, pinned to a specific shapez analysis run. Reproducible by construction (the policy provenance work from the wire-contracts discussion); fast in production because matching is NFA traversal, no sketches, no per-event learning.
5. **Detect drift.** Events that fail to match any category are a first-class signal: the live distribution has moved away from the analyzed one. Counting them is cheap; surfacing the count and a reservoir sample of unmatched events is exactly the prompt to trigger a re-analysis. The feedback loop closes — shapez analyzes, compiles to Quamina, Quamina classifies, unmatched count surfaces, shapez re-analyzes.

Where categories come from is the interesting design question:

- **Cluster arms**: each entry in a position's Space-Saving top-K *is* a category by construction. The signature gives the pattern; the operator gives the name.
- **`ShapeKind::Variant { arms }`** in the inferred shape tree: any variant decision is a partition of the value space; each arm is a candidate category.
- **Discriminated records**: when a record has a const-like field (`type = "purchase"`, `kind = "click"`) that visibly splits the structure, the discriminator value is the category name straight out of the data, no human annotation required.
- **Per-field format families**: a string field where 95% match UUID + 5% match email is two categories at that path; operators routinely want to route those separately.

This is one of the more natural ways shapez's analytical output becomes operational. `PromotionPlan` goes to storage layers; a compiled matcher goes to routing, classification, alerting, billing tagging — anywhere a downstream system wants *"given this event, what is it?"* answered at production speed without re-running analysis. The compiled-matcher artifact deserves its own stable serialization (same case as `AnalysisOutcome` and `PromotionPlan`); Quamina's pattern language is JSON, so what we emit should round-trip cleanly through it — checked into a config repo, diffed across re-analyses, code-reviewed, rolled back like any other deployed artifact.

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

Run scope is rarely "every byte ever stored." Operators want windows — "the last 24 hours," "October 2023," "between when we deployed v2.1 and now." The machinery exposes a uniform `TimePredicate { start, end }`. The interesting question is *where the timestamp comes from* on each record, which decomposes into two strategies every format / source needs to expose:

1. **Extraction** — where to find the raw timestamp value for this record. Strategies vary enormously by backend:
   - **File metadata** (the default for filesystem sources without per-record metadata): `JsonlDir`, `CsvDir`, et al. use file mtime, at per-file granularity.
   - **Object-store metadata**: the `Last-Modified` header on each blob, per-object granularity.
   - **Native typed column / field**: Parquet/ORC files often have a self-describing timestamp column; SQL queries name a column with the database's native time type; Avro records with `logicalType: timestamp-millis` carry the semantics in the schema.
   - **Broker / framing metadata**: Kafka messages carry a broker-assigned timestamp on each record independent of the message body — the default for Kafka sources without a designated content time.
   - **Path into the decoded value**: a `shapez::path::Path` (`.event.ts`, `.created_at`) for sources where the timestamp lives in the payload. Composes with `.at()` — the time field can be anywhere in the document, including outside the analyzed subtree.
   - **Regex on raw bytes**: log lines where the timestamp is text at a known prefix position. The adapter declares the pattern and capture group.
2. **Parsing / interpretation** — how to lift the raw extracted value into a `SystemTime`. This is data-specific, not source-specific, and the same parser usually applies across many sources:
   - **Epoch integers**: `epoch_seconds` / `epoch_millis` / `epoch_micros` / `epoch_nanos`. The same windows `NumericStats::epoch_guess` uses to *detect* an epoch field at analysis time are the parsers an operator would name to *consume* one.
   - **String timestamps**: a permissive parser via the `dateparser` crate. Covers ISO 8601 / RFC 3339, RFC 2822, US-style `M/D/Y`, numeric epoch as string, common log formats. Strings without a zone marker route through `NaiveZonePolicy` (Refuse by default; operators override per field with `AssumeUtc`, `AssumeFixedOffset { seconds }`, or `Local`).
   - **Native typed**: no parsing needed when the source delivers an already-typed timestamp (Parquet column, SQL column with the right type).
   - **Custom format strings**: `strftime`-shaped patterns for the long tail of bespoke timestamps. Operator-supplied.
   - **UUIDv7 and ULID**: both encode a 48-bit epoch-millis timestamp in their leading bits — UUIDv7 in the first 12 hex chars (RFC 9562, version nibble validated), ULID in the first 10 Crockford Base32 chars (lexicographic ordering by time). Each gets its own `TimeInterpreter` variant because the pattern is common: primary-key columns in modern systems frequently *are* timestamps with random cruft attached, and writing an envelope `created_at` field next to the ID is a workaround for tooling that didn't realize it could extract the timestamp from the ID itself. Saying so once at the analyzer level beats every consumer rolling its own bit-twiddle.

`dateparser` is the cold path. A natural refinement — not built yet, flagged here — is to do what the analyzer does everywhere else: sample, infer, commit. For the first N records of a field, run the full fuzzy ladder and tally which formats matched. Once one or two dominate, pin a fast `chrono::DateTime::parse_from_str` with the winning format string and parse the rest at roughly 1/N the cost. Fall back to the ladder if the pinned parser starts missing. The fit between dateparser and our throughput goals isn't as close as just calling it for every record makes it look; it's the right tool for warmup, not the steady state.

### Per-field interpretation policy (TODO)

The `TimeInterpreter` enum names *how* to parse but not *what to do when the parse is ambiguous*. Real timestamp pipelines need a small policy slot for questions the parser alone can't answer:

- **Naive-zone fallback** — *built*. `NaiveZonePolicy` carries the per-field choice: `Refuse` (default — reject zoneless strings, the design preference because silently picking any zone for data that didn't name one is a correctness hazard), `AssumeUtc`, `AssumeFixedOffset { seconds }`, or `Local` (dateparser's host-clock fallback, available for compat but almost always wrong). Planned variants on the same enum: `AssumeNamed(String)` for IANA zones (needs `chrono-tz`), `StickyPrevious { fallback }` for "use the offset of the most recent zoned record" (stateful), and `FromSiblingField { path, interpreter }` for the very common `{"ts": "...", "tz": "America/Los_Angeles"}` and `{"ts": "...", "utc_offset_min": -480}` patterns (needs interpret's contract widened so the policy sees the surrounding record, not just the extracted value). Whether also to track and surface a per-field count of zoneless observations is open — useful as audit even when the policy is `Refuse`, since the count tells the operator how many records the policy actually filtered out.
- **Parse-failure handling.** Silent skip (current), count and surface in `AnalysisOutcome`, abort the whole run if more than X% fail, retry under a different interpreter. Each is right for a different deployment. Not built yet.
- **Resolution-drift detection.** If a field is supposed to be epoch-millis but some records arrive in seconds (off by 1000× — common when two upstream services disagree), the parser silently gives nonsense. A policy bit that pins the expected magnitude would catch it. Not built yet.
- **Locale hints.** US (`M/D/Y`) vs EU (`D/M/Y`) date order is genuinely ambiguous for "01/02/2024." Operators usually know which one their data uses; the policy slot is the place to tell us. Not built yet.

The natural home is a small struct hung off `TimeInterpreter` or off the per-field configuration that names it — something like:

```rust
TimeInterpreter::StringTimestamp(InterpretationPolicy {
    naive_zone: Some(NaiveZonePolicy::AssumeUtc),
    on_parse_failure: ParseFailurePolicy::CountAndSurface,
    locale_hint: Some(Locale::US),
    ..Default::default()
})
```

Or a small DSL — `"naive_zone=UTC; on_failure=count; locale=US"` — when the config is operator-typed rather than code-defined. Both shapes work; the right one depends on whether these policies usually come from code or from a config file.

None of this is shapez-specific — every system that parses timestamps from semi-structured data faces the same four questions. The eventual home for the policy struct is therefore not `shapez::batch` but `_meta` (alongside `ValueType::Timestamp`), so downstream storage layers, the planned Avro / Parquet / Kafka adapters, and any other consumer of the shared type set can share the policy vocabulary. shapez references it; shapez doesn't own it. `NaiveZonePolicy` lives in `shapez::batch` for now as the first concrete slice; the migration to `_meta` happens when at least one other consumer would also use it.

#### Architecture choice for context-aware policies: materialize vs reorder

Making the surrounding record available to a policy is the load-bearing implementation cost for `FromSiblingField`, `StickyPrevious`, and any future context-aware policy. There are two paths with very different consequences:

**Materialize the record.** Hold the whole decoded record (as `serde_json::Value`, or equivalent in-memory tree) while interpreting any field. Policies navigate freely. This is what `shapez-json`'s `drive_document` already does — we receive a fully-materialized Value, walk it into the analyzer one event at a time, and the Value lives for the duration of one record. For JSON sources we're paying this cost already; for SQL and Parquet sources the underlying client materializes rows itself. The simple path. Two factors take most of the sting out of this option:

- *Sampling*: once the BTRBlocks-style sample-rate work lands, full materialization runs on only ~5% of documents. The other 95% pay a counter bump. The materialization cost is multiplied by 0.05 across the steady-state ingest path.
- *simd-json has a DOM-shape mode*: the planned `shapez-simdjson` adapter can produce a tree representation (`simdjson::dom::element`) that's several times faster than `serde_json::Value` to parse and walk. Materializing isn't the same word in both libraries.

**Static analysis + reordered event-driven parsing.** At policy configuration time, collect the set of paths every active policy depends on. At ingest time, the parser peeks ahead within a record to deliver the context-required fields first, then proceeds with the rest. The interpretation still happens at the right field (the timestamp), but with the cross-field context already in hand. This is the spicier path: it scales to records that don't fit in memory at all, supports pure event-stream sources that have no natural DOM (Avro container files, Protobuf streams), and avoids the tree-allocation cost entirely for sources that natively emit events.

The two are not mutually exclusive — Value-shaped sources can materialize while event-shaped sources use static-analysis-driven reordering, both delivering the same `(record_context, raw_value) → SystemTime` contract upstream of the policy. *Default expectation*: materialize for the JSON adapter (free given the existing Value plumbing), materialize-via-simd-json-DOM for the simdjson adapter (still cheap because of the DOM-mode speed, doubly cheap because of sampling), and reorder only for the genuinely event-shaped sources where no DOM form exists. The "spicier" path is the right design when there's no tree to begin with — not the universal answer.

### Cross-type timestamp tracking (TODO)

The analyzer currently surfaces timestamp signal through two independent trackers that never talk to each other:

- **`StringFormat::IsoTimestamp`** on per-string-leaf observations: detects strings that match the ISO 8601 / RFC 3339 family.
- **`NumericStats::epoch_guess()`** on per-numeric-leaf observations: flags integer fields whose min/max sits inside one of the canonical epoch windows (seconds, millis, micros, nanos).

Both produce evidence for the same conceptual thing — "this position holds timestamps" — but they live in different stats trackers and the signal isn't aggregated. The pathological case that escapes both is the epoch-shaped string: a varchar column containing `"1705314600"`. The string format detector doesn't match a known format (closest is `AllDigits`), the numeric detector never sees the value because it isn't a number, and the most we surface is a `_punct` skeleton like `9` (a 10-digit run) which is *suggestive* of a Unix-epoch field but doesn't actually claim so. Reality is full of varchar columns holding epoch numbers; we should not let them slip past.

The future move is a `TimestampStats` tracker that aggregates evidence from any path that could produce a timestamp:

- ISO-format string matches at this position (current `StringFormat::IsoTimestamp` signal).
- Numeric values in any of the four epoch magnitude windows (current `NumericStats::epoch_guess` signal).
- *String* values whose digit count and magnitude land in an epoch window — the epoch-as-string case the existing trackers miss.
- The resolution distribution observed (seconds / millis / micros / nanos / *mixed*), which catches the "two upstream services disagree" failure named in the per-field policy section above.
- The source-type distribution (came in as `i64`, came in as `string`, came in as `f64` with non-zero fractional part). Useful for shredding advice: a column that's 95% i64 and 5% string-of-digits wants a typed promotion plus a residual.
- A percentile sketch (DDSketch over the resolved `SystemTime` values), so the report can surface "p50 was Tuesday afternoon, p99 was last August" — useful both as a sanity check and as a window predicate hint.
- Naive-vs-zoned breakdown for string sources, since the per-field interpretation policy needs the count to decide its defaults.

The reason to keep the existing string and numeric stats as-is, and add `TimestampStats` *on top*, rather than refactoring detection into one place: most strings aren't timestamps and most numbers aren't either; the existing per-type trackers do a lot of other work (format families, length sketches, sign breakdowns, range hints). The new tracker is a specialist that fires only when at least one detector at the position has flagged "timestamp-shaped," and then accumulates the cross-type evidence.

Connection to the interpretation-policy TODO: stats and policy are complements. `TimestampStats` characterizes what the field is doing; `InterpretationPolicy` says what to do about it. Both want their eventual canonical home in `_meta`, not shapez.

#### Boundary conditions on epoch-precision detection

The current `NumericStats::epoch_guess()` uses four magnitude windows separated by ~3 orders of magnitude each, chosen conservatively so a value lands in at most one window. The gaps are not free — they're places where epoch values *are* timestamps but our classifier won't say so, or where adjacent precisions risk being confused. Worth enumerating before the cross-type tracker tries to do better:

| Window | Range (`min` ≤ … ≤ `max`) | Approx. date coverage |
|---|---|---|
| `EpochSeconds` | 9e8 to 3e9 | 2001-09 to 2065-01 |
| `EpochMillis` | 9e11 to 3e12 | 2001-09 to 2065-01 |
| `EpochMicros` | 9e14 to 3e15 | 2001-09 to 2065-01 |
| `EpochNanos` | 9e17 to 3e18 | 2001-09 to 2065-01 |

**1. Inter-window dead zones** — values between 3e9 and 9e11 (and the equivalent gaps between micros and nanos) are *unclassified*. A millisecond timestamp from early 1970 (e.g., 1970-01-21 ≈ 1.8e9 ms) lands in this dead zone and is silently missed. Same for any value between 3e12 and 9e14, etc. These are 3-order-of-magnitude blind spots, deliberately so to prevent collision, but they exclude real timestamps from the late-`epoch` era.

**2. Cross-window false positives at the extremes** — a 2024-era epoch-seconds value is ~1.7e9 (safely in window). But a far-future epoch-seconds value, say year 3000 (≈ 3.25e10), crosses out of the seconds window and lands in… nowhere (it's in the dead zone). Meanwhile a *micros* value from 1970-01-12 (≈ 1e12) would land in the millis window and be misclassified as a 2001 millis timestamp. The window edges are where adjacent-precision confusion happens, not the middles.

**3. Off-by-1000 in a single mixed field** — the canonical "two upstream services disagree" case: API v1 sends `current_time_seconds` and v2 sends `current_time_millis` to the same column. Range becomes `[~1.7e9, ~1.7e12]` — spanning two windows. The current classifier requires `min ≥ LO ∧ max ≤ HI` within one window and silently returns `None`. The field becomes undetected even though *every individual record* is a valid timestamp. This is the case `TimestampStats`'s **resolution distribution** is designed to catch: aggregate per-record-precision-guesses and surface "67% look like seconds, 33% look like millis."

**4. Non-timestamps that land in epoch windows** — Twitter-style snowflake IDs in 2024 are ~1.7e18, which sits *inside* the `EpochNanos` window and would currently be misidentified as 2024 nanosecond timestamps. Sequential database IDs at high-traffic services can pass 1e9 (epoch-seconds) and 1e12 (epoch-millis). The defense is corroborating evidence: a true timestamp field's values usually grow monotonically with insertion order, cluster around a small relative range (a few months, not 60 years), and have low Variance-to-Mean ratio compared to ID streams. None of that is currently checked.

**5. Far-past epoch values silently dropped** — `epoch_guess` requires `all_non_negative`, so negative epochs (before 1970) are excluded. The 2.5e9-second gap below `9e8` (the seconds window floor) covers most of the 1970–2001 epoch range, which is plausibly legacy log data. We err toward false-negative here on purpose; mentioning it so it's not a surprise when "but the data is from 1995" comes up.

**6. Sub-second precision serialized as float** — `1705314600.5` (epoch seconds with fractional part) currently fails `integer_valued`, so `epoch_guess` rejects it. Lots of systems emit fractional epochs; we miss them today.

**7. JavaScript precision loss** — values above 2^53 (~9e15) lose i64 precision when round-tripped through a JS `number`. A 2024 epoch-nanos value (~1.7e18) is well past 2^53 and arrives at the analyzer as f64 with non-zero fractional part — same fate as #6. JavaScript pipelines pin epoch-millis, not nanos, so this rarely bites; but as nanosecond precision spreads (Iceberg, ClickHouse), it will.

The `TimestampStats` tracker, when built, should treat these as the *test suite*: a field is correctly identified if and only if every case above is either flagged with the right precision, flagged with explicit ambiguity (resolution-mixed), or explicitly rejected (snowflake-shaped, counter-shaped). The current windows are good enough as a one-shot heuristic; a stats tracker with corroborating signal is what closes the boundary conditions.

The trait surface stays uniform — `source.within(predicate)` — but each source exposes builder methods for the strategy slot: `.with_time_field(path, parser)`, `.with_broker_time()`, `.with_column("ingested_at")`, etc. Default strategies are the no-config common case; overrides handle the rest.

Two cross-cutting consequences worth naming:

- **The analyzer's format detectors double as advisors for this configuration.** A field that `NumericStats::epoch_guess` flagged as "looks like epoch millis" is by construction a candidate for `.with_time_field("<that path>", EpochMillis)` on the next pass. The advice loop runs naturally from analysis output to time-extraction config to next analysis.
- **Sources must handle unparseable values explicitly.** A time field whose parser fails on some records yields `Option<SystemTime>` per record. Sources need to declare a policy — include silently, exclude silently, count and surface in `AnalysisOutcome` — and document the choice. A silent exclusion that drops 30% of records is the kind of mistake an operator finds only by accident.

Until per-record extraction is wired up, file / object / row metadata is the only honest time signal across all current sources.

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
