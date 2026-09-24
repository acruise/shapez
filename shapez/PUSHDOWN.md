# Query planning and index layout

Companion to `DESIGN.md`. That doc describes how shapez infers shape from a stream of values; this one describes the seam where shape inference meets query execution — how a promotion plan turns into pushdown semantics, what side data the storage layer can carry alongside the typed columns to make pushdown cheap, and what the contract between shapez and a downstream query planner should look like.

This is still exploratory; nothing here is wired up. The point is to write down the shape of the answer while it's fresh and let the implementation chase it deliberately.

## The pushdown frontier

shapez emits a `PromotionPlan`: a set of `(path, type)` pairs that should be hoisted into typed columns, possibly behind wildcard steps for high-cardinality maps. Everything else stays in the variant / JSONB residual.

That plan implicitly defines a *pushdown frontier*. A predicate whose path terminates at a promoted path can run as a columnar scan; a predicate whose path terminates somewhere only the residual can answer pays JSON-parsing cost row by row. The frontier isn't binary at the query level — most non-trivial predicates straddle it — so the planner's job is to decompose:

- Conjuncts that terminate on the promoted side run as typed-column tests.
- Conjuncts that don't fall through to residual scan on the rows surviving the promoted half.
- Disjunctions are mappable only when every branch has a non-trivial mapping; one un-mappable branch collapses the whole `OR` to "no pruning."

The interesting design point is that "promoted" isn't a single bit per path. A path can be promoted as a typed leaf, as a typed leaf *with format detection* (so equality literals get coerced once before the scan instead of once per row), as a variant with arm tags (so a predicate over the discriminator runs against the tag column rather than the variant blob), or as a wildcard-through-map (so an existential `.foo.<anything>.bar = 'baz'` answers from one typed scan). Each of these supports a different vocabulary of pushable predicates and a different selectivity story. The plan should make those distinctions visible.

## The pushdown contract per promoted path

For each promoted path, shapez should publish a small structured record telling the query planner what it can do columnar:

- **Declared type.** What `ValueType` the column carries. Drives the basic comparability rules.
- **Supported predicate shapes.** Equality, range, `IN`, `LIKE`, `IS NULL`, prefix. Type-determined, mostly; format detection extends the set (e.g., date arithmetic on a `String` column where the format is ISO-8601).
- **Wildcard quantifier semantics.** For paths with `.*` or `[*]` steps, whether the path is existential-by-default (most engines), universal-by-default (rare), and which operator shapes are still cheap under negation. Existential `=` is cheap; universal `!=` ("for all K, .foo[K].bar ≠ 'baz'") is doable but a different physical operator.
- **Literal coercions.** If the column is a typed timestamp and the literal is a string, the coercion runs once at plan time. The contract names which coercions are safe and which lose precision.
- **Variant arm structure.** If the promoted path is `Variant{Int, String}`, the contract names the arms and points at the discriminator column. Predicates over one arm filter the discriminator first.
- **Numeric / range bounds.** From the distribution sketch: declared min/max, integrality (all-integer flag), nullable. Lets the planner answer "is this cast lossless" without scanning.
- **Format tags.** If a `String` column has format `Uuid` / `IsoTimestamp` / `Ipv4` / etc., the planner can rewrite equality predicates into the storage-native binary form (`uuid_bytes` equality is memcmp), or pre-compile a parsed comparand once.
- **Selectivity estimates** for the dimensions above. Tells the cost model whether pushing is worth it.

The contract is small and per-path. It's also *parameterized over the plan*: the same path under two different plans of the same data has two different contracts. Versioning falls out naturally because the plan is itself a versioned artifact.

## Casts and the implicit predicates they introduce

`WHERE CAST(.foo.*.bar AS INT) > 10` is not one predicate — it's two, an implicit "is castable to INT" conjunct and an explicit "> 10" conjunct over the cast value. Engines handle this without help by treating uncastable rows as nulls and letting `WHERE` drop them. Shape knowledge collapses the implicit half in several productive ways:

- **Already-typed paths skip the cast.** If `.foo.*.bar` is `Int`, the cast is a no-op; the planner drops it before the column is even read.
- **Format-typed strings carry castability stats.** If the column is `String` with format `Integer`, the analyzer already knows what fraction parses; the castability conjunct becomes a known selectivity rather than a row-by-row gamble.
- **Variant arms partition castability cleanly.** A `Variant{Int, String, Bool}` has zero castable Bool rows, identity-castable Int rows, and partially-castable String rows. With arm tags, filter the discriminator first; the cast then runs only over arms where it's meaningful, and the predicate's implicit "is castable" conjunct decomposes to "tag ∈ {Int, parseable-String}" — a columnar arm-membership filter.
- **Numeric distribution sketches answer lossless-cast questions ahead of time.** If the sketch reports all values fit in `i32`, `CAST(... AS INT)` is lossless and the implicit castability conjunct vanishes. If 5% have nonzero fractional part, `CAST(... AS BIGINT)` has known-selectivity truncation cost.
- **String-format coercions go the other way too.** `.foo.*.timestamp > '2024-01-01'` parses the literal once because the column and format are known; the per-row cost is one timestamp comparison, not one parse plus one comparison.

These are not five unrelated optimizations. They are five faces of one move: shape knowledge makes implicit type predicates *visible* to the planner, and once visible the planner can rewrite, eliminate, or push them down individually.

A specific predicate worth naming: `typeof(.x) = 'string'`. In a fully-typed schema this is meaningless. In a heterogeneously-typed JSON column it's a perfectly reasonable query, and the analyzer already maintains the per-path source-type distribution that answers it. With an arm-tag column at the discriminating position, this predicate becomes a single equality against a tiny typed column, with selectivity known at plan time from the arm proportions. It's the easiest case the contract supports, and it's the case engines today can't push at all.

## Per-row shape sidecars

The pushdown contract tells the planner what's promoted. But on the row scan itself, the planner often wants cheap, *vectorizable* row-skip tests — "this row's root is an array, so any predicate that starts with `.foo` skips it." These don't replace typed-column scans; they precede them, the same way a bloom filter or zone-map precedes a scan today.

The natural representation is a per-row *shape sidecar*: a small set of typed-column tags that record structural facts about each row, alongside the actual data. The sidecar at row `i` answers questions like:

- What's the root type? (Object / array / scalar.)
- At each promoted variant position, which arm did this row land in?
- Which optional promoted paths were *present* in this row? (One bit per path of interest.)
- Which cluster-shape signature did this row's structure match? (Pointer into the chunk's per-position top-K dictionary; `UNKNOWN` bucket for the long tail.)
- For each promoted string-format column, which format did the value actually exhibit? (Useful when the analyzer detected a mixed-format column and the writer kept the originals.)

The cluster signature is the most important entry, because shapez already computes it. The Space-Saving sketch over per-position signatures *is* the dictionary of "known shape classes" for that position; the per-row sidecar tag is the row's signature ID against that dictionary, with `UNKNOWN` for evicted rare signatures. Everything else (root type, arm tag, presence bit) is a degenerate case of the same idea — a low-cardinality categorical that came out of the analyzer for free.

Sidecar tests are necessary, not sufficient. A predicate that compiles to `shape_id ∈ {12, 47}` filters out rows whose shape *can't* satisfy the predicate; it doesn't confirm the predicate. The engine has to follow up with the typed-column scan on the survivors and the residual fallback on `UNKNOWN`. This is the same filter-after-scan discipline engines already enforce for bloom filters and Parquet page statistics; sidecars slot into existing predicate-evaluation machinery as another participant rather than a new operator.

## Bit budget: abundance, not scarcity

The temptation when designing a sidecar is to be pessimistic about which bits to spend, on the theory that storage and read cost dominate. The arithmetic doesn't actually back this up.

A `u64` per row is *eight bytes*. Compared with a JSON object whose typical encoded size is hundreds to thousands of bytes, the sidecar is essentially free at the row level, and dictionary-encoded categorical columns compress to nearly nothing on top of that. **Sixty-four bits is a lot.** SIMD bitwise operations across multiple bitvectors are some of the cheapest operations a modern CPU performs — on the order of one cycle per 256 or 512 bits depending on the instruction set. Engines that already use bitmap-style predicate evaluation absorb additional bitvectors without changing their cost model meaningfully.

So the design rule should bias liberal:

- **Any island of regular structure deserves a bit, as long as its interpretation is unambiguous within the chunk.** Root type, arm at each promoted variant position, presence of each promoted optional path, shape-class ID against the chunk's top-K — all worth assigning by default.
- **Bias toward bits-at-root.** Coarse structural facts (root type, presence of top-level paths) prune the most rows for the fewest bits, and their interpretation is the least ambiguous. Deeper bits earn their keep when the structure at depth has high entropy.
- **The cost of a bit that turns out to be useless is small.** A constant-valued sidecar column dictionary-encodes to near zero; the worst case is one extra read of one near-zero column per scan. The expected case is "occasionally useful," and the upside is asymmetric — the planner can prune entire row groups using the bits that *did* turn out to be high-entropy.
- **Pruning hints are reversible.** If a workload trace shows a particular bit is never read at query time, future chunks can stop emitting it. The bit-vocabulary is per-chunk; dropping a bit from new chunks doesn't disturb old ones.

The mental model: **assign bits like you'd add zone maps or column statistics — generously, on the assumption that some queries will exploit them and the rest cost almost nothing.** The decision shouldn't be "is this bit worth its weight?" but "is there any plausible predicate this bit could help?" — and at 64 bits per row, that bar is low.

This also reverses the earlier framing that the planner should *only* emit a sidecar tag if the entropy × predicate-frequency product clears the cost. That rule still applies for *large* sidecar dimensions (a 16-bit shape-class ID against a per-chunk dictionary is meaningful storage; a path-presence bit isn't). The scarcity-bias rule fits the large dimensions; the abundance-bias rule fits the small ones. Both live in the same plan, distinguished by their cost class.

### Path-dependence and the cost of depth

A complication that lurks in the bias-toward-bits-at-root rule: bits at non-trivial depth are *path-dependent*. A bit like "arm at `.events[*].payload.target` is `User`" only has meaning when the row's structure actually reaches that path. For rows whose `.events` is absent, or empty, or whose every `.payload` is missing, the deep bit has no meaningful answer — it isn't "false," it's "the question was not asked of this row."

A clean way to handle this without spending two bits per deep predicate is to **define the deep bit as "the predicate succeeds at every reached position; reads as 0 when the path is not reached."** The compiler then has to `AND` in the prefix-presence bits whenever it uses the deep bit, so "reads as 0" can be disambiguated into "predicate failed at a reached position" versus "path wasn't reached." Since the prefix-presence bits are usually worth carrying on their own (cheap, prune well), the disambiguation is essentially free. The compiler treats the deep bit's type as "false-or-unreachable" and the prefix bits do the work.

That handles correctness. The harder question is *value*. A deep bit earns its keep only when queries routinely exercise the full path. The economic test isn't "does this bit have entropy in the data" — it might — but "do queries hit this depth often enough to amortize the analyzer's promotion decision and the writer's per-row cost." A bit that is high-entropy structurally but rarely probed by the workload is a wasted bit, and the deeper the path the more strongly that bites. Shallow bits are spendable on first principles because the conditions for their usefulness are nearly always met; deep bits should require workload evidence proportional to their depth.

This is exactly the place the diagnostic feedback loop earns its keep. The compiler already tracks which conjuncts pushed and which fell through; aggregated across a workload trace, that signal tells the analyzer which deep bits would have paid for themselves under the observed queries. Promote the ones that would; retire the ones that wouldn't. The bit-vocabulary being per-chunk means promotion and retirement happen at the write boundary, with no in-place rewrites — the lifetime-scoping rule applies unchanged. So the rule for depth folds neatly into the rule for the lifecycle: shallow bits are emitted by default; deep bits are *proposed* by the analyzer (because the structure has entropy at depth) and *confirmed* by the workload trace (because predicates actually go there), with the proposed-but-unconfirmed set running in parallel-analyzer tournaments until evidence accrues either way.

A concrete shape, for grounding: at a single deep path `.foo.bar.baz.quux` shapez might allocate bit 37 = "value is a number," bit 38 = "value is null," bit 39 = "value is a string." One bit per arm is uniform, mutually exclusive (modulo the unreached case, handled by the prefix-presence bits), and SIMD-friendly — `typeof(.foo.bar.baz.quux) = 'number'` is a one-bit test, `IS NULL` is a different one-bit test, and the compiler ANDs them with the prefix-presence bits at `.foo`, `.foo.bar`, `.foo.bar.baz` to disambiguate "not-a-number" from "never got there." If `.foo.bar.baz.quux` turns out to be monomorphically `Number` in this chunk, bits 38 and 39 are constant-zero and dictionary-encode to nothing; the bit budget pays only for what actually splits. If queries rarely look at this path, all three bits are wasted attention but cost essentially nothing on disk — the loss is the analyzer's promotion attention plus a small per-row write cost, not stored data. That's the right asymmetry for the abundance-bias rule to apply at depth.

## The compiler from predicate to tag pattern

The seam between the pushdown contract and the sidecar is a small compiler. Given a query predicate AST and the chunk's plan fragment, produce the strongest sound sidecar test:

- **Leaf predicates compile to a conjunction of tag tests.** `.foo.*.bar = 'baz'` becomes `root-type = object AND path-presence(.foo) AND arm-tag(.foo.*) ∈ {object} AND arm-tag(.foo.*.bar) = string AND (if format-detected) format-tag(.foo.*.bar) admits 'baz'`. None of those alone is the predicate; their conjunction is the strongest necessary condition.
- **AND composes by AND-ing the leaf conjunctions.** Trivial and sound.
- **OR composes by OR-ing — but only if every disjunct has a non-trivial sidecar test.** One un-mappable disjunct collapses the OR to `always-true`. The compiler has to recognize this and degrade explicitly rather than emit a partial test.
- **NOT is only sound when the inner test is exact.** `typeof(.x) = 'string'` is exactly an arm-tag equality; its negation is sound. A leaf predicate whose sidecar test is approximate (presence-bit, shape-class membership) can't be safely negated — the compiler returns `always-true` for the negation and lets the engine do the work. This is a known bloom-filter footgun and the same caution applies here.
- **The mapping is plan-versioned.** The same predicate against two different chunk plans produces two different tag patterns. The compiler accepts the chunk's plan fragment as input and resolves tag names to bit positions per chunk.

The output is a small boolean expression over the sidecar columns of one chunk. Execution is whatever the engine uses for vectorized boolean evaluation — typically SIMD bitwise ops over bitvectors, which is where the 64-bits-is-a-lot argument pays off.

### Multi-predicate compilation

For workloads that evaluate many predicates over the same data — saved queries, dashboard backends, streaming detectors — compiling each predicate independently is wasteful. The shape is the same as AWS event-ruler and Tim Bray's Quamina: compile all the patterns into a shared automaton over the sidecar columns and evaluate them in a single pass.

This is a natural extension, not a different system. The single-predicate compiler is a degenerate case of the multi-predicate compiler with one accepting pattern. Storage and execution don't have to know which mode is running; only the planner does.

### Diagnostic feedback

A useful side effect of explicit compilation is that the planner knows *which conjuncts it couldn't push, and why*. "The predicate touched `.user.preferences.theme`, which isn't promoted; estimated 92% of rows could have been pruned by a presence bit." That signal is exactly the input the tournament-style policy lifecycle wants: workload-driven evidence that a candidate plan tweak — promoting a path, adding a sidecar tag — would have paid off retroactively. A parallel analyzer can try the tweak; if the diagnostic count goes up under the new plan, promote it.

A candid note about this loop: closing workload-trace → index-strategy feedback end-to-end is a thing the data-systems world talks about constantly and rarely actually builds. Telemetry plumbing is engine-specific; the joint cost model that translates "predicate P appeared N times with selectivity S" into "promote path X with sidecar Y" tends not to live in any single component (the optimizer knows query cost, the storage layer knows write/read cost, no one knows the joint cost); acting on the feedback is operationally expensive (rewriting columns, rebuilding indexes), so even when the data is there a human approval step usually sits in the middle and the loop degrades to a recommendation system; and the signal is noisy and slow relative to workload drift, so a backwards-looking trace often chases the last incident rather than the next one. The systems that *do* close it — Snowflake's automatic clustering, Redshift's table optimization, BigQuery's adaptive partitioning — are bespoke per-vendor and don't publish much about how they decide.

shapez has a few structural advantages that make the loop more tractable here than at the general auto-indexing level. The promotion plan is small and explicit, so "proposed plan vs current plan" is a diff rather than a search. The bit vocabulary is per-chunk, so promotion and retirement happen at the write boundary with no in-place rewrites — the high-consequence action stops being high-consequence. Parallel analyzers make tournaments a normal mode of operation, so the human-approval step can be skipped without giving up safety; bad plans retire by attrition rather than by rollback. And the compiler's "couldn't push, here's why" diagnostic is already a natural trace format — no separate telemetry plumbing required for the minimum useful version, which is offline: log the diagnostics, periodically aggregate them, the analyzer reads the aggregate as a prior for the next batch run. That isn't real-time, but it's still the loop closed at a much smaller integration cost than "the optimizer notifies the analyzer in-band." Whether anyone wires it up beyond the minimum is a deployment question, not a design one — but the minimum is there for the taking and unusually cheap.

## Filter-after-scan: an existing pattern, an additional participant

Everything in this design slots into existing engine infrastructure:

- Bloom filter says "value V might be in this block" → scan confirms.
- Zone map says "min ≤ predicate-value ≤ max for this block" → scan confirms.
- Iceberg manifest stats say "partition includes the value" → file-level scan confirms.

Shape sidecars are another rung on the same ladder. "This row's signature is in the candidate set for predicate P" → typed-column scan confirms → residual fallback for `UNKNOWN`. The engine doesn't need a new operator, a new contract, or a new semantic. It needs the sidecar to look like a bloom filter or zone map for plan-composition purposes, and the planner to know its selectivity estimate.

The "necessary, not sufficient" discipline is already enforced by the same machinery — engines never treat a bloom hit as the answer. Sidecars inherit that discipline for free. Honesty about which conjuncts were actually evaluated versus only approximated is already part of the engine's bookkeeping; sidecars participate in it.

The richer vocabulary is the only novel piece: bloom filters answer hash-equality questions, sidecars answer arm-membership, path-presence, format-tag, shape-class questions. Same composition rules; richer alphabet.

## Lifetime scoping: bit assignments live and die with the chunk

The most important storage-layer discipline in this design is that **the meaning of every sidecar bit is scoped to the chunk it lives in**. Per-chunk metadata declares "bit 0 of the arm-tag column means `like-event`; bit 1 means `post-event`; shape-id 17 means signature `Sig(...)`." The compiler resolves tag names to bit positions *per chunk*, not globally.

This is not novel infrastructure. It's the same discipline Parquet uses for per-row-group dictionary encoding, ORC for per-stripe encodings, Druid for per-segment string columns, Iceberg for manifest-scoped statistics. Engines that already do per-partition planning absorb it without new plumbing.

A few consequences fall out cleanly:

- **The chunk has to carry its plan fragment in its footer.** Tag vocabularies, bit-to-meaning maps, declared selectivities, format tags. Small by design; sits alongside the chunk's column statistics.
- **Cross-chunk queries compose at the result-bitmap level.** You can't AND bitmaps from chunk A and chunk B because their bit positions mean different things, but you don't have to — each chunk produces its own surviving-row bitmap, results concatenate, standard partitioned execution.
- **Plan evolution is non-disruptive.** When the operator promotes a new plan, old chunks keep their old vocabularies and old chunks keep their old meaning. The planner reads each chunk's metadata and compiles accordingly. No rewrite required.
- **The tournament lifecycle becomes operationally cheap.** Parallel analyzers can independently write chunks under their own plans; chunks coexist; old plans retire by no longer producing new chunks. The "spin up the neat ideas, let them die if they can't compete" lifecycle works precisely because no chunk has to be touched when a plan changes.

The trap to avoid is the obvious shortcut: a global bit assignment that lets cross-chunk bitmap operations work directly. It removes the per-chunk metadata cost and looks like a simplification. It is also the move that silently mis-evaluates predicates the first time the bit vocabulary drifts under it. The data shape doesn't change; only the encoding's meaning does, which makes the bugs especially difficult to detect. Hard-pinning bit assignments to chunk lifetime kills the class of bugs at the cost of a few bytes of footer metadata per chunk — a trivially good trade.

A small implementation wrinkle: a name-to-bit lookup runs at query time, per chunk, to translate the compiled tag pattern into the chunk's local bit layout. Engines already cache per-chunk metadata at the planner level (column-existence checks, stats lookup, partition pruning); the sidecar's plan fragment lives next to them.

## Drift and the necessary-not-sufficient discipline

The sidecar's value depends on the writer being faithful. A row whose actual shape contradicts its sidecar bucket — because the writer was lazy, because shape inference at write time was wrong, because the row genuinely changed shape after the plan was committed — produces a wrong answer if the engine accepts the sidecar test as final.

The discipline that handles this is the same one the assertion layer in shapez handles for the analyzer:

- Every row that violates the chunk's plan is an exception, counted and logged.
- The engine's "filter-after-scan" pattern protects correctness as long as the scan is honest: it confirms what the sidecar approximated, including the cases where the sidecar over-approximated. The residual fallback catches the rows the sidecar misclassified.
- "How often do we lie?" becomes a first-class metric, available to the planner. When the exception rate at a chunk crosses a threshold, the planner can choose to bypass the sidecar entirely for that chunk and rely on the residual scan, or trigger a re-analysis.

Two writes-side disciplines support this:

- **The writer is responsible for the sidecar.** Computing the per-row signature, presence bits, and arm tags happens at write time, from the same `JsonEventSink` traversal that produces the typed columns. There is no "compute the sidecar later" mode.
- **The plan fragment in the chunk footer is the source of truth.** A reader that can't parse it pessimistically falls through to residual scan; never silently substitutes another plan's interpretation.

## What this enables

If the contract, the sidecar, and the compiler all work, the operational surface for storage and query layers downstream of shapez looks like this:

- **Predicate pushdown is a contract, not a guess.** The planner asks "can I push this?" and gets a typed answer per path, per chunk. The cases where the answer is "yes, with this rewrite" expand significantly compared with current variant-storage tooling.
- **Casts become known-cost.** Implicit castability predicates collapse to known selectivities or arm-membership filters; the planner stops gambling on cast cost.
- **The tournament lifecycle composes with existing storage.** Multiple parallel plans coexist in the same table; the planner navigates whichever plan each chunk was written under; promotion and retirement happen at the write boundary, not by in-place rewrite.
- **Workload feedback becomes structural.** The compiler reports which conjuncts it couldn't push; the analyzer uses that signal to propose plan tweaks; the back-test runs in a parallel analyzer; promoted tweaks become the new boring.
- **The "head of the distribution is the schema, the tail is the residual" tenet extends to the query layer.** Hot paths get columnar treatment with sidecar acceleration; cold paths get residual scans with no pretense of speed. The promise is fast queries on the structure that exists, honest fallback on the structure that doesn't.

## What's open

Naming the loose ends so the next pass has something to chase:

- **The contract's serialization format.** Probably a small typed structure embedded in the chunk footer next to existing column statistics; needs to round-trip through whichever storage layers consume it (typed buffer adapters, Spark/Iceberg variant writers, JSONB column promoters, etc.).
- **The compiler implementation.** Probably a small rule-based rewriter over the engine's predicate AST. Engine-specific because predicate ASTs differ; the rewrite rules are not.
- **Cross-chunk selectivity aggregation.** Per-chunk selectivity estimates are useful at scan time; cross-chunk aggregation is useful at plan time. Standard catalog-level rollup pattern, but it has to respect per-chunk plan versioning.
- **Workload trace ingestion.** The diagnostic feedback loop wants a way to read "which predicates ran, which conjuncts pushed, which fell through" from the engine. Engines vary in what they expose; this is integration work, not design work.
- **The `UNKNOWN` bucket's behavior at scale.** When a non-trivial fraction of rows land in `UNKNOWN`, the sidecar's value erodes. The tournament lifecycle wants this to be a trigger for plan promotion. The threshold and the response are open.
- **Negation safety in the compiler.** A formal treatment of which leaf-predicate mappings are exact and which are approximate, so the negation logic is right by construction rather than by audit.
- **Pruning hint retirement.** A mechanism for "this bit hasn't been read in 30 days; future chunks can stop emitting it" — purely a write-side optimization, but worth specifying.

None of this is blocking; the analyzer-side and storage-side stories can advance independently while this seam catches up. Writing it down now is mostly so the analyzer doesn't make assumptions that close off interesting query-side options.
