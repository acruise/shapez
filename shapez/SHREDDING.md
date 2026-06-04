# What do you actually shred?

*A look at the open question lurking inside the new wave of variant types.*

## The problem variant types solve, and the one they don't

In the past few years, every serious analytical engine has acquired a story for "we'll just stuff arbitrary JSON in there and figure it out later." Spark and Iceberg standardized on the `VARIANT` type and an associated shredded binary encoding. ClickHouse has had `JSON` (and now `Variant`, and `Dynamic`) for a while. BigQuery exposes `JSON` natively, with implicit indexing. DuckDB ships a structural JSON type. Snowflake's `VARIANT` has been there long enough to be a verb. Postgres `JSONB` is so old it doesn't count as a trend.

The convergence is real, and it solves something real. Pre-variant, the choice was between two ugly options:

1. Strict schema at ingest. Every new field is a migration; the producers can't ship faster than the consumers can review schemas. The unstructured tail of the data either gets dropped on the floor or hidden inside a `payload TEXT` column that nobody can query.
2. Document store. You don't pay the schema tax but you also can't do columnar analytics — the data sits in a row store, query plans look like loops, and the engineers who care about p99 latency learn to hate the data model.

Variant types collapse the dilemma. The engine accepts a tree of typed values; storage decides where to put the parts. Hot paths get hoisted into typed sub-columns ("shredded"); the cold tail stays in the variant blob. Reads against shredded paths get columnar pruning, vectorized decoding, and predicate pushdown; reads against the tail pay the document-store cost they would have paid anyway.

So far so good. But the question that does *not* have an obvious answer — and is almost entirely missing from the marketing materials — is:

> **Which paths do you shred?**

The shredded-variant binary format doesn't tell you. The catalog doesn't tell you. The query workload, if you have one, might tell you — eventually, expensively, after you've already chosen wrong for a while. And if the data is fresh enough that there is no representative workload yet, you have nothing.

This is the question that schema inference is supposed to answer. It is striking how thin the answer is, given how much engineering effort has gone into the storage side.

## What "schema inference" looks like in practice today

If you go shopping for tools that will look at a stream of JSON and tell you what shape it is, you find a handful of categories.

**Per-record inference libraries.** `quicktype`, `json-schema-inferrer`, `genson`, Spark's `spark.read.json(...).schema`, ClickHouse's `SCHEMA_INFERENCE_MAKE_COLUMNS_NULLABLE`. Walk the records, merge the per-record types, emit a union. The merge rule is almost always "dominant primitive type wins; otherwise widen to string." The output is a schema, often a draft one for human review. The unit of work is one record; the merge across records is unweighted.

These libraries are tremendously useful for what they do. They are also doing almost nothing about the shredding question. They tell you "the field `user_id` is a string"; they don't tell you whether `user_id` is worth pulling out of the variant blob into its own typed column. They tell you "the field `events` is an array of object"; they don't tell you whether those objects are five distinct event types you should split into a discriminated union, or one stable shape with a couple of optional fields, or a high-cardinality grab bag where the keys *are* the data.

**Variant-aware inference in storage engines.** Spark's `inferSchema` over variant, ClickHouse's `JSONExtractKeysAndValues`-based promotion, BigQuery's automatic JSON path indexing. These do start to take frequency into account. The pattern is generally: maintain per-path statistics during ingest, promote paths that exceed some threshold, leave the rest in the blob. The thresholds are configurable; the policy is hand-tuned per deployment.

This is closer to a real answer, but it has the shape of "we'll try things and back off." The output is a list of promoted paths and a set of knobs. There is no model of *why* one set of paths beats another, and the input to the decision is restricted to "how often did we see this path." Two things that are critical to a shredding decision are missing: whether the *shape* at that path is stable, and whether the *contents* are amenable to typed storage (or just a fancy way of storing JSON twice).

**Document-store auto-indexing.** MongoDB and Elasticsearch maintain dynamic field mappings. Couchbase, Marklogic, and Solr have done versions of this for decades. These systems do answer something like the shredding question — for the indexing decision. The answers are tuned to ad-hoc filter-and-fetch workloads, not columnar scans; whether the mapping is also the right shredding plan for an analytical engine is rarely asked, and rarely yes.

**Industrial schema-inference, summarized.** The unit of work is one record. The merge rule is dominant-type-wins. The frequency signal, when it exists, is per-path. The notion of *shape* — record vs map, variant vs union, polymorphic array vs tuple, recurring format inside a string — is barely present. There is no notion of a *promotion plan* as a distinct artifact, separable from the schema. The schema is the plan, and the plan is the schema, and both are draft.

This is not a swipe at the field. The state of the art was load-bearing for a long time when the alternative was hand-written schemas. It is just notably thinner than the storage technology it is feeding.

## A field that already has the vocabulary: grammar induction

There is an adjacent academic discipline that has spent forty-some years on the question of "given a stream of structured observations, what is the grammar that generated them?" It is called *grammar induction*, or *grammatical inference* if you went to a slightly different conference. It lives in formal-language theory, computational linguistics, and the parts of machine learning that grew out of those traditions.

The canonical reference is Stolcke and Omohundro's 1994 work on Bayesian state merging — given a finite automaton initially specialized to the observed strings, repeatedly merge states whenever the merge reduces a description-length objective (or, equivalently, increases the Bayesian posterior under a prior that prefers smaller grammars). The output is a regular grammar that explains the observations more concisely than rote memorization but less generally than the universal one. Forty years of follow-up work has refined the priors, the merge criteria, the expressiveness of the target grammar class (context-free, mildly context-sensitive, probabilistic), and the soundness guarantees (PAC-learnability, identifiability in the limit). Modern variants do everything from inferring protein structure to learning the syntax of legal documents.

None of this work routinely shows up in schema-inference papers. The audiences barely overlap. Schema inference, when it cites theory, tends to cite type theory or database normal forms; grammar induction, when it cites applications, tends to cite NLP or bioinformatics. The mechanical similarity is hard to miss once you look for it:

- A schema is a grammar over the language of JSON value trees.
- A `Variant { arms }` at a position is a disjunctive production at a non-terminal.
- A record-vs-map decision is a state-merging decision: do these per-key children stay distinct, or do they collapse into a single recursive production?
- The threshold for "this field has too many distinct values; treat it as a map" is, mechanically, an MDL trade-off: the description length of one nullable struct field per key versus one production that ranges over the key alphabet.
- A `StringFormat` (UUID, ISO-8601 timestamp, etc.) is a terminal tagged with a regular sub-grammar; format detection is a lexer over the value alphabet.
- A skeleton sketch (compress `ORD-9999-NNNNNN` to `A-9-9`) is learning a regular template at a position whose terminals have been collapsed.

What schema inference treats as engineering knobs (when to merge, when to split, when to keep a rare arm, when to drop it), grammar induction has full formalisms for: state merging, minimum description length, Bayesian priors on grammar size, smoothing for unseen productions. The vocabularies don't translate cleanly, but the structural moves do.

There is a reason the schema-inference world did not grow up speaking this language. The grammar-induction community largely cared about soundness — recognizing the target language correctly, including the rare productions — and the schema-inference world largely cares about *the head of the distribution*. The shredding question is exactly that: what is the typed columnar layout that captures most of the bytes, where "most" is defined by some product-side notion of cost? An algorithm that refuses to drop any production is, in the shredding context, a bug.

This is a real disagreement, and it is fixable, but as far as I can tell nobody has fixed it.

## JSON Tiles: the one credible answer

There is one piece of work that does sit cleanly in the intersection. Durner, Leis, and Neumann's *JSON Tiles* (SIGMOD 2021) is the cleanest published treatment of the shredding-as-inference question I have come across. The system is built around a few core ideas:

- **Tiles are batches of records that share a structural profile.** Rather than treating the whole table as one shape problem, JSON Tiles partitions the input stream into tiles (a few thousand records each) and infers a per-tile structure. A typed-column "skeleton" is shared across tiles where shapes match; tile-local outliers stay in the variant blob.
- **The promotion decision is per-tile.** A path becomes a typed column inside a tile if it appears frequently *in that tile* with a stable type. Tiles where the path is rare or shape-unstable leave it in the residual.
- **The residual is JSON-shaped, not opaque.** Rare paths can still be queried; they just don't get the columnar fast path.
- **The result is a columnar layout that approximates the per-tile shape distribution.** Queries against the dominant shape get columnar scans; queries against the tail get a fallback path. The format gets within a small constant factor of hand-tuned schemas on the workloads they tested.

The paper is good. The ideas are good. It is, as far as I can tell, the canonical reference for "we're going to infer a shredding plan from data rather than from a schema in the operator's head." A few authors have built on it (extending the format, evaluating it in different storage layers); large pieces of the JSON Tiles design recur in the design of the new variant-shredding formats arriving in Spark, Iceberg, and elsewhere.

There is also exactly one production-grade implementation I am aware of, and it is the prototype from the original paper. The ideas have diffused, but the substrate has not. If you go looking for "the JSON Tiles library," what you find is a research artifact, partially implemented, that lots of people cite and few people deploy.

This is not a failing of the paper. It is a sign of where the field is. The storage-side variant formats are now standardized and shipping. The inference-side answer to "which paths do you shred?" is still a SIGMOD paper.

## What JSON Tiles leaves open

JSON Tiles is a strong baseline. Naming the gaps is not a criticism of the work — it is an attempt to draw the perimeter of the open problem.

**Variants beyond "the dominant arm plus a residual."** JSON Tiles' per-tile shape collapses to a typed skeleton plus a residual blob. If the data is genuinely *polymorphic* — there are three event types in a feed and you want each one to get its own typed columns — the choice is to live with a single skeleton that covers the intersection (paying nullability tax everywhere) or to manually split the stream upstream. A first-class notion of *discriminated variant* — pick one of N typed arms, with a per-arm shredded plan — is conspicuously missing from the design space.

**Record vs map.** A JSON object with a few hundred stable keys and a JSON object with millions of unique keys are different shapes that deserve different storage strategies. JSON Tiles, like most schema inference, treats the high-cardinality case as a degenerate object — the keys become noise that the per-tile threshold suppresses. The result is "this path is opaque, leave it in the variant blob." But for many real high-cardinality maps, the values share a stable shape — `flags.<uuid>.enabled`, where the UUID is the noise but `.enabled` is a perfectly promotable boolean. Recognizing that pattern requires the inference to keep going *through* a wildcard, not stop at it.

**Format families inside strings.** A `varchar` that is 95% UUIDs and 5% something else is not the same as a `varchar` that is 95% free text. The first is a typed column waiting to happen; the second is not. JSON Tiles classifies these the same way, because the path-level inference doesn't look inside string values. Format detection (UUID, timestamp, IPv4, URL, dictionary-able enum) is one of the higher-ROI things a schema inferrer can do, and it falls outside the JSON Tiles model.

**Polymorphic arrays.** Is `[ {...}, {...}, {...} ]` a *bag* (every element from the same distribution, get one element schema) or a *tuple* (each position has its own schema, the array is acting as a fixed-shape record)? Or something between (most elements from one distribution, position zero is special)? The bag-vs-tuple decision changes what storage layout you want; it does not currently have a clean treatment in any inference system I have read.

**Cross-tile consistency, and drift.** JSON Tiles is per-tile by design, which is great for handling local heterogeneity. It is silent on the question of "the shape of tile 1000 has drifted from the shape of tile 1, and we want to know about it" — i.e., on drift detection as a first-class concern. In a long-running pipeline, that signal is often more important than the initial inference: shapes change, and the question is when and how loudly.

**The promotion criterion itself.** JSON Tiles' threshold-based promotion is reasonable. It is not a model. There is no objective function that captures the actual trade-off — bytes-saved-by-columnar-storage versus engineering-cost-of-the-wider-schema versus query-cost-of-the-residual-path — and lets you choose between two candidate plans by computing which one wins under your weights. That kind of cost model is standard in query optimization; it is more or less absent in shredding-plan optimization.

These are not five small bugs. They are five faces of one larger gap: there is a single open problem — *given a stream of semi-structured input, output a promotion plan that is optimal under some explicit cost function* — and the literature has roughly one published answer, partly implemented.

## Where does that leave the industry?

If you are an analytical engine vendor, the variant binary format is a solved problem. There is a spec, there are reference implementations, the integration story is straightforward. You can ship "we support shredded variants" tomorrow.

If you are a user of one of those engines, the variant binary format is not the thing standing between you and good query performance. The thing standing between you and good query performance is the promotion plan. Which paths get shredded, which stay in the residual, which become discriminated variants, which become wildcards over high-cardinality maps with promoted inner fields. And for that, you have:

- Hand-written schemas, if you have a person with the time and the domain knowledge.
- Threshold-driven auto-promotion, with knobs and a feedback loop measured in days.
- One academic paper, partially implemented.
- A neighboring field with forty years of relevant formalism that almost nobody has plugged in.

There is something interesting here. The same kind of work that went into making columnar formats fast — careful cost models, principled compression-vs-decode trade-offs, predicate pushdown, statistics — has not yet been done on the inference layer that decides what to feed into those formats. The storage layer is industrial; the layout layer is artisanal.

The shape of an answer is, I think, clear enough: importing MDL or Bayesian-prior scoring from grammar induction, generalizing tiles to include variants and discriminated maps, treating format detection inside strings as a recursive instance of the same problem, defining an explicit cost function over plans, and shipping the whole thing as a library that engines can call.

I don't think it is mostly an open *research* question. The hard parts are documented; the gap is one of integration and engineering will. Worth saying out loud, though, that the question is open at all — because from the way variant-type announcements have been written for the past two years, you might not have noticed.

---

*This essay sketches the perimeter of the problem and stops short of describing one in-progress answer. The system that prompted this writeup — shapez — is a streaming-and-batch structural shape inferrer designed around exactly this gap. The design doc in the same repo (`DESIGN.md`) goes into the specific moves; this essay is the prose version of why those moves are worth making at all.*
