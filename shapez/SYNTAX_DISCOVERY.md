# Syntax discovery from first principles

Companion to `DESIGN.md`, which describes how shapez infers shape from a stream of *already-decoded values*, and to `SHREDDING.md`, which describes why the promotion plan — not the binary format — is the hard part. This doc describes the layer *below* both: what happens when nobody tells you what the bytes are.

It expands the sketch at `DESIGN.md` § *Bitstream-level syntax discovery*. That section is fifteen lines and a promise; this is the design those lines were pointing at.

Status: stages 0–2 are implemented in the `shapez-sniff` crate. Stages 3–6 are specified here and not built. Nothing in this doc changes the core analyzer's contract — the whole thing sits upstream of `JsonEventSink` and hands it events like any other adapter.

## The problem: no hypothesis to test

`DESIGN.md` § *Recursive syntax shifts* describes recursing into a leaf string when it "looks like JSON." That works because there is a hypothesis in hand: something in the pipeline — a column name, an operator hint, a leading `{` — proposed a syntax, and the analyzer's job is to confirm or refute it.

The general case has no such gift. You are handed a byte range. It might be UTF-8 JSON. It might be UTF-16LE CSV exported by a Windows tool in 2009. It might be length-prefixed protobuf. It might be gzip. It might be a base64 field inside a log line inside a JSON string inside a Kafka message whose envelope you already stripped. It might be three of those concatenated because a retention job appended files without checking.

The distinguishing property of this layer is that **every downstream stage is conditioned on a decision the data itself has to justify**. Get the code unit wrong and the ngram tables are noise. Get the syntax wrong and the parser emits confident nonsense. Get the framing wrong and you infer one enormous document instead of ten million small ones — which is not a parse error, just a totally useless answer.

So the design commitment that matters most here is the same one the analyzer already makes elsewhere: **decisions are deferred, evidence accumulates, and multiple hypotheses stay alive until one earns the commitment.** Dual-view ingest, one layer down.

## The pipeline

```
bytes
  │
  ├─ stage 0  code-unit and text-ness detection      ──►  Alphabet
  ├─ stage 1  ngram sketches over that alphabet      ──►  NgramProfile
  ├─ stage 2  fingerprint scoring                    ──►  Vec<Candidate>
  ├─ stage 3  framing detection                      ──►  Vec<Framing>
  ├─ stage 4  speculative parsing + bankruptcy       ──►  winning SyntaxModule
  ├─ stage 5  child-structure events                 ──►  JsonEventSink
  └─ stage 6  chaos-driven descent at leaves         ──►  recurse to stage 0
```

Stages 0–3 are cheap, streaming, and bounded-memory; they are sketches like any other sketch in shapez. Stage 4 is where real work and real risk live. Stages 5–6 are plumbing back into the machinery that already exists.

## Stage 0: is it text, and in what code unit

Before an ngram means anything you have to know what a *gram* is. Counting bytes in UTF-16LE ASCII gives you a beautiful, useless distribution dominated by `0x00`.

The signals here are unusually crisp, which is a nice change:

- **BOM.** `EF BB BF` → UTF-8. `FF FE` → UTF-16LE. `FE FF` → UTF-16BE. `FF FE 00 00` → UTF-32LE (test before UTF-16LE; the prefix collides). Present maybe a third of the time in the wild, decisive when present.
- **Null-byte position parity.** This is the good one. UTF-16LE-encoded ASCII puts a `0x00` at every odd offset; UTF-16BE at every even offset. Measure the fraction of nulls landing on odd vs even offsets: a strong parity skew with a null density near 0.5 is UTF-16 with near-certainty, and the skew direction gives you the endianness for free. No BOM required.
- **Null density without parity skew.** Nulls scattered without parity structure mean binary. Text formats essentially never contain interior nulls.
- **UTF-8 continuation validity.** Bytes ≥ `0x80` in a text stream should form valid UTF-8 lead/continuation sequences. Measure the fraction of high bytes that participate in a well-formed sequence. Near 1.0 → UTF-8 with non-ASCII content. Near chance → Latin-1, or binary.
- **Printable ratio.** Fraction of bytes in `0x20..0x7E` plus `\t\r\n`. Above ~0.95 is text-shaped; below ~0.7 is not.
- **Shannon entropy over the byte histogram.** English-ish text lands around 4.0–4.7 bits/byte; JSON and other punctuation-heavy syntaxes a bit lower; base64 around 6; compressed or encrypted data pins near 8.0 and stays there. Entropy above ~7.5 with a flat histogram is a *terminal* answer, not a failure: the correct report is "this is compressed or encrypted, there is no syntax to find," and the right move is to stop rather than burn a year of parser speculation on ciphertext.

The output is an `Alphabet` — the code unit to count in, plus the confidence that the input is text at all. When the answer is "not text," stages 1–2 still run over bytes, because binary formats have their own fingerprints (protobuf's field-tag byte distribution is distinctive; so is the `PK\x03\x04` of a zip, the varint density of a length-prefixed stream, the `0x0A`-heavy tag bytes of a repeated-string proto). We just don't pretend the grams are characters.

## Stage 1: ngram sketches as the universal front end

Once the code unit is fixed, accumulate three tables:

- **Unigram: exact.** 256 counters for bytes, or a folded table for wider code units. Two kilobytes. There is no reason to sketch this.
- **Bigram: Space-Saving, cap ~256.** The full table is 64K entries; affordable once at the top level, not affordable per-leaf when stage 6 starts recursing into every chaotic string column in a wide schema. Sketch it, and inherit the eviction-rate signal for free.
- **Trigram: Space-Saving, cap ~256.** Optional — the `DESIGN.md` sketch says "*maybe* trigrams," and that hedge was correct. Trigrams are what separate near-neighbors (`</` and `/>` for XML vs SGML-ish soup; `"""` for Python-ish; `-->`), and they are dead weight when the bigram evidence is already decisive. Make them a policy knob, on by default at the top level and off in stage-6 recursion.

This is the same `SpaceSaving` used for cluster signatures and string skeletons in the analyzer, at a different scope. That is deliberate: the eviction rate carries the same meaning here that it does there. A bigram sketch that is evicting on most observations is telling you the input has no characteristic distribution at its head — which is the entropy answer arriving by a second road.

Two refinements that pay for themselves:

- **Line-anchored grams.** Count `^c` (first character of a line) and `c$` (last) as their own gram classes. Line-leading `<` is much stronger XML evidence than `<` anywhere; line-trailing `,` is much stronger evidence of a wrapped array than `,` anywhere; line-leading `-` plus space is YAML sequence syntax. A handful of anchored counters buys more discrimination than doubling the bigram cap.
- **Per-line delimiter-count variance.** Not a gram, but it lives in the same pass and it is the single best CSV signal in existence. Frequency of commas says nothing — English prose is full of commas. *Every line having exactly seven commas* says CSV and essentially nothing else says it. Keep a small running mean/variance per candidate delimiter (`,` `\t` `;` `|`), plus a count of lines agreeing with the modal value.

The whole profile is bounded, mergeable, and serializable — same properties every other shapez sketch has, for the same reasons.

## Stage 2: fingerprints and evidence in bits

The `DESIGN.md` sketch says `}` + `,` + `nu` + `,[` is "almost certainly JSON." True, and the temptation is to write that as a pile of hand-tuned thresholds joined by `&&`. That produces a classifier nobody can debug and nobody can extend.

Instead: **each candidate syntax is a set of features, each feature contributes signed evidence measured in bits, and the candidate's score is the sum.**

```
score(syntax) = Σ_f  weight(syntax, f) · satisfaction(f, profile)
```

where `satisfaction` returns a value in `[-1, +1]` — fully violated to fully satisfied — and `weight` is that feature's evidentiary strength in bits for that syntax. Confidence is a logistic over the summed bits. This is naive Bayes wearing work clothes: the features are not independent and we are not pretending otherwise, but log-odds accumulation degrades gracefully under correlated evidence in a way that threshold cascades do not.

Feature kinds worth having:

- **Presence with expected density.** "`{` occurs at 1–8% of characters." Satisfaction ramps within the band and falls off outside it. Density, not count, so it is length-invariant.
- **Balance invariants.** `count('{') ≈ count('}')`, `count('[') ≈ count(']')`, `count('"')` even, `count('<') ≈ count('>')`. Cheap, and *very* discriminating: prose contains braces, but prose does not contain balanced braces.
- **Anchored markers.** Line-leading `<`, line-trailing `,`, line-leading `#`, `[section]` on its own line.
- **Cadence.** The delimiter-count variance above, and its cousins: line-length variance (fixed-width formats have near-zero), indentation-step consistency (YAML uses a consistent unit).
- **Alphabet restriction.** Base64 is "every character is in `[A-Za-z0-9+/=]`, `=` only at the end, length divisible by 4." Hex is tighter still. These are nearly boolean and deserve big weights, positive *and* negative — a single character outside the alphabet is decisive refutation.
- **Negative evidence.** URL-encoded data should have essentially no raw whitespace. JSON should have no unescaped raw newlines inside strings. Absence of an expected-absent marker is weak positive evidence; presence is strong negative evidence. The asymmetry is the point, and it wants first-class support: a feature carries *two* weights, one for satisfaction and one for refutation, and the strongest form sets the positive weight to zero. A refutation-only feature can sink a candidate and can never float one.

  That last case is not a nicety. Symmetric weights mean each syntax accumulates a large positive baseline from all the things the input *isn't* — and since most inputs aren't most things, the baseline is nearly always collected. A file of bare integers, one per line, scored 0.70 as CSV purely on the strength of not being JSON, not being XML, and containing no control characters. There were no commas in it at all.

Three properties this design has to preserve:

**Candidates are not mutually exclusive.** JSONL *is* JSON plus a framing commitment. A file can be simultaneously valid CSV and valid TSV (one column, tabs in the data). Do not softmax over an exclusive set; emit an independently-scored ranked list and let stage 4 sort out the overlap. The natural output is `Vec<Candidate>` sorted by confidence, not an `enum Syntax`.

**Every candidate carries its evidence.** `Candidate { syntax, confidence, evidence: Vec<Evidence> }` where each `Evidence` names the feature, its satisfaction, and its bit contribution. This is not a debugging nicety. It is the same commitment `PUSHDOWN.md` makes about the predicate compiler reporting which conjuncts it couldn't push: a classifier that can explain itself can be corrected by a workload trace, and one that can't is a black box that ages badly. When the sniffer is wrong in production — and it will be — the evidence list is the difference between a ten-minute fix to one feature weight and a week of bisecting sample files.

**`Unknown` is a first-class answer.** Below a confidence floor the honest output is an empty candidate list. Downstream, "opaque bytes" is a perfectly good shape; `String`/`Blob` with a length distribution is a real answer and often the correct one. The failure mode to design against is not "we didn't identify it," it is "we identified it as CSV and produced a one-column table with ten million rows of JSON."

### Field notes: the confusions that actually bit

Four calibration failures showed up the moment the scorer met real data. All four are the same species — a feature that looked discriminating in the abstract and turned out to be satisfied by things it was never meant to admit — and they are worth recording because the next syntax added to the roster will hit the same class of problem.

**Perfect CSV cadence is the default state of JSON Lines.** A stream of records with a stable shape has exactly the same number of commas on every line. That is CSV's single strongest signal, satisfied at 1.0, on a file that is not remotely CSV. The fix is the refutation-only "and there is no JSON structure here" feature above, weighted heavily enough to sink the hypothesis outright. This is the single most important interaction in the module and nothing else comes close.

**A JSON signature keyed on quoted keys misses array-shaped JSON.** The obvious formulation of "does this look like JSON" is quoted-key colons *and* structural brackets *and* balance. But a line like `[-34067,null,false,"",5567,…]` — 200 comma-separated scalars in brackets — has almost no quoted keys, so the conjunction reports "not JSON" and hands the file straight to CSV. Worse, that line genuinely *is* a CSV row except for the two brackets wrapped around it. The brackets have to carry the discrimination on their own, which means the signature needs a disjunction over markers (quoted keys *or* bracket density *or* bracket-framed lines) rather than a conjunction, gated on balance.

**"Depth returns to zero at end of line" is vacuously true.** It was the JSON Lines framing feature, and it holds for any line containing no brackets at all — which is most lines of most files. An INI file scored 0.86 as JSON Lines on the strength of it, helped along by `[section]` headers supplying a leading bracket and perfect bracket balance. The fix is to pair the depth test with evidence that the line actually *was* a whole value: bracket-framed, or a bare scalar literal. A predicate that is satisfied by absence is not a predicate.

**Alphabet-restriction features nest.** Every hex digit is a valid base64 character, so hex input satisfies base64's alphabet test at 1.0. Entropy separates them in principle — hex sits near 4 bits/byte, base64 near 6 — but a gentle band ramp left base64 narrowly ahead. Where one syntax's alphabet is a subset of another's, the superset needs to refute the subset by name; hoping a secondary feature breaks the tie is not a design.

The generalizable lesson: **the dangerous feature is not the one that fails to fire, it's the one that fires for the wrong reason.** Every feature added to a fingerprint should be interrogated for what else satisfies it, and if the answer is "most inputs," it belongs on the refutation-only side or nowhere.

A second, smaller lesson: **sampling breaks exact structural invariants.** Bracket balance tested with `==` reports "unbalanced" for perfectly good JSON as soon as the sampler takes windows that open mid-value — which handed the CSV hypothesis two free bits on exactly the large files where the sampler engages. Any invariant a fingerprint depends on has to be expressed as a graded ratio, not an equality, or the sampling budget silently changes the answer.

### Field note: bare scalars are still JSON

A stream of `5440\n-5626\n5875\n` is valid JSON Lines and has none of the markers every JSON feature looks for — no braces, no quotes, no colons. Scored against object-shaped features it reads as unstructured text.

The resolution is a cheap lexical check for whether a line is, in its entirety, a single JSON scalar literal, and then treating the object-shaped features as *inapplicable* rather than refuted when scalars dominate. That distinction — inapplicable versus refuted — is the same one the deep-bit design in `PUSHDOWN.md` needs for path-dependence, where a bit at an unreached path is not "false" but "the question was not asked." It shows up here for the same reason and wants the same treatment.

It also nudges at the stage-3/stage-4 boundary: a lexical scalar check is a very small parser, and the argument for keeping it in stage 1 is only that it costs one pass and no state. Anything more should wait for the speculative-parser harness rather than accreting into the sketch.

## Stage 3: framing is a separate axis

Syntax and framing are orthogonal and conflating them is a classic mistake. The same JSON syntax appears as:

- **Newline-delimited** — one value per line, the JSONL/NDJSON convention.
- **Self-delimiting concatenation** — values back to back with no separator, because JSON is prefix-decodable and somebody noticed.
- **Length-prefixed** — a varint or fixed-width count before each record; standard for protobuf streams, occasionally used for JSON.
- **Sentinel-delimited** — `0x1E` record separator (RFC 7464), `\0`, `---` for YAML documents, a custom marker.
- **Single document** — the whole input is one value, typically a top-level array. The pathological case that makes streaming hard.
- **Externally framed** — the framing lived in a layer already stripped (Kafka message boundaries, HTTP chunking, file boundaries). Nothing in the bytes tells you; the caller has to.

Detecting framing is mostly cheap, and mostly a matter of testing whether a candidate boundary rule yields *consistent* records: split on the rule, check that the pieces individually satisfy the syntax's balance invariants. Newline framing for JSON is confirmed by "brace depth returns to zero at every newline and nowhere else." Length-prefix framing is confirmed by "read the prefix, skip that many bytes, land on something that looks like another prefix" — and repeats. Both tests are a single pass and both fail fast.

The output is a `Vec<Framing>` scored the same way as syntax, and the cross product with the syntax candidates is what stage 4 speculates over. In practice the cross product is small: most syntaxes admit two or three plausible framings, most framings are refuted within a few kilobytes, and the surviving set is usually one or two entries by the time it matters.

## Stage 4: speculative parsing and the bankruptcy protocol

Fork a parser per surviving `(syntax, framing)` pair. Run them all against the same stream. Kill the ones that fail. The last one standing wins.

That is the `DESIGN.md` sketch, and it is half right. The half it gets wrong is the most important thing in this document.

### Parse success is the wrong criterion

**A CSV parser never fails.** Hand it a JSON file and it will cheerfully report a one-column table whose single column contains one long string per line. No error. No contradiction. No bankruptcy. It parsed everything you gave it, at 100% success, and the answer is worthless.

This generalizes: the more permissive a syntax, the more reliably its parser "succeeds" on input that isn't it. Free-text log-line parsing succeeds on literally any text. Fixed-width parsing succeeds on anything with newlines. The permissive parsers are exactly the ones that will out-survive the strict ones under a survival-of-the-non-crashing rule, which means a naive bankruptcy protocol is **biased toward the least informative answer.** That is the opposite of what you want, and it fails silently.

So bankruptcy has to have a second clause, and the second clause is the interesting one.

### Solvency: yield, not survival

A parser is solvent when it is *earning its bytes*. Track, per module:

- **Error rate.** Bytes consumed per hard error. The traditional signal; still necessary.
- **Structural yield.** Non-terminal events emitted per kilobyte consumed. The one-column-CSV parse of a JSON file emits one `object_begin`, one `object_key`, one `string`, one `object_end` per line — a yield floor. A real JSON parse of the same bytes emits dozens. Low yield relative to the field is grounds for bankruptcy even with a zero error rate.
- **Residual mass.** Fraction of input bytes that end up inside terminal string leaves rather than being consumed by structure. The one-column CSV parse has residual mass ≈ 1.0. This is the sharpest single number, and it is a direct analogue of the residual/promoted split that the rest of shapez is organized around.
- **Progress.** Bytes consumed with zero records emitted is a hang, not a parse. Cap it.
- **Memory.** A parser whose stack or buffer grows without bound on adversarial input is bankrupt regardless of what it thinks it's doing.

Any of these tripping past a threshold declares bankruptcy. Together they kill the permissive-parser bias: the CSV module survives the error-rate test on JSON input and dies immediately on residual mass.

### Winner selection is a shredding-cost question

Once the survivors are down to a small set, ranking them by "least residual mass" is a decent heuristic. But there is a better criterion available, and shapez is unusually well positioned to compute it: **run the analyzer on each survivor's event stream and rank by the quality of the resulting promotion plan.**

This is minimum description length, and it is the same MDL that `SHREDDING.md` and `DESIGN.md` § *A tenuous connection to grammar induction* both circle. The total cost of an interpretation is

```
cost = |schema| + |data given schema|
```

A parse that yields a tight `Record{7 fields, stable types}` has a small schema and shreds the data into typed columns: low total cost. A parse that yields `String` has a schema of essentially zero size and a residual containing every byte of the input: high total cost. The correct syntax is, almost by definition, the one under which the data compresses — because syntax *is* the structure that makes the data compressible, and a wrong syntax finds no structure to exploit.

The payoff of framing it this way is that it needs no new machinery. Shape inference is already the thing that measures this; the analyzer already computes cluster signatures, eviction rates, and — in the planned cluster-aware ROI advisor — an explicit cost over layouts. Syntax selection becomes a call into that advisor with `n` candidate event streams instead of one. The tournament framing from `DESIGN.md` § *Tournament-style policy lifecycle* applies verbatim: spin up the speculative parsers, let them compete on a cost metric, let the losers die by attrition.

There is a real cost to being honest about: this runs the analyzer `n` times over the sniffing prefix. With `n` typically 2–4 and the prefix bounded to a megabyte or so, that is fine. It would not be fine as a per-leaf operation in stage 6 without a much smaller budget, which is one of several reasons stage 6 gets its own budget rules below.

### The bankruptcy protocol, concretely

```rust
trait SyntaxModule {
    /// Feed a chunk. Returns records emitted and bytes consumed.
    fn feed(&mut self, chunk: &[u8], sink: &mut dyn JsonEventSink) -> Progress;
    /// Current solvency; the harness polls after every chunk.
    fn solvency(&self) -> Solvency;
    /// Voluntary bankruptcy — the module knows it is done.
    fn concede(&self) -> Option<BankruptcyReason>;
}
```

Modules may declare their own bankruptcy (a hard parse error, an internal contradiction, an invariant they know they violated) and the harness may declare it for them (yield floor, residual ceiling, no progress, memory cap, timeout). Both paths produce a `BankruptcyReason` that is retained and reported. A run where every module went bankrupt is not a crash — it is the `Unknown` answer arriving with a full explanation of what was tried and how each attempt died, which is exactly what an operator needs to see.

## Stage 5: modules expose child structure

The winning module drives `JsonEventSink` and everything downstream is unchanged. This is the whole reason the trait is defined as an *event vocabulary* rather than a JSON commitment — `DESIGN.md` § *Beyond JSON* already made this argument and already listed what the mappings look like for protobuf, CSV, Avro, Arrow, and XML.

Two notes specific to discovered (rather than declared) syntax:

- **CSV without a trustworthy header.** Discovery mode can't assume the first line is a header. Emit positional field names (`col0`, `col1`, …) and let the analyzer's own machinery notice that row 0's values are all `AllAlpha` strings while every subsequent row's column 3 parses as a timestamp. Header detection becomes a shape question — one the analyzer is better at than a heuristic in the parser.
- **XML remains awkward, and discovery doesn't fix that.** `DESIGN.md` is already candid that mixed content and attribute-vs-element ambiguity don't lift cleanly to JSON's event vocabulary. Discovering that the input is XML doesn't make the mapping less opinionated; it just means nobody explicitly chose the opinion. Flag it in the report.

## Stage 6: chaos-driven descent

The recursive step: a leaf that the winning parser called a string might be a structured payload in another syntax. Re-run the whole pipeline on its bytes.

The trigger is already computed. `StringStats` at a leaf carries the format histogram, the length sketch, and the Space-Saving skeleton table. The descent signal is a conjunction:

- Dominant `StringFormat` is `Other` (the built-in formats didn't claim it),
- **and** skeleton eviction rate is high (no small set of recurring shapes — so it isn't a templated ID either),
- **and** the length distribution is long-tailed and wide (structured payloads vary in size; opaque tokens don't),
- **and** the leaf carries enough total bytes to be worth the attention.

That last clause matters more than it looks. A chaotic 12-character column is chaotic because it's a random ID; a chaotic 4-kilobyte column is chaotic because something is hiding in it. **Weight the descent decision by bytes, not by row count.**

Rules the recursion needs, all of them learned from the general shape of "recursion into untrusted structure":

- **Bounded depth.** A hard cap, defaulting low — two or three. `DESIGN.md` already calls for this. Beyond the cap, the leaf is opaque and that is the final answer.
- **A byte budget per descent, decreasing with depth.** Stage 4's MDL tournament is affordable at the root and not affordable at depth 3 across a hundred leaves.
- **Negative caching.** A leaf that descends and finds nothing is marked opaque and *not retried*. Without this, a wide schema full of random IDs re-sniffs every one of them on every tile, forever. This is the single most likely way for this feature to become a performance incident.
- **No feedback into the parent's parse.** A successful descent adds a sub-shape at that position; it does not retroactively change the parent's syntax decision. Allowing it to would make the whole thing non-terminating in principle and non-debuggable in practice.

Path syntax extends as `DESIGN.md` proposes: `.payload@json.user.id` addresses a field inside a discovered JSON payload inside the `.payload` string. The `@syntax` step is a layer transition, and it is *syntactically visible*, which matters — an operator reading a promotion plan needs to see that `.payload@json.user.id` costs a parse per row in a way that `.user.id` does not.

## Budgets, sampling, and where the bytes come from

Sniffing does not read the whole input, for the same reason the analyzer doesn't fully walk every document. But a naive prefix is worse than it looks:

- **A prefix alone is biased.** Files start with headers, BOMs, comment blocks, and one unrepresentative record. A CSV whose first 40 lines are a comment banner sniffs as free text.
- **Sample the tail too.** Take a prefix (enough to catch BOM, header, and framing) plus sampled windows from further in. Windows, not scattered bytes: ngram and cadence features need locality, and a byte sampled at random has no bigram.
- **Align sampled windows to a plausible boundary.** Start a window at the first newline after the offset, so line-anchored features aren't garbage. Cheap, and it stops half the false signals.
- **Budget in bytes, not records.** Records don't exist yet. That's the whole problem.

Defaults worth starting from: 64 KB prefix, plus up to 8 windows of 32 KB each, capped at 1 MB total. Enough for a decisive answer on every format listed here; small enough to run at file-open time.

### The input is a stream, not an array

The framing above quietly assumes random access — "windows from further in" means seeking to a known offset in a known length. That assumption does not survive contact with the actual inputs. A file too large to map, a socket read, a Kafka partition, the output side of a decompressor: all of them arrive as a sequence of chunks, once, in order, with no length known in advance. A design that only works on `&[u8]` works on the easy case and not the general one.

Two things have to hold for the general case, and they are independent.

**Bounded memory.** Every accumulator has to be fixed-size or capped, in the length of the stream. Most already are — the unigram table is 256 counters, the ngram tables are Space-Saving sketches with a cap, the cadence histograms are capped, line geometry is running sums. The one that isn't, and the one that bites, is any *per-line* buffer: a single-document JSON stream is legitimately one line of forty gigabytes. So the per-line state has to be counters (length, indent, delimiter counts, first and last byte) with a small bounded buffer retained only for the checks that genuinely need the bytes — the bare-scalar test, which by construction only ever matters for short lines. Beyond the cap the line is marked as overflowed and the check gives up, which is the honest answer rather than an out-of-memory.

**Chunk boundaries have to be invisible.** Every statistic that spans more than one byte needs explicit carried state: a shift register for bigrams and trigrams, the CSV quote state including the one byte of lookahead the doubled-`""` escape needs, the JSON string-and-escape state, the bracket depth, the partial UTF-16 code unit, the partial BOM. Getting one of these wrong produces a bug that only appears at particular chunk sizes against particular inputs — the worst kind. The only real defense is a test that feeds the same bytes at every chunk size and at every possible split point and demands an identical answer, which is cheap to write and worth more than reading the code twice.

The distinction that makes this tractable is between a **chunk edge** and a **boundary**. A chunk edge is an artifact of how the caller happened to slice its reads; nothing may reset. A boundary is a genuine discontinuity — a sampling seam, where the next byte does not follow the previous one — and there the bracket depth, the string state, and the gram register all must reset. Conflating the two is exactly the bug this separation exists to prevent.

### Sampling without seeking

Windows spread evenly across a known length need both random access and the length. With neither, the workable substitute is to profile a contiguous prefix, then alternate window and skip with **the skip doubling each time**: dense coverage early, sparse late, reach growing exponentially in the number of windows, and no byte offset known in advance.

The window count has to be bounded, and the reason is worth recording because it is not obvious in advance. With unbounded doubling the budget is never spent: filling a 1 MB profile 32 KB at a time with a doubling skip requires something on the order of *thirty terabytes* of stream to get through. A "have I seen enough?" predicate built on the byte budget would never fire, and a reader-driven sniff would never stop early. Bounding the window count fixes it — with eight windows the sampler reaches about 8 MB into the stream and profiles about 320 KB of it.

What this cannot do is sample the end of a stream it refuses to read. Sequential access with a bounded read budget sees a prefix, however cleverly subsampled. That is a real limitation and not a fixable one: a caller who is streaming the whole input anyway, where the cost is CPU rather than I/O, should raise the window and byte budgets and get coverage as deep as it wants; a caller who wants a cheap answer at open time gets a prefix-weighted one. The honest move is to make the trade a policy knob rather than to pretend the sampler is unbiased.

Two consequences fall out that are worth knowing before they surprise someone:

- **The two access models sample different bytes.** Below the prefix size the slice path and the stream path are identical by construction. Above it they diverge, and can report slightly different confidences for the same input. This is not a defect to paper over by crippling the slice path — random access genuinely is more information than sequential access, and the path that has it should use it. What must agree is the verdict, and that is what the tests assert.
- **Each seam manufactures a line boundary.** The partial line at the end of a window gets flushed before the next window starts, so a sampled stream never reports exactly one line even when the input genuinely is one enormous line. The distortion is one spurious line per window, negligible against a window's worth of real lines — but it means line-count assertions have to be written as bounds, not equalities.

The same discipline extends forward. Stage 4's speculative parsers are fed by the same chunk stream and need the same treatment: a parser that can only accept a complete buffer is not a candidate. That is a constraint on the `SyntaxModule` trait, and it is cheaper to impose now than to retrofit.

## Honest failure modes

Stating these plainly, because a sniffer that hides its failure modes is worse than no sniffer:

- **Concatenated heterogeneous input.** A file that is CSV for the first half and JSON for the second is a real thing that happens, and this design will confidently report whichever dominates the sample. Detecting it needs a change-point test over the ngram profile, which is not in scope and is not free.
- **Nested encodings that reduce entropy in the wrong direction.** Base64-of-gzip looks like base64 (correctly), decodes to something that looks like high-entropy binary (correctly), and terminates there (correctly, but unhelpfully). Chaining decoders — base64, then gzip, then re-sniff — is an obvious extension and an obvious way to build a decompression bomb. It needs its own budget discipline before it goes anywhere near untrusted input.
- **Adversarial input.** Everything here consumes attacker-controlled bytes. Every module needs bounded memory, bounded recursion, and a timeout, and the harness has to enforce them rather than trusting modules to behave. Treat a module that exceeds its budget as bankrupt, not as an error to propagate.
- **The JSON refutation is aggressive, and CSV-with-an-embedded-JSON-column pays for it.** The feature that stops JSON Lines from reading as CSV keys on bracket density and balance across the whole sample. A genuine CSV file with one column full of JSON blobs trips it, and CSV gets refuted on a file that really is CSV. This is a deliberate trade in the direction the cost asymmetry argues for, but it is a real false negative on a real shape — and it is precisely the chocolate-and-peanut-butter case `DESIGN.md` § *Recursive syntax shifts* cares most about. The designed resolution is stage 4: let both parsers run and let residual mass decide, rather than trying to settle it from ngram statistics that genuinely cannot.
- **Near-neighbor confusion is permanent.** TSV vs single-column CSV containing tabs. YAML vs a colon-heavy config format. JSON vs JSON5 vs relaxed-JSON-with-trailing-commas. In some of these cases the bytes genuinely do not distinguish the alternatives, and the right behavior is to report both candidates with close confidences rather than to pick. Stage 4 will often break the tie on shredding cost; when it can't, neither can anything else.
- **The confident-wrong case is the one that costs money.** A false negative costs one opaque column. A false positive costs a promotion plan built on a fictional schema, which propagates into storage layout and query plans. Weight the thresholds accordingly: this classifier should be biased toward `Unknown`.

## What this composes with

- **`DESIGN.md` § *One person's syntax, another's semantics*** — this is that principle at its limit. Every other layer in shapez promotes structure into meaning at a boundary the caller declared. This layer infers the boundary itself.
- **The grammar-induction thread.** Stage 4's MDL winner-selection is the first place in shapez where description length is an actual computed criterion rather than an acknowledged influence. If MDL scoring lands anywhere first, it should probably land here — the objective is unambiguous and the search space is tiny.
- **The tournament lifecycle.** Speculative parsers are a tournament. Same shape, same "let the losers die by attrition" discipline, much shorter time horizon.
- **`PUSHDOWN.md`'s diagnostic loop.** The evidence lists from stage 2 and the bankruptcy reasons from stage 4 are a trace format for free. Aggregated over a corpus, they say which feature weights are miscalibrated and which syntaxes are missing from the roster.
- **`should_bail`.** Stage 6's descent trigger and the analyzer's existing chaos signal are the same signal read at different scopes.

## What's open

- **Feature weight calibration.** Hand-assigned from first principles, then adjusted until the ranking held across the `samples/` corpus and a dozen synthetic fixtures. That is calibration by anecdote: it fixes the cases that were tried and says nothing about the ones that weren't. The right answer is to fit the weights on a labelled corpus, and a held-out set of the pathological cases above matters more than volume. Until then the confidences should be read as an ordering, not as probabilities.
- **The roster's blind spots.** No fingerprint yet for length-prefixed binary, protobuf, TOML (as distinct from INI), fixed-width, or JSON5. Each is a small addition; each also needs interrogating against every existing fingerprint for the subset-alphabet and vacuous-predicate failures above, which is the part that isn't small.
- **Change-point detection over the ngram profile,** for the concatenated-heterogeneous case. Probably a windowed comparison of successive profiles against a divergence threshold. Cheap to compute, hard to threshold.
- **Decoder chaining** (base64 → gzip → re-sniff) and its budget discipline.
- **The MDL cost function, concretely.** Stage 4's winner selection is specified as "rank by promotion-plan quality" and that is not yet a number. It becomes one when the cluster-aware ROI advisor lands; until then, residual mass is the stand-in.
- **Interaction with the epoch model.** Is the sniffing decision per-tile or per-source? Per-source is obviously cheaper. Per-tile handles the case where a source's format changes mid-stream, which is exactly the case that motivated change-point detection. Likely: decide per-source, re-validate cheaply per-tile, re-sniff on divergence.
- **Where the roster of candidate syntaxes lives.** Compiled-in is simplest and adequate for now. A registry that lets a deployment add a proprietary format's fingerprint without a shapez release is the obvious next step and a much larger commitment.
- **Whether stage 4 belongs in shapez at all.** Stages 0–3 are pure sketching over bytes and clearly belong. Stage 4 pulls parser implementations into the dependency graph, which is exactly the coupling the `shapez` / `shapez-json` / `shapez-gen` split exists to prevent. Most likely resolution: the harness and the `SyntaxModule` trait live in a `shapez-sniff`-adjacent crate, and each parser module is its own optional crate behind a feature flag.

Scope-honesty, restated from `DESIGN.md` and still true: this is a feature that runs for a year before it's reliable. Stages 0–2 are a weekend and are genuinely useful on their own — "what is this file" is a question worth answering even if nothing recurses into it. Everything past stage 3 is where the year goes.
