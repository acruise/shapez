# shapez samples

JSONL fixtures covering every scenario the eyeball runner emits. Checked
into the tree so they double as:

- **Documentation**: a future reader can `head -2 samples/<scenario>.jsonl`
  and see what kind of input each scenario is exercising.
- **Test corpus**: `shapez-json/tests/samples_regression.rs` drives every
  file through the analyzer and asserts structural invariants on the
  inferred shape. Changes to the analyzer that drift the canonical
  output break those tests by design.

## Scenarios

Tonal range, top to bottom:

| Scenario                          | Shows                                                                              |
|-----------------------------------|------------------------------------------------------------------------------------|
| `scalar_root_int`                 | The boring case — a document is a bare integer.                                    |
| `record_stable`                   | All-required four-field record. The ordinary case.                                 |
| `record_optional`                 | One required field, three optional — nullability vs variant.                       |
| `map_uuid_keys`                   | UUID-keyed map, record_view drops, wildcard injection at one level.                |
| `tuple_heterogeneous`             | Fixed-arity `[string, f64, i64, bool]` tuple.                                      |
| `polymorphic_array_discriminated` | Array of three discriminated record arms — Space-Saving variant emergence.         |
| `random_tree`                     | Arbitrary nesting and type mixing. We survive.                                     |
| `deep_mixed_spine_150`            | 150-level deep alternating object/array spine. Stack survives.                     |
| `wide_object_200_keys`            | 200 keys/doc, record_view drops, MAP fires.                                        |
| `wide_array_200`                  | 200-element arrays, positional_view drops, BAG fires.                              |
| `nested_wildcards`                | Two stacked high-cardinality map levels — wildcard injection compounds.            |
| `templated_skew`                  | Realistic event arrays in a 70/20/8/2 distribution plus noise.                     |
| `epoch_events`                    | Numeric `ts` fields in the epoch-millis range — `NumericStats::epoch_guess` fires. |
| `skeleton_ids`                    | `ORD-2024-NNNNNN`-shaped strings — `_punct`-style skeleton dominates.              |

See [`shapez/DESIGN.md`](../shapez/DESIGN.md) and the eyeball example
source for what each scenario is meant to demonstrate.

## Regenerating

These were produced by the `eyeball` example at `count=200, seed=0`:

```sh
cargo run -p shapez-json --example eyeball -- 200 0
cp target/eyeball/*.jsonl samples/
```

The seed pins the random-value generators so output is byte-identical
across runs *given the same shapez-gen / chaos generator code*. Changes
to those generators (new scalar kinds, new template shapes, different
distributions) will drift the samples. When that happens, regenerate
and review the diff — the analyzer's behavior on the new inputs is the
real question, which is what `samples_regression.rs` is for.

## Sizes

Two scenarios are chunky on purpose: `templated_skew` carries 120
realistic event records per doc, and `nested_wildcards` carries
~120 inner records per doc behind two wildcard levels. They're the load-
bearing demonstrations and worth their disk footprint. If a future
trimming pass shrinks the corpus, those two are the obvious candidates.

## What the regression tests *don't* assert

The tests check structural invariants — root shape kind, key decisions
appearing in the report, format detectors firing where expected. They
deliberately don't snapshot the full report text, because that text
evolves with the analyzer's presentation. If you regen the samples and
the regression tests still pass, the analyzer hasn't regressed; if the
report's *wording* changed, the eye-readable reports under
`target/eyeball/` are the place to compare.
