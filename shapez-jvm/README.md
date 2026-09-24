# shapez-jvm

C-ABI bindings for shapez, designed to be called via JNA from the JVM (Scala / Java / Kotlin). Two output paths:

- **Text report.** Human-readable, matches the `StreamingAnalyzer::report` pretty-printer. Best for debugging.
- **Protobuf `AnalysisReport`.** Typed structured output. The schema lives at `shapez/proto/report.proto` and is compiled into a Rust module via `prost-build` (Rust side) and into Scala case classes via `sbt-protoc` + ScalaPB (JVM side).
- **Protobuf `AnalyzerState`.** Full-fidelity snapshot of the analyzer's *in-progress* state (`shapez/proto/state.proto`). Designed for Spark UDAGG-style aggregation: snapshot at quiescence, deserialize on another node, keep feeding. Round-trip is byte-identical at the report level; per-partition `reduce` then driver-side `merge` is the eventual pattern, though merge itself is not yet implemented.

JNA was chosen over JNI to keep the Java side clean: no static-native shim class, no symbol-mangling games when wrapping with a Scala `object`, no `System.loadLibrary` ritual that has to live in exactly the right class initializer. The Rust side exports plain `extern "C"` functions and the JVM reaches them through a single `Library` interface.

## Building

```sh
cargo build -p shapez-jvm --release
```

Output lands at `target/release/libshapez_jvm.{dylib,so,dll}` depending on the host. JNA resolves the library by name (`shapez_jvm`) from either `jna.library.path` or `java.library.path` (or the OS default search path). The sbt build wires these for tests.

## Scala usage

```scala
import org.shapez.Analyzer

// One-shot, human-readable text
val text: String = Analyzer.analyzeJsonl(jsonlBytes)

// One-shot, structured proto
val report: shapez.report.AnalysisReport = Analyzer.analyzeJsonlProto(jsonlBytes)
println(s"docs: ${report.docCount}")
println(s"shape kind: ${report.shape.flatMap(_.kind).flatMap(_.variant)}")

// Streaming (per Spark partition, say)
val a = new Analyzer
try {
  a.feedJsonl(chunk1)
  a.feedJsonl(chunk2)
  a.feed("""{"k": 1}""")              // per-row
  println(s"docs: ${a.docCount()}")
  println(a.report())                  // text
  val proto = a.reportProto()          // shapez.report.AnalysisReport
  val rawProto = a.reportProtoBytes()  // Array[Byte], for shipping over the wire
} finally a.close()
```

## Schema

`shapez/proto/report.proto` is a small, **shapez-specific** schema — it does not mirror the upstream `meta::ValueType` type system. It carries only the leaf categories the analyzer actually emits today and the shape-language structural decisions the analyzer makes:

- `AnalysisReport { uint64 doc_count; ShapeNode shape; }`
- `ShapeNode { Stats stats; ShapeKind kind; }`
- `ShapeKind` as a `oneof` over `LeafKind | VariantKind | TupleKind | AbsentKind | ArrayKind | RecordKind | MapKind`
- `LeafKind { LeafType type; }` where `LeafType` is a flat enum: `NULL | BOOL | I64 | U64 | F64 | STRING`

When the analyzer compresses a uniform subtree to a `Type(ValueType::Array/Map/Struct)` internally, the serializer expands it back into the equivalent rich `ShapeKind` form on the wire, so the proto never carries upstream-`ValueType` recursion.

If shapez ever needs additional leaf categories (e.g. promoting format-detected `Uuid` / `Timestamp` / `Ipv4` to first-class leaf types), extend `LeafType` in the proto. Proto3 evolution rules apply: field numbers and enum values are append-only; new variants are wire-compatible additions.

## API surface

C ABI (`libshapez_jvm`):

| Function | Returns | Notes |
| --- | --- | --- |
| `shapez_create()` | `*mut c_void` (null on failure) | Allocates a `StreamingAnalyzer`. |
| `shapez_destroy(h)` | — | Frees the analyzer. Safe with null. |
| `shapez_feed_jsonl(h, bytes, len)` | `i64` accepted, or `-1` | Skips blank / malformed lines. |
| `shapez_feed_json_string(h, cstr)` | `1` / `0` / `-1` | Single document. |
| `shapez_finalize(h)` | `*mut c_char` (caller frees) | Text report. |
| `shapez_finalize_proto(h, *out_len)` | `*mut u8` (caller frees) | Encoded `AnalysisReport`; length written into `out_len`. |
| `shapez_doc_count(h)` | `i64`, or `-1` | Accepted so far. |
| `shapez_analyze_jsonl(bytes, len)` | `*mut c_char` (caller frees) | One-shot text. |
| `shapez_analyze_jsonl_proto(bytes, len, *out_len)` | `*mut u8` (caller frees) | One-shot proto. |
| `shapez_state_to_proto(h, *out_len)` | `*mut u8` (caller frees) | Encoded `AnalyzerState` — full in-progress snapshot. Panics if mid-document. |
| `shapez_create_from_state_proto(bytes, len)` | `*mut c_void` (null on decode failure) | New handle initialized from a serialized state. |
| `shapez_free_string(s)` | — | Release a string returned by the above. Safe with null. |
| `shapez_free_bytes(ptr, len)` | — | Release a byte buffer. `len` must match the producer's value. |

JNA binding lives in `scala/src/main/java/org/shapez/ShapezNative.java`; idiomatic Scala wrapper in `scala/src/main/scala/org/shapez/Shapez.scala`.

Errors at the FFI boundary surface as sentinel values (`null` / `-1`); panics inside Rust are caught and converted. The Scala wrapper raises on those sentinels rather than propagating them silently.

## Integration tests (sbt)

A minimal sbt project lives under `scala/` and exercises the FFI end-to-end via MUnit. ScalaPB + `sbt-protoc` compile the same `shapez/proto/report.proto` into Scala case classes available to the wrapper and the tests.

```sh
cd shapez-jvm/scala
sbt test
```

The `cargoBuild` task gates test execution, so `sbt test` automatically runs `cargo build -p shapez-jvm` first and then loads the freshly-built dylib from the cargo workspace's `target/debug/` directory. Tests fork the JVM and pass `-Djna.library.path` and `-Djava.library.path` pointing at that directory, so no environment variables are required.

Requires sbt 1.10+, a JDK on PATH, and a `protoc` binary (sbt-protoc downloads one automatically). Scala 2.13.14.

## Threading

A handle is **not** thread-safe. Use one handle per thread. The handle pointer itself can be moved across threads as long as feeds and finalization are serialized.

## What this is not (yet)

- **No Spark UDF / UDAF.** `AnalyzerState` and `stateBytes` / `fromStateBytes` are the substrate; a Spark `Aggregator` would still need a `merge(state1, state2)` primitive in shapez (every sketch needs a defined merge), and that's not implemented yet. Round-trip serialization works today, so `reduce` is straightforward (`deserialize → feed row → serialize`); only `merge` is open.
- **No published artifact.** The sbt project is for integration testing, not artifact publishing. A `publishLocal` / cross-Scala / fat-JAR setup is a follow-up once the API stabilizes.
- **No automatic dylib loading at runtime.** JNA needs the binary findable via `jna.library.path`, `java.library.path`, or the OS default search path. The sbt build wires this up for tests; production deployments still need their own packaging. Bundling the dylib into a fat JAR with JNA's `Native.extractFromResourcePath`-style loader is on the roadmap.
