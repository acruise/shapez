//! Streaming tests.
//!
//! The contract these pin down: **chunk boundaries are invisible to the
//! answer**. Every statistic that spans more than one byte — bigrams,
//! trigrams, line geometry, CSV quote state, JSON string state, bracket
//! depth, UTF-16 code units, the BOM itself — has to carry across a
//! chunk edge, and the only way to be sure is to feed the same bytes
//! every possible way and demand the same result.

use std::io::Cursor;

use shapez_sniff::{sniff, sniff_reader, Encoding, SniffPolicy, SniffStream, Syntax};

/// A comparable digest of a report: which candidates, in what order,
/// with what scores. Bits are quantized so the comparison doesn't turn
/// into a float-equality test.
fn digest(r: &shapez_sniff::SniffReport) -> Vec<(Syntax, i64)> {
    r.candidates
        .iter()
        .chain(r.rejected.iter())
        .map(|c| (c.syntax, (c.bits * 1e6).round() as i64))
        .collect()
}

fn stream_in_chunks(bytes: &[u8], chunk: usize) -> shapez_sniff::SniffReport {
    let mut s = SniffStream::new();
    for c in bytes.chunks(chunk.max(1)) {
        s.push(c);
    }
    s.finish()
}

fn jsonl(n: usize) -> String {
    (0..n)
        .map(|i| format!("{{\"id\":{i},\"name\":\"user{i}\",\"ok\":{}}}\n", i % 2 == 0))
        .collect()
}

fn csv(rows: usize) -> String {
    let mut s = String::from("id,name,email,age\n");
    for i in 0..rows {
        s.push_str(&format!("{i},user{i},u{i}@example.com,{}\n", 20 + i % 50));
    }
    s
}

// ---------------------------------------------------------------------------
// The core invariant
// ---------------------------------------------------------------------------

#[test]
fn chunking_does_not_change_the_answer() {
    // Inputs stay under `Budget::prefix` (64 KiB) so the slice sampler
    // and the stream sampler look at exactly the same bytes. Above that
    // they legitimately diverge — see `sampling_diverges_above_the_prefix`.
    let cases: Vec<(&str, Vec<u8>)> = vec![
        ("jsonl", jsonl(300).into_bytes()),
        ("csv", csv(300).into_bytes()),
        ("pretty", b"{\n  \"a\": [1, 2, 3],\n  \"b\": {\"c\": \"d\"}\n}\n".to_vec()),
        ("xml", "<a><b id=\"1\">x</b></a>\n".repeat(200).into_bytes()),
        ("scalars", (0..400).map(|i| format!("{i}\n")).collect::<String>().into_bytes()),
        ("empty", Vec::new()),
        ("tiny", b"{}".to_vec()),
    ];

    for (name, bytes) in cases {
        let want = digest(&sniff(&bytes));
        for chunk in [1usize, 2, 3, 5, 7, 13, 64, 511, 4096, 65536] {
            let got = digest(&stream_in_chunks(&bytes, chunk));
            assert_eq!(got, want, "{name}: chunk size {chunk} changed the answer");
        }
    }
}

#[test]
fn splitting_at_every_offset_is_safe() {
    // Two chunks, split at every possible position. Catches carry bugs
    // that a fixed chunk size would step over.
    let inputs: Vec<Vec<u8>> = vec![
        // A `}` and a `,` inside a string literal: the brace scanner and
        // the CSV quote scanner must both ignore them.
        br#"{"a":"}}},,,","b":[1,2]}"#.to_vec(),
        // A backslash-escaped quote right at the end of a string.
        br#"{"a":"he said \"}\" once","b":1}"#.to_vec(),
        // A doubled `""` CSV escape, which needs one byte of lookahead.
        b"a,b\n\"x,y\",\"p\"\"q\"\n\"r\",s\n".to_vec(),
        // CRLF line endings.
        b"{\"a\":1}\r\n{\"a\":2}\r\n{\"a\":3}\r\n".to_vec(),
    ];

    for bytes in inputs {
        let want = digest(&sniff(&bytes));
        for split in 0..=bytes.len() {
            let mut s = SniffStream::new();
            s.push(&bytes[..split]);
            s.push(&bytes[split..]);
            assert_eq!(
                digest(&s.finish()),
                want,
                "split at {split} of {:?}",
                String::from_utf8_lossy(&bytes)
            );
        }
    }
}

#[test]
fn utf16_code_units_survive_chunk_edges() {
    let src = csv(200);
    let bytes: Vec<u8> = src.encode_utf16().flat_map(|u| u.to_le_bytes()).collect();
    let want = digest(&sniff(&bytes));
    // Odd chunk sizes guarantee code units get split down the middle.
    for chunk in [1usize, 3, 5, 7, 1001] {
        let r = stream_in_chunks(&bytes, chunk);
        assert_eq!(r.alphabet.encoding, Encoding::Utf16Le, "chunk {chunk}");
        assert_eq!(r.best().unwrap().syntax, Syntax::Csv, "chunk {chunk}");
        assert_eq!(digest(&r), want, "chunk {chunk}");
    }
}

#[test]
fn a_bom_split_across_chunks_is_still_stripped() {
    let mut bytes = vec![0xEF, 0xBB, 0xBF];
    bytes.extend_from_slice(jsonl(100).as_bytes());
    let want = digest(&sniff(&bytes));
    for chunk in [1usize, 2, 3, 4] {
        let r = stream_in_chunks(&bytes, chunk);
        assert_eq!(r.alphabet.bom_len, 3, "chunk {chunk}");
        assert_eq!(digest(&r), want, "chunk {chunk}");
    }
}

#[test]
fn utf16_endianness_survives_odd_sized_chunks() {
    // NUL parity is measured against the absolute stream offset. Feeding
    // odd-sized chunks would scramble it if the offset were per-chunk.
    let src = csv(100);
    for (label, bytes) in [
        ("le", src.encode_utf16().flat_map(|u| u.to_le_bytes()).collect::<Vec<u8>>()),
        ("be", src.encode_utf16().flat_map(|u| u.to_be_bytes()).collect::<Vec<u8>>()),
    ] {
        let want = if label == "le" { Encoding::Utf16Le } else { Encoding::Utf16Be };
        for chunk in [1usize, 3, 7, 999] {
            let r = stream_in_chunks(&bytes, chunk);
            assert_eq!(r.alphabet.encoding, want, "{label} at chunk {chunk}");
        }
    }
}

// ---------------------------------------------------------------------------
// Unbounded input
// ---------------------------------------------------------------------------

#[test]
fn an_arbitrarily_long_stream_profiles_a_bounded_slice_of_itself() {
    // 16 MiB pushed 64 KiB at a time, never materialized as one buffer.
    let block = jsonl(1200); // ~64 KiB of JSON Lines
    let mut s = SniffStream::new();
    let target = 16 << 20;
    while s.bytes_consumed() < target {
        s.push(block.as_bytes());
    }
    let consumed = s.bytes_consumed();
    let r = s.finish();

    assert!(consumed >= target);
    assert_eq!(r.bytes_total, consumed);
    assert!(
        r.bytes_profiled <= (1 << 20),
        "profiled {} of {consumed} — should be capped by the budget",
        r.bytes_profiled
    );
    assert!(r.bytes_profiled * 8 < consumed, "profiled too much of the stream");
    assert_eq!(r.best().unwrap().syntax, Syntax::JsonLines, "{r}");
}

#[test]
fn a_long_line_does_not_grow_the_line_buffer() {
    // A single 32 KiB line — over the internal line-buffer cap, under
    // the sampling prefix, so there are no seams to confuse the count.
    let mut s = SniffStream::new();
    s.push("x".repeat(32 << 10).as_bytes());
    s.push(b"\n");
    let r = s.finish();
    assert_eq!(r.profile.lines, 1);
    // Way past the buffer cap, so the bare-scalar check gave up rather
    // than retaining the whole line.
    assert_eq!(r.profile.scalar_lines, 0);
}

#[test]
fn one_enormous_line_stays_bounded() {
    // 4 MiB with no newline anywhere: the pathological shape for
    // anything that buffers per line. Each sampling seam flushes a
    // partial line, so the count is one per admitted window rather than
    // one overall — bounded either way, which is the point.
    let mut s = SniffStream::new();
    let block = "x".repeat(64 << 10);
    for _ in 0..64 {
        s.push(block.as_bytes());
    }
    let r = s.finish();
    assert!(r.profile.lines <= 16, "lines {} should be bounded by windows", r.profile.lines);
    assert_eq!(r.profile.scalar_lines, 0);
}

#[test]
fn the_stream_reports_when_it_has_seen_enough() {
    let block = jsonl(1200);
    let mut s = SniffStream::new();
    assert!(!s.is_satisfied());
    let mut pushes = 0;
    while !s.is_satisfied() && pushes < 10_000 {
        s.push(block.as_bytes());
        pushes += 1;
    }
    assert!(s.is_satisfied(), "budget should be exhausted eventually");
    assert_eq!(s.report().best().unwrap().syntax, Syntax::JsonLines);
}

#[test]
fn sampling_diverges_above_the_prefix_but_agrees_on_the_verdict() {
    // Above `Budget::prefix` the slice path (which can seek, and spreads
    // windows evenly over a known length) and the stream path (which
    // cannot, and backs off geometrically) look at different bytes. The
    // scores may differ; the verdict must not.
    let data = jsonl(60_000);
    assert!(data.len() > 1 << 20, "fixture is {} bytes", data.len());
    let slice = sniff(data.as_bytes());
    let streamed = stream_in_chunks(data.as_bytes(), 8192);
    assert_eq!(slice.best().unwrap().syntax, Syntax::JsonLines);
    assert_eq!(streamed.best().unwrap().syntax, Syntax::JsonLines);
    assert!(slice.candidate(Syntax::Csv).unwrap().confidence < 0.6);
    assert!(streamed.candidate(Syntax::Csv).unwrap().confidence < 0.6);
}

// ---------------------------------------------------------------------------
// Mid-stream interrogation and the Read adapter
// ---------------------------------------------------------------------------

#[test]
fn a_report_can_be_taken_mid_stream() {
    let mut s = SniffStream::new();
    // Before anything at all.
    assert!(s.report().is_unknown());
    for i in 0..400 {
        s.push(format!("{{\"id\":{i}}}\n").as_bytes());
        if i == 50 {
            assert_eq!(s.report().best().map(|c| c.syntax), Some(Syntax::JsonLines));
        }
    }
    assert_eq!(s.finish().best().unwrap().syntax, Syntax::JsonLines);
}

#[test]
fn report_does_not_disturb_the_stream() {
    let data = jsonl(300);
    let mut a = SniffStream::new();
    let mut b = SniffStream::new();
    for c in data.as_bytes().chunks(101) {
        a.push(c);
        b.push(c);
        let _ = b.report(); // peeking must not consume or mutate
    }
    assert_eq!(digest(&a.finish()), digest(&b.finish()));
}

#[test]
fn sniff_reader_matches_sniff() {
    for data in [jsonl(200).into_bytes(), csv(200).into_bytes()] {
        let want = digest(&sniff(&data));
        let got = sniff_reader(Cursor::new(data.clone())).expect("read");
        assert_eq!(digest(&got), want);
        assert_eq!(got.bytes_total, data.len() as u64);
    }
}

#[test]
fn policy_is_honored_on_the_stream_path() {
    let strict = SniffPolicy { min_confidence: 0.99, ..SniffPolicy::default() };
    let data = csv(300);
    let mut s = SniffStream::with_policy(strict);
    s.push(data.as_bytes());
    let r = s.finish();
    assert!(r.is_unknown());
    assert!(r.candidate(Syntax::Csv).is_some(), "still scored, just rejected");
}

#[test]
fn empty_and_degenerate_streams_do_not_panic() {
    assert!(SniffStream::new().finish().is_unknown());

    let mut s = SniffStream::new();
    for _ in 0..100 {
        s.push(b"");
    }
    assert!(s.finish().is_unknown());

    let mut s = SniffStream::new();
    s.push(&[0u8; 4]);
    let _ = s.finish();
}
