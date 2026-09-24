//! Corpus tests for stages 0-2.
//!
//! Two feedstocks: synthetic fixtures generated in-process (so the
//! non-JSON syntaxes get positive coverage without checking binaries
//! into the repo), and the workspace's `samples/` JSONL corpus, which
//! is the same data the analyzer's own tests run on.
//!
//! The assertions are deliberately about *ranking* rather than exact
//! confidences. Confidences move whenever a feature weight is
//! recalibrated; the ordering is the contract.

use shapez_sniff::{sniff, sniff_with, Encoding, SniffPolicy, Syntax};

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

fn csv(rows: usize) -> String {
    let mut s = String::from("id,name,email,age,city\n");
    for i in 0..rows {
        s.push_str(&format!("{i},user{i},u{i}@example.com,{},Springfield\n", 20 + i % 50));
    }
    s
}

fn tsv(rows: usize) -> String {
    let mut s = String::from("id\tname\tscore\n");
    for i in 0..rows {
        s.push_str(&format!("{i}\tname{i}\t{}\n", i * 3));
    }
    s
}

fn xml(items: usize) -> String {
    let mut s = String::from("<?xml version=\"1.0\"?>\n<catalog>\n");
    for i in 0..items {
        s.push_str(&format!(
            "  <book id=\"b{i}\"><title>Title {i}</title><price>{i}.99</price></book>\n"
        ));
    }
    s.push_str("</catalog>\n");
    s
}

fn urlencoded(fields: usize) -> String {
    (0..fields).map(|i| format!("field{i}=value%20{i}%2Fx")).collect::<Vec<_>>().join("&")
}

fn yaml(reps: usize) -> String {
    let unit = "# config\nservice:\n  name: gateway\n  port: 8080\n  tags:\n    - edge\n    - public\nlimits:\n  cpu: 2\n  memory: 4Gi\nreplicas: 3\n";
    unit.repeat(reps)
}

fn ini(reps: usize) -> String {
    let unit = "[server]\nhost = 0.0.0.0\nport = 8080\n\n[logging]\nlevel = info\nfile = /var/log/app.log\n\n[limits]\nmax_conn = 1024\ntimeout = 30\n";
    unit.repeat(reps)
}

fn log_lines(n: usize) -> String {
    (0..n)
        .map(|i| {
            format!(
                "2024-03-1{} 12:{:02}:11 INFO  [worker-{}] processed request in {} ms for account {i}\n",
                i % 10,
                i % 60,
                i % 8,
                i % 400
            )
        })
        .collect()
}

fn jsonl(n: usize) -> String {
    (0..n)
        .map(|i| format!("{{\"id\":{i},\"name\":\"user{i}\",\"active\":{},\"score\":{}.5}}\n", i % 2 == 0, i))
        .collect()
}

fn pretty_json(n: usize) -> String {
    let body: Vec<String> = (0..n)
        .map(|i| format!("    {{\n      \"id\": {i},\n      \"name\": \"user{i}\"\n    }}"))
        .collect();
    format!("{{\n  \"users\": [\n{}\n  ]\n}}\n", body.join(",\n"))
}

/// A cheap LCG stands in for compressed bytes: flat histogram, entropy
/// pinned near 8, no positional structure.
fn pseudo_random(n: usize) -> Vec<u8> {
    let mut x: u32 = 0x1234_5678;
    (0..n)
        .map(|_| {
            x = x.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
            (x >> 24) as u8
        })
        .collect()
}

fn to_hex(bytes: &[u8], per_line: usize) -> String {
    let mut s = String::new();
    for chunk in bytes.chunks(per_line) {
        for b in chunk {
            s.push_str(&format!("{b:02x}"));
        }
        s.push('\n');
    }
    s
}

const B64_ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

fn to_base64(bytes: &[u8], per_line: usize) -> String {
    let mut raw = String::new();
    for chunk in bytes.chunks(3) {
        let b = [chunk[0], *chunk.get(1).unwrap_or(&0), *chunk.get(2).unwrap_or(&0)];
        let n = ((b[0] as u32) << 16) | ((b[1] as u32) << 8) | b[2] as u32;
        for i in 0..4 {
            if i <= chunk.len() {
                raw.push(B64_ALPHABET[((n >> (18 - 6 * i)) & 0x3F) as usize] as char);
            } else {
                raw.push('=');
            }
        }
    }
    let chars: Vec<char> = raw.chars().collect();
    chars.chunks(per_line).map(|c| c.iter().collect::<String>() + "\n").collect()
}

fn utf16le(s: &str) -> Vec<u8> {
    s.encode_utf16().flat_map(|u| u.to_le_bytes()).collect()
}

/// The winning syntax, or a readable panic naming what did win.
#[track_caller]
fn assert_best(label: &str, bytes: &[u8], want: Syntax) {
    let r = sniff(bytes);
    let got = r.best().unwrap_or_else(|| panic!("{label}: nothing cleared the floor\n{r}"));
    assert_eq!(got.syntax, want, "{label}: wrong winner\n{r}");
}

#[track_caller]
fn assert_outranks(label: &str, bytes: &[u8], winner: Syntax, loser: Syntax) {
    let r = sniff(bytes);
    let w = r.candidate(winner).unwrap_or_else(|| panic!("{label}: {winner} not scored\n{r}"));
    let l = r.candidate(loser).unwrap_or_else(|| panic!("{label}: {loser} not scored\n{r}"));
    assert!(w.bits > l.bits, "{label}: expected {winner} > {loser}\n{r}");
}

// ---------------------------------------------------------------------------
// Each syntax identifies itself
// ---------------------------------------------------------------------------

#[test]
fn csv_identifies() {
    assert_best("csv", csv(300).as_bytes(), Syntax::Csv);
}

#[test]
fn tsv_identifies() {
    assert_best("tsv", tsv(300).as_bytes(), Syntax::Tsv);
}

#[test]
fn xml_identifies() {
    assert_best("xml", xml(200).as_bytes(), Syntax::Xml);
}

#[test]
fn urlencoded_identifies() {
    assert_best("urlencoded", urlencoded(300).as_bytes(), Syntax::UrlEncoded);
}

#[test]
fn yaml_identifies() {
    assert_best("yaml", yaml(20).as_bytes(), Syntax::Yaml);
}

#[test]
fn ini_identifies() {
    assert_best("ini", ini(20).as_bytes(), Syntax::Ini);
}

#[test]
fn log_lines_identify() {
    assert_best("log", log_lines(400).as_bytes(), Syntax::LogLines);
}

#[test]
fn base64_identifies() {
    let b64 = to_base64(&pseudo_random(4096), 76);
    assert_best("base64", b64.as_bytes(), Syntax::Base64);
}

#[test]
fn hex_identifies_and_outranks_base64() {
    // Every hex digit is a base64 character, so hex satisfies base64's
    // alphabet test outright. The tie is broken by explicit refutation,
    // not by luck.
    let h = to_hex(&pseudo_random(4096), 32);
    assert_best("hex", h.as_bytes(), Syntax::Hex);
    assert_outranks("hex", h.as_bytes(), Syntax::Hex, Syntax::Base64);
}

#[test]
fn compressed_bytes_read_as_opaque() {
    let bytes = pseudo_random(8192);
    let r = sniff(&bytes);
    assert_eq!(r.alphabet.encoding, Encoding::Binary);
    assert!(r.alphabet.entropy_bits > 7.5, "entropy {}", r.alphabet.entropy_bits);
    assert_eq!(r.best().map(|c| c.syntax), Some(Syntax::OpaqueBinary), "{r}");
}

// ---------------------------------------------------------------------------
// JSON, and the framing distinction
// ---------------------------------------------------------------------------

#[test]
fn jsonl_beats_plain_json_and_plain_json_beats_jsonl() {
    // The two hypotheses are not exclusive: both score high on both
    // inputs. What must hold is the *ordering* flipping with framing.
    assert_outranks("jsonl", jsonl(200).as_bytes(), Syntax::JsonLines, Syntax::Json);
    assert_outranks("pretty", pretty_json(200).as_bytes(), Syntax::Json, Syntax::JsonLines);
}

#[test]
fn a_stream_of_bare_scalars_is_still_json_lines() {
    // No braces, no quotes, no colons — every object-shaped JSON
    // feature is absent, and the file is still JSON Lines.
    let ints: String = (0..200).map(|i| format!("{}\n", i * 37 - 3000)).collect();
    assert_best("bare ints", ints.as_bytes(), Syntax::JsonLines);
}

// ---------------------------------------------------------------------------
// The confusions worth guarding
// ---------------------------------------------------------------------------

#[test]
fn stable_shaped_jsonl_does_not_read_as_csv() {
    // Every record has the same field count, so every line has exactly
    // the same number of commas — perfect CSV cadence. Only the
    // explicit "and there is no JSON structure here" refutation
    // separates them.
    let data = jsonl(300);
    let r = sniff(data.as_bytes());
    assert_eq!(r.best().unwrap().syntax, Syntax::JsonLines, "{r}");
    let csv_score = r.candidate(Syntax::Csv).expect("csv is scored");
    assert!(csv_score.confidence < 0.6, "csv should be refuted, got {}\n{r}", csv_score.confidence);
}

#[test]
fn arrays_of_scalars_do_not_read_as_csv() {
    // `[1,2,3,...]` per line is a CSV row wrapped in brackets. The
    // brackets are the entire difference and have to carry the weight.
    let rows: String = (0..200)
        .map(|i| {
            let cells: Vec<String> = (0..40).map(|j| (i * j).to_string()).collect();
            format!("[{}]\n", cells.join(","))
        })
        .collect();
    let r = sniff(rows.as_bytes());
    assert_eq!(r.best().unwrap().syntax, Syntax::JsonLines, "{r}");
    assert_outranks("array rows", rows.as_bytes(), Syntax::JsonLines, Syntax::Csv);
}

#[test]
fn ini_sections_do_not_read_as_json() {
    // An INI file opens with `[section]`: a leading bracket, and
    // perfectly balanced brackets throughout.
    let data = ini(20);
    assert_outranks("ini", data.as_bytes(), Syntax::Ini, Syntax::Json);
    assert_outranks("ini", data.as_bytes(), Syntax::Ini, Syntax::JsonLines);
}

#[test]
fn yaml_outranks_prose() {
    assert_outranks("yaml", yaml(20).as_bytes(), Syntax::Yaml, Syntax::LogLines);
}

#[test]
fn csv_quoted_fields_containing_delimiters_still_agree() {
    let mut s = String::from("a,b,c\n");
    for i in 0..200 {
        s.push_str(&format!("\"x,y {i}\",plain{i},\"p,q,r\"\n"));
    }
    assert_best("quoted csv", s.as_bytes(), Syntax::Csv);
}

// ---------------------------------------------------------------------------
// Stage 0 composes with stage 2
// ---------------------------------------------------------------------------

#[test]
fn utf16_csv_scores_against_the_same_fingerprint_as_utf8_csv() {
    let bytes = utf16le(&csv(300));
    let r = sniff(&bytes);
    assert_eq!(r.alphabet.encoding, Encoding::Utf16Le, "{r}");
    assert_eq!(r.best().unwrap().syntax, Syntax::Csv, "{r}");
    // The profile ran over the folded ASCII plane, so roughly half the
    // input bytes.
    assert!(r.bytes_profiled * 2 <= r.bytes_total as u64 + 4);
}

#[test]
fn a_bom_does_not_reach_the_fingerprints() {
    let mut bytes = vec![0xEF, 0xBB, 0xBF];
    bytes.extend_from_slice(jsonl(50).as_bytes());
    let r = sniff(&bytes);
    assert_eq!(r.alphabet.bom_len, 3);
    assert_eq!(r.best().unwrap().syntax, Syntax::JsonLines, "{r}");
}

// ---------------------------------------------------------------------------
// Honest failure
// ---------------------------------------------------------------------------

#[test]
fn unknown_is_a_real_answer() {
    // High-entropy printable noise: too much entropy for prose, wrong
    // alphabet for base64 or hex, no delimiter agreement, no markup.
    // Every fingerprint gets a look and none of them earns the call.
    let alpha: Vec<u8> = (0x21u8..0x7F).collect();
    let mut x: u32 = 99;
    let noise: Vec<u8> = (0..3000)
        .flat_map(|i| {
            x = x.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
            let c = alpha[((x >> 16) as usize) % alpha.len()];
            // Newlines so the line-gated candidates get evaluated too,
            // rather than being excluded before they can be rejected.
            if i % 60 == 59 {
                vec![c, b'\n']
            } else {
                vec![c]
            }
        })
        .collect();

    let r = sniff(&noise);
    assert!(r.is_unknown(), "expected unknown, got:\n{r}");
    // And the reasoning survives the miss — "why didn't you say CSV?"
    // has an answer.
    assert!(!r.rejected.is_empty());
    assert!(r.rejected.iter().all(|c| !c.evidence.is_empty()));
    assert!(r.candidate(Syntax::Csv).is_some(), "csv was scored and rejected");
}

#[test]
fn too_short_to_judge_scores_nothing_at_all() {
    // Below the minimum length the gates reject every candidate, so
    // there is no evidence to report either. Distinct from the case
    // above, where everything was scored and nothing convinced.
    let r = sniff(b"aaaa aaaa\nbbbb\n");
    assert!(r.is_unknown());
    assert!(r.rejected.is_empty(), "nothing should have been scored");
}

#[test]
fn empty_and_tiny_inputs_do_not_panic() {
    for input in [b"".as_slice(), b"x", b"{}", b"\n\n\n", &[0u8; 8]] {
        let r = sniff(input);
        assert!(r.is_unknown() || r.best().is_some());
    }
}

#[test]
fn every_candidate_explains_itself() {
    let r = sniff(csv(300).as_bytes());
    let best = r.best().expect("csv identified");
    assert!(!best.evidence.is_empty());
    // Decisive evidence is ordered by absolute contribution.
    let d = best.decisive();
    for w in d.windows(2) {
        assert!(w[0].bits().abs() >= w[1].bits().abs());
    }
    // And the bits add up to the reported score.
    let sum: f64 = best.evidence.iter().map(|e| e.bits()).sum();
    assert!((sum - best.bits).abs() < 1e-9);
}

#[test]
fn the_confidence_floor_is_honored() {
    let strict = SniffPolicy { min_confidence: 0.99, ..SniffPolicy::default() };
    let data = csv(300);
    assert!(sniff(data.as_bytes()).best().is_some());
    assert!(sniff_with(data.as_bytes(), &strict).is_unknown());
    // Nothing is lost — it moved to `rejected`.
    assert!(sniff_with(data.as_bytes(), &strict)
        .candidate(Syntax::Csv)
        .is_some());
}

// ---------------------------------------------------------------------------
// Sampling
// ---------------------------------------------------------------------------

#[test]
fn sampling_a_large_file_reaches_the_same_verdict() {
    // Well past the 1 MiB budget, so the sampler takes a prefix plus
    // windows and the bracket balance is only approximate at the seams.
    let big = jsonl(40_000);
    assert!(big.len() > (1 << 20), "fixture must exceed the budget");
    let r = sniff(big.as_bytes());
    assert!(r.bytes_profiled < big.len() as u64, "should not have read it all");
    assert_eq!(r.best().unwrap().syntax, Syntax::JsonLines, "{r}");
    assert!(r.candidate(Syntax::Csv).unwrap().confidence < 0.6, "{r}");
}

// ---------------------------------------------------------------------------
// The workspace sample corpus
// ---------------------------------------------------------------------------

#[test]
fn workspace_jsonl_samples_all_identify_as_json_lines() {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../samples");
    let Ok(entries) = std::fs::read_dir(&dir) else {
        eprintln!("skipping: {} not present", dir.display());
        return;
    };
    let mut checked = 0;
    for e in entries.flatten() {
        let path = e.path();
        if path.extension().and_then(|s| s.to_str()) != Some("jsonl") {
            continue;
        }
        let bytes = std::fs::read(&path).expect("read sample");
        let r = sniff(&bytes);
        let name = path.file_name().unwrap().to_string_lossy().to_string();
        assert_eq!(
            r.best().map(|c| c.syntax),
            Some(Syntax::JsonLines),
            "{name}\n{r}"
        );
        assert_eq!(r.alphabet.encoding, Encoding::Utf8, "{name}");
        // The CSV hypothesis must be refuted on every one of these,
        // including the stable-record and array-of-scalars shapes.
        assert!(
            r.candidate(Syntax::Csv).map(|c| c.confidence < 0.6).unwrap_or(true),
            "{name}: csv not refuted\n{r}"
        );
        checked += 1;
    }
    assert!(checked >= 10, "expected the sample corpus, found {checked} files");
}
