//! Stage 2: fingerprints and evidence in bits.
//!
//! Each candidate syntax is a set of features; each feature contributes
//! signed evidence measured in bits; the candidate's score is the sum
//! and its confidence a base-2 logistic over that sum. This is naive
//! Bayes wearing work clothes — the features are correlated and we are
//! not pretending otherwise, but log-odds accumulation degrades
//! gracefully under correlated evidence in a way that a cascade of
//! hand-tuned thresholds does not.
//!
//! Three properties the design has to preserve, all of them load-bearing:
//!
//! - **Candidates are not mutually exclusive.** JSON Lines *is* JSON
//!   plus a framing commitment; both should score high on a `.jsonl`
//!   file. There is no softmax here.
//! - **Every candidate carries its evidence.** A classifier that can
//!   explain itself can be corrected by a workload trace.
//! - **Negative evidence is explicit.** CSV's fingerprint says "and
//!   there is no JSON structure here" out loud, because per-line comma
//!   agreement alone is satisfied by a JSON Lines file with a stable
//!   record shape. That is the single most likely confusion in this
//!   whole module and it is refuted by feature, not by luck.
//!
//! Weights are hand-assigned from first principles and calibrated so a
//! clean match lands near 4-6 bits (confidence 0.94-0.98). Fitting them
//! on a labelled corpus is listed as open work in
//! `shapez/SYNTAX_DISCOVERY.md` § *What's open*.

use crate::alphabet::{Alphabet, Encoding};
use crate::evidence::{atleast, atmost, balance, band, confidence_from_bits, frac, yes_no, Evidence, Scorer};
use crate::ngram::NgramProfile;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, PartialOrd, Ord)]
pub enum Syntax {
    /// JSON under any framing — one document, concatenated values, or
    /// newline-delimited.
    Json,
    /// JSON with a newline framing commitment: one complete value per
    /// line. Scores *in addition to* `Json`, never instead of it.
    JsonLines,
    Csv,
    Tsv,
    Xml,
    UrlEncoded,
    Base64,
    Hex,
    Yaml,
    Ini,
    /// Text with no machine-readable syntax: log lines, prose, free
    /// text. The honest answer for a great deal of real input.
    LogLines,
    /// Compressed or encrypted. A terminal answer, not a failure —
    /// there is no syntax to find and no point speculating further.
    OpaqueBinary,
}

impl Syntax {
    pub fn name(self) -> &'static str {
        match self {
            Syntax::Json => "json",
            Syntax::JsonLines => "jsonl",
            Syntax::Csv => "csv",
            Syntax::Tsv => "tsv",
            Syntax::Xml => "xml",
            Syntax::UrlEncoded => "urlencoded",
            Syntax::Base64 => "base64",
            Syntax::Hex => "hex",
            Syntax::Yaml => "yaml",
            Syntax::Ini => "ini",
            Syntax::LogLines => "loglines",
            Syntax::OpaqueBinary => "opaque",
        }
    }
}

impl std::fmt::Display for Syntax {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

/// One scored hypothesis, with the reasoning attached.
#[derive(Clone, Debug)]
pub struct Candidate {
    pub syntax: Syntax,
    /// Net evidence in bits. Signed.
    pub bits: f64,
    /// Base-2 logistic of `bits`, in `(0, 1)`.
    pub confidence: f64,
    pub evidence: Vec<Evidence>,
}

impl Candidate {
    /// Evidence sorted by absolute contribution — what actually decided
    /// this candidate, strongest first.
    pub fn decisive(&self) -> Vec<&Evidence> {
        let mut v: Vec<&Evidence> = self.evidence.iter().collect();
        v.sort_by(|a, b| b.bits().abs().partial_cmp(&a.bits().abs()).unwrap_or(std::cmp::Ordering::Equal));
        v
    }
}

const ALL: [Syntax; 12] = [
    Syntax::Json,
    Syntax::JsonLines,
    Syntax::Csv,
    Syntax::Tsv,
    Syntax::Xml,
    Syntax::UrlEncoded,
    Syntax::Base64,
    Syntax::Hex,
    Syntax::Yaml,
    Syntax::Ini,
    Syntax::LogLines,
    Syntax::OpaqueBinary,
];

/// Score every candidate whose gate admits the input. Returns them
/// sorted by confidence, descending. Gates are structural
/// preconditions, not evidence: a one-line input simply cannot be
/// evaluated for line-framing, so those candidates are absent rather
/// than refuted.
pub fn score(p: &NgramProfile, a: &Alphabet) -> Vec<Candidate> {
    let mut out: Vec<Candidate> = ALL
        .iter()
        .filter(|s| gate(**s, p, a))
        .map(|s| {
            let mut sc = Scorer::new();
            eval(*s, p, a, &mut sc);
            let bits = sc.total_bits();
            Candidate { syntax: *s, bits, confidence: confidence_from_bits(bits), evidence: sc.out }
        })
        .collect();
    out.sort_by(|x, y| y.bits.partial_cmp(&x.bits).unwrap_or(std::cmp::Ordering::Equal));
    out
}

fn gate(s: Syntax, p: &NgramProfile, _a: &Alphabet) -> bool {
    if p.units < 16 {
        return false;
    }
    match s {
        // Anything that reasons about line cadence needs lines to reason about.
        Syntax::JsonLines | Syntax::Csv | Syntax::Tsv | Syntax::Yaml | Syntax::Ini | Syntax::LogLines => {
            p.lines >= 3
        }
        _ => true,
    }
}

fn eval(s: Syntax, p: &NgramProfile, a: &Alphabet, sc: &mut Scorer) {
    match s {
        Syntax::Json => json(p, sc),
        Syntax::JsonLines => {
            json(p, sc);
            json_lines(p, sc);
        }
        Syntax::Csv => delimited(p, b',', "csv", sc),
        Syntax::Tsv => delimited(p, b'\t', "tsv", sc),
        Syntax::Xml => xml(p, sc),
        Syntax::UrlEncoded => urlencoded(p, sc),
        Syntax::Base64 => base64(p, a, sc),
        Syntax::Hex => hex(p, a, sc),
        Syntax::Yaml => yaml(p, sc),
        Syntax::Ini => ini(p, sc),
        Syntax::LogLines => loglines(p, a, sc),
        Syntax::OpaqueBinary => opaque(p, a, sc),
    }
}

// ---------------------------------------------------------------------------
// Shared measurements
// ---------------------------------------------------------------------------

const KEY_COLON: [u8; 2] = [b'"', b':'];
const STRUCTURAL: &[u8] = b"{}[]";

/// How strongly this input carries JSON's structural signature, in
/// `[0, 1]`. Used as *negative* evidence by every syntax that a JSON
/// document could otherwise masquerade as.
///
/// Conjunctive on purpose: quoted keys, structural brackets, and
/// balance must all be present. Any one alone is common in text that
/// isn't JSON.
fn json_signature(p: &NgramProfile) -> f64 {
    let colon = (p.bigram_density(KEY_COLON) / 0.004).min(1.0);
    let braces = (p.set_density(STRUCTURAL) / 0.004).min(1.0);
    let framed = bracketed_lines(p);
    let opens = p.brace.obj_open + p.brace.arr_open;
    let closes = p.brace.obj_close + p.brace.arr_close;
    // Graded balance, not the strict `balanced()` predicate: a sampled
    // window can open mid-value, so an exact-equality test on a
    // multi-segment read reports "unbalanced" for perfectly good JSON —
    // which used to hand the CSV hypothesis a free two bits. Objects
    // and arrays are pooled so an array-only document still registers.
    let balanced = if opens + closes == 0 {
        0.0
    } else {
        ((balance(opens, closes) + 1.0) / 2.0).clamp(0.0, 1.0)
    };
    // `max`, not `min`: JSON whose payload is arrays of scalars has
    // almost no quoted keys, so requiring every marker under-detects it
    // and hands the whole file to the CSV hypothesis. A line of 200
    // comma-separated scalars *is* a CSV row except for the brackets
    // wrapped around it, so those brackets have to carry weight on
    // their own.
    colon.max(braces * 0.9).max(framed * 0.95).min(1.0) * balanced
}

/// Fraction of lines that both open and close with a JSON bracket.
/// Distinguishes `[1,2,3]` from `1,2,3` — the entire difference between
/// a JSON Lines array row and a CSV row.
fn bracketed_lines(p: &NgramProfile) -> f64 {
    let opens = p.lead_frac(b'[') + p.lead_frac(b'{');
    let closes = p.last_frac(b']') + p.last_frac(b'}');
    opens.min(closes).min(1.0)
}

/// Whether this input is dominated by bare JSON scalars, one per line.
/// Such a stream is legitimately JSON but has no objects, so the
/// object-shaped features below don't apply to it and are skipped
/// rather than counted as refutations.
fn scalar_dominant(p: &NgramProfile) -> bool {
    p.lines >= 3 && p.scalar_lines_frac() > 0.9
}

/// Satisfaction for "essentially every byte is in the expected
/// alphabet." Steeper than `frac`: for alphabet-restricted encodings a
/// single stray character is decisive refutation, not a small penalty.
fn purity(ratio: f64, tolerance: f64) -> f64 {
    (1.0 - (1.0 - ratio) / tolerance).clamp(-1.0, 1.0)
}

fn control_ratio(p: &NgramProfile) -> f64 {
    p.ratio_where(|b| b < 0x20 && !matches!(b, b'\t' | b'\r' | b'\n'))
}

// ---------------------------------------------------------------------------
// Fingerprints
// ---------------------------------------------------------------------------

fn json(p: &NgramProfile, sc: &mut Scorer) {
    let scalars = scalar_dominant(p);
    let root = p.first_nonspace;
    sc.feat(
        "json_root",
        0.9,
        yes_no(scalars || matches!(root, Some(b'{') | Some(b'['))),
        if scalars {
            format!("{:.2} of lines are bare JSON scalars", p.scalar_lines_frac())
        } else {
            format!("first non-space byte {:?}", root.map(|b| b as char))
        },
    );
    sc.feat(
        "brace_balance",
        1.0,
        balance(p.brace.obj_open, p.brace.obj_close),
        format!("{{ {} vs }} {}", p.brace.obj_open, p.brace.obj_close),
    );
    sc.feat(
        "bracket_balance",
        0.5,
        balance(p.brace.arr_open, p.brace.arr_close),
        format!("[ {} vs ] {}", p.brace.arr_open, p.brace.arr_close),
    );
    // Object-shaped evidence. A stream of bare scalars has no objects
    // to measure; scoring these as refutations there would punish
    // perfectly good JSON for the crime of being simple.
    if !scalars {
        let kc = p.bigram_density(KEY_COLON);
        sc.feat("key_colon", 1.0, band(kc, 0.004, 0.15), format!("`\":` density {kc:.4}"));
        let q = p.density(b'"');
        sc.feat("quote_density", 0.5, band(q, 0.02, 0.40), format!("`\"` density {q:.4}"));
        let st = p.set_density(STRUCTURAL);
        sc.feat("structural_density", 0.5, band(st, 0.004, 0.25), format!("`{{}}[]` density {st:.4}"));
    }
    let ctl = control_ratio(p);
    sc.feat_asym("no_raw_control", 0.2, 0.6, atmost(ctl, 0.0002), format!("control-char ratio {ctl:.5}"));
    let angle = p.density(b'<');
    sc.feat_asym("absent_markup", 0.15, 0.6, atmost(angle, 0.002), format!("`<` density {angle:.4}"));
    // JSON has no assignment operator. An INI file opens with
    // `[section]` — a leading bracket and perfectly balanced brackets —
    // and would otherwise pick up real JSON evidence for it.
    let eq = p.density(b'=');
    sc.refute("no_assignments", 1.0, atmost(eq, 0.001), format!("`=` density {eq:.4}"));
    // Only meaningful on a contiguous read; a sampled window can open
    // mid-string through no fault of the data.
    if p.segments == 1 {
        sc.feat_asym(
            "terminated_strings",
            0.15,
            0.6,
            yes_no(!p.brace.unterminated_string),
            format!("unterminated string: {}", p.brace.unterminated_string),
        );
    }
}

fn json_lines(p: &NgramProfile, sc: &mut Scorer) {
    // "Depth returns to zero at end of line" is vacuously true for a
    // line containing no brackets at all — which is most lines of most
    // files. Pair it with evidence that the line actually *was* a whole
    // value, or an INI file scores as excellent JSON Lines.
    let closes = p.brace.line_framed_frac();
    let whole = bracketed_lines(p).max(p.scalar_lines_frac());
    let framed = closes.min(whole);
    sc.feat(
        "line_framed",
        1.5,
        frac(framed),
        format!("{closes:.2} of lines close at depth 0, {whole:.2} are a whole value"),
    );
    let lead = (p.lead_frac(b'{') + p.lead_frac(b'[')).max(p.scalar_lines_frac());
    sc.feat("line_lead_value", 0.8, frac(lead.min(1.0)), format!("{lead:.2} of lines are a whole value"));
    sc.feat("multi_line", 0.5, atleast(p.lines as f64, 3.0), format!("{} non-blank lines", p.lines));
}

fn delimited(p: &NgramProfile, delim: u8, _label: &str, sc: &mut Scorer) {
    let cad = p.cadence_for(delim).expect("delimiter is one of DELIMITERS");
    let agreement = cad.agreement();
    let (modal, agreeing) = cad.modal().unwrap_or((0, 0));
    sc.feat(
        "delim_agreement",
        2.5,
        frac(agreement),
        format!("{agreeing}/{} lines carry exactly {modal} delimiters", cad.lines),
    );
    sc.feat("delim_present", 0.8, atleast(modal as f64, 1.0), format!("modal count {modal}"));
    let dd = p.density(delim);
    sc.feat("delim_density", 0.5, band(dd, 0.008, 0.30), format!("delimiter density {dd:.4}"));
    sc.feat(
        "field_count",
        0.4,
        band(modal as f64 + 1.0, 2.0, 512.0),
        format!("{} fields implied", modal + 1),
    );
    // Refutation-only. A JSON Lines file with a stable record shape has
    // perfect per-line comma agreement, so without this it scores as
    // excellent CSV. It has to be able to sink the hypothesis hard —
    // and it must not reward every input that merely isn't JSON, which
    // is why it earns nothing when satisfied.
    let jsig = json_signature(p);
    sc.refute("not_json", 3.5, 1.0 - 2.0 * jsig, format!("JSON signature strength {jsig:.2}"));
    let framed = bracketed_lines(p);
    sc.refute(
        "not_bracketed_lines",
        2.0,
        atmost(framed, 0.05),
        format!("{framed:.2} of lines open and close a bracket"),
    );
    let st = p.set_density(STRUCTURAL);
    sc.refute("no_brackets", 1.5, atmost(st, 0.0005), format!("`{{}}[]` density {st:.4}"));
    let angle = p.density(b'<');
    sc.refute("no_markup", 0.8, atmost(angle, 0.002), format!("`<` density {angle:.4}"));
    let ctl = control_ratio(p);
    sc.refute("no_raw_control", 0.8, atmost(ctl, 0.0002), format!("control-char ratio {ctl:.5}"));
}

fn xml(p: &NgramProfile, sc: &mut Scorer) {
    sc.feat(
        "angle_balance",
        1.0,
        balance(p.count(b'<'), p.count(b'>')),
        format!("< {} vs > {}", p.count(b'<'), p.count(b'>')),
    );
    let close = p.bigram_density([b'<', b'/']);
    sc.feat("close_tags", 1.2, band(close, 0.0005, 0.06), format!("`</` density {close:.4}"));
    let attr = p.bigram_density([b'=', b'"']);
    sc.feat("attributes", 0.6, band(attr, 0.0002, 0.08), format!("`=\"` density {attr:.4}"));
    sc.feat(
        "xml_root",
        0.8,
        yes_no(p.first_nonspace == Some(b'<')),
        format!("first non-space byte {:?}", p.first_nonspace.map(|b| b as char)),
    );
    let angle = p.density(b'<');
    sc.feat("angle_density", 0.6, band(angle, 0.005, 0.15), format!("`<` density {angle:.4}"));
    let kc = p.bigram_density(KEY_COLON);
    sc.refute("not_json_keys", 1.0, atmost(kc, 0.001), format!("`\":` density {kc:.4}"));
}

fn urlencoded(p: &NgramProfile, sc: &mut Scorer) {
    let eq = p.density(b'=');
    sc.feat("eq_density", 0.7, band(eq, 0.02, 0.20), format!("`=` density {eq:.4}"));
    let amp = p.density(b'&');
    sc.feat("amp_density", 0.7, band(amp, 0.01, 0.20), format!("`&` density {amp:.4}"));
    let ws = p.ratio_where(|b| b == b' ' || b == b'\t');
    sc.feat_asym("no_whitespace", 0.4, 1.5, atmost(ws, 0.002), format!("space/tab ratio {ws:.4}"));
    let allowed = p.ratio_where(|b| {
        b.is_ascii_alphanumeric() || b"%&=+._~:/?#[]@!$'()*,;-\r\n".contains(&b)
    });
    sc.feat("alphabet", 0.8, purity(allowed, 0.02), format!("{allowed:.4} in the URL alphabet"));
    let pct = p.density(b'%');
    sc.feat("escapes", 0.4, band(pct, 0.005, 0.30), format!("`%` density {pct:.4}"));
}

const B64: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=\r\n";

fn base64(p: &NgramProfile, a: &Alphabet, sc: &mut Scorer) {
    let inside = p.ratio_where(|b| B64.contains(&b));
    sc.feat("alphabet", 2.0, purity(inside, 0.005), format!("{inside:.4} in the base64 alphabet"));
    let pad = p.density(b'=');
    sc.feat("padding", 0.5, atmost(pad, 0.02), format!("`=` density {pad:.4}"));
    // Base64 lines are a multiple of 4 characters wide and all the same
    // width; that pair of facts is close to definitive.
    let mean = p.line_len_mean;
    let uniform = p.line_len_stddev < 1.0;
    let quad = mean.round() as u64 % 4 == 0 && mean >= 4.0;
    sc.feat(
        "line_geometry",
        0.8,
        yes_no(uniform && quad),
        format!("mean line {mean:.1}, stddev {:.2}", p.line_len_stddev),
    );
    sc.feat(
        "entropy",
        0.7,
        band(a.entropy_bits, 5.2, 6.2),
        format!("{:.2} bits/byte", a.entropy_bits),
    );
    // Every hex digit is a base64 character, so hex input satisfies the
    // alphabet test outright. Refute explicitly rather than hoping the
    // entropy band separates them.
    let hexish = p.ratio_where(|b| b.is_ascii_hexdigit() || b.is_ascii_whitespace());
    sc.refute("not_hex", 1.5, yes_no(hexish < 0.99), format!("{hexish:.4} hex digits"));
}

fn hex(p: &NgramProfile, a: &Alphabet, sc: &mut Scorer) {
    let inside = p.ratio_where(|b| b.is_ascii_hexdigit() || b.is_ascii_whitespace());
    sc.feat("alphabet", 2.5, purity(inside, 0.005), format!("{inside:.4} hex digits"));
    let digits = p.ratio_where(|b| b.is_ascii_hexdigit());
    sc.feat(
        "even_length",
        0.3,
        yes_no((p.units as f64 * digits).round() as u64 % 2 == 0),
        "hex digits come in pairs",
    );
    sc.feat(
        "entropy",
        0.6,
        band(a.entropy_bits, 3.4, 4.4),
        format!("{:.2} bits/byte", a.entropy_bits),
    );
}

fn yaml(p: &NgramProfile, sc: &mut Scorer) {
    let cs = p.bigram_density([b':', b' ']);
    sc.feat("colon_space", 1.2, band(cs, 0.004, 0.15), format!("`: ` density {cs:.4}"));
    let st = p.set_density(STRUCTURAL);
    sc.refute("no_brackets", 1.5, atmost(st, 0.002), format!("`{{}}[]` density {st:.4}"));
    let kc = p.bigram_density(KEY_COLON);
    sc.refute("not_json_keys", 1.5, atmost(kc, 0.0005), format!("`\":` density {kc:.4}"));
    // Indentation that steps by a consistent unit is YAML's other
    // structural tell. A flat file (one indent width) says nothing
    // either way, so this scores neutral rather than negative.
    let widths: Vec<u32> = p.indents.keys().copied().filter(|w| *w > 0).collect();
    let step = widths.iter().copied().min().unwrap_or(0);
    let consistent = step > 0 && widths.iter().all(|w| w % step == 0);
    sc.feat(
        "indent_ladder",
        0.7,
        if widths.is_empty() { 0.0 } else { yes_no(consistent) },
        format!("indent widths {widths:?}"),
    );
    let markers = p.lead_frac(b'-') + p.lead_frac(b'#');
    sc.feat("yaml_markers", 0.4, atleast(markers, 0.05), format!("lead `-`/`#` {markers:.2}"));
}

fn ini(p: &NgramProfile, sc: &mut Scorer) {
    let sections = p.lead_frac(b'[').min(p.last_frac(b']'));
    sc.feat("section_headers", 1.2, atleast(sections, 0.03), format!("{sections:.3} of lines are `[section]`"));
    let eq = p.density(b'=');
    sc.feat("assignments", 1.0, band(eq, 0.008, 0.12), format!("`=` density {eq:.4}"));
    let st = p.set_density(b"{}");
    sc.refute("no_braces", 1.2, atmost(st, 0.002), format!("`{{}}` density {st:.4}"));
    let kc = p.bigram_density(KEY_COLON);
    sc.refute("not_json_keys", 1.5, atmost(kc, 0.0005), format!("`\":` density {kc:.4}"));
    let cad = p.cadence_for(b',').expect("comma is a candidate delimiter");
    sc.refute("not_delimited", 0.8, atmost(cad.agreement(), 0.6), format!("comma agreement {:.2}", cad.agreement()));
}

fn loglines(p: &NgramProfile, a: &Alphabet, sc: &mut Scorer) {
    let sp = p.density(b' ');
    sc.feat("word_spacing", 1.0, band(sp, 0.08, 0.28), format!("space density {sp:.4}"));
    sc.feat("printable", 0.5, frac(a.printable_ratio), format!("{:.3} printable", a.printable_ratio));
    sc.feat("entropy", 0.6, band(a.entropy_bits, 3.6, 5.4), format!("{:.2} bits/byte", a.entropy_bits));
    let st = p.set_density(b"{}[]<>");
    sc.refute("low_structure", 1.8, atmost(st, 0.008), format!("bracket density {st:.4}"));
    let cad = p.cadence_for(b',').expect("comma is a candidate delimiter");
    sc.refute("not_delimited", 1.0, atmost(cad.agreement(), 0.6), format!("comma agreement {:.2}", cad.agreement()));
    sc.refute(
        "not_scalar_stream",
        1.5,
        atmost(p.scalar_lines_frac(), 0.5),
        format!("{:.2} of lines are bare JSON scalars", p.scalar_lines_frac()),
    );
    let cs = p.bigram_density([b':', b' ']);
    sc.refute("not_key_value", 1.2, atmost(cs, 0.01), format!("`: ` density {cs:.4}"));
    let ragged = if p.line_len_mean > 0.0 { p.line_len_stddev / p.line_len_mean } else { 0.0 };
    sc.feat("ragged_lines", 0.4, atleast(ragged, 0.08), format!("line-length CV {ragged:.2}"));
}

fn opaque(p: &NgramProfile, a: &Alphabet, sc: &mut Scorer) {
    sc.feat("entropy", 2.0, atleast(a.entropy_bits, 7.2), format!("{:.2} bits/byte", a.entropy_bits));
    sc.feat("unprintable", 1.5, atmost(a.printable_ratio, 0.45), format!("{:.3} printable", a.printable_ratio));
    sc.feat(
        "binary_alphabet",
        1.0,
        yes_no(a.encoding == Encoding::Binary),
        format!("stage-0 said {}", a.encoding.name()),
    );
    let _ = p;
}
