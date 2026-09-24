//! Stage 0: is it text, and in what code unit?
//!
//! Before an ngram means anything you have to know what a *gram* is.
//! Counting bytes in UTF-16LE ASCII gives a beautiful, useless
//! distribution dominated by `0x00`. This module decides the code unit,
//! then produces a normalized byte view that the rest of the pipeline
//! profiles — so a UTF-16LE CSV scores against the same CSV fingerprint
//! as a UTF-8 one, with no per-encoding fingerprint duplication.
//!
//! See `shapez/SYNTAX_DISCOVERY.md` § *Stage 0*.

use std::borrow::Cow;

use crate::evidence::{atleast, atmost, frac, yes_no, Evidence, Scorer};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Encoding {
    /// ASCII or UTF-8. The overwhelmingly common answer for text.
    Utf8,
    Utf16Le,
    Utf16Be,
    Utf32Le,
    Utf32Be,
    /// Text-shaped, but the high bytes don't form valid UTF-8. Latin-1
    /// or one of its cousins; we don't try to tell them apart.
    Latin1,
    /// Not text. Stages 1-2 still run — binary formats have
    /// fingerprints too — but the grams are bytes, not characters.
    Binary,
}

impl Encoding {
    pub fn is_text(self) -> bool {
        !matches!(self, Encoding::Binary)
    }

    pub fn name(self) -> &'static str {
        match self {
            Encoding::Utf8 => "UTF-8",
            Encoding::Utf16Le => "UTF-16LE",
            Encoding::Utf16Be => "UTF-16BE",
            Encoding::Utf32Le => "UTF-32LE",
            Encoding::Utf32Be => "UTF-32BE",
            Encoding::Latin1 => "Latin-1",
            Encoding::Binary => "binary",
        }
    }
}

/// The stage-0 verdict, plus every measurement that fed it.
#[derive(Clone, Debug)]
pub struct Alphabet {
    pub encoding: Encoding,
    /// Bytes of byte-order mark to skip. Zero when absent.
    pub bom_len: usize,
    /// Confidence that this input is text at all, in `(0, 1)`.
    pub text_confidence: f64,
    /// Shannon entropy of the raw byte histogram, in bits/byte. Near
    /// 8.0 with a flat histogram means compressed or encrypted, which
    /// is a terminal answer rather than a failure.
    pub entropy_bits: f64,
    pub printable_ratio: f64,
    pub null_ratio: f64,
    /// Position bias of NUL bytes in `[-1, +1]`: `+1` every NUL at an
    /// odd offset (UTF-16LE ASCII), `-1` every NUL at an even offset
    /// (UTF-16BE ASCII), `0` no positional structure (binary).
    pub null_odd_bias: f64,
    pub high_bit_ratio: f64,
    /// Fraction of bytes >= 0x80 that participate in a well-formed
    /// UTF-8 sequence. Near 1.0 is UTF-8; near chance is Latin-1 or
    /// binary. Defined as 1.0 when there are no high bytes.
    pub utf8_valid_ratio: f64,
    pub evidence: Vec<Evidence>,
}

/// Raw-byte measurements, accumulated incrementally so they can be fed
/// a chunk at a time. The byte histogram, NUL parity, and high-bit
/// counts all compose across chunk boundaries; UTF-8 sequence validity
/// does not (it needs up to four bytes of lookahead) and is measured on
/// the buffered head instead — see [`decide`].
#[derive(Clone, Debug)]
pub struct RawStats {
    hist: [u64; 256],
    len: u64,
    odd_nulls: u64,
}

impl Default for RawStats {
    fn default() -> Self {
        Self { hist: [0; 256], len: 0, odd_nulls: 0 }
    }
}

impl RawStats {
    pub fn push(&mut self, chunk: &[u8]) {
        // Parity is measured against the absolute offset in the stream,
        // not the offset in this chunk — otherwise a stream fed in
        // odd-sized chunks would scramble the UTF-16 endianness signal.
        let base = self.len;
        for (i, &b) in chunk.iter().enumerate() {
            self.hist[b as usize] += 1;
            if b == 0 && (base + i as u64) % 2 == 1 {
                self.odd_nulls += 1;
            }
        }
        self.len += chunk.len() as u64;
    }

    /// Raw bytes accumulated so far.
    pub fn bytes(&self) -> u64 {
        self.len
    }
}

const BOMS: &[(&[u8], Encoding)] = &[
    // UTF-32LE before UTF-16LE: the two-byte prefix collides.
    (&[0xFF, 0xFE, 0x00, 0x00], Encoding::Utf32Le),
    (&[0x00, 0x00, 0xFE, 0xFF], Encoding::Utf32Be),
    (&[0xEF, 0xBB, 0xBF], Encoding::Utf8),
    (&[0xFF, 0xFE], Encoding::Utf16Le),
    (&[0xFE, 0xFF], Encoding::Utf16Be),
];

pub fn detect(bytes: &[u8]) -> Alphabet {
    let mut stats = RawStats::default();
    stats.push(bytes);
    decide(&stats, bytes)
}

/// Decide the alphabet from accumulated statistics plus a buffered
/// head.
///
/// The split matters for streaming. The *encoding decision* is made
/// once, early, from the head — it has to be, because nothing can be
/// normalized until it is known — while entropy, printable ratio and
/// NUL density keep accumulating over everything admitted. The BOM and
/// the UTF-8 sequence check read the head only; both are prefix
/// properties by nature.
pub fn decide(stats: &RawStats, head: &[u8]) -> Alphabet {
    let hist = stats.hist;
    let n = stats.len as f64;

    let entropy_bits = if n == 0.0 { 0.0 } else { shannon(&hist, n) };
    let printable = hist[0x09] + hist[0x0A] + hist[0x0D] + (0x20..=0x7E).map(|i| hist[i]).sum::<u64>();
    let printable_ratio = if n == 0.0 { 0.0 } else { printable as f64 / n };
    let nulls = hist[0];
    let null_ratio = if n == 0.0 { 0.0 } else { nulls as f64 / n };
    let high_bits: u64 = (0x80..=0xFF).map(|i| hist[i]).sum();
    let high_bit_ratio = if n == 0.0 { 0.0 } else { high_bits as f64 / n };

    let null_odd_bias =
        if nulls == 0 { 0.0 } else { 2.0 * (stats.odd_nulls as f64 / nulls as f64) - 1.0 };
    let head_high: u64 = head.iter().filter(|b| **b >= 0x80).count() as u64;
    let utf8_valid_ratio = utf8_sequence_validity(head, head_high);

    // --- BOM: decisive when present. ------------------------------------
    let bom = BOMS.iter().find(|(m, _)| head.starts_with(m));

    let mut s = Scorer::new();
    let encoding = if let Some((marker, enc)) = bom {
        s.feat("bom", 3.0, 1.0, format!("{} BOM ({} bytes)", enc.name(), marker.len()));
        *enc
    } else if null_ratio > 0.20 && null_odd_bias.abs() > 0.85 {
        // Null-parity: the sharpest test in this module. Half the bytes
        // are NUL and they all land on the same parity => UTF-16 ASCII,
        // and the parity direction gives the endianness for free.
        let enc = if null_odd_bias > 0.0 { Encoding::Utf16Le } else { Encoding::Utf16Be };
        s.feat(
            "null_parity",
            2.5,
            1.0,
            format!("null ratio {null_ratio:.2}, odd-offset bias {null_odd_bias:+.2} => {}", enc.name()),
        );
        enc
    } else if null_ratio > 0.005 {
        s.feat("interior_nulls", 2.0, -1.0, format!("null ratio {null_ratio:.3} without parity structure"));
        Encoding::Binary
    } else if printable_ratio < 0.70 {
        s.feat("printable_ratio", 2.0, frac(printable_ratio), format!("{printable_ratio:.3} printable"));
        Encoding::Binary
    } else if high_bits > 0 && utf8_valid_ratio < 0.90 {
        s.feat(
            "utf8_sequences",
            1.0,
            frac(utf8_valid_ratio),
            format!("{utf8_valid_ratio:.2} of high bytes in valid UTF-8 sequences"),
        );
        Encoding::Latin1
    } else {
        s.feat("printable_ratio", 1.5, frac(printable_ratio), format!("{printable_ratio:.3} printable"));
        if high_bits > 0 {
            s.feat("utf8_sequences", 1.0, frac(utf8_valid_ratio), format!("{utf8_valid_ratio:.2} valid"));
        }
        Encoding::Utf8
    };

    // Entropy is scored for every input, because "this is compressed or
    // encrypted" is a real answer and the caller needs to see the number
    // that produced it.
    s.feat(
        "entropy",
        1.5,
        atmost(entropy_bits, 6.5),
        format!("{entropy_bits:.2} bits/byte"),
    );
    if encoding.is_text() {
        s.feat("no_interior_nulls", 0.5, yes_no(null_ratio < 0.005), format!("null ratio {null_ratio:.4}"));
    }
    s.feat("length", 0.5, atleast(n, 64.0), format!("{} bytes", stats.len));

    let text_confidence = crate::evidence::confidence_from_bits(s.total_bits());

    Alphabet {
        encoding,
        bom_len: bom.map(|(m, _)| m.len()).unwrap_or(0),
        text_confidence: if encoding.is_text() { text_confidence } else { 1.0 - text_confidence },
        entropy_bits,
        printable_ratio,
        null_ratio,
        null_odd_bias,
        high_bit_ratio,
        utf8_valid_ratio,
        evidence: s.out,
    }
}

impl Alphabet {
    /// The byte view stages 1-2 profile: BOM stripped, and wide code
    /// units folded down to their ASCII plane so a UTF-16 document
    /// scores against the same fingerprints as a UTF-8 one.
    ///
    /// The fold is lossy on purpose. Non-ASCII code units become a
    /// single placeholder byte; ngram fingerprints care about
    /// punctuation cadence, and no fingerprint in this crate keys on a
    /// character above `0x7F`.
    pub fn normalize<'a>(&self, bytes: &'a [u8]) -> Cow<'a, [u8]> {
        let body = &bytes[self.bom_len.min(bytes.len())..];
        match self.encoding {
            Encoding::Utf8 | Encoding::Latin1 | Encoding::Binary => Cow::Borrowed(body),
            Encoding::Utf16Le => Cow::Owned(fold(body, 2, 0)),
            Encoding::Utf16Be => Cow::Owned(fold(body, 2, 1)),
            Encoding::Utf32Le => Cow::Owned(fold(body, 4, 0)),
            Encoding::Utf32Be => Cow::Owned(fold(body, 4, 3)),
        }
    }
}

const PLACEHOLDER: u8 = b'\x1a';

/// Take the ASCII-plane byte out of one `width`-byte code unit. Units
/// whose other bytes are non-zero are non-ASCII; they collapse to a
/// single placeholder so densities stay meaningful.
fn fold_unit(unit: &[u8], lo: usize) -> u8 {
    let ascii_plane = unit.iter().enumerate().all(|(i, &b)| i == lo || b == 0);
    if ascii_plane && unit[lo] < 0x80 {
        unit[lo]
    } else {
        PLACEHOLDER
    }
}

fn fold(body: &[u8], width: usize, lo: usize) -> Vec<u8> {
    body.chunks_exact(width).map(|u| fold_unit(u, lo)).collect()
}

/// Which byte of a code unit carries the ASCII plane, and how wide the
/// unit is. `None` for encodings that need no folding.
fn unit_layout(e: Encoding) -> Option<(usize, usize)> {
    match e {
        Encoding::Utf8 | Encoding::Latin1 | Encoding::Binary => None,
        Encoding::Utf16Le => Some((2, 0)),
        Encoding::Utf16Be => Some((2, 1)),
        Encoding::Utf32Le => Some((4, 0)),
        Encoding::Utf32Be => Some((4, 3)),
    }
}

/// Stateful normalizer for streamed input: strips the BOM and folds
/// wide code units down to their ASCII plane a chunk at a time,
/// carrying a partial code unit — or a partial BOM — across chunk
/// edges. A stream fed one byte at a time normalizes to exactly the
/// same bytes as one fed all at once.
#[derive(Clone, Debug)]
pub struct Folder {
    layout: Option<(usize, usize)>,
    bom_left: usize,
    carry: [u8; 4],
    carry_len: usize,
}

impl Folder {
    pub fn new(encoding: Encoding, bom_len: usize) -> Self {
        Self { layout: unit_layout(encoding), bom_left: bom_len, carry: [0; 4], carry_len: 0 }
    }

    pub fn push<'a>(&mut self, chunk: &'a [u8]) -> Cow<'a, [u8]> {
        let mut rest = chunk;
        if self.bom_left > 0 {
            let n = self.bom_left.min(rest.len());
            self.bom_left -= n;
            rest = &rest[n..];
        }
        let Some((width, lo)) = self.layout else {
            return Cow::Borrowed(rest);
        };

        let mut out = Vec::with_capacity((self.carry_len + rest.len()) / width + 1);
        let mut i = 0usize;
        if self.carry_len > 0 {
            while self.carry_len < width && i < rest.len() {
                self.carry[self.carry_len] = rest[i];
                self.carry_len += 1;
                i += 1;
            }
            if self.carry_len < width {
                return Cow::Owned(out); // still an incomplete unit
            }
            out.push(fold_unit(&self.carry[..width], lo));
            self.carry_len = 0;
        }

        let remaining = &rest[i..];
        let whole = remaining.len() / width * width;
        out.extend(remaining[..whole].chunks_exact(width).map(|u| fold_unit(u, lo)));

        let tail = &remaining[whole..];
        self.carry[..tail.len()].copy_from_slice(tail);
        self.carry_len = tail.len();
        Cow::Owned(out)
    }
}

fn shannon(hist: &[u64; 256], n: f64) -> f64 {
    let mut h = 0.0;
    for &c in hist.iter() {
        if c > 0 {
            let p = c as f64 / n;
            h -= p * p.log2();
        }
    }
    h
}

/// Fraction of bytes >= 0x80 that sit in a well-formed UTF-8 multi-byte
/// sequence. Defined as 1.0 when there are none.
fn utf8_sequence_validity(bytes: &[u8], high_bits: u64) -> f64 {
    if high_bits == 0 {
        return 1.0;
    }
    let mut ok = 0u64;
    let mut i = 0usize;
    while i < bytes.len() {
        let b = bytes[i];
        let need = match b {
            0x00..=0x7F => {
                i += 1;
                continue;
            }
            0xC2..=0xDF => 1,
            0xE0..=0xEF => 2,
            0xF0..=0xF4 => 3,
            // A continuation byte or an overlong lead in head position
            // is by definition not the start of a valid sequence.
            _ => {
                i += 1;
                continue;
            }
        };
        if i + need < bytes.len() && bytes[i + 1..=i + need].iter().all(|&c| (0x80..=0xBF).contains(&c)) {
            ok += 1 + need as u64;
            i += 1 + need;
        } else {
            i += 1;
        }
    }
    (ok as f64 / high_bits as f64).min(1.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn utf16le(s: &str) -> Vec<u8> {
        s.encode_utf16().flat_map(|u| u.to_le_bytes()).collect()
    }
    fn utf16be(s: &str) -> Vec<u8> {
        s.encode_utf16().flat_map(|u| u.to_be_bytes()).collect()
    }

    #[test]
    fn plain_ascii_is_utf8_text() {
        let a = detect(b"hello, world. this is a perfectly ordinary line of text.\n");
        assert_eq!(a.encoding, Encoding::Utf8);
        assert_eq!(a.bom_len, 0);
        assert!(a.text_confidence > 0.9);
    }

    #[test]
    fn utf16le_detected_by_parity_without_a_bom() {
        let bytes = utf16le("id,name,email\n1,alice,a@example.com\n2,bob,b@example.com\n");
        let a = detect(&bytes);
        assert_eq!(a.encoding, Encoding::Utf16Le, "evidence: {:#?}", a.evidence);
        assert!(a.null_odd_bias > 0.9);
    }

    #[test]
    fn utf16be_parity_points_the_other_way() {
        let bytes = utf16be("id,name,email\n1,alice,a@example.com\n2,bob,b@example.com\n");
        let a = detect(&bytes);
        assert_eq!(a.encoding, Encoding::Utf16Be);
        assert!(a.null_odd_bias < -0.9);
    }

    #[test]
    fn utf16_normalizes_back_to_its_ascii_plane() {
        let src = "id,name\n1,alice\n";
        for bytes in [utf16le(src), utf16be(src)] {
            let a = detect(&bytes);
            assert_eq!(&*a.normalize(&bytes), src.as_bytes());
        }
    }

    #[test]
    fn boms_win_and_are_stripped() {
        let mut bytes = vec![0xEF, 0xBB, 0xBF];
        bytes.extend_from_slice(b"{\"a\":1}");
        let a = detect(&bytes);
        assert_eq!(a.encoding, Encoding::Utf8);
        assert_eq!(a.bom_len, 3);
        assert_eq!(&*a.normalize(&bytes), b"{\"a\":1}");
    }

    #[test]
    fn utf32le_bom_beats_the_utf16le_prefix() {
        let mut bytes = vec![0xFF, 0xFE, 0x00, 0x00];
        bytes.extend_from_slice(&[b'a', 0, 0, 0, b'b', 0, 0, 0]);
        let a = detect(&bytes);
        assert_eq!(a.encoding, Encoding::Utf32Le);
        assert_eq!(&*a.normalize(&bytes), b"ab");
    }

    #[test]
    fn high_entropy_reads_as_binary_not_text() {
        // A cheap LCG stands in for compressed bytes: flat histogram,
        // entropy pinned near 8, no parity structure in the nulls.
        let mut x: u32 = 0x1234_5678;
        let bytes: Vec<u8> = (0..8192)
            .map(|_| {
                x = x.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
                (x >> 24) as u8
            })
            .collect();
        let a = detect(&bytes);
        assert_eq!(a.encoding, Encoding::Binary);
        assert!(a.entropy_bits > 7.5, "entropy was {}", a.entropy_bits);
    }

    #[test]
    fn valid_utf8_prose_stays_utf8() {
        let a = detect("il était une fois, un café — très bien\n".repeat(20).as_bytes());
        assert_eq!(a.encoding, Encoding::Utf8);
        assert!(a.utf8_valid_ratio > 0.99);
    }

    #[test]
    fn latin1_high_bytes_fail_the_sequence_test() {
        // 0xE9 ('é' in Latin-1) is a 3-byte UTF-8 lead; followed by
        // ASCII it never forms a valid sequence.
        let mut bytes = Vec::new();
        for _ in 0..200 {
            bytes.extend_from_slice(b"caf\xE9 na\xEFve resum\xE9 ");
        }
        let a = detect(&bytes);
        assert_eq!(a.encoding, Encoding::Latin1, "evidence: {:#?}", a.evidence);
    }

    #[test]
    fn empty_input_does_not_panic() {
        let a = detect(b"");
        assert_eq!(a.entropy_bits, 0.0);
        assert!(a.normalize(b"").is_empty());
    }
}
