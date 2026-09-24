//! Evidence records: the reason a candidate scored the way it did.
//!
//! Every scoring decision in this crate emits one `Evidence` per
//! feature considered, whether the feature helped or hurt. This is
//! deliberate — a classifier that can explain itself can be corrected
//! by a workload trace; one that can't is a black box that ages badly.
//! See `shapez/SYNTAX_DISCOVERY.md` § *Stage 2*.
//!
//! Features may be asymmetric. Absence of an expected-absent marker is
//! weak positive evidence; presence is strong negative evidence, and
//! some features are refutation-only — see [`Scorer::refute`].

/// One feature's contribution to a candidate's score.
#[derive(Clone, Debug, PartialEq)]
pub struct Evidence {
    /// Stable feature name. Used as a diagnostic key; treat it as part
    /// of the (informal) contract with anything that aggregates traces.
    pub feature: &'static str,
    /// How well the observation satisfied the feature, in `[-1, +1]`.
    /// `+1` fully satisfied, `0` no information, `-1` fully refuted.
    pub satisfaction: f64,
    /// The feature's evidentiary weight for this candidate, in bits.
    /// Always non-negative; the sign of the contribution comes from
    /// `satisfaction`.
    pub weight: f64,
    /// Human-readable observation ("`{`/`}` 412 vs 412"). Diagnostics
    /// only; never parsed.
    pub note: String,
}

impl Evidence {
    /// Signed contribution to the candidate's total, in bits.
    pub fn bits(&self) -> f64 {
        self.weight * self.satisfaction
    }
}

impl std::fmt::Display for Evidence {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:<22} {:+.2} sat x {:.2}w = {:+.2} bits  ({})",
            self.feature,
            self.satisfaction,
            self.weight,
            self.bits(),
            self.note
        )
    }
}

// ---------------------------------------------------------------------------
// Satisfaction helpers. Each maps an observation onto [-1, +1].
// ---------------------------------------------------------------------------

/// "This density should land inside `[lo, hi]`." Full satisfaction in
/// band; outside, decays linearly in the *ratio* to the nearer edge, so
/// being 2x over the top is the same penalty as being half the floor.
pub fn band(x: f64, lo: f64, hi: f64) -> f64 {
    debug_assert!(lo <= hi);
    if x >= lo && x <= hi {
        1.0
    } else if x < lo {
        if lo <= 0.0 {
            return 1.0;
        }
        (2.0 * (x / lo) - 1.0).clamp(-1.0, 1.0)
    } else {
        if x <= 0.0 {
            return 1.0;
        }
        (2.0 * (hi / x) - 1.0).clamp(-1.0, 1.0)
    }
}

/// "These two counts should match." Returns `0.0` (no information) when
/// both are zero — absence of brackets is not evidence that brackets
/// balance.
pub fn balance(a: u64, b: u64) -> f64 {
    let (a, b) = (a as f64, b as f64);
    let max = a.max(b);
    if max == 0.0 {
        return 0.0;
    }
    2.0 * (a.min(b) / max) - 1.0
}

/// "This value should be at least `t`." Saturates at `t`, ramps to `-1`
/// at zero.
pub fn atleast(x: f64, t: f64) -> f64 {
    if t <= 0.0 {
        return 1.0;
    }
    (2.0 * (x / t) - 1.0).clamp(-1.0, 1.0)
}

/// "This value should be at most `t`." Saturates at `t`; a value `2t`
/// or worse is fully refuted.
pub fn atmost(x: f64, t: f64) -> f64 {
    if x <= t {
        return 1.0;
    }
    if x <= 0.0 {
        return 1.0;
    }
    (2.0 * (t / x) - 1.0).clamp(-1.0, 1.0)
}

/// A fraction in `[0, 1]` read directly as satisfaction.
pub fn frac(f: f64) -> f64 {
    (2.0 * f - 1.0).clamp(-1.0, 1.0)
}

/// A boolean feature. Present the `false` case honestly: it is
/// refutation, not absence of evidence.
pub fn yes_no(b: bool) -> f64 {
    if b {
        1.0
    } else {
        -1.0
    }
}

/// Accumulates evidence for one candidate.
pub(crate) struct Scorer {
    pub(crate) out: Vec<Evidence>,
}

impl Scorer {
    pub(crate) fn new() -> Self {
        Self { out: Vec::new() }
    }

    /// A symmetric feature: satisfaction and refutation carry the same
    /// weight.
    pub(crate) fn feat(
        &mut self,
        feature: &'static str,
        weight: f64,
        satisfaction: f64,
        note: impl Into<String>,
    ) {
        self.feat_asym(feature, weight, weight, satisfaction, note);
    }

    /// An asymmetric feature: satisfaction is worth `weight_pos` bits,
    /// refutation `weight_neg`.
    ///
    /// The asymmetry is the whole point of negative evidence. "There is
    /// no JSON structure here" must be able to *punish* a CSV
    /// hypothesis hard without *rewarding* it — otherwise every
    /// syntax's fingerprint accumulates a large positive baseline just
    /// from all the things the input isn't, and a file of bare integers
    /// scores 0.7 as CSV for want of any commas to disagree about.
    pub(crate) fn feat_asym(
        &mut self,
        feature: &'static str,
        weight_pos: f64,
        weight_neg: f64,
        satisfaction: f64,
        note: impl Into<String>,
    ) {
        debug_assert!(weight_pos >= 0.0 && weight_neg >= 0.0);
        let sat = satisfaction.clamp(-1.0, 1.0);
        let weight = if sat >= 0.0 { weight_pos } else { weight_neg };
        self.out.push(Evidence { feature, satisfaction: sat, weight, note: note.into() });
    }

    /// A refutation-only feature: it can sink a candidate but never
    /// float one. Use for "and this isn't <other syntax>" checks.
    pub(crate) fn refute(
        &mut self,
        feature: &'static str,
        weight: f64,
        satisfaction: f64,
        note: impl Into<String>,
    ) {
        self.feat_asym(feature, 0.0, weight, satisfaction, note);
    }

    pub(crate) fn total_bits(&self) -> f64 {
        self.out.iter().map(Evidence::bits).sum()
    }
}

/// Base-2 logistic: bits of evidence to a confidence in `(0, 1)`.
/// Four bits of net evidence reads as ~0.94.
pub fn confidence_from_bits(bits: f64) -> f64 {
    1.0 / (1.0 + (2.0f64).powf(-bits))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn band_saturates_inside_and_decays_outside() {
        assert_eq!(band(0.05, 0.01, 0.10), 1.0);
        assert_eq!(band(0.01, 0.01, 0.10), 1.0);
        assert!((band(0.005, 0.01, 0.10) - 0.0).abs() < 1e-9); // half the floor
        assert!((band(0.20, 0.01, 0.10) - 0.0).abs() < 1e-9); // twice the ceiling
        assert_eq!(band(0.0, 0.01, 0.10), -1.0);
    }

    #[test]
    fn balance_is_silent_when_both_absent() {
        assert_eq!(balance(0, 0), 0.0);
        assert_eq!(balance(10, 10), 1.0);
        assert_eq!(balance(10, 5), 0.0);
        assert_eq!(balance(10, 0), -1.0);
    }

    #[test]
    fn refutation_only_features_sink_but_never_float() {
        let mut s = Scorer::new();
        s.refute("not_json", 3.0, 1.0, "no JSON structure");
        assert_eq!(s.total_bits(), 0.0, "satisfaction earns nothing");

        let mut s = Scorer::new();
        s.refute("not_json", 3.0, -1.0, "definitely JSON");
        assert_eq!(s.total_bits(), -3.0, "refutation costs full weight");
    }

    #[test]
    fn confidence_hits_the_calibration_target() {
        let c = confidence_from_bits(4.0);
        assert!(c > 0.93 && c < 0.95, "4 bits should read ~0.94, got {c}");
        assert!((confidence_from_bits(0.0) - 0.5).abs() < 1e-12);
        assert!(confidence_from_bits(-4.0) < 0.07);
    }
}
