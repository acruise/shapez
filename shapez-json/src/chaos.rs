//! Random JSON value generators for adversarial-shape testing.
//!
//! These produce values with arbitrary root type, mixed scalar
//! distributions, and bounded nesting. The default `random_value` is
//! recursive and chooses a compound vs scalar branch with a probability
//! that decays toward leaves. Helpers for extreme patterns
//! (`nested_array_spine`, `nested_mixed_spine`) cover the deep-but-thin
//! shapes the recursive generator effectively never produces.
//!
//! Used by integration tests and by the eyeball example to stress the
//! analyzer beyond what schema-driven corpora can express.

use std::ops::Range;

use rand::Rng;
use rand::rngs::StdRng;
use serde_json::{Map, Number, Value};

/// Generate a random JSON value bounded by `depth_budget` levels of
/// nesting and `max_branch` children per container. Always returns a
/// scalar when `depth_budget == 0`.
pub fn random_value(rng: &mut StdRng, depth_budget: u32, max_branch: u32) -> Value {
    let compound_prob = if depth_budget == 0 {
        0.0
    } else {
        0.5 * (depth_budget as f64 / (depth_budget as f64 + 2.0))
    };
    if rng.gen_bool(compound_prob) {
        if rng.gen_bool(0.5) {
            random_array(rng, depth_budget, max_branch)
        } else {
            random_object(rng, depth_budget, max_branch)
        }
    } else {
        random_scalar(rng)
    }
}

/// Sample uniformly across null, bool, i64, u64 (above i64::MAX),
/// f64, and string. Strings are short lowercase ASCII.
pub fn random_scalar(rng: &mut StdRng) -> Value {
    match rng.gen_range(0..7) {
        0 => Value::Null,
        1 => Value::Bool(rng.gen()),
        2 => Value::Number(Number::from(rng.gen_range(-100_000i64..=100_000))),
        3 => Value::Number(Number::from(
            rng.gen_range((i64::MAX as u64 + 1)..=(i64::MAX as u64 + 1_000_000)),
        )),
        4 => Number::from_f64(rng.gen_range(-1e6..1e6))
            .map(Value::Number)
            .unwrap_or(Value::Null),
        5 => Value::String(random_string(rng, 0..16)),
        _ => Value::String(String::new()),
    }
}

pub fn random_array(rng: &mut StdRng, depth_budget: u32, max_branch: u32) -> Value {
    let n = rng.gen_range(0..=max_branch);
    let items: Vec<Value> = (0..n)
        .map(|_| random_value(rng, depth_budget - 1, max_branch))
        .collect();
    Value::Array(items)
}

pub fn random_object(rng: &mut StdRng, depth_budget: u32, max_branch: u32) -> Value {
    let n = rng.gen_range(0..=max_branch);
    let mut map = Map::new();
    for _ in 0..n {
        let key = random_string(rng, 1..8);
        map.insert(key, random_value(rng, depth_budget - 1, max_branch));
    }
    Value::Object(map)
}

pub fn random_string(rng: &mut StdRng, len_range: Range<usize>) -> String {
    let len = rng.gen_range(len_range);
    (0..len).map(|_| rng.gen_range(b'a'..=b'z') as char).collect()
}

/// A depth-N nested array chain ending in a scalar at the bottom.
/// Useful for stress-testing recursion in the analyzer and finalizer
/// independent of the random generator's depth-decay bias.
pub fn nested_array_spine(depth: u32) -> Value {
    if depth == 0 {
        Value::Number(Number::from(1i64))
    } else {
        Value::Array(vec![nested_array_spine(depth - 1)])
    }
}

/// A depth-N spine alternating object (even levels) and array (odd
/// levels). Object levels use the key `"next"`.
pub fn nested_mixed_spine(depth: u32) -> Value {
    if depth == 0 {
        Value::Bool(true)
    } else if depth % 2 == 0 {
        let mut m = Map::new();
        m.insert("next".to_string(), nested_mixed_spine(depth - 1));
        Value::Object(m)
    } else {
        Value::Array(vec![nested_mixed_spine(depth - 1)])
    }
}
