use rand::{Rng, SeedableRng};
use rand::rngs::StdRng;
use serde_json::Value;

use crate::corpus::Corpus;
use crate::instantiate::instantiate;
use crate::shape::Shape;

/// Infinite stream of `serde_json::Value`s drawn uniformly from a corpus.
pub struct Generator {
    rng: StdRng,
    shapes: Vec<Shape>,
    actual_seed: u64,
}

impl Generator {
    pub fn new(corpus: &Corpus, seed: Option<u64>) -> Self {
        let actual_seed = seed.unwrap_or_else(|| rand::thread_rng().gen());
        let rng = StdRng::seed_from_u64(actual_seed);
        Self {
            rng,
            shapes: corpus.shapes().to_vec(),
            actual_seed,
        }
    }

    pub fn seed(&self) -> u64 {
        self.actual_seed
    }
}

impl Iterator for Generator {
    type Item = Value;
    fn next(&mut self) -> Option<Value> {
        let i = self.rng.gen_range(0..self.shapes.len());
        let shape = self.shapes[i].clone();
        Some(instantiate(&shape, &mut self.rng))
    }
}
