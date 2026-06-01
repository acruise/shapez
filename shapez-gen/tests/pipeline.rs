use std::path::PathBuf;

use shapez_gen::{Corpus, Generator};

fn corpus_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("corpus")
}

#[test]
fn corpus_loads_all_schemas() {
    let corpus = Corpus::load(corpus_path()).unwrap();
    assert_eq!(corpus.shapes().len(), corpus.manifest().schemas.len());
    assert!(corpus.shapes().len() >= 6);
}

#[test]
fn generator_emits_values() {
    let corpus = Corpus::load(corpus_path()).unwrap();
    let values: Vec<_> = Generator::new(&corpus, Some(42)).take(50).collect();
    assert_eq!(values.len(), 50);
}

#[test]
fn seed_is_deterministic() {
    let corpus = Corpus::load(corpus_path()).unwrap();
    let a: Vec<_> = Generator::new(&corpus, Some(7)).take(20).collect();
    let b: Vec<_> = Generator::new(&corpus, Some(7)).take(20).collect();
    assert_eq!(a, b);
}

#[test]
fn different_seeds_diverge() {
    let corpus = Corpus::load(corpus_path()).unwrap();
    let a: Vec<_> = Generator::new(&corpus, Some(1)).take(20).collect();
    let b: Vec<_> = Generator::new(&corpus, Some(2)).take(20).collect();
    assert_ne!(a, b);
}
