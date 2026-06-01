use std::path::PathBuf;

use shapez_gen::{Corpus, Generator};

#[test]
fn lower_corpus_stream_without_panic() {
    let corpus_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("shapez-gen")
        .join("corpus");
    let corpus = Corpus::load(corpus_path).unwrap();
    let gen = Generator::new(&corpus, Some(13));
    let lowered: Vec<_> = gen.take(200).map(|v| shapez_json::lower(&v)).collect();
    assert_eq!(lowered.len(), 200);
}
