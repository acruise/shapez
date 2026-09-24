//! `cargo run -p shapez-sniff --example sniff -- [-v] <file>...`
//!
//! Streams each file rather than reading it whole, so it works on
//! inputs larger than memory. With no arguments it sniffs stdin, which
//! is the case that has no length to know in advance.

use std::fs::File;
use std::io::{self, Read};

use shapez_sniff::{sniff_reader, SniffReport};

fn main() {
    let mut args: Vec<String> = std::env::args().skip(1).collect();
    let verbose = args.iter().any(|a| a == "-v");
    args.retain(|a| a != "-v");

    if args.is_empty() {
        run("<stdin>", io::stdin().lock(), verbose);
        return;
    }
    for path in &args {
        match File::open(path) {
            Ok(f) => run(path, f, verbose),
            Err(e) => eprintln!("{path}: {e}"),
        }
    }
}

fn run<R: Read>(label: &str, r: R, verbose: bool) {
    match sniff_reader(r) {
        Ok(report) => print_report(label, &report, verbose),
        Err(e) => eprintln!("{label}: {e}"),
    }
}

fn print_report(label: &str, r: &SniffReport, verbose: bool) {
    println!("== {label}");
    print!("{r}");
    if verbose {
        if let Some(best) = r.best() {
            println!("   evidence for {}:", best.syntax);
            for e in best.decisive() {
                println!("     {e}");
            }
        }
    }
    println!();
}
