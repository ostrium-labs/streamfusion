//! The scope manifest stays in step with its generator and with the corpus (FC1 Task 7).

use std::collections::BTreeSet;
use std::path::PathBuf;

use loams_stream_conformance::profile::{OPERATORS, read_functions, render_page, render_profile};

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn functions() -> Vec<String> {
    read_functions(
        &std::fs::read_to_string(root().join("conformance/vendor/flink-2.2.1/functions.txt"))
            .unwrap(),
    )
}

/// `conformance/profile.toml` and `docs/compat/flink.md` are what the generator writes
/// (regenerate with `loams-stream-oracle profile`).
#[test]
fn profile_is_generated() {
    let f = functions();
    for (rel, want) in [
        ("conformance/profile.toml", render_profile(&f)),
        ("docs/compat/flink.md", render_page(&f)),
    ] {
        let got = std::fs::read_to_string(root().join(rel)).unwrap_or_default();
        assert_eq!(
            got, want,
            "{rel} is stale: run `loams-stream-oracle profile`"
        );
    }
    let profile: toml::Table = toml::from_str(&render_profile(&f)).unwrap();
    for section in ["functions", "operators", "statements", "connectors"] {
        assert!(profile.contains_key(section), "{section}");
    }
}

/// Every operator in a corpus case's `operators.txt` (written by `loams-stream-oracle tag`) is
/// in the manifest, so the profile maps every feature the corpus exercises.
#[test]
fn profile_and_corpus_agree() {
    let known: BTreeSet<&str> = OPERATORS.iter().map(|(n, _)| *n).collect();
    let mut tagged = 0;
    let mut unknown = BTreeSet::new();
    for corpus in ["smoke", "nexmark", "flink-programs"] {
        let dir = root().join("dev/corpus").join(corpus);
        for entry in std::fs::read_dir(&dir).unwrap().flatten() {
            let ops = entry.path().join("operators.txt");
            let Ok(text) = std::fs::read_to_string(&ops) else {
                continue;
            };
            tagged += 1;
            for op in text.lines().filter(|l| !l.is_empty()) {
                // EXPLAIN prints the stream variants without a prefix; tolerate one anyway.
                let op = op.trim_start_matches("Stream");
                if !known.contains(op) {
                    unknown.insert(format!("{op} ({})", entry.file_name().to_string_lossy()));
                }
            }
        }
    }
    println!("{tagged} tagged cases");
    assert!(
        unknown.is_empty(),
        "operators missing from profile::OPERATORS: {unknown:?}"
    );
}
