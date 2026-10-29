//! The committed corpus: every case loads, and the extractor's manifest accounts for every
//! Flink test program (FC1 Task 6).

use std::collections::BTreeSet;
use std::path::PathBuf;

use loams_stream_conformance::case::discover;

fn corpus() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../dev/corpus")
}

#[test]
fn every_corpus_case_loads() {
    for dir in ["smoke", "nexmark", "flink-programs"] {
        let cases = discover(&corpus().join(dir)).unwrap_or_else(|e| panic!("{dir}: {e}"));
        assert!(!cases.is_empty(), "{dir}");
        for c in &cases {
            if c.spec.restore {
                assert!(
                    c.dir.join("data").is_dir() || c.spec.restore_before_rows == Some(0),
                    "{}: a restore case ships its data",
                    c.name
                );
            }
        }
        println!("{dir}: {} cases", cases.len());
    }
}

/// Every program the extractor found by reflection is either exported (with a case directory
/// for each case it wrote) or listed as not exported with a reason.
#[test]
fn extractor_covers_every_program() {
    let root = corpus().join("flink-programs");
    let manifest: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(root.join("manifest.json")).unwrap())
            .unwrap();
    let found = manifest["programs_found"].as_u64().unwrap() as usize;
    let exported = manifest["programs_exported"].as_u64().unwrap() as usize;
    let programs = manifest["programs"].as_array().unwrap();
    assert_eq!(
        programs.len(),
        found,
        "one manifest entry per program found"
    );
    let not_exported = manifest["not_exported"].as_u64().unwrap_or_else(|| {
        manifest["not_exported"]
            .as_array()
            .map(|a| a.len() as u64)
            .unwrap_or(0)
    }) as usize;
    assert_eq!(
        exported + not_exported,
        found,
        "exported + not exported = found"
    );
    assert!(
        manifest["unloadable"].as_array().unwrap().is_empty(),
        "no unloadable class"
    );

    let dirs: BTreeSet<String> = std::fs::read_dir(&root)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().join("case.toml").is_file())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect();
    let mut written = 0;
    let mut with_reason = 0;
    for p in programs {
        let cases = p["cases"].as_array().cloned().unwrap_or_default();
        if cases.is_empty() {
            assert!(
                p.get("reason")
                    .and_then(|r| r.as_str())
                    .is_some_and(|r| !r.is_empty())
                    || p.get("not_exported").is_some(),
                "{}: not exported without a reason",
                p["id"]
            );
            with_reason += 1;
        }
        for c in cases {
            let c = c.as_str().unwrap();
            assert!(dirs.contains(c), "{}: case {c} has no directory", p["id"]);
            written += 1;
        }
    }
    assert_eq!(
        written,
        dirs.len(),
        "every case directory belongs to a program"
    );
    assert_eq!(with_reason, not_exported);
    println!(
        "{found} programs: {exported} exported as {written} cases, {not_exported} not exported"
    );
}
