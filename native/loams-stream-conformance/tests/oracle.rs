//! `oracle_records_expectations`: three smoke cases recorded from the live
//! oracle (Apache Flink 2.2.1 in `dev/oracle/compose.yaml`), re-run and
//! matched, and the recordings equal to the committed `expected.jsonl`.
//!
//! Needs Podman, so it is ignored by default:
//!
//! ```text
//! cargo test -p loams-stream-conformance --test oracle -- --ignored
//! ```
//!
//! It starts the oracle when it is not already up, and stops it afterwards
//! only in that case.

use std::path::{Path, PathBuf};
use std::time::Duration;

use loams_stream_conformance::case::discover;
use loams_stream_conformance::client::{ApiVersion, GatewayClient};
use loams_stream_conformance::compose::Oracle;
use loams_stream_conformance::harness::{self, RunOptions};
use loams_stream_conformance::normalise::compare;
use loams_stream_conformance::result::ResultTable;
use loams_stream_conformance::runner::{Shared, Target};
use loams_stream_conformance::triage::Verdict;

const CASES: [&str; 3] = [
    "tumble_count",
    "dedup_last_row",
    "changelog_into_upsert_sink",
];

fn copy_case(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let name = entry.file_name();
        if name == "expected.jsonl" {
            continue;
        }
        if entry.path().is_dir() {
            copy_case(&entry.path(), &to.join(&name));
        } else {
            std::fs::copy(entry.path(), to.join(&name)).unwrap();
        }
    }
}

#[tokio::test]
#[ignore = "needs Podman and the Flink 2.2.1 image"]
async fn oracle_records_expectations() {
    let repo = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let smoke = repo.join("dev/corpus/smoke");
    let oracle = Oracle::new(repo.join("dev/oracle/compose.yaml"));
    let started = !oracle.is_ready().await;
    if started {
        oracle.up().expect("podman compose up");
    }
    oracle
        .wait_ready(Duration::from_secs(180))
        .await
        .expect("the oracle is ready");

    let corpus = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join("oracle-records-expectations");
    let _ = std::fs::remove_dir_all(&corpus);
    for c in CASES {
        copy_case(&smoke.join(c), &corpus.join(c));
    }

    let client = GatewayClient::new(&oracle.gateway_url, ApiVersion::V1)
        .unwrap()
        .negotiate()
        .await
        .unwrap();
    assert_eq!(client.version(), ApiVersion::V4);
    let target = Target {
        name: "flink".into(),
        client,
        shared: Shared {
            local: oracle.work_dir(),
            remote: "/oracle/work".into(),
        },
    };
    let opts = RunOptions {
        run_id: harness::new_run_id(),
        ..RunOptions::default()
    };

    let cases = discover(&corpus).unwrap();
    assert_eq!(cases.len(), 3);
    let recorded = harness::record(&cases, &target, &opts).await;
    assert!(
        recorded
            .lines
            .iter()
            .all(|l| l.verdict == Verdict::Recorded),
        "{}",
        recorded.summary()
    );

    // A second run matches what was recorded.
    let cases = discover(&corpus).unwrap();
    let rerun = harness::run(&cases, std::slice::from_ref(&target), &opts).await;
    assert!(
        rerun.lines.iter().all(|l| l.verdict == Verdict::Match),
        "{}",
        rerun.summary()
    );

    // And the recordings equal the committed expectations under each
    // case's rules.
    for case in &cases {
        let fresh = ResultTable::read(&case.dir.join("expected.jsonl")).unwrap();
        let committed = ResultTable::read(&smoke.join(&case.name).join("expected.jsonl")).unwrap();
        let rules = case.rules(&committed.columns).unwrap();
        assert_eq!(compare(&committed, &fresh, &rules), None, "{}", case.name);
    }

    std::fs::remove_dir_all(&corpus).unwrap();
    let _ = std::fs::remove_dir_all(oracle.work_dir().join("reports"));
    if started {
        oracle.down().expect("podman compose down");
    }
}
