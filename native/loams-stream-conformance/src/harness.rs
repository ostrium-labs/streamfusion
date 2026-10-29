//! Record and run: a corpus on one or more targets, compared under each
//! case's rules.

use std::collections::BTreeMap;
use std::time::Instant;

use crate::case::Case;
use crate::error::{Error, Result};
use crate::normalise::compare;
use crate::result::ResultTable;
use crate::runner::{Target, run_case};
use crate::triage::{Report, Triage, Verdict};

/// Options for a run.
#[derive(Debug, Clone, Default)]
pub struct RunOptions {
    /// Keeps each case's work directory (data and sink files).
    pub keep: bool,
    /// A name for this run's directory under the shared `runs/`.
    pub run_id: String,
    /// Prints one line per case to stderr as it finishes.
    pub verbose: bool,
}

async fn timed(target: &Target, case: &Case, opts: &RunOptions) -> (Result<ResultTable>, u128) {
    let start = Instant::now();
    let r = run_case(target, case, &opts.run_id, opts.keep).await;
    (r, start.elapsed().as_millis())
}

/// Oracle-only mode: runs every case on `oracle` and writes its result to
/// the case's `expected.jsonl`. A case whose run fails keeps its old
/// `expected.jsonl` (if any) and is reported `error-one`.
pub async fn record(cases: &[Case], oracle: &Target, opts: &RunOptions) -> Report {
    let mut report = Report::default();
    for case in cases {
        let (got, millis) = timed(oracle, case, opts).await;
        let got = got.and_then(|t| {
            case.rules(&t.columns)?;
            t.write(&case.dir.join("expected.jsonl"))?;
            Ok(t)
        });
        let line = match got {
            Ok(t) => Triage {
                case: case.name.clone(),
                targets: vec![oracle.name.clone()],
                verdict: Verdict::Recorded,
                first_difference: None,
                errors: BTreeMap::new(),
                rows: BTreeMap::from([(oracle.name.clone(), t.rows.len())]),
                millis,
            },
            Err(e) => Triage {
                case: case.name.clone(),
                targets: vec![oracle.name.clone()],
                verdict: Verdict::ErrorOne,
                first_difference: None,
                errors: BTreeMap::from([(oracle.name.clone(), e.summary())]),
                rows: BTreeMap::new(),
                millis,
            },
        };
        if opts.verbose {
            eprintln!("{:<40} {} ({} ms)", line.case, line.verdict, line.millis);
        }
        report.lines.push(line);
    }
    report
}

/// Runs every case on every target. The reference is the case's
/// `expected.jsonl` when it has one, else the first target; every other
/// side is compared with it.
pub async fn run(cases: &[Case], targets: &[Target], opts: &RunOptions) -> Report {
    let mut report = Report::default();
    for case in cases {
        let mut results = Vec::with_capacity(targets.len());
        for t in targets {
            results.push((t.name.clone(), timed(t, case, opts).await));
        }
        let (ref_name, reference, ref_millis): (String, Result<ResultTable>, u128) =
            match &case.expected {
                Some(e) => ("expected".into(), Ok(e.clone()), 0),
                None => {
                    let (n, (r, ms)) = results.remove(0);
                    (n, r, ms)
                }
            };
        if results.is_empty() {
            let mut line = Triage {
                case: case.name.clone(),
                targets: vec![ref_name.clone()],
                verdict: Verdict::NoExpectation,
                first_difference: None,
                errors: BTreeMap::new(),
                rows: BTreeMap::new(),
                millis: ref_millis,
            };
            match &reference {
                Ok(t) => {
                    line.rows.insert(ref_name, t.rows.len());
                }
                Err(e) => {
                    line.verdict = Verdict::ErrorOne;
                    line.errors.insert(ref_name, e.summary());
                }
            }
            report.lines.push(line);
            continue;
        }
        for (name, (got, millis)) in results {
            let line = judge(case, &ref_name, &reference, &name, got, millis);
            if opts.verbose {
                eprintln!("{:<40} {} ({} ms)", line.case, line.verdict, line.millis);
            }
            report.lines.push(line);
        }
    }
    report
}

fn judge(
    case: &Case,
    ref_name: &str,
    reference: &Result<ResultTable>,
    name: &str,
    got: Result<ResultTable>,
    millis: u128,
) -> Triage {
    let mut line = Triage {
        case: case.name.clone(),
        targets: vec![ref_name.to_owned(), name.to_owned()],
        verdict: Verdict::Match,
        first_difference: None,
        errors: BTreeMap::new(),
        rows: BTreeMap::new(),
        millis,
    };
    match (reference, got) {
        (Err(a), Err(b)) => {
            line.verdict = Verdict::ErrorBoth;
            line.errors.insert(ref_name.to_owned(), a.summary());
            line.errors.insert(name.to_owned(), b.summary());
        }
        (Err(a), Ok(t)) => {
            line.verdict = Verdict::ErrorOne;
            line.errors.insert(ref_name.to_owned(), a.summary());
            line.rows.insert(name.to_owned(), t.rows.len());
        }
        (Ok(e), Err(b)) => {
            line.verdict = Verdict::ErrorOne;
            line.rows.insert(ref_name.to_owned(), e.rows.len());
            line.errors.insert(name.to_owned(), b.summary());
        }
        (Ok(e), Ok(t)) => {
            line.rows.insert(ref_name.to_owned(), e.rows.len());
            line.rows.insert(name.to_owned(), t.rows.len());
            match case.rules(&e.columns) {
                Err(err) => {
                    line.verdict = Verdict::ErrorBoth;
                    line.errors.insert("case".into(), err.to_string());
                }
                Ok(rules) => {
                    if let Some(d) = compare(e, &t, &rules) {
                        line.verdict = Verdict::Mismatch;
                        line.first_difference = Some(d);
                    }
                }
            }
        }
    }
    line
}

/// A run id from the clock (`run-<unix seconds>`).
pub fn new_run_id() -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    format!("run-{secs}-{}", std::process::id())
}

/// Fails with a one-line summary unless every line passed.
///
/// # Errors
///
/// [`Error::Invalid`] naming the counts.
pub fn check(report: &Report) -> Result<()> {
    if report.all_passed() {
        Ok(())
    } else {
        let counts = report
            .counts()
            .iter()
            .map(|(k, v)| format!("{v} {k}"))
            .collect::<Vec<_>>()
            .join(", ");
        Err(Error::Invalid(format!("not every case passed: {counts}")))
    }
}
