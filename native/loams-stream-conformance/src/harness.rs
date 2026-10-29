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
            Ok(t) => {
                let differs = if case.spec.expect_error.is_some() {
                    None
                } else {
                    crate::selfcheck::check(case, &t)
                };
                Triage {
                    case: case.name.clone(),
                    targets: vec![oracle.name.clone()],
                    verdict: if differs.is_some() {
                        Verdict::OracleDiffers
                    } else {
                        Verdict::Recorded
                    },
                    first_difference: None,
                    errors: differs
                        .map(|d| BTreeMap::from([("flink-test".to_owned(), d)]))
                        .unwrap_or_default(),
                    rows: BTreeMap::from([(oracle.name.clone(), t.rows.len())]),
                    millis,
                }
            }
            Err(e) => Triage {
                case: case.name.clone(),
                targets: vec![oracle.name.clone()],
                verdict: if matches!(e, Error::Skipped(_)) {
                    Verdict::Skipped
                } else {
                    Verdict::ErrorOne
                },
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
    if let Err(Error::Skipped(m)) = &got {
        line.verdict = Verdict::Skipped;
        line.errors.insert(name.to_owned(), m.clone());
        return line;
    }
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

/// Writes `oracle-xfail.toml`: every case the oracle could not record, or whose recording differs
/// from the expectation of Flink's own test program, with the reason. Cases the oracle skipped
/// (a missing artefact) are listed separately.
///
/// # Errors
///
/// [`Error::Io`] when the file cannot be written.
pub fn write_oracle_xfail(report: &Report, path: &std::path::Path) -> Result<()> {
    let mut out = String::from(
        "# Cases the oracle (Apache Flink 2.2.1 through the harness) does not record cleanly.\n\
         # Written by `loams-stream-oracle record`; review before committing.\n",
    );
    let escape = |s: &str| {
        s.replace('\\', "\\\\")
            .replace('"', "\\\"")
            .replace('\n', " ")
    };
    for line in &report.lines {
        if line.verdict == Verdict::Recorded {
            continue;
        }
        let reason = line.errors.values().cloned().collect::<Vec<_>>().join("; ");
        out.push_str(&format!(
            "\n[\"{}\"]\nverdict = \"{}\"\nreason = \"{}\"\n",
            escape(&line.case),
            line.verdict,
            escape(&reason)
        ));
    }
    std::fs::write(path, out).map_err(|e| Error::io(path.display(), e))
}

/// The operator kinds in the optimized physical plan of `EXPLAIN` output: the identifier before
/// the `(` of each plan node line (`+- GroupAggregate(groupBy=[b], ...)` gives `GroupAggregate`).
pub fn plan_operators(explain: &str) -> Vec<String> {
    let section = explain
        .split("== Optimized Physical Plan ==")
        .nth(1)
        .map(|rest| rest.split("\n== ").next().unwrap_or(rest))
        .unwrap_or(explain);
    let mut ops = std::collections::BTreeSet::new();
    for line in section.lines() {
        let t = line.trim_start_matches([' ', '+', '-', ':']).trim_start();
        let Some(open) = t.find('(') else { continue };
        let name = &t[..open];
        if !name.is_empty() && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
            ops.insert(name.to_owned());
        }
    }
    ops.into_iter().collect()
}

/// Tag mode: for every case, runs `EXPLAIN` of its query on `oracle` after its setup, and writes
/// the plan's operator kinds to the case's `operators.txt` (one per line), which maps features to
/// cases (FC1 Task 6, Task 7's profile).
pub async fn tag(cases: &[Case], oracle: &Target) -> Report {
    use crate::client::{ExecuteStatement, FetchLimits, OpenSession};
    let mut report = Report::default();
    for case in cases {
        let start = Instant::now();
        let outcome: Result<Vec<String>> = async {
            if case.spec.expect_error.is_some() {
                return Ok(vec![]);
            }
            for need in &case.spec.requires {
                if !oracle.jars.contains_key(need) {
                    return Err(Error::Skipped(format!("requires {need}")));
                }
            }
            let mut properties = crate::runner::session_defaults();
            properties.insert(
                "execution.runtime-mode".into(),
                case.spec.mode.runtime_mode().into(),
            );
            properties.extend(case.spec.config.clone());
            let client = &oracle.client;
            let session = client
                .open_session(&OpenSession {
                    session_name: Some(format!("tag-{}", case.name)),
                    properties,
                })
                .await?;
            let limits = FetchLimits::default();
            let result = async {
                for need in &case.spec.requires {
                    let jar = &oracle.jars[need];
                    client
                        .execute_and_collect(
                            &session,
                            &ExecuteStatement::new(format!("ADD JAR '{jar}'")),
                            limits,
                        )
                        .await?;
                }
                for stmt in &case.setup {
                    let sql = crate::case::substitute(
                        stmt,
                        "/oracle/work/tag/data",
                        "/oracle/work/tag/sink",
                    )?;
                    client
                        .execute_and_collect(&session, &ExecuteStatement::new(sql), limits)
                        .await?;
                }
                let query = crate::case::substitute(
                    &case.query,
                    "/oracle/work/tag/data",
                    "/oracle/work/tag/sink",
                )?;
                let got = client
                    .execute_and_collect(
                        &session,
                        &ExecuteStatement::new(format!("EXPLAIN {query}")),
                        limits,
                    )
                    .await?;
                let text: String = got
                    .table
                    .rows
                    .iter()
                    .filter_map(|r| r.fields.first())
                    .filter_map(|f| serde_json::from_str::<String>(f).ok())
                    .collect::<Vec<_>>()
                    .join("\n");
                Ok(plan_operators(&text))
            }
            .await;
            let _ = client.close_session(&session).await;
            result
        }
        .await;
        let millis = start.elapsed().as_millis();
        let line = match outcome {
            Ok(ops) => {
                let path = case.dir.join("operators.txt");
                let body = if ops.is_empty() {
                    String::new()
                } else {
                    ops.join("\n") + "\n"
                };
                match std::fs::write(&path, body) {
                    Ok(()) => Triage {
                        case: case.name.clone(),
                        targets: vec![oracle.name.clone()],
                        verdict: Verdict::Recorded,
                        first_difference: None,
                        errors: BTreeMap::new(),
                        rows: BTreeMap::from([(oracle.name.clone(), ops.len())]),
                        millis,
                    },
                    Err(e) => Triage {
                        case: case.name.clone(),
                        targets: vec![oracle.name.clone()],
                        verdict: Verdict::ErrorOne,
                        first_difference: None,
                        errors: BTreeMap::from([(oracle.name.clone(), e.to_string())]),
                        rows: BTreeMap::new(),
                        millis,
                    },
                }
            }
            Err(e) => Triage {
                case: case.name.clone(),
                targets: vec![oracle.name.clone()],
                verdict: if matches!(e, Error::Skipped(_)) {
                    Verdict::Skipped
                } else {
                    Verdict::ErrorOne
                },
                first_difference: None,
                errors: BTreeMap::from([(oracle.name.clone(), e.summary())]),
                rows: BTreeMap::new(),
                millis,
            },
        };
        report.lines.push(line);
    }
    report
}
