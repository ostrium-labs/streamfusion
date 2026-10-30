//! Corpus cases planned by `loams-stream-plan`, run by the local executor and compared with the
//! results recorded from Apache Flink 2.2.1 (FC1 Task 10).

use std::path::{Path, PathBuf};

use loams_stream_conformance::case::{Case, Collect, substitute};
use loams_stream_conformance::normalise::compare;
use loams_stream_conformance::result::{Column, ResultTable, Row, RowKind};
use loams_stream_conformance::runner::read_debezium_dir;
use loams_stream_connectors::format::json::{TimestampFormat, value_text_in};
use loams_stream_plan::physical::PhysicalOp;
use loams_stream_plan::{Planned, Planner};
use loams_stream_runtime::local::{self, LocalOptions};
use loams_stream_sql::types::{LogicalKind, LogicalType};

fn corpus() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../dev/corpus")
}

/// Runs one case in-process; returns its result table.
fn run_case(case: &Case) -> Result<ResultTable, String> {
    let work = Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join("loams-stream-corpus")
        .join(&case.name);
    let _ = std::fs::remove_dir_all(&work);
    let sink_dir = work.join("sink");
    std::fs::create_dir_all(&sink_dir).map_err(|e| e.to_string())?;
    // A restore case runs on `data/` and then, after a savepoint, on `data-after-restore/` too;
    // without savepoints yet (Task 14), the local run reads both in order in one job, with a
    // watermark per row as the restore flow's sources emit them.
    let case_dir = if case.spec.restore {
        let merged = work.join("data");
        for (prefix, part) in [("0-", "data"), ("1-", "data-after-restore")] {
            let Ok(tables) = std::fs::read_dir(case.dir.join(part)) else {
                continue;
            };
            for table in tables {
                let table = table.map_err(|e| e.to_string())?.path();
                let name = table.file_name().unwrap().to_owned();
                let target = merged.join(&name);
                std::fs::create_dir_all(&target).map_err(|e| e.to_string())?;
                let files: Vec<_> = if table.is_dir() {
                    std::fs::read_dir(&table)
                        .map_err(|e| e.to_string())?
                        .map(|f| f.unwrap().path())
                        .collect()
                } else {
                    vec![table.clone()]
                };
                for f in files {
                    let fname = format!("{prefix}{}", f.file_name().unwrap().to_string_lossy());
                    std::fs::copy(&f, target.join(fname)).map_err(|e| e.to_string())?;
                }
            }
        }
        merged
    } else {
        case.dir.join("data")
    };
    let options = LocalOptions {
        batch_rows: if case.spec.restore { 1 } else { 1024 },
        ..LocalOptions::default()
    };
    let sub = |s: &str| {
        substitute(s, &case_dir.to_string_lossy(), &sink_dir.to_string_lossy())
            .map_err(|e| e.to_string())
    };
    let mut planner = Planner::new();
    // The case's configuration, as the runner sets it on the session.
    for (k, v) in &case.spec.config {
        planner
            .plan_script(&format!("SET '{k}' = '{v}';"))
            .map_err(|e| format!("config: {e}"))?;
    }
    for stmt in &case.setup {
        planner
            .plan_script(&sub(stmt)?)
            .map_err(|e| format!("setup: {e}"))?;
    }
    let planned = planner
        .plan_script(&sub(&case.query)?)
        .map_err(|e| format!("plan: {e}"))?;
    let Some(Planned::Job(plan)) = planned.into_iter().last() else {
        return Err("the query planned no job".into());
    };
    let output = local::run(&plan, &options).map_err(|e| format!("run: {e}"))?;
    let result = (|| {
        let sink = plan
            .nodes
            .iter()
            .find(|n| matches!(n.op, PhysicalOp::Sink(_)))
            .ok_or("no sink")?;
        let columns: Vec<Column> = sink
            .fields
            .iter()
            .map(|f| {
                let ty = match case.spec.collect {
                    Collect::Sink => f.ty.to_string(),
                    Collect::Query => gateway_type(&f.ty),
                };
                Column::new(&f.name, ty)
            })
            .collect();
        match case.spec.collect {
            Collect::Sink => {
                let table = case.spec.sink.as_deref().ok_or("no sink name")?;
                let rows = read_debezium_dir(&sink_dir.join(table), &columns)
                    .map_err(|e| e.to_string())?;
                Ok(ResultTable { columns, rows })
            }
            Collect::Query => {
                let mut rows = vec![];
                for batch in output.collected.get(&sink.id).into_iter().flatten() {
                    let kinds = local::row_kinds(batch);
                    for (r, kind) in kinds.iter().enumerate() {
                        let fields = sink
                            .fields
                            .iter()
                            .enumerate()
                            .map(|(i, f)| {
                                value_text_in(batch.column(i), r, &f.ty, TimestampFormat::Iso8601)
                            })
                            .collect::<Result<Vec<_>, _>>()
                            .map_err(|e| e.to_string())?;
                        rows.push(Row::new(RowKind::parse(kind).unwrap(), fields));
                    }
                }
                Ok(ResultTable { columns, rows })
            }
        }
    })();
    let _ = std::fs::remove_dir_all(&work);
    result
}

/// A column type as the harness renders the SQL Gateway's JSON logical type.
fn gateway_type(ty: &LogicalType) -> String {
    let base = match &ty.kind {
        LogicalKind::Char(n) => format!("CHAR({n})"),
        LogicalKind::Varchar(n) => format!("VARCHAR({n})"),
        LogicalKind::Binary(n) => format!("BINARY({n})"),
        LogicalKind::Varbinary(n) => format!("VARBINARY({n})"),
        LogicalKind::Decimal { precision, scale } => format!("DECIMAL({precision}, {scale})"),
        LogicalKind::Time(p) => format!("TIME({p})"),
        LogicalKind::Timestamp { precision, .. } => format!("TIMESTAMP({precision})"),
        LogicalKind::TimestampLtz { precision, .. } => format!("TIMESTAMP_LTZ({precision})"),
        LogicalKind::TimestampTz(p) => format!("TIMESTAMP({p}) WITH TIME ZONE"),
        LogicalKind::Array(e) => format!("ARRAY<{}>", gateway_type(e)),
        LogicalKind::Multiset(e) => format!("MULTISET<{}>", gateway_type(e)),
        LogicalKind::Map(k, v) => format!("MAP<{}, {}>", gateway_type(k), gateway_type(v)),
        LogicalKind::Row(fields) => format!(
            "ROW<{}>",
            fields
                .iter()
                .map(|f| format!("`{}` {}", f.name, gateway_type(&f.ty)))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        LogicalKind::Boolean => "BOOLEAN".into(),
        LogicalKind::TinyInt => "TINYINT".into(),
        LogicalKind::SmallInt => "SMALLINT".into(),
        LogicalKind::Int => "INTEGER".into(),
        LogicalKind::BigInt => "BIGINT".into(),
        LogicalKind::Float => "FLOAT".into(),
        LogicalKind::Double => "DOUBLE".into(),
        LogicalKind::Date => "DATE".into(),
        LogicalKind::Null => "NULL".into(),
        _ => ty.to_string(),
    };
    if ty.nullable {
        base
    } else {
        format!("{base} NOT NULL")
    }
}

/// Runs `case` and compares it with its recorded expectation.
fn check(case: &Case) -> Result<(), String> {
    let expected = case.expected.as_ref().ok_or("no expectation recorded")?;
    if let Some(error) = &case.spec.expect_error {
        // Flink failed the job: so must Loams Stream, with Flink's message.
        return match run_case(case) {
            Err(e) if e.contains(error.as_str()) => Ok(()),
            Err(e) => Err(format!("failed differently: {e}")),
            Ok(_) => Err(format!("expected the error: {error}")),
        };
    }
    let actual = run_case(case)?;
    let rules = case.rules(&expected.columns).map_err(|e| e.to_string())?;
    match compare(expected, &actual, &rules) {
        None => Ok(()),
        Some(d) => Err(format!("mismatch: {d}")),
    }
}

#[test]
fn nexmark_q0_to_q2_match_the_oracle() {
    for q in ["q0", "q1", "q2"] {
        let case = Case::load(&corpus().join("nexmark").join(q)).unwrap();
        check(&case).unwrap_or_else(|e| panic!("nexmark {q}: {e}"));
    }
}

/// The FC1 operators of the scope manifest (`conformance/profile.toml`).
fn fc1_operators() -> Vec<String> {
    let text = std::fs::read_to_string(corpus().join("../../conformance/profile.toml")).unwrap();
    let mut in_ops = false;
    let mut out = vec![];
    for line in text.lines() {
        if line.starts_with('[') {
            in_ops = line == "[operators]";
            continue;
        }
        if in_ops
            && let Some((k, v)) = line.split_once('=')
            && v.trim() == "\"planned:FC1\""
        {
            out.push(k.trim().trim_matches('"').to_owned());
        }
    }
    out
}

/// The cases with a recorded result that are not oracle xfails, and whether each is in FC1's
/// scope (every operator of its Flink plan is an FC1 operator).
fn recorded_cases(corpus_name: &str) -> Vec<(Case, bool)> {
    let root = corpus().join(corpus_name);
    let xfail = std::fs::read_to_string(root.join("oracle-xfail.toml")).unwrap_or_default();
    let fc1 = fc1_operators();
    let mut out = vec![];
    for case in loams_stream_conformance::case::discover(&root).unwrap() {
        // The oracle's own recording is the reference, also where it differs from what Flink's
        // test program expects (`oracle-differs`); cases the oracle could not run are skipped.
        let listed = xfail
            .split(&format!("[\"{}\"]", case.name))
            .nth(1)
            .map(|rest| rest.split("\n[").next().unwrap_or(""));
        let runnable = listed.is_none_or(|entry| entry.contains("oracle-differs"));
        if case.expected.is_none() || !runnable {
            continue;
        }
        let ops = std::fs::read_to_string(case.dir.join("operators.txt")).unwrap_or_default();
        let in_scope = ops
            .lines()
            .filter(|l| !l.is_empty())
            .all(|op| fc1.iter().any(|f| f == op));
        out.push((case, in_scope));
    }
    out
}

/// Runs the recorded corpus and prints a verdict per case and the counts (FC1's bar: 144 of the
/// 180 in-scope Flink test programs, plus Nexmark except q6). A nightly-style local run.
#[test]
#[ignore = "runs the whole recorded corpus; run locally with --ignored --nocapture"]
fn corpus_report() {
    let mut summary = std::collections::BTreeMap::<String, (usize, usize)>::new();
    let mut reasons = std::collections::BTreeMap::<String, usize>::new();
    for corpus_name in ["nexmark", "flink-programs", "smoke"] {
        for (case, in_scope) in recorded_cases(corpus_name) {
            if corpus_name == "flink-programs" && !in_scope {
                continue;
            }
            let verdict = std::panic::catch_unwind(|| check(&case))
                .unwrap_or_else(|_| Err("panicked".into()));
            let entry = summary.entry(corpus_name.to_owned()).or_default();
            entry.1 += 1;
            match verdict {
                Ok(()) => {
                    entry.0 += 1;
                    println!("MATCH    {corpus_name}/{}", case.name);
                }
                Err(e) => {
                    let first = e.lines().next().unwrap_or("").to_owned();
                    let key: String = first.chars().take(90).collect();
                    *reasons.entry(key).or_default() += 1;
                    println!("FAIL     {corpus_name}/{}: {first}", case.name);
                }
            }
        }
    }
    println!("\n== reasons ==");
    let mut reasons: Vec<_> = reasons.into_iter().collect();
    reasons.sort_by_key(|r| std::cmp::Reverse(r.1));
    for (r, n) in reasons {
        println!("{n:4}  {r}");
    }
    println!("\n== summary ==");
    for (name, (ok, total)) in summary {
        println!("{name}: {ok} of {total} match");
    }
}
