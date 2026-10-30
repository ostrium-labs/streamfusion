//! Nexmark's queries through the planner.

use std::path::{Path, PathBuf};

use loams_stream_plan::{Planned, Planner};

fn corpus() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../dev/corpus")
}

fn plan(case: &str) -> Result<String, String> {
    let dir = corpus().join(case);
    let setup = std::fs::read_to_string(dir.join("setup.sql")).unwrap();
    let query = std::fs::read_to_string(dir.join("query.sql")).unwrap();
    let script = format!("{setup}\n{query}")
        .replace("${CASE_DIR}", "/case")
        .replace("${SINK_DIR}", "/sink");
    let mut planner = Planner::new();
    let mut explained = String::new();
    for statement in loams_stream_sql::parse(&script).map_err(|e| e.to_string())? {
        if let Planned::Job(plan) = planner.plan(&statement).map_err(|e| e.to_string())? {
            explained.push_str(&loams_stream_plan::explain::explain(&plan, true));
        }
    }
    Ok(explained)
}

#[test]
fn nexmark_q0_to_q2_plan() {
    for q in ["q0", "q1", "q2"] {
        let text = plan(&format!("nexmark/{q}")).unwrap_or_else(|e| panic!("{q}: {e}"));
        println!("{q}:\n{text}");
        assert!(text.contains("Sink("), "{q}: {text}");
    }
}

/// The FC1 operators of the scope manifest.
fn fc1_operators() -> Vec<String> {
    let text = std::fs::read_to_string(corpus().join("../../conformance/profile.toml")).unwrap();
    let mut in_ops = false;
    let mut out = vec![];
    for line in text.lines() {
        if line.starts_with('[') {
            in_ops = line == "[operators]";
        } else if in_ops
            && let Some((k, v)) = line.split_once('=')
            && v.trim() == "\"planned:FC1\""
        {
            out.push(k.trim().trim_matches('"').to_owned());
        }
    }
    out
}

/// Plans a case's setup and query; `Err` carries the error and whether it names a feature.
fn plan_case(dir: &Path) -> Result<(), (String, bool)> {
    let setup = std::fs::read_to_string(dir.join("setup.sql")).unwrap_or_default();
    let query = std::fs::read_to_string(dir.join("query.sql")).unwrap();
    let script = format!("{setup}\n{query}")
        .replace("${CASE_DIR}", "/case")
        .replace("${SINK_DIR}", "/sink");
    let mut planner = Planner::new();
    let statements = loams_stream_sql::parse(&script).map_err(|e| (e.to_string(), false))?;
    for statement in &statements {
        planner
            .plan(statement)
            .map_err(|e| (e.to_string(), e.feature().is_some()))?;
    }
    Ok(())
}

/// Every FC1 case of the corpus plans or names the feature it needs (Review Focus 3); prints the
/// counts. The gate "all FC1 cases plan" is reached when the named count is zero.
#[test]
fn plans_every_fc1_case() {
    let fc1 = fc1_operators();
    let (mut planned, mut named, mut unnamed) = (0, 0, vec![]);
    for corpus_name in ["nexmark", "flink-programs", "smoke"] {
        let mut dirs: Vec<PathBuf> = std::fs::read_dir(corpus().join(corpus_name))
            .unwrap()
            .map(|e| e.unwrap().path())
            .filter(|p| p.join("query.sql").exists())
            .collect();
        dirs.sort();
        for dir in dirs {
            let ops = std::fs::read_to_string(dir.join("operators.txt")).unwrap_or_default();
            let in_scope = ops
                .lines()
                .filter(|l| !l.is_empty())
                .all(|op| fc1.iter().any(|f| f == op));
            if !in_scope || !dir.join("expected.jsonl").exists() {
                continue;
            }
            match plan_case(&dir) {
                Ok(()) => planned += 1,
                Err((_, true)) => named += 1,
                Err((e, false)) => unnamed.push(format!("{}: {e}", dir.display())),
            }
        }
    }
    println!(
        "planned {planned}, named feature {named}, other {}",
        unnamed.len()
    );
    for u in &unnamed {
        println!("  {u}");
    }
    assert!(
        unnamed.is_empty(),
        "cases that neither plan nor name a feature:\n{}",
        unnamed.join("\n")
    );
}
