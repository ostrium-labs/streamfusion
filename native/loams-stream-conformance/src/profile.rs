//! The scope manifest (`conformance/profile.toml`) and the compatibility page
//! (`docs/compat/flink.md`), generated (FC1 Task 7; design 82 §15.2, D2272).
//!
//! Every function, physical operator, statement form and connector Loams Stream knows of is
//! marked `supported`, `planned` with its phase, or `wont_do`. Functions are generated from
//! Flink 2.2.1's function list (`conformance/vendor/flink-2.2.1/functions.txt`) and the binder's
//! table; the other sections are the design's phase tables, kept here so a test fails when the
//! corpus exercises an operator the manifest does not list.

use std::collections::BTreeMap;
use std::fmt::Write as _;

/// A feature's status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Status {
    Supported,
    Planned(&'static str),
    WontDo,
}

impl Status {
    fn toml(self) -> String {
        match self {
            Status::Supported => "\"supported\"".into(),
            Status::Planned(p) => format!("\"planned:{p}\""),
            Status::WontDo => "\"wont_do\"".into(),
        }
    }
}

/// Flink's physical plan nodes as `EXPLAIN` prints them, and when Loams Stream runs them.
pub const OPERATORS: &[(&str, Status)] = &[
    ("TableSourceScan", Status::Planned("FC1")),
    ("Values", Status::Planned("FC1")),
    ("Calc", Status::Planned("FC1")),
    ("Exchange", Status::Planned("FC1")),
    ("Sink", Status::Planned("FC1")),
    ("WatermarkAssigner", Status::Planned("FC1")),
    ("MiniBatchAssigner", Status::Planned("FC1")),
    ("WindowTableFunction", Status::Planned("FC1")),
    ("WindowAggregate", Status::Planned("FC1")),
    ("LocalWindowAggregate", Status::Planned("FC1")),
    ("GlobalWindowAggregate", Status::Planned("FC1")),
    ("GroupAggregate", Status::Planned("FC1")),
    ("LocalGroupAggregate", Status::Planned("FC1")),
    ("GlobalGroupAggregate", Status::Planned("FC1")),
    ("IncrementalGroupAggregate", Status::Planned("FC1")),
    ("Join", Status::Planned("FC1")),
    ("IntervalJoin", Status::Planned("FC1")),
    ("WindowJoin", Status::Planned("FC1")),
    ("Rank", Status::Planned("FC1")),
    ("WindowRank", Status::Planned("FC1")),
    ("Deduplicate", Status::Planned("FC1")),
    ("WindowDeduplicate", Status::Planned("FC1")),
    ("ChangelogNormalize", Status::Planned("FC1")),
    ("DropUpdateBefore", Status::Planned("FC1")),
    ("Union", Status::Planned("FC1")),
    ("Expand", Status::Planned("FC1")),
    ("Limit", Status::Planned("FC1")),
    ("SortLimit", Status::Planned("FC1")),
    ("SinkMaterializer", Status::Planned("FC1")),
    ("ConstraintEnforcer", Status::Planned("FC1")),
    ("GroupWindowAggregate", Status::Planned("FC2")),
    ("PythonGroupWindowAggregate", Status::Planned("FC2")),
    ("OverAggregate", Status::Planned("FC2")),
    ("TemporalJoin", Status::Planned("FC2")),
    ("LookupJoin", Status::Planned("FC2")),
    ("Match", Status::Planned("FC2")),
    ("Correlate", Status::Planned("FC2")),
    ("AsyncCalc", Status::Planned("FC2")),
    ("AsyncCorrelate", Status::Planned("FC2")),
    ("PythonCalc", Status::Planned("FC2")),
    ("PythonCorrelate", Status::Planned("FC2")),
    ("TemporalSort", Status::Planned("FC2")),
    ("Sort", Status::Planned("FC2")),
    ("MultiJoin", Status::Planned("FC4")),
    ("ProcessTableFunction", Status::Planned("FC4")),
    ("MLPredictTableFunction", Status::Planned("FC3")),
    ("VectorSearchTableFunction", Status::Planned("FC4")),
];

/// Statement forms (design 82 §4.1).
pub const STATEMENTS: &[(&str, Status)] = &[
    ("CREATE TABLE", Status::Planned("FC1")),
    ("CREATE TEMPORARY TABLE", Status::Planned("FC1")),
    ("CREATE VIEW", Status::Planned("FC1")),
    ("CREATE TABLE AS", Status::Planned("FC2")),
    ("REPLACE TABLE AS", Status::Planned("FC2")),
    ("CREATE CATALOG generic_in_memory", Status::Planned("FC1")),
    ("CREATE CATALOG iceberg", Status::Planned("FC1")),
    ("CREATE CATALOG jdbc", Status::Planned("FC2")),
    ("CREATE CATALOG hive", Status::WontDo),
    ("CREATE DATABASE", Status::Planned("FC2")),
    ("CREATE FUNCTION (Java, Python)", Status::Planned("FC2")),
    ("INSERT INTO", Status::Planned("FC1")),
    ("INSERT OVERWRITE", Status::Planned("FC1")),
    ("EXECUTE STATEMENT SET", Status::Planned("FC1")),
    ("SET / RESET", Status::Planned("FC1")),
    ("USE / USE CATALOG", Status::Planned("FC1")),
    ("SHOW", Status::Planned("FC1")),
    ("DESCRIBE", Status::Planned("FC1")),
    ("EXPLAIN", Status::Planned("FC1")),
    ("COMPILE PLAN / EXECUTE PLAN", Status::Planned("FC2")),
    ("CALL", Status::Planned("FC2")),
    ("DELETE / UPDATE / TRUNCATE", Status::Planned("FC2")),
    ("ADD JAR / REMOVE JAR", Status::Planned("FC2")),
    ("ANALYZE TABLE", Status::Planned("FC4")),
    ("LOAD MODULE / UNLOAD MODULE", Status::WontDo),
    ("Hive dialect", Status::WontDo),
    ("CREATE MATERIALIZED TABLE", Status::Planned("FC3")),
    ("CREATE MODEL", Status::Planned("FC3")),
];

/// Connectors (`connector = '...'`, design 82 §4.5).
pub const CONNECTORS: &[(&str, Status)] = &[
    ("kafka", Status::Planned("FC1")),
    ("upsert-kafka", Status::Planned("FC1")),
    ("iggy", Status::Planned("FC1")),
    ("iceberg", Status::Planned("FC1")),
    ("filesystem", Status::Planned("FC1")),
    ("datagen", Status::Planned("FC1")),
    ("print", Status::Planned("FC1")),
    ("blackhole", Status::Planned("FC1")),
    ("postgres-cdc", Status::Planned("FC2")),
    ("jdbc", Status::Planned("FC2")),
    ("fluss", Status::Planned("FC4")),
    ("hive", Status::WontDo),
];

fn function_status(name: &str) -> Status {
    if loams_stream_sql::binder::is_builtin_function(name) {
        Status::Supported
    } else if matches!(name, "ML_PREDICT" | "ML_EVALUATE") {
        Status::Planned("FC3")
    } else if matches!(name, "VECTOR_SEARCH") {
        Status::Planned("FC4")
    } else {
        Status::Planned("FC2")
    }
}

/// Function names from the vendored list (comment lines skipped).
pub fn read_functions(list: &str) -> Vec<String> {
    list.lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .map(str::to_owned)
        .collect()
}

/// The manifest's TOML text.
pub fn render_profile(functions: &[String]) -> String {
    let mut out = String::from(
        "# Loams Stream's scope manifest (design 82 §15.2). Generated by\n\
         # `loams-stream-oracle profile`; do not edit by hand. Status: \"supported\" (the binder\n\
         # types it; execution follows the operator rows), \"planned:<phase>\", or \"wont_do\".\n\
         flink = \"2.2.1\"\n",
    );
    let section =
        |out: &mut String, name: &str, rows: &mut dyn Iterator<Item = (String, Status)>| {
            let _ = writeln!(out, "\n[{name}]");
            for (k, s) in rows {
                let _ = writeln!(out, "{k:?} = {}", s.toml());
            }
        };
    let funcs: BTreeMap<String, Status> = functions
        .iter()
        .map(|f| (f.clone(), function_status(f)))
        .collect();
    section(&mut out, "functions", &mut funcs.into_iter());
    section(
        &mut out,
        "operators",
        &mut OPERATORS.iter().map(|(k, s)| (k.to_string(), *s)),
    );
    section(
        &mut out,
        "statements",
        &mut STATEMENTS.iter().map(|(k, s)| (k.to_string(), *s)),
    );
    section(
        &mut out,
        "connectors",
        &mut CONNECTORS.iter().map(|(k, s)| (k.to_string(), *s)),
    );
    out
}

fn counts(rows: &[Status]) -> (usize, usize, usize) {
    let s = rows.iter().filter(|r| **r == Status::Supported).count();
    let w = rows.iter().filter(|r| **r == Status::WontDo).count();
    (s, rows.len() - s - w, w)
}

/// The compatibility page: counts per layer, never one percentage of "Flink" (D2272).
pub fn render_page(functions: &[String]) -> String {
    let f: Vec<Status> = functions.iter().map(|n| function_status(n)).collect();
    let o: Vec<Status> = OPERATORS.iter().map(|(_, s)| *s).collect();
    let st: Vec<Status> = STATEMENTS.iter().map(|(_, s)| *s).collect();
    let c: Vec<Status> = CONNECTORS.iter().map(|(_, s)| *s).collect();
    let mut out = String::from(
        "# Loams Stream: compatibility with Apache Flink 2.2 (preview)\n\n\
         Generated from `conformance/profile.toml` by `loams-stream-oracle profile`; do not edit by hand.\n\
         Loams Stream is **preview** software: nothing below has run end to end unless a conformance\n\
         run says so. \"Typed\" functions bind with Flink's types; running them needs the operators.\n\n\
         | Layer | Supported | Planned | Won't do |\n|---|---|---|---|\n",
    );
    for (name, rows) in [
        ("Built-in functions (typed by the binder)", &f),
        ("Physical operators (run natively)", &o),
        ("Statement forms", &st),
        ("Connectors", &c),
    ] {
        let (s, p, w) = counts(rows);
        let _ = writeln!(out, "| {name} | {s} | {p} | {w} |");
    }
    out
}
