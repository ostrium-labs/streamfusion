//! The case format.
//!
//! A case is a directory:
//!
//! ```text
//! dev/corpus/smoke/tumble_count/
//!   case.toml        name, mode, the normaliser's rules, timeout, collection
//!   setup.sql        DDL and source data, statements separated by `;`
//!   query.sql        one statement: a SELECT (collect = "query") or an INSERT (collect = "sink")
//!   expected.jsonl   the oracle's result (written by `loams-stream-oracle record`)
//!   data/            optional files the case ships, for `filesystem` sources
//! ```
//!
//! `case.toml`:
//!
//! ```toml
//! description = "Tumbling event-time window, COUNT per window"
//! mode = "streaming"              # streaming | batch
//! order_sensitive = false         # rows may arrive in any order
//! changelog_significant = true    # every row and its RowKind must match
//! # upsert_key = ["k"]            # fold by key (changelog_significant = false only)
//! timeout_secs = 120
//! collect = "query"               # query: the gateway's result stream
//!                                 # sink: the debezium-json files of table `sink`
//! # sink = "snk"
//!
//! [config]                        # session configuration for this case
//! "table.exec.mini-batch.enabled" = "false"
//! ```
//!
//! Sources are deterministic by construction (design 82 §15): `datagen`
//! with `number-of-rows` and every physical field `kind = 'sequence'`
//! (Flink 2.2.1's datagen has no seed option, so `random` is refused),
//! `filesystem` on files under `${CASE_DIR}`, or SQL `VALUES`. Sinks are
//! `filesystem` under `${SINK_DIR}`. The runner replaces the two
//! placeholders with paths the target can read and write.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::Deserialize;

use crate::error::{Error, Result};
use crate::normalise::Rules;
use crate::result::{Column, ResultTable};

/// `streaming` or `batch` (`execution.runtime-mode`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Mode {
    /// Streaming mode: changelogs.
    Streaming,
    /// Batch mode: final results.
    Batch,
}

impl Mode {
    /// The value of `execution.runtime-mode`.
    pub fn runtime_mode(self) -> &'static str {
        match self {
            Mode::Streaming => "streaming",
            Mode::Batch => "batch",
        }
    }
}

/// Where a case's result is read from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Collect {
    /// The gateway's result stream of the query (bounded queries).
    #[default]
    Query,
    /// The `debezium-json` files a `filesystem` sink wrote.
    Sink,
}

/// `case.toml`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CaseSpec {
    /// The case's name; defaults to the directory's.
    #[serde(default)]
    pub name: Option<String>,
    /// What the case checks.
    #[serde(default)]
    pub description: String,
    /// The runtime mode.
    pub mode: Mode,
    /// Whether rows must arrive in the same order.
    pub order_sensitive: bool,
    /// Whether every changelog row and its kind must match.
    pub changelog_significant: bool,
    /// Column names of the upsert key (only with `changelog_significant = false`).
    #[serde(default)]
    pub upsert_key: Option<Vec<String>>,
    /// Seconds the query may run.
    #[serde(default = "default_timeout")]
    pub timeout_secs: u64,
    /// Where the result is read from.
    #[serde(default)]
    pub collect: Collect,
    /// The sink table, for `collect = "sink"`.
    #[serde(default)]
    pub sink: Option<String>,
    /// Session configuration.
    #[serde(default)]
    pub config: BTreeMap<String, String>,
}

fn default_timeout() -> u64 {
    120
}

/// A loaded case.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Case {
    /// The case's name.
    pub name: String,
    /// Its directory.
    pub dir: PathBuf,
    /// `case.toml`.
    pub spec: CaseSpec,
    /// `setup.sql`, split into statements.
    pub setup: Vec<String>,
    /// `query.sql`.
    pub query: String,
    /// `expected.jsonl`, if recorded.
    pub expected: Option<ResultTable>,
}

impl Case {
    /// Loads and checks the case in `dir`.
    ///
    /// # Errors
    ///
    /// [`Error::Io`] for a missing file, [`Error::Invalid`] for a case that
    /// breaks the format or the source rules.
    pub fn load(dir: &Path) -> Result<Self> {
        let read = |f: &str| {
            let p = dir.join(f);
            std::fs::read_to_string(&p).map_err(|e| Error::io(p.display(), e))
        };
        let spec: CaseSpec = toml::from_str(&read("case.toml")?)
            .map_err(|e| Error::Invalid(format!("{}/case.toml: {e}", dir.display())))?;
        let name = spec.name.clone().unwrap_or_else(|| {
            dir.file_name()
                .map(|n| n.to_string_lossy().into_owned())
                .unwrap_or_default()
        });
        let bad = |m: String| Error::Invalid(format!("case {name}: {m}"));
        let setup_path = dir.join("setup.sql");
        let setup = if setup_path.exists() {
            split_statements(&read("setup.sql")?)
        } else {
            Vec::new()
        };
        let mut query = split_statements(&read("query.sql")?);
        if query.len() != 1 {
            return Err(bad(format!(
                "query.sql must hold exactly one statement, found {}",
                query.len()
            )));
        }
        let query = query.remove(0);
        match (spec.collect, &spec.sink) {
            (Collect::Sink, None) => return Err(bad("collect = \"sink\" needs sink".into())),
            (Collect::Query, Some(_)) => {
                return Err(bad("sink is only for collect = \"sink\"".into()));
            }
            _ => {}
        }
        if spec.order_sensitive && !spec.changelog_significant {
            return Err(bad(
                "order_sensitive = true needs changelog_significant = true".into(),
            ));
        }
        if spec.upsert_key.is_some() && spec.changelog_significant {
            return Err(bad(
                "upsert_key only applies when changelog_significant = false".into(),
            ));
        }
        for stmt in &setup {
            lint_source(stmt).map_err(|m| bad(format!("setup.sql: {m}")))?;
        }
        lint_source(&query).map_err(|m| bad(format!("query.sql: {m}")))?;
        let exp_path = dir.join("expected.jsonl");
        let expected = if exp_path.exists() {
            Some(ResultTable::read(&exp_path)?)
        } else {
            None
        };
        Ok(Self {
            name,
            dir: dir.to_owned(),
            spec,
            setup,
            query,
            expected,
        })
    }

    /// The normaliser's rules for this case, resolved against `columns`.
    ///
    /// # Errors
    ///
    /// [`Error::Invalid`] for an upsert key column that is not there, or
    /// contradictory rules.
    pub fn rules(&self, columns: &[Column]) -> Result<Rules> {
        let upsert_key = match &self.spec.upsert_key {
            None => None,
            Some(names) => Some(
                names
                    .iter()
                    .map(|n| {
                        columns.iter().position(|c| &c.name == n).ok_or_else(|| {
                            Error::Invalid(format!(
                                "case {}: upsert_key column {n:?} is not in the result",
                                self.name
                            ))
                        })
                    })
                    .collect::<Result<Vec<_>>>()?,
            ),
        };
        let rules = Rules {
            order_sensitive: self.spec.order_sensitive,
            changelog_significant: self.spec.changelog_significant,
            upsert_key,
        };
        rules.validate(columns.len())?;
        Ok(rules)
    }
}

/// Every case under `root`: `root` itself when it holds a `case.toml`,
/// else each subdirectory that does (recursively), sorted by path.
///
/// # Errors
///
/// As [`Case::load`], or [`Error::Io`] for an unreadable directory.
pub fn discover(root: &Path) -> Result<Vec<Case>> {
    let mut dirs = Vec::new();
    collect_dirs(root, &mut dirs)?;
    dirs.sort();
    dirs.iter().map(|d| Case::load(d)).collect()
}

fn collect_dirs(dir: &Path, out: &mut Vec<PathBuf>) -> Result<()> {
    if dir.join("case.toml").is_file() {
        out.push(dir.to_owned());
        return Ok(());
    }
    let entries = std::fs::read_dir(dir).map_err(|e| Error::io(dir.display(), e))?;
    for entry in entries {
        let entry = entry.map_err(|e| Error::io(dir.display(), e))?;
        if entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
            collect_dirs(&entry.path(), out)?;
        }
    }
    Ok(())
}

/// Splits a script on `;` outside quotes, backticks and comments, and
/// drops statements that hold only whitespace and comments.
pub fn split_statements(sql: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut has_code = false;
    let mut chars = sql.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '\'' | '"' | '`' => {
                has_code = true;
                cur.push(c);
                while let Some(d) = chars.next() {
                    cur.push(d);
                    if d == c {
                        // A doubled quote is an escaped quote.
                        if chars.peek() == Some(&c) {
                            cur.push(chars.next().expect("peeked"));
                        } else {
                            break;
                        }
                    }
                }
            }
            '-' if chars.peek() == Some(&'-') => {
                for d in chars.by_ref() {
                    if d == '\n' {
                        cur.push('\n');
                        break;
                    }
                }
            }
            '/' if chars.peek() == Some(&'*') => {
                chars.next();
                let mut prev = '\0';
                for d in chars.by_ref() {
                    if prev == '*' && d == '/' {
                        break;
                    }
                    prev = d;
                }
                cur.push(' ');
            }
            ';' => {
                if has_code {
                    out.push(cur.trim().to_owned());
                }
                cur.clear();
                has_code = false;
            }
            _ => {
                if !c.is_whitespace() {
                    has_code = true;
                }
                cur.push(c);
            }
        }
    }
    if has_code {
        out.push(cur.trim().to_owned());
    }
    out
}

/// Replaces `${CASE_DIR}` and `${SINK_DIR}`.
///
/// # Errors
///
/// [`Error::Invalid`] for any other `${…}`.
pub fn substitute(stmt: &str, case_dir: &str, sink_dir: &str) -> Result<String> {
    let out = stmt
        .replace("${CASE_DIR}", case_dir)
        .replace("${SINK_DIR}", sink_dir);
    if let Some(i) = out.find("${") {
        let end = out[i..].find('}').map_or(out.len(), |j| i + j + 1);
        return Err(Error::Invalid(format!(
            "unknown placeholder {}",
            &out[i..end]
        )));
    }
    Ok(out)
}

/// The `WITH (...)` options of a `CREATE TABLE`, keys lower-cased.
fn with_options(stmt: &str) -> BTreeMap<String, String> {
    let mut opts = BTreeMap::new();
    let lower = stmt.to_ascii_lowercase();
    let Some(w) = find_with(&lower) else {
        return opts;
    };
    let body = &stmt[w..];
    let mut strings = Vec::new();
    let mut chars = body.char_indices().peekable();
    while let Some((_, c)) = chars.next() {
        if c == '\'' {
            let mut s = String::new();
            while let Some((_, d)) = chars.next() {
                if d == '\'' {
                    if matches!(chars.peek(), Some((_, '\''))) {
                        chars.next();
                        s.push('\'');
                    } else {
                        break;
                    }
                } else {
                    s.push(d);
                }
            }
            strings.push(s);
        }
    }
    for pair in strings.chunks(2) {
        if let [k, v] = pair {
            opts.insert(k.to_ascii_lowercase(), v.clone());
        }
    }
    opts
}

/// The byte offset just after the `WITH (` that opens a table's options.
fn find_with(lower: &str) -> Option<usize> {
    let mut from = 0;
    while let Some(i) = lower[from..].find("with") {
        let at = from + i;
        let rest = lower[at + 4..].trim_start();
        let before_ok = at == 0 || !lower.as_bytes()[at - 1].is_ascii_alphanumeric();
        if before_ok && rest.starts_with('(') {
            return Some(lower.len() - rest.len() + 1);
        }
        from = at + 4;
    }
    None
}

/// The physical column names of a `CREATE TABLE` (computed columns,
/// watermarks and constraints skipped).
fn physical_columns(stmt: &str) -> Vec<String> {
    let Some(open) = stmt.find('(') else {
        return Vec::new();
    };
    let mut depth = 0usize;
    let mut items = Vec::new();
    let mut cur = String::new();
    let mut in_quote = None;
    for c in stmt[open + 1..].chars() {
        if let Some(q) = in_quote {
            cur.push(c);
            if c == q {
                in_quote = None;
            }
            continue;
        }
        match c {
            '\'' | '`' | '"' => {
                in_quote = Some(c);
                cur.push(c);
            }
            '(' | '<' => {
                depth += 1;
                cur.push(c);
            }
            ')' | '>' if depth > 0 => {
                depth -= 1;
                cur.push(c);
            }
            ')' => {
                items.push(std::mem::take(&mut cur));
                break;
            }
            ',' if depth == 0 => items.push(std::mem::take(&mut cur)),
            _ => cur.push(c),
        }
    }
    items
        .iter()
        .filter_map(|item| {
            let mut words = item.split_whitespace();
            let first = words.next()?;
            let second = words.next().unwrap_or_default().to_ascii_uppercase();
            let head = first.to_ascii_uppercase();
            if matches!(
                head.as_str(),
                "WATERMARK" | "PRIMARY" | "CONSTRAINT" | "PERIOD"
            ) || second == "AS"
            {
                return None;
            }
            Some(first.trim_matches('`').to_owned())
        })
        .collect()
}

/// Refuses sources that are not deterministic and paths outside the case.
fn lint_source(stmt: &str) -> std::result::Result<(), String> {
    let head: Vec<String> = stmt
        .split_whitespace()
        .take(4)
        .map(str::to_ascii_uppercase)
        .collect();
    let is_create_table = head.first().is_some_and(|w| w == "CREATE")
        && head.iter().skip(1).take(2).any(|w| w == "TABLE")
        && !head.iter().any(|w| w == "VIEW");
    if !is_create_table {
        return Ok(());
    }
    let opts = with_options(stmt);
    let connector = opts
        .get("connector")
        .ok_or_else(|| "CREATE TABLE without 'connector'".to_owned())?;
    match connector.as_str() {
        "datagen" => {
            if !opts.contains_key("number-of-rows") {
                return Err("datagen needs 'number-of-rows' (bounded input)".into());
            }
            for col in physical_columns(stmt) {
                let key = format!("fields.{}.kind", col.to_ascii_lowercase());
                match opts.get(&key).map(String::as_str) {
                    Some("sequence") => {}
                    _ => {
                        return Err(format!(
                            "datagen column {col} needs '{key}' = 'sequence' \
                             (Flink 2.2.1's datagen has no seed; random fields are not deterministic)"
                        ));
                    }
                }
            }
            Ok(())
        }
        "filesystem" => {
            let path = opts
                .get("path")
                .ok_or_else(|| "filesystem table without 'path'".to_owned())?;
            if path.starts_with("${CASE_DIR}") || path.starts_with("${SINK_DIR}") {
                Ok(())
            } else {
                Err(format!(
                    "filesystem path {path:?} must start with ${{CASE_DIR}} or ${{SINK_DIR}}"
                ))
            }
        }
        other => Err(format!(
            "connector {other:?} is not allowed (datagen, filesystem, or SQL VALUES)"
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn statements_split_outside_quotes_and_comments() {
        let s = "-- a comment; still a comment\nCREATE TABLE t (a INT) WITH ('x'='a;b');\n/* ; */ SELECT ';' ; ;\n-- trailing";
        let v = split_statements(s);
        assert_eq!(v.len(), 2, "{v:?}");
        assert!(v[0].starts_with("CREATE TABLE"));
        assert!(v[0].ends_with("('x'='a;b')"));
        assert_eq!(v[1], "SELECT ';'");
    }

    #[test]
    fn placeholders_are_replaced_or_refused() {
        assert_eq!(substitute("'${SINK_DIR}/x'", "/c", "/s").unwrap(), "'/s/x'");
        assert!(substitute("${HOME}", "/c", "/s").is_err());
    }

    #[test]
    fn datagen_must_be_sequence_and_bounded() {
        let ok = "CREATE TABLE s (id BIGINT, ts AS TO_TIMESTAMP_LTZ(id, 3), WATERMARK FOR ts AS ts) \
                  WITH ('connector'='datagen','number-of-rows'='5','fields.id.kind'='sequence','fields.id.start'='1','fields.id.end'='5')";
        assert_eq!(lint_source(ok), Ok(()));
        let random = "CREATE TABLE s (id BIGINT, v INT) WITH ('connector'='datagen','number-of-rows'='5','fields.id.kind'='sequence')";
        assert!(lint_source(random).unwrap_err().contains("column v"));
        let unbounded =
            "CREATE TABLE s (id BIGINT) WITH ('connector'='datagen','fields.id.kind'='sequence')";
        assert!(lint_source(unbounded).is_err());
        let kafka = "CREATE TABLE s (id BIGINT) WITH ('connector'='kafka')";
        assert!(lint_source(kafka).is_err());
        let fs = "CREATE TABLE s (id BIGINT) WITH ('connector'='filesystem','path'='/etc','format'='csv')";
        assert!(lint_source(fs).is_err());
        let fs = "CREATE TABLE s (id BIGINT) WITH ('connector'='filesystem','path'='${CASE_DIR}/a.csv','format'='csv')";
        assert_eq!(lint_source(fs), Ok(()));
        assert_eq!(
            lint_source("CREATE VIEW v AS SELECT * FROM (VALUES (1)) AS t(a)"),
            Ok(())
        );
    }
}
