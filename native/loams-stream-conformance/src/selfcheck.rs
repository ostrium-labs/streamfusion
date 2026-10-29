//! The oracle's self-check: does Apache Flink 2.2.1, run through the harness, meet the
//! expectations Flink's own test program declares (`flink-expected.json`, written by the
//! corpus extractor)?
//!
//! The extractor's expectations are Flink's `Row.toString()` texts (`+I[1, Hi, 2023-12-16T01:01]`)
//! of the test sink; the harness reads a `debezium-json` file sink, which collapses `-U`/`+U`
//! into delete and insert events. So both sides are folded into their final table (by the
//! primary key when the sink has one, else as a multiset under retraction) and compared as
//! sorted row texts. The comparison renders values the way Java prints them: timestamps as
//! `LocalDateTime.toString()` (a `T`, zero seconds dropped), strings unquoted.

use std::collections::BTreeMap;
use std::path::Path;

use serde::Deserialize;

use crate::case::Case;
use crate::result::{ResultTable, RowKind};

#[derive(Deserialize)]
struct SinkExpectation {
    materialized: Vec<String>,
    #[serde(default)]
    primary_key: Vec<String>,
}

/// The first difference between the oracle's folded result and Flink's own expectation, or
/// `None` when they agree (or the case has no `flink-expected.json`).
pub fn check(case: &Case, got: &ResultTable) -> Option<String> {
    let path = case.dir.join("flink-expected.json");
    let expected = read(&path)?;
    let sink = case.spec.sink.as_deref()?;
    let want = expected.get(sink)?;
    let key: Vec<usize> = want
        .primary_key
        .iter()
        .filter_map(|k| got.columns.iter().position(|c| &c.name == k))
        .collect();
    let flink: Vec<(char, String)> = want
        .materialized
        .iter()
        .filter_map(|r| parse_flink_row(r))
        .collect();
    let ours: Vec<(char, String)> = got
        .rows
        .iter()
        .map(|r| {
            let sign = match r.kind {
                RowKind::Insert | RowKind::UpdateAfter => '+',
                RowKind::Delete | RowKind::UpdateBefore => '-',
            };
            let fields: Vec<String> = r.fields.iter().map(|f| render(f)).collect();
            (sign, fields.join(", "))
        })
        .collect();
    let a = fold(&flink, &key);
    let b = fold(&ours, &key);
    if a == b {
        return None;
    }
    let missing: Vec<_> = a.iter().filter(|r| !b.contains(r)).take(3).collect();
    let extra: Vec<_> = b.iter().filter(|r| !a.contains(r)).take(3).collect();
    Some(format!(
        "oracle differs from Flink's own expectation ({} rows expected, {} produced); missing {missing:?}; extra {extra:?}",
        a.len(),
        b.len()
    ))
}

fn read(path: &Path) -> Option<BTreeMap<String, SinkExpectation>> {
    let text = std::fs::read_to_string(path).ok()?;
    serde_json::from_str(&text).ok()
}

/// `+I[a, b]` into (`+`, `a, b`).
fn parse_flink_row(s: &str) -> Option<(char, String)> {
    let sign = match s.get(..2)? {
        "+I" | "+U" => '+',
        "-U" | "-D" => '-',
        _ => return None,
    };
    let body = s.get(3..s.len().checked_sub(1)?)?;
    Some((sign, body.to_owned()))
}

/// Splits a row text at its top-level `, ` separators.
fn split_fields(s: &str) -> Vec<&str> {
    let mut out = vec![];
    let (mut depth, mut start) = (0i32, 0usize);
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'[' | b'{' | b'(' => depth += 1,
            b']' | b'}' | b')' => depth -= 1,
            b',' if depth == 0 && bytes.get(i + 1) == Some(&b' ') => {
                out.push(&s[start..i]);
                start = i + 2;
                i += 1;
            }
            _ => {}
        }
        i += 1;
    }
    out.push(&s[start..]);
    out
}

/// Folds a changelog into its final rows: by `key` columns when given (last write wins, a
/// delete removes the key), else as a multiset under retraction.
fn fold(rows: &[(char, String)], key: &[usize]) -> Vec<String> {
    if key.is_empty() {
        let mut counts: BTreeMap<&str, i64> = BTreeMap::new();
        for (sign, row) in rows {
            *counts.entry(row.as_str()).or_default() += if *sign == '+' { 1 } else { -1 };
        }
        let mut out = vec![];
        for (row, n) in counts {
            for _ in 0..n.max(0) {
                out.push(row.to_owned());
            }
        }
        out
    } else {
        let mut table: BTreeMap<Vec<String>, String> = BTreeMap::new();
        for (sign, row) in rows {
            let fields = split_fields(row);
            let k: Vec<String> = key
                .iter()
                .map(|i| fields.get(*i).map(|s| s.to_string()).unwrap_or_default())
                .collect();
            if *sign == '+' {
                table.insert(k, row.clone());
            } else {
                table.remove(&k);
            }
        }
        table.into_values().collect()
    }
}

/// A JSON field as Java prints the value.
fn render(json: &str) -> String {
    match serde_json::from_str::<serde_json::Value>(json) {
        Ok(v) => render_value(&v),
        Err(_) => json.to_owned(),
    }
}

fn render_value(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::Null => "null".into(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::String(s) => java_datetime(s).unwrap_or_else(|| s.clone()),
        serde_json::Value::Array(a) => format!(
            "[{}]",
            a.iter().map(render_value).collect::<Vec<_>>().join(", ")
        ),
        serde_json::Value::Object(o) => format!(
            "{{{}}}",
            o.iter()
                .map(|(k, v)| format!("{k}={}", render_value(v)))
                .collect::<Vec<_>>()
                .join(", ")
        ),
    }
}

/// `2023-12-16 01:01:00` (debezium-json's SQL timestamp format) as `LocalDateTime.toString()`
/// prints it: `2023-12-16T01:01`; a trailing `Z` (an instant) is kept.
fn java_datetime(s: &str) -> Option<String> {
    let b = s.as_bytes();
    let looks = b.len() >= 19
        && b[4] == b'-'
        && b[7] == b'-'
        && (b[10] == b' ' || b[10] == b'T')
        && b[13] == b':'
        && b[16] == b':';
    if !looks {
        return None;
    }
    let (body, zone) = match s.strip_suffix('Z') {
        Some(b) => (b, "Z"),
        None => (s, ""),
    };
    let (date, time) = (&body[..10], &body[11..]);
    let (hms, frac) = match time.split_once('.') {
        Some((h, f)) => (h, f.trim_end_matches('0')),
        None => (time, ""),
    };
    let time = if frac.is_empty() {
        hms.strip_suffix(":00").unwrap_or(hms).to_owned()
    } else {
        // Java prints fractions in groups of three digits.
        let width = frac.len().div_ceil(3) * 3;
        format!("{hms}.{frac:0<width$}")
    };
    Some(format!("{date}T{time}{zone}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_like_java() {
        assert_eq!(render("\"2023-12-16 01:01:00\""), "2023-12-16T01:01");
        assert_eq!(
            render("\"1970-01-01 00:00:00.001Z\""),
            "1970-01-01T00:00:00.001Z"
        );
        assert_eq!(render("\"Hi\""), "Hi");
        assert_eq!(render("2.0"), "2.0");
        assert_eq!(render("null"), "null");
    }

    #[test]
    fn folds_by_key_and_by_retraction() {
        let rows = vec![
            ('+', "2, 1, Hello".to_owned()),
            ('+', "2, 2, Hello".to_owned()),
            ('+', "1, 1, Hi".to_owned()),
        ];
        assert_eq!(fold(&rows, &[0]), vec!["1, 1, Hi", "2, 2, Hello"]);
        let rows = vec![
            ('+', "a".to_owned()),
            ('-', "a".to_owned()),
            ('+', "b".to_owned()),
        ];
        assert_eq!(fold(&rows, &[]), vec!["b"]);
    }
}
