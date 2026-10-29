//! A collected result: columns with their types, and changelog rows whose
//! fields keep the JSON text the target wrote.
//!
//! The on-disk form is `expected.jsonl`: a header line, then one line per
//! row, each field written verbatim.
//!
//! ```text
//! {"columns":[{"name":"k","type":"BIGINT NOT NULL"},{"name":"c","type":"BIGINT"}]}
//! {"kind":"+I","fields":[1,2]}
//! {"kind":"-U","fields":[1,2]}
//! {"kind":"+U","fields":[1,3]}
//! ```

use std::fmt;
use std::path::Path;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use serde_json::value::RawValue;

use crate::error::{Error, Result};

/// The change a row carries (Flink's `RowKind`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RowKind {
    /// `+I`, a new row.
    Insert,
    /// `-U`, the old row of an update.
    UpdateBefore,
    /// `+U`, the new row of an update.
    UpdateAfter,
    /// `-D`, a retraction.
    Delete,
}

impl RowKind {
    /// Every kind, in Flink's order.
    pub const ALL: [RowKind; 4] = [
        RowKind::Insert,
        RowKind::UpdateBefore,
        RowKind::UpdateAfter,
        RowKind::Delete,
    ];

    /// Flink's short string: `+I`, `-U`, `+U`, `-D`.
    pub fn short(self) -> &'static str {
        match self {
            RowKind::Insert => "+I",
            RowKind::UpdateBefore => "-U",
            RowKind::UpdateAfter => "+U",
            RowKind::Delete => "-D",
        }
    }

    /// Whether the row adds to the table (`+I`, `+U`).
    pub fn is_addition(self) -> bool {
        matches!(self, RowKind::Insert | RowKind::UpdateAfter)
    }

    /// Parses the short string or the gateway's long name (`UPDATE_AFTER`).
    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "+I" | "INSERT" => RowKind::Insert,
            "-U" | "UPDATE_BEFORE" => RowKind::UpdateBefore,
            "+U" | "UPDATE_AFTER" => RowKind::UpdateAfter,
            "-D" | "DELETE" => RowKind::Delete,
            _ => return None,
        })
    }
}

impl fmt::Display for RowKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.short())
    }
}

impl Serialize for RowKind {
    fn serialize<S: serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        s.serialize_str(self.short())
    }
}

impl<'de> Deserialize<'de> for RowKind {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        RowKind::parse(&s)
            .ok_or_else(|| serde::de::Error::custom(format!("unknown row kind {s:?}")))
    }
}

/// A result column: its name and its type, as a string.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Column {
    /// The column name.
    pub name: String,
    /// The type, e.g. `BIGINT NOT NULL` or `DECIMAL(10, 2)`.
    #[serde(rename = "type")]
    pub ty: String,
}

impl Column {
    /// A column.
    pub fn new(name: impl Into<String>, ty: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            ty: ty.into(),
        }
    }
}

/// One changelog row. Each field is the JSON text the target wrote for it,
/// with insignificant whitespace removed and nothing else changed.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Row {
    /// The change.
    pub kind: RowKind,
    /// The fields, in column order.
    pub fields: Vec<String>,
}

impl Row {
    /// A row from field texts (each is minified, see [`minify_json`]).
    pub fn new(kind: RowKind, fields: impl IntoIterator<Item = impl AsRef<str>>) -> Self {
        Self {
            kind,
            fields: fields
                .into_iter()
                .map(|f| minify_json(f.as_ref()))
                .collect(),
        }
    }

    /// The row as one `expected.jsonl` line.
    pub fn to_line(&self) -> String {
        format!(
            "{{\"kind\":\"{}\",\"fields\":[{}]}}",
            self.kind.short(),
            self.fields.join(",")
        )
    }
}

impl fmt::Display for Row {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}[{}]", self.kind.short(), self.fields.join(", "))
    }
}

/// A collected result.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ResultTable {
    /// The columns.
    pub columns: Vec<Column>,
    /// The changelog, in arrival order.
    pub rows: Vec<Row>,
}

#[derive(Serialize, Deserialize)]
struct Header {
    columns: Vec<Column>,
}

#[derive(Deserialize)]
struct RawRow<'a> {
    kind: RowKind,
    #[serde(borrow)]
    fields: Vec<&'a RawValue>,
}

impl ResultTable {
    /// The `expected.jsonl` text.
    pub fn to_jsonl(&self) -> String {
        let mut out = serde_json::to_string(&Header {
            columns: self.columns.clone(),
        })
        .expect("a header serializes");
        out.push('\n');
        for row in &self.rows {
            out.push_str(&row.to_line());
            out.push('\n');
        }
        out
    }

    /// Parses `expected.jsonl` text.
    ///
    /// # Errors
    ///
    /// [`Error::Invalid`] naming the bad line.
    pub fn from_jsonl(text: &str) -> Result<Self> {
        let mut lines = text
            .lines()
            .enumerate()
            .filter(|(_, l)| !l.trim().is_empty());
        let (_, header) = lines
            .next()
            .ok_or_else(|| Error::Invalid("expected.jsonl is empty (no header)".into()))?;
        let header: Header = serde_json::from_str(header)
            .map_err(|e| Error::Invalid(format!("expected.jsonl line 1: {e}")))?;
        let mut rows = Vec::new();
        for (n, line) in lines {
            let raw: RawRow<'_> = serde_json::from_str(line)
                .map_err(|e| Error::Invalid(format!("expected.jsonl line {}: {e}", n + 1)))?;
            if raw.fields.len() != header.columns.len() {
                return Err(Error::Invalid(format!(
                    "expected.jsonl line {}: {} fields for {} columns",
                    n + 1,
                    raw.fields.len(),
                    header.columns.len()
                )));
            }
            rows.push(Row::new(raw.kind, raw.fields.iter().map(|f| f.get())));
        }
        Ok(Self {
            columns: header.columns,
            rows,
        })
    }

    /// Reads `expected.jsonl`.
    ///
    /// # Errors
    ///
    /// [`Error::Io`] or [`Error::Invalid`].
    pub fn read(path: &Path) -> Result<Self> {
        let text = std::fs::read_to_string(path).map_err(|e| Error::io(path.display(), e))?;
        Self::from_jsonl(&text).map_err(|e| Error::Invalid(format!("{}: {e}", path.display())))
    }

    /// Writes `expected.jsonl`.
    ///
    /// # Errors
    ///
    /// [`Error::Io`].
    pub fn write(&self, path: &Path) -> Result<()> {
        std::fs::write(path, self.to_jsonl()).map_err(|e| Error::io(path.display(), e))
    }
}

/// Removes whitespace outside JSON strings and leaves every other byte
/// alone: `1.0E20` stays `1.0E20`, `{"b":1,"a":2}` keeps its key order.
pub fn minify_json(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut in_string = false;
    let mut escaped = false;
    for c in text.chars() {
        if in_string {
            out.push(c);
            if escaped {
                escaped = false;
            } else if c == '\\' {
                escaped = true;
            } else if c == '"' {
                in_string = false;
            }
        } else if c == '"' {
            in_string = true;
            out.push(c);
        } else if !c.is_whitespace() {
            out.push(c);
        }
    }
    out
}

/// Renders the SQL Gateway's serialized `LogicalType`
/// (`{"type":"DECIMAL","nullable":false,"precision":3,"scale":2}`) as a
/// type string (`DECIMAL(3, 2) NOT NULL`). Anything this does not know is
/// kept as its compact JSON, so two different types never render alike.
pub fn render_logical_type(t: &Value) -> String {
    let Some(obj) = t.as_object() else {
        return t.to_string();
    };
    let Some(root) = obj.get("type").and_then(Value::as_str) else {
        return t.to_string();
    };
    let int = |k: &str| obj.get(k).and_then(Value::as_u64);
    let base = match root {
        "CHAR" | "VARCHAR" | "BINARY" | "VARBINARY" => match int("length") {
            Some(n) => format!("{root}({n})"),
            None => root.to_owned(),
        },
        "DECIMAL" => match (int("precision"), int("scale")) {
            (Some(p), Some(s)) => format!("DECIMAL({p}, {s})"),
            _ => return t.to_string(),
        },
        "TIME_WITHOUT_TIME_ZONE" => format!("TIME({})", int("precision").unwrap_or(0)),
        "TIMESTAMP_WITHOUT_TIME_ZONE" => format!("TIMESTAMP({})", int("precision").unwrap_or(6)),
        "TIMESTAMP_WITH_TIME_ZONE" => format!(
            "TIMESTAMP({}) WITH TIME ZONE",
            int("precision").unwrap_or(6)
        ),
        "TIMESTAMP_WITH_LOCAL_TIME_ZONE" => {
            format!("TIMESTAMP_LTZ({})", int("precision").unwrap_or(6))
        }
        "ARRAY" | "MULTISET" => match obj.get("elementType") {
            Some(e) => format!("{root}<{}>", render_logical_type(e)),
            None => return t.to_string(),
        },
        "MAP" => match (obj.get("keyType"), obj.get("valueType")) {
            (Some(k), Some(v)) => format!(
                "MAP<{}, {}>",
                render_logical_type(k),
                render_logical_type(v)
            ),
            _ => return t.to_string(),
        },
        "ROW" => {
            let Some(fields) = obj.get("fields").and_then(Value::as_array) else {
                return t.to_string();
            };
            let mut parts = Vec::with_capacity(fields.len());
            for f in fields {
                let (Some(name), Some(ft)) =
                    (f.get("name").and_then(Value::as_str), f.get("fieldType"))
                else {
                    return t.to_string();
                };
                parts.push(format!("`{name}` {}", render_logical_type(ft)));
            }
            format!("ROW<{}>", parts.join(", "))
        }
        "BOOLEAN" | "TINYINT" | "SMALLINT" | "INTEGER" | "BIGINT" | "FLOAT" | "DOUBLE" | "DATE"
        | "NULL" => root.to_owned(),
        _ => return t.to_string(),
    };
    // The keys this renderer reads; any other key would be lost.
    let known = [
        "type",
        "nullable",
        "length",
        "precision",
        "scale",
        "elementType",
        "keyType",
        "valueType",
        "fields",
    ];
    if obj.keys().any(|k| !known.contains(&k.as_str())) {
        return t.to_string();
    }
    match obj.get("nullable").and_then(Value::as_bool) {
        Some(false) => format!("{base} NOT NULL"),
        _ => base,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn minify_keeps_strings_and_number_text() {
        assert_eq!(
            minify_json(" { \"a b\" : 1.0E20 , \"c\":[ 1 , 2 ] } "),
            "{\"a b\":1.0E20,\"c\":[1,2]}"
        );
        assert_eq!(minify_json("\"x \\\" y\""), "\"x \\\" y\"");
    }

    #[test]
    fn jsonl_round_trips_field_text() {
        let t = ResultTable {
            columns: vec![Column::new("a", "DOUBLE"), Column::new("b", "STRING")],
            rows: vec![
                Row::new(RowKind::Insert, ["1.0E20", "\"x\""]),
                Row::new(RowKind::UpdateBefore, ["0.10", "null"]),
            ],
        };
        let back = ResultTable::from_jsonl(&t.to_jsonl()).unwrap();
        assert_eq!(back, t);
        assert_eq!(back.rows[0].fields[0], "1.0E20");
        assert_eq!(back.rows[1].fields[0], "0.10");
    }

    #[test]
    fn types_render_like_flink() {
        assert_eq!(
            render_logical_type(&json!({"type":"INTEGER","nullable":false})),
            "INTEGER NOT NULL"
        );
        assert_eq!(
            render_logical_type(&json!({"type":"DECIMAL","nullable":true,"precision":3,"scale":2})),
            "DECIMAL(3, 2)"
        );
        assert_eq!(
            render_logical_type(
                &json!({"type":"ROW","nullable":false,"fields":[{"name":"EXPR$0","fieldType":{"type":"INTEGER","nullable":false}}]})
            ),
            "ROW<`EXPR$0` INTEGER NOT NULL> NOT NULL"
        );
        // Unknown keys keep the JSON, so nothing is lost.
        let odd = json!({"type":"INTEGER","nullable":true,"extra":1});
        assert_eq!(render_logical_type(&odd), odd.to_string());
    }
}
