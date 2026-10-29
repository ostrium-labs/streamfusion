//! The normaliser and the comparison (design 82 §15; FC1 Review Focus 1).
//!
//! The rules, all of them:
//!
//! 1. **Columns** are compared first and exactly: count, names, and type
//!    strings (nullability included). No rule touches them.
//! 2. **Field text** is compared exactly. A field is the JSON text the
//!    target wrote, minus whitespace outside strings ([`crate::result::minify_json`]).
//!    `1.0`, `1.00`, `1` and `1E0` are four different values; so are two
//!    maps with their keys in a different order.
//! 3. **Order** is kept unless the case says `order_sensitive = false`;
//!    then rows are sorted by `(fields, kind)` on both sides.
//! 4. **The changelog** is kept, row by row with its `RowKind`, unless the
//!    case says `changelog_significant = false`; then it is folded into the
//!    final table it describes:
//!    - without an upsert key, as a multiset: `+I` and `+U` add one copy,
//!      `-U` and `-D` remove one. A retraction of a row that is not there
//!      stays in the output as a `-D` row (one per missing copy), so it can
//!      never cancel against something else;
//!    - with `upsert_key`, as an upsert table: `+I`/`+U` replace the row with
//!      that key, `-U`/`-D` remove it; a removal of an absent key stays in the
//!      output as a `-D` row.
//!
//!    The folded table is sorted (a table has no order), so
//!    `order_sensitive = true` with `changelog_significant = false` is
//!    refused when the case is loaded.
//! 5. **Row counts** are never normalised: a row more or less on either side
//!    is a mismatch under every rule above (rule 4 compares counts per row).
//!
//! The property tests in `tests/normaliser.rs`
//! (`normaliser_never_hides_rows_types_or_rowkinds`) hold these rules.

use std::collections::BTreeMap;
use std::fmt;

use serde::Serialize;

use crate::error::{Error, Result};
use crate::result::{Column, ResultTable, Row, RowKind};

/// How a case's results may be normalised.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Rules {
    /// Rows must arrive in the same order.
    pub order_sensitive: bool,
    /// The changelog (every row with its kind) must match, not only the
    /// table it folds into.
    pub changelog_significant: bool,
    /// Column positions of the upsert key, for folding (rule 4).
    pub upsert_key: Option<Vec<usize>>,
}

impl Rules {
    /// Checks the rules against each other and against `columns`.
    ///
    /// # Errors
    ///
    /// [`Error::Invalid`] for a folded changelog that is also
    /// order-sensitive, or a key position outside the columns.
    pub fn validate(&self, columns: usize) -> Result<()> {
        if self.order_sensitive && !self.changelog_significant {
            return Err(Error::Invalid(
                "order_sensitive = true needs changelog_significant = true: \
                 a folded changelog is a table, which has no order"
                    .into(),
            ));
        }
        if let Some(key) = &self.upsert_key {
            if key.is_empty() {
                return Err(Error::Invalid("upsert_key is empty".into()));
            }
            if let Some(bad) = key.iter().find(|&&i| i >= columns) {
                return Err(Error::Invalid(format!(
                    "upsert_key position {bad} is outside the {columns} columns"
                )));
            }
            if self.changelog_significant {
                return Err(Error::Invalid(
                    "upsert_key only applies when changelog_significant = false".into(),
                ));
            }
        }
        Ok(())
    }
}

/// Applies `rules` to `table`'s rows (columns are never changed).
pub fn normalise(table: &ResultTable, rules: &Rules) -> ResultTable {
    let rows = if rules.changelog_significant {
        let mut rows = table.rows.clone();
        if !rules.order_sensitive {
            rows.sort_by(|a, b| a.fields.cmp(&b.fields).then(a.kind.cmp(&b.kind)));
        }
        rows
    } else {
        match &rules.upsert_key {
            None => fold_multiset(&table.rows),
            Some(key) => fold_upsert(&table.rows, key),
        }
    };
    ResultTable {
        columns: table.columns.clone(),
        rows,
    }
}

fn fold_multiset(rows: &[Row]) -> Vec<Row> {
    let mut counts: BTreeMap<&Vec<String>, i64> = BTreeMap::new();
    for row in rows {
        *counts.entry(&row.fields).or_default() += if row.kind.is_addition() { 1 } else { -1 };
    }
    let mut out = Vec::new();
    for (fields, n) in counts {
        let kind = if n > 0 {
            RowKind::Insert
        } else {
            RowKind::Delete
        };
        for _ in 0..n.unsigned_abs() {
            out.push(Row {
                kind,
                fields: fields.clone(),
            });
        }
    }
    out
}

fn fold_upsert(rows: &[Row], key: &[usize]) -> Vec<Row> {
    let mut table: BTreeMap<Vec<String>, Vec<String>> = BTreeMap::new();
    let mut dangling = Vec::new();
    for row in rows {
        let k: Vec<String> = key
            .iter()
            .map(|&i| row.fields.get(i).cloned().unwrap_or_default())
            .collect();
        if row.kind.is_addition() {
            table.insert(k, row.fields.clone());
        } else if table.remove(&k).is_none() {
            dangling.push(Row {
                kind: RowKind::Delete,
                fields: row.fields.clone(),
            });
        }
    }
    let mut out: Vec<Row> = table
        .into_values()
        .map(|fields| Row {
            kind: RowKind::Insert,
            fields,
        })
        .collect();
    out.extend(dangling);
    out.sort_by(|a, b| a.fields.cmp(&b.fields).then(a.kind.cmp(&b.kind)));
    out
}

/// Why two results differ.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "what", rename_all = "snake_case")]
pub enum Difference {
    /// Different columns (count, a name or a type).
    Columns {
        /// The reference's columns.
        expected: Vec<Column>,
        /// The other side's.
        actual: Vec<Column>,
    },
    /// The first differing row, after normalisation.
    Row {
        /// Its position in the normalised rows.
        index: usize,
        /// The reference's row there (`None`: it has fewer rows).
        expected: Option<String>,
        /// The other side's row there (`None`: it has fewer rows).
        actual: Option<String>,
        /// Normalised row counts on each side.
        expected_rows: usize,
        /// Normalised row counts on each side.
        actual_rows: usize,
    },
}

impl fmt::Display for Difference {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Difference::Columns { expected, actual } => {
                let show = |cs: &[Column]| {
                    cs.iter()
                        .map(|c| format!("{} {}", c.name, c.ty))
                        .collect::<Vec<_>>()
                        .join(", ")
                };
                write!(
                    f,
                    "columns: expected ({}), got ({})",
                    show(expected),
                    show(actual)
                )
            }
            Difference::Row {
                index,
                expected,
                actual,
                expected_rows,
                actual_rows,
            } => write!(
                f,
                "row {index}: expected {}, got {} ({expected_rows} vs {actual_rows} rows)",
                expected.as_deref().unwrap_or("nothing"),
                actual.as_deref().unwrap_or("nothing"),
            ),
        }
    }
}

/// Compares `actual` with `expected` under `rules`: `None` when they are
/// equal, else the first difference.
pub fn compare(expected: &ResultTable, actual: &ResultTable, rules: &Rules) -> Option<Difference> {
    if expected.columns != actual.columns {
        return Some(Difference::Columns {
            expected: expected.columns.clone(),
            actual: actual.columns.clone(),
        });
    }
    let e = normalise(expected, rules);
    let a = normalise(actual, rules);
    let n = e.rows.len().max(a.rows.len());
    (0..n).find_map(|i| {
        let (x, y) = (e.rows.get(i), a.rows.get(i));
        (x != y).then(|| Difference::Row {
            index: i,
            expected: x.map(Row::to_line),
            actual: y.map(Row::to_line),
            expected_rows: e.rows.len(),
            actual_rows: a.rows.len(),
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t(rows: &[(RowKind, &[&str])]) -> ResultTable {
        ResultTable {
            columns: vec![Column::new("k", "INT"), Column::new("v", "DOUBLE")],
            rows: rows
                .iter()
                .map(|(k, f)| Row::new(*k, f.iter().copied()))
                .collect(),
        }
    }

    #[test]
    fn folding_a_retraction_cancels_its_row() {
        use RowKind::*;
        let changelog = t(&[
            (Insert, &["1", "1.0"]),
            (UpdateBefore, &["1", "1.0"]),
            (UpdateAfter, &["1", "2.0"]),
        ]);
        let table = t(&[(Insert, &["1", "2.0"])]);
        let fold = Rules::default();
        assert_eq!(compare(&table, &changelog, &fold), None);
        let strict = Rules {
            changelog_significant: true,
            ..Rules::default()
        };
        assert!(compare(&table, &changelog, &strict).is_some());
    }

    #[test]
    fn a_dangling_retraction_stays_visible() {
        use RowKind::*;
        let a = t(&[(Delete, &["9", "0.0"]), (Insert, &["1", "1.0"])]);
        let b = t(&[(Insert, &["1", "1.0"])]);
        assert!(compare(&b, &a, &Rules::default()).is_some());
    }

    #[test]
    fn upsert_folding_keeps_the_last_write_per_key() {
        use RowKind::*;
        let rules = Rules {
            upsert_key: Some(vec![0]),
            ..Rules::default()
        };
        let a = t(&[
            (Insert, &["1", "1.0"]),
            (UpdateAfter, &["1", "3.0"]),
            (Insert, &["2", "1.0"]),
            (Delete, &["2", "1.0"]),
        ]);
        let b = t(&[(Insert, &["1", "3.0"])]);
        assert_eq!(compare(&b, &a, &rules), None);
    }

    #[test]
    fn contradictory_rules_are_refused() {
        let r = Rules {
            order_sensitive: true,
            changelog_significant: false,
            upsert_key: None,
        };
        assert!(r.validate(2).is_err());
        let r = Rules {
            upsert_key: Some(vec![2]),
            ..Rules::default()
        };
        assert!(r.validate(2).is_err());
    }
}
