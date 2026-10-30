//! Flink's sink `ConstraintEnforcer`: `NOT NULL` columns (`table.exec.sink.not-null-enforcer`),
//! character and binary lengths (`table.exec.sink.type-length-enforcer`) and how deep into rows
//! and collections they apply (`table.exec.sink.nested-constraint-enforcer`). Messages are
//! Flink's.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, BinaryArray, BooleanArray, RecordBatch, StringArray, StructArray,
};
use loams_stream_plan::physical::{EnforcerSpec, LengthEnforcer, NestedEnforcer, NotNullEnforcer};
use loams_stream_sql::types::{LogicalKind, LogicalType};

/// A row the enforcer rejects.
enum Violation {
    Drop,
    Fail(String),
}

/// Applies the constraints to a batch: drops rows, trims and pads, or fails with Flink's
/// message.
pub fn enforce(batch: RecordBatch, spec: &EnforcerSpec) -> Result<RecordBatch, String> {
    let batch = if spec.length == LengthEnforcer::TrimPad {
        trim_pad_batch(batch, spec)?
    } else {
        batch
    };
    let mut keep = vec![true; batch.num_rows()];
    for (row, k) in keep.iter_mut().enumerate() {
        for (i, field) in spec.fields.iter().enumerate() {
            let top_not_null = spec.not_null_columns.contains(&i);
            let checker = Checker { spec, top_not_null };
            match checker.check(batch.column(i), row, &field.ty, &field.name, 0, false) {
                None => {}
                Some(Violation::Drop) => *k = false,
                Some(Violation::Fail(message)) => return Err(message),
            }
        }
    }
    if keep.iter().all(|k| *k) {
        return Ok(batch);
    }
    arrow::compute::filter_record_batch(&batch, &BooleanArray::from(keep))
        .map_err(|e| e.to_string())
}

struct Checker<'a> {
    spec: &'a EnforcerSpec,
    top_not_null: bool,
}

impl Checker<'_> {
    fn applies(&self, depth: usize, in_collection: bool) -> bool {
        depth == 0
            || match self.spec.nested {
                NestedEnforcer::Ignore => false,
                NestedEnforcer::Rows => !in_collection,
                NestedEnforcer::RowsAndCollections => true,
            }
    }

    fn check(
        &self,
        array: &ArrayRef,
        row: usize,
        ty: &LogicalType,
        path: &str,
        depth: usize,
        in_collection: bool,
    ) -> Option<Violation> {
        if !self.applies(depth, in_collection) {
            return None;
        }
        if array.is_null(row) {
            let violated = if depth == 0 {
                self.top_not_null
            } else {
                !ty.nullable
            };
            return violated.then(|| match self.spec.not_null {
                NotNullEnforcer::Drop => Violation::Drop,
                NotNullEnforcer::Error => Violation::Fail(format!(
                    "Column '{path}' is NOT NULL, however, a null value is being written into it. You can set job configuration 'table.exec.sink.not-null-enforcer'='DROP' to suppress this exception and drop such records silently."
                )),
            });
        }
        if self.spec.length == LengthEnforcer::Error
            && let Some((name, n, fixed)) = length_constraint(ty)
        {
            let len = match array.data_type() {
                arrow::datatypes::DataType::Utf8 => {
                    array.as_string::<i32>().value(row).chars().count()
                }
                arrow::datatypes::DataType::Binary => array.as_binary::<i32>().value(row).len(),
                _ => return None,
            };
            if (fixed && len != n) || len > n {
                return Some(Violation::Fail(format!(
                    "Column '{path}' is {name}({n}), however, a string of length {len} is being written into it. You can set job configuration 'table.exec.sink.type-length-enforcer' to control this behaviour."
                )));
            }
        }
        match &ty.kind {
            LogicalKind::Row(fields) => {
                let s = array.as_struct();
                fields.iter().enumerate().find_map(|(i, f)| {
                    self.check(
                        s.column(i),
                        row,
                        &f.ty,
                        &format!("{path}.{}", f.name),
                        depth + 1,
                        in_collection,
                    )
                })
            }
            LogicalKind::Array(element) | LogicalKind::Multiset(element)
                if matches!(array.data_type(), arrow::datatypes::DataType::List(_)) =>
            {
                let list = array.as_list::<i32>();
                let items = list.value(row);
                (0..items.len()).find_map(|j| {
                    self.check(&items, j, element, &format!("{path}[{j}]"), depth + 1, true)
                })
            }
            _ => None,
        }
    }
}

/// `(type name, length, fixed)` of a length-constrained type; `STRING` and `BYTES` are not.
fn length_constraint(ty: &LogicalType) -> Option<(&'static str, usize, bool)> {
    let max = loams_stream_sql::types::MAX_LENGTH;
    match ty.kind {
        LogicalKind::Char(n) => Some(("CHAR", n as usize, true)),
        LogicalKind::Binary(n) => Some(("BINARY", n as usize, true)),
        LogicalKind::Varchar(n) if n != max => Some(("VARCHAR", n as usize, false)),
        LogicalKind::Varbinary(n) if n != max => Some(("VARBINARY", n as usize, false)),
        _ => None,
    }
}

/// `TRIM_PAD`: `CHAR`/`BINARY` padded (spaces, zero bytes) or cut to their length,
/// `VARCHAR`/`VARBINARY` cut, at the top level and in nested rows.
fn trim_pad_batch(batch: RecordBatch, spec: &EnforcerSpec) -> Result<RecordBatch, String> {
    let nested = spec.nested != NestedEnforcer::Ignore;
    let columns: Vec<ArrayRef> = batch
        .columns()
        .iter()
        .zip(&spec.fields)
        .map(|(c, f)| trim_pad(c, &f.ty, nested))
        .collect::<Result<_, _>>()?;
    RecordBatch::try_new(batch.schema(), columns).map_err(|e| e.to_string())
}

fn trim_pad(array: &ArrayRef, ty: &LogicalType, nested: bool) -> Result<ArrayRef, String> {
    if let Some((_, n, fixed)) = length_constraint(ty) {
        return Ok(match array.data_type() {
            arrow::datatypes::DataType::Utf8 => {
                let s = array.as_string::<i32>();
                let out: StringArray = s
                    .iter()
                    .map(|v| {
                        v.map(|v| {
                            let mut t: String = v.chars().take(n).collect();
                            if fixed {
                                let len = t.chars().count();
                                t.extend(std::iter::repeat_n(' ', n.saturating_sub(len)));
                            }
                            t
                        })
                    })
                    .collect();
                Arc::new(out)
            }
            arrow::datatypes::DataType::Binary => {
                let b = array.as_binary::<i32>();
                let out: BinaryArray = b
                    .iter()
                    .map(|v| {
                        v.map(|v| {
                            let mut t: Vec<u8> = v.iter().copied().take(n).collect();
                            if fixed {
                                t.resize(n, 0);
                            }
                            t
                        })
                    })
                    .collect();
                Arc::new(out)
            }
            _ => array.clone(),
        });
    }
    match (&ty.kind, nested) {
        (LogicalKind::Row(fields), true) => {
            let s = array.as_struct();
            let children = fields
                .iter()
                .enumerate()
                .map(|(i, f)| trim_pad(s.column(i), &f.ty, nested))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Arc::new(
                StructArray::try_new(s.fields().clone(), children, s.nulls().cloned())
                    .map_err(|e| e.to_string())?,
            ))
        }
        _ => Ok(array.clone()),
    }
}
