//! Flink's `json` and `debezium-json` formats over Arrow batches in the engine's layout (FC1
//! Task 10).
//!
//! Decoding follows Flink 2.2.1's `JsonToRowDataConverters` with the format's defaults
//! (`json.timestamp-format.standard = SQL`, missing fields and nulls are NULL, a parse error fails);
//! encoding follows `RowDataToJsonConverters` and Jackson's number rendering, so the text a sink
//! writes is the text Flink writes.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder,
    Float32Builder, Float64Builder, Int8Builder, Int16Builder, Int32Builder, Int64Builder,
    StringBuilder, StructArray, Time32MillisecondBuilder,
};
use arrow::buffer::NullBuffer;
use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use base64::Engine;
use loams_stream_sql::types::arrow::{timestamp_struct, to_arrow_field};
use loams_stream_sql::types::{LogicalKind, LogicalType};
use serde_json::value::RawValue;

use super::{FormatError, ROW_KIND_COLUMN, RowKind, row_kind_field};

/// A column of a format's row: its name and Flink type.
pub type Column = (String, LogicalType);

fn err(message: impl Into<String>) -> FormatError {
    FormatError(message.into())
}

/// The batch schema for `columns` (plus the row-kind column for a changelog).
pub fn schema(columns: &[Column], changelog: bool) -> Result<Arc<Schema>, FormatError> {
    let mut fields = columns
        .iter()
        .map(|(n, t)| to_arrow_field(n, t).map_err(|e| err(e.to_string())))
        .collect::<Result<Vec<_>, _>>()?;
    if changelog {
        fields.push(row_kind_field());
    }
    Ok(Arc::new(Schema::new(fields)))
}

// -------------------------------------------------------------------------------------------
// Decoding
// -------------------------------------------------------------------------------------------

/// One column being built from JSON values.
enum Builder {
    Boolean(BooleanBuilder),
    Int8(Int8Builder),
    Int16(Int16Builder),
    Int32(Int32Builder),
    Int64(Int64Builder),
    Float32(Float32Builder),
    Float64(Float64Builder),
    Decimal(Decimal128Builder, u8, u8),
    Utf8(StringBuilder),
    Binary(BinaryBuilder),
    Date(Date32Builder),
    Time(Time32MillisecondBuilder),
    /// Timestamp components and validity; `ltz` expects the trailing `Z` of the SQL format.
    Timestamp {
        millis: Vec<i64>,
        nanos: Vec<i32>,
        valid: Vec<bool>,
        ltz: bool,
    },
    IntervalMonths(Int32Builder),
    IntervalMillis(Int64Builder),
    /// `ROW`, `ARRAY`, `MAP`, `MULTISET`: the values are kept and built at the end.
    Nested {
        ty: LogicalType,
        values: Vec<Option<serde_json::Value>>,
    },
}

impl Builder {
    fn new(ty: &LogicalType) -> Result<Self, FormatError> {
        Ok(match &ty.kind {
            LogicalKind::Boolean => Builder::Boolean(BooleanBuilder::new()),
            LogicalKind::TinyInt => Builder::Int8(Int8Builder::new()),
            LogicalKind::SmallInt => Builder::Int16(Int16Builder::new()),
            LogicalKind::Int => Builder::Int32(Int32Builder::new()),
            LogicalKind::BigInt => Builder::Int64(Int64Builder::new()),
            LogicalKind::Float => Builder::Float32(Float32Builder::new()),
            LogicalKind::Double => Builder::Float64(Float64Builder::new()),
            LogicalKind::Decimal { precision, scale } => Builder::Decimal(
                Decimal128Builder::new()
                    .with_precision_and_scale(*precision, *scale as i8)
                    .map_err(|e| err(e.to_string()))?,
                *precision,
                *scale,
            ),
            LogicalKind::Char(_) | LogicalKind::Varchar(_) => Builder::Utf8(StringBuilder::new()),
            LogicalKind::Binary(_) | LogicalKind::Varbinary(_) => {
                Builder::Binary(BinaryBuilder::new())
            }
            LogicalKind::Date => Builder::Date(Date32Builder::new()),
            LogicalKind::Time(_) => Builder::Time(Time32MillisecondBuilder::new()),
            LogicalKind::Timestamp { .. } | LogicalKind::TimestampLtz { .. } => {
                Builder::Timestamp {
                    millis: vec![],
                    nanos: vec![],
                    valid: vec![],
                    ltz: matches!(ty.kind, LogicalKind::TimestampLtz { .. }),
                }
            }
            LogicalKind::IntervalYearMonth { .. } => Builder::IntervalMonths(Int32Builder::new()),
            LogicalKind::IntervalDayTime { .. } => Builder::IntervalMillis(Int64Builder::new()),
            LogicalKind::Row(_)
            | LogicalKind::Array(_)
            | LogicalKind::Map(..)
            | LogicalKind::Multiset(_) => {
                // Check the element types up front.
                build_nested(&[], ty)?;
                Builder::Nested {
                    ty: ty.clone(),
                    values: vec![],
                }
            }
            _ => {
                return Err(err(format!(
                    "the json format of Loams Stream does not read {ty} columns yet"
                )));
            }
        })
    }

    fn append_null(&mut self) {
        match self {
            Builder::Boolean(b) => b.append_null(),
            Builder::Int8(b) => b.append_null(),
            Builder::Int16(b) => b.append_null(),
            Builder::Int32(b) => b.append_null(),
            Builder::Int64(b) => b.append_null(),
            Builder::Float32(b) => b.append_null(),
            Builder::Float64(b) => b.append_null(),
            Builder::Decimal(b, ..) => b.append_null(),
            Builder::Utf8(b) => b.append_null(),
            Builder::Binary(b) => b.append_null(),
            Builder::Date(b) => b.append_null(),
            Builder::Time(b) => b.append_null(),
            Builder::Timestamp {
                millis,
                nanos,
                valid,
                ..
            } => {
                millis.push(0);
                nanos.push(0);
                valid.push(false);
            }
            Builder::IntervalMonths(b) => b.append_null(),
            Builder::IntervalMillis(b) => b.append_null(),
            Builder::Nested { values, .. } => values.push(None),
        }
    }

    /// Appends one JSON value given as its raw text (not `null`).
    fn append(&mut self, raw: &str) -> Result<(), FormatError> {
        let value: serde_json::Value =
            serde_json::from_str(raw).map_err(|e| err(format!("{e}: {raw}")))?;
        if let Builder::Nested { values, .. } = self {
            values.push(Some(value));
            return Ok(());
        }
        // Flink reads a non-textual node through its text (`asText`) for numbers and booleans.
        let text = || -> String {
            match &value {
                serde_json::Value::String(s) => s.clone(),
                _ => raw.to_owned(),
            }
        };
        let bad = |what: &str| err(format!("Failed to deserialize JSON '{raw}' as {what}"));
        match self {
            Builder::Boolean(b) => b.append_value(match &value {
                serde_json::Value::Bool(v) => *v,
                _ => text().trim().eq_ignore_ascii_case("true"),
            }),
            Builder::Int8(b) => b.append_value(text().trim().parse().map_err(|_| bad("TINYINT"))?),
            Builder::Int16(b) => {
                b.append_value(text().trim().parse().map_err(|_| bad("SMALLINT"))?)
            }
            Builder::Int32(b) => b.append_value(text().trim().parse().map_err(|_| bad("INT"))?),
            Builder::Int64(b) => b.append_value(text().trim().parse().map_err(|_| bad("BIGINT"))?),
            Builder::Float32(b) => b.append_value(text().trim().parse().map_err(|_| bad("FLOAT"))?),
            Builder::Float64(b) => {
                b.append_value(text().trim().parse().map_err(|_| bad("DOUBLE"))?)
            }
            Builder::Decimal(b, precision, scale) => {
                match parse_decimal(text().trim(), *precision, *scale) {
                    Some(Some(v)) => b.append_value(v),
                    // Flink's DecimalData.fromBigDecimal gives NULL when the precision overflows.
                    Some(None) => b.append_null(),
                    None => return Err(bad("DECIMAL")),
                }
            }
            Builder::Utf8(b) => match &value {
                serde_json::Value::String(s) => b.append_value(s),
                // Flink's STRING converter keeps a non-textual node's JSON text.
                _ => b.append_value(raw),
            },
            Builder::Binary(b) => b.append_value(
                base64::engine::general_purpose::STANDARD
                    .decode(text())
                    .map_err(|_| bad("BYTES"))?,
            ),
            Builder::Date(b) => b.append_value(parse_date(&text()).ok_or_else(|| bad("DATE"))?),
            Builder::Time(b) => {
                let (ms, _) = parse_time(&text()).ok_or_else(|| bad("TIME"))?;
                b.append_value(ms as i32)
            }
            Builder::Timestamp {
                millis,
                nanos,
                valid,
                ltz,
            } => {
                let t = text();
                let body = if *ltz {
                    t.strip_suffix('Z')
                        .ok_or_else(|| bad("TIMESTAMP_LTZ (SQL format, ending in Z)"))?
                } else {
                    t.as_str()
                };
                if body.contains('T') {
                    return Err(bad("TIMESTAMP (SQL format)"));
                }
                let (ms, ns) = parse_timestamp(body).ok_or_else(|| bad("TIMESTAMP"))?;
                millis.push(ms);
                nanos.push(ns);
                valid.push(true);
            }
            Builder::IntervalMonths(b) => {
                b.append_value(text().trim().parse().map_err(|_| bad("INTERVAL"))?)
            }
            Builder::IntervalMillis(b) => {
                b.append_value(text().trim().parse().map_err(|_| bad("INTERVAL"))?)
            }
            Builder::Nested { .. } => unreachable!("handled above"),
        }
        Ok(())
    }

    fn finish(self) -> Result<ArrayRef, FormatError> {
        Ok(match self {
            Builder::Nested { ty, values } => {
                let refs: Vec<Option<&serde_json::Value>> =
                    values.iter().map(Option::as_ref).collect();
                build_nested(&refs, &ty)?
            }
            Builder::Boolean(mut b) => Arc::new(b.finish()),
            Builder::Int8(mut b) => Arc::new(b.finish()),
            Builder::Int16(mut b) => Arc::new(b.finish()),
            Builder::Int32(mut b) => Arc::new(b.finish()),
            Builder::Int64(mut b) => Arc::new(b.finish()),
            Builder::Float32(mut b) => Arc::new(b.finish()),
            Builder::Float64(mut b) => Arc::new(b.finish()),
            Builder::Decimal(mut b, ..) => Arc::new(b.finish()),
            Builder::Utf8(mut b) => Arc::new(b.finish()),
            Builder::Binary(mut b) => Arc::new(b.finish()),
            Builder::Date(mut b) => Arc::new(b.finish()),
            Builder::Time(mut b) => Arc::new(b.finish()),
            Builder::Timestamp {
                millis,
                nanos,
                valid,
                ..
            } => timestamp_array(millis, nanos, valid),
            Builder::IntervalMonths(mut b) => Arc::new(b.finish()),
            Builder::IntervalMillis(mut b) => Arc::new(b.finish()),
        })
    }
}

/// `field` with the data type of `array`, nullable if `array` has nulls.
fn relaxed(field: &arrow::datatypes::FieldRef, array: &ArrayRef) -> arrow::datatypes::FieldRef {
    let f = field
        .as_ref()
        .clone()
        .with_data_type(array.data_type().clone());
    Arc::new(if array.null_count() > 0 {
        f.with_nullable(true)
    } else {
        f
    })
}

/// Builds a column of `ty` from JSON values (NULL for `None` or JSON `null`), recursing into
/// `ROW`, `ARRAY`, `MAP` and `MULTISET` as Flink's converters do: a ROW is an object by field
/// name, an ARRAY a JSON array, a MAP an object whose keys are converted to the key type.
fn build_nested(
    values: &[Option<&serde_json::Value>],
    ty: &LogicalType,
) -> Result<ArrayRef, FormatError> {
    use arrow::array::{ListArray, MapArray};
    use arrow::buffer::OffsetBuffer;
    let present = |v: &Option<&serde_json::Value>| v.is_some_and(|v| !v.is_null());
    let nulls = || {
        let valid: Vec<bool> = values.iter().map(present).collect();
        (!valid.iter().all(|v| *v)).then(|| NullBuffer::from(valid))
    };
    let (DataType::Struct(_) | DataType::List(_) | DataType::Map(..)) = to_arrow_field("v", ty)
        .map_err(|e| err(e.to_string()))?
        .data_type()
        .clone()
    else {
        // A scalar: through the scalar builder, from each value's JSON text.
        let mut b = Builder::new(ty)?;
        for v in values {
            match v {
                Some(v) if !v.is_null() => b.append(&v.to_string())?,
                _ => b.append_null(),
            }
        }
        return b.finish();
    };
    let field = to_arrow_field("v", ty).map_err(|e| err(e.to_string()))?;
    match (&ty.kind, field.data_type()) {
        (LogicalKind::Row(fields), DataType::Struct(arrow_fields)) => {
            let mut children = vec![];
            for f in fields {
                let column: Vec<Option<&serde_json::Value>> = values
                    .iter()
                    .map(|v| v.and_then(|v| v.get(&f.name)))
                    .collect();
                children.push(build_nested(&column, &f.ty)?);
            }
            // A NULL in a NOT NULL field is data the sink's constraint enforcer judges, not a
            // decoding error: the field becomes nullable in the batch.
            let fields: arrow::datatypes::Fields = arrow_fields
                .iter()
                .zip(&children)
                .map(|(f, c)| relaxed(f, c))
                .collect();
            Ok(Arc::new(
                StructArray::try_new(fields, children, nulls()).map_err(|e| err(e.to_string()))?,
            ))
        }
        (LogicalKind::Array(element), DataType::List(item)) => {
            let mut flat = vec![];
            let mut lengths = vec![];
            for v in values {
                let items = v.and_then(|v| v.as_array());
                lengths.push(items.map_or(0, Vec::len));
                flat.extend(items.into_iter().flatten().map(Some));
            }
            let child = build_nested(&flat, element)?;
            Ok(Arc::new(
                ListArray::try_new(
                    relaxed(item, &child),
                    OffsetBuffer::from_lengths(lengths),
                    child,
                    nulls(),
                )
                .map_err(|e| err(e.to_string()))?,
            ))
        }
        (LogicalKind::Map(..) | LogicalKind::Multiset(_), DataType::Map(entries, sorted)) => {
            let (key_ty, value_ty) = match &ty.kind {
                LogicalKind::Map(k, v) => ((**k).clone(), (**v).clone()),
                LogicalKind::Multiset(e) => ((**e).clone(), LogicalType::int().not_null()),
                _ => unreachable!(),
            };
            let DataType::Struct(kv_fields) = entries.data_type() else {
                return Err(err("map entries are not a struct"));
            };
            let mut keys = vec![];
            let mut vals = vec![];
            let mut lengths = vec![];
            for v in values {
                let object = v.and_then(|v| v.as_object());
                lengths.push(object.map_or(0, |o| o.len()));
                for (k, val) in object.into_iter().flatten() {
                    keys.push(serde_json::Value::String(k.clone()));
                    vals.push(val);
                }
            }
            // Keys are JSON strings; a non-string key type reads the key's text.
            let key_refs: Vec<Option<&serde_json::Value>> = keys.iter().map(Some).collect();
            let key_column =
                if matches!(key_ty.kind, LogicalKind::Char(_) | LogicalKind::Varchar(_)) {
                    build_nested(&key_refs, &key_ty)?
                } else {
                    let parsed: Vec<serde_json::Value> = keys
                        .iter()
                        .map(|k| {
                            let s = k.as_str().unwrap_or_default();
                            serde_json::from_str(s).unwrap_or_else(|_| k.clone())
                        })
                        .collect();
                    let refs: Vec<Option<&serde_json::Value>> = parsed.iter().map(Some).collect();
                    build_nested(&refs, &key_ty)?
                };
            let val_refs: Vec<Option<&serde_json::Value>> = vals.into_iter().map(Some).collect();
            let value_column = build_nested(&val_refs, &value_ty)?;
            let entries_array =
                StructArray::try_new(kv_fields.clone(), vec![key_column, value_column], None)
                    .map_err(|e| err(e.to_string()))?;
            Ok(Arc::new(
                MapArray::try_new(
                    entries.clone(),
                    OffsetBuffer::from_lengths(lengths),
                    entries_array,
                    nulls(),
                    *sorted,
                )
                .map_err(|e| err(e.to_string()))?,
            ))
        }
        _ => Err(err(format!(
            "the json format of Loams Stream does not read {ty} columns yet"
        ))),
    }
}

/// A column in the engine's timestamp layout.
pub fn timestamp_array(millis: Vec<i64>, nanos: Vec<i32>, valid: Vec<bool>) -> ArrayRef {
    let DataType::Struct(fields) = timestamp_struct() else {
        unreachable!("timestamps are structs")
    };
    let nulls = if valid.iter().all(|v| *v) {
        None
    } else {
        Some(NullBuffer::from(valid))
    };
    Arc::new(StructArray::new(
        fields,
        vec![
            Arc::new(arrow::array::Int64Array::from(millis)),
            Arc::new(arrow::array::Int32Array::from(nanos)),
        ],
        nulls,
    ))
}

/// Builds rows of `columns` from JSON objects.
struct RowsBuilder {
    names: Vec<String>,
    builders: Vec<Builder>,
    kinds: Vec<i8>,
}

impl RowsBuilder {
    fn new(columns: &[Column]) -> Result<Self, FormatError> {
        Ok(RowsBuilder {
            names: columns.iter().map(|(n, _)| n.clone()).collect(),
            builders: columns
                .iter()
                .map(|(_, t)| Builder::new(t))
                .collect::<Result<_, _>>()?,
            kinds: vec![],
        })
    }

    fn push(&mut self, object: &str, kind: RowKind) -> Result<(), FormatError> {
        let map: std::collections::HashMap<&str, &RawValue> =
            serde_json::from_str(object).map_err(|e| err(format!("{e}: {object}")))?;
        for (name, b) in self.names.iter().zip(self.builders.iter_mut()) {
            match map.get(name.as_str()) {
                Some(v) if v.get() != "null" => b.append(v.get())?,
                _ => b.append_null(),
            }
        }
        self.kinds.push(kind as i8);
        Ok(())
    }

    fn finish(self, schema: Arc<Schema>, changelog: bool) -> Result<RecordBatch, FormatError> {
        let rows = self.kinds.len();
        let mut columns: Vec<ArrayRef> = self
            .builders
            .into_iter()
            .map(Builder::finish)
            .collect::<Result<_, _>>()?;
        if changelog {
            columns.push(Arc::new(arrow::array::Int8Array::from(self.kinds)));
        }
        // Nested types may have been relaxed to nullable by the data (see `relaxed`).
        let schema = Arc::new(Schema::new(
            schema
                .fields()
                .iter()
                .zip(&columns)
                .map(|(f, c)| relaxed(f, c))
                .collect::<Vec<_>>(),
        ));
        RecordBatch::try_new_with_options(
            schema,
            columns,
            &arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(rows)),
        )
        .map_err(|e| err(e.to_string()))
    }
}

/// Decodes `json` lines (one object per line) into a batch of `columns`.
pub fn decode_json(text: &str, columns: &[Column]) -> Result<RecordBatch, FormatError> {
    let mut rows = RowsBuilder::new(columns)?;
    for line in text.lines().filter(|l| !l.trim().is_empty()) {
        rows.push(line, RowKind::Insert)?;
    }
    rows.finish(schema(columns, false)?, false)
}

/// Decodes `debezium-json` lines into a changelog batch of `columns` with the row-kind column.
pub fn decode_debezium(text: &str, columns: &[Column]) -> Result<RecordBatch, FormatError> {
    #[derive(serde::Deserialize)]
    struct Event<'a> {
        #[serde(borrow, default)]
        before: Option<&'a RawValue>,
        #[serde(borrow, default)]
        after: Option<&'a RawValue>,
        op: String,
    }
    let mut rows = RowsBuilder::new(columns)?;
    for line in text.lines().filter(|l| !l.trim().is_empty()) {
        let ev: Event<'_> = serde_json::from_str(line).map_err(|e| err(format!("{e}: {line}")))?;
        let image = |v: Option<&RawValue>| -> Result<String, FormatError> {
            match v {
                Some(v) if v.get() != "null" => Ok(v.get().to_owned()),
                _ => Err(err(format!(
                    "The \"before\" or \"after\" field of a debezium '{}' event is null: {line}",
                    ev.op
                ))),
            }
        };
        match ev.op.as_str() {
            "c" | "r" => rows.push(&image(ev.after)?, RowKind::Insert)?,
            "u" => {
                rows.push(&image(ev.before)?, RowKind::UpdateBefore)?;
                rows.push(&image(ev.after)?, RowKind::UpdateAfter)?;
            }
            "d" => rows.push(&image(ev.before)?, RowKind::Delete)?,
            other => {
                return Err(err(format!(
                    "Unknown \"op\" value \"{other}\". The Debezium JSON message is '{line}'"
                )));
            }
        }
    }
    rows.finish(schema(columns, true)?, true)
}

/// `yyyy-MM-dd` as days since the epoch.
pub fn parse_date(s: &str) -> Option<i32> {
    let s = s.trim();
    let mut it = s.splitn(3, '-');
    let y: i64 = it.next()?.parse().ok()?;
    let m: u32 = it.next()?.parse().ok()?;
    let d: u32 = it.next()?.parse().ok()?;
    if !(1..=12).contains(&m) || d < 1 || d > days_in_month(y, m) {
        return None;
    }
    i32::try_from(days_from_civil(y, m, d)).ok()
}

fn days_in_month(y: i64, m: u32) -> u32 {
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        _ if (y % 4 == 0 && y % 100 != 0) || y % 400 == 0 => 29,
        _ => 28,
    }
}

fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp as i64 + 2) / 5 + d as i64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// The civil date of `days` since the epoch.
pub fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// `HH:mm[:ss[.fffffffff]]` as (milliseconds of the day, nanoseconds within the millisecond).
pub fn parse_time(s: &str) -> Option<(i64, i32)> {
    let s = s.trim();
    let (hms, frac) = s.split_once('.').unwrap_or((s, ""));
    let mut it = hms.split(':');
    let h: i64 = it.next()?.parse().ok()?;
    let m: i64 = it.next()?.parse().ok()?;
    let sec: i64 = match it.next() {
        Some(x) => x.parse().ok()?,
        None => 0,
    };
    if it.next().is_some() || h > 23 || m > 59 || sec > 59 {
        return None;
    }
    if frac.len() > 9 || !frac.chars().all(|c| c.is_ascii_digit()) {
        return None;
    }
    let nanos: i64 = format!("{frac:0<9}").parse().ok()?;
    let millis = ((h * 60 + m) * 60 + sec) * 1000 + nanos / 1_000_000;
    Some((millis, (nanos % 1_000_000) as i32))
}

/// `yyyy-MM-dd HH:mm:ss[.f]` as epoch milliseconds and sub-millisecond nanoseconds.
pub fn parse_timestamp(s: &str) -> Option<(i64, i32)> {
    let (date, time) = s.trim().split_once(' ')?;
    let days = parse_date(date)? as i64;
    let (ms, nanos) = parse_time(time)?;
    Some((days * 86_400_000 + ms, nanos))
}

/// A decimal text at `scale`, rounded HALF_UP; `Some(None)` when it overflows `precision`.
pub fn parse_decimal(s: &str, precision: u8, scale: u8) -> Option<Option<i128>> {
    let (negative, s) = match s.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, s.strip_prefix('+').unwrap_or(s)),
    };
    let (mantissa, exponent) = match s.find(['e', 'E']) {
        Some(i) => (&s[..i], s[i + 1..].parse::<i32>().ok()?),
        None => (s, 0),
    };
    let (whole, frac) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    if whole.is_empty() && frac.is_empty()
        || !whole
            .chars()
            .chain(frac.chars())
            .all(|c| c.is_ascii_digit())
    {
        return None;
    }
    let digits = format!("{whole}{frac}");
    let digits = digits.trim_start_matches('0');
    // value = digits * 10^(exponent - frac.len())
    let shift = exponent - frac.len() as i32 + scale as i32;
    let mut value: i128 = 0;
    let mut round_up = false;
    if shift >= 0 {
        for c in digits.chars() {
            value = value
                .checked_mul(10)?
                .checked_add(c as i128 - '0' as i128)?;
        }
        for _ in 0..shift {
            value = value.checked_mul(10)?;
        }
    } else {
        let keep = digits.len() as i64 + shift as i64;
        if keep > 0 {
            for c in digits[..keep as usize].chars() {
                value = value
                    .checked_mul(10)?
                    .checked_add(c as i128 - '0' as i128)?;
            }
            round_up = digits.as_bytes()[keep as usize] >= b'5';
        } else if keep == 0 {
            round_up = digits.as_bytes().first().is_some_and(|d| *d >= b'5');
        }
    }
    if round_up {
        value += 1;
    }
    let limit = 10i128.checked_pow(precision as u32)?;
    if value >= limit {
        return Some(None);
    }
    Some(Some(if negative { -value } else { value }))
}

// -------------------------------------------------------------------------------------------
// Encoding
// -------------------------------------------------------------------------------------------

/// Appends `s` as a JSON string the way Jackson escapes it.
pub fn write_json_string(out: &mut String, s: &str) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{8}' => out.push_str("\\b"),
            '\u{c}' => out.push_str("\\f"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04X}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
}

/// Java's `Double.toString` / `Float.toString` from the shortest round-trip digits.
pub fn java_float(v: f64, single: bool) -> String {
    if v.is_nan() {
        return "NaN".into();
    }
    if v.is_infinite() {
        return if v > 0.0 { "Infinity" } else { "-Infinity" }.into();
    }
    if v == 0.0 {
        return if v.is_sign_negative() { "-0.0" } else { "0.0" }.into();
    }
    let sci = if single {
        format!("{:e}", v as f32)
    } else {
        format!("{v:e}")
    };
    let (mantissa, exp) = sci.split_once('e').expect("scientific form");
    let exp: i32 = exp.parse().expect("exponent");
    let negative = mantissa.starts_with('-');
    let digits: String = mantissa.chars().filter(|c| c.is_ascii_digit()).collect();
    let sign = if negative { "-" } else { "" };
    if (-3..7).contains(&exp) {
        // Plain: the point sits after exp + 1 digits.
        let point = exp + 1;
        let (int, frac) = if point <= 0 {
            (
                "0".to_owned(),
                format!("{}{digits}", "0".repeat((-point) as usize)),
            )
        } else if point as usize >= digits.len() {
            (
                format!("{digits}{}", "0".repeat(point as usize - digits.len())),
                String::new(),
            )
        } else {
            (
                digits[..point as usize].to_owned(),
                digits[point as usize..].to_owned(),
            )
        };
        let frac = if frac.is_empty() {
            "0".to_owned()
        } else {
            frac
        };
        format!("{sign}{int}.{frac}")
    } else {
        let frac = if digits.len() > 1 { &digits[1..] } else { "0" };
        format!("{sign}{}.{frac}E{exp}", &digits[..1])
    }
}

/// Jackson's text for a `DECIMAL` (a `BigDecimal` with trailing zeros stripped, then
/// `BigDecimal.toString`).
pub fn java_decimal(unscaled: i128, scale: i8) -> String {
    if unscaled == 0 {
        return "0".into();
    }
    let mut v = unscaled;
    let mut scale = scale as i32;
    while v % 10 == 0 {
        v /= 10;
        scale -= 1;
    }
    let negative = v < 0;
    let coeff = v.unsigned_abs().to_string();
    let len = coeff.len() as i32;
    let adjusted = -scale + (len - 1);
    let sign = if negative { "-" } else { "" };
    if scale >= 0 && adjusted >= -6 {
        if scale == 0 {
            return format!("{sign}{coeff}");
        }
        if len > scale {
            let (a, b) = coeff.split_at((len - scale) as usize);
            format!("{sign}{a}.{b}")
        } else {
            format!("{sign}0.{}{coeff}", "0".repeat((scale - len) as usize))
        }
    } else {
        let rest = if len > 1 {
            format!(".{}", &coeff[1..])
        } else {
            String::new()
        };
        let exp_sign = if adjusted > 0 { "+" } else { "" };
        format!("{sign}{}{rest}E{exp_sign}{adjusted}", &coeff[..1])
    }
}

/// `HH:mm:ss` plus the shortest fraction (Flink's SQL time formats).
fn time_text(millis_of_day: i64, nanos: i64) -> String {
    let total_nanos = millis_of_day * 1_000_000 + nanos;
    let secs = total_nanos / 1_000_000_000;
    let frac = total_nanos % 1_000_000_000;
    let mut out = format!(
        "{:02}:{:02}:{:02}",
        secs / 3600,
        (secs / 60) % 60,
        secs % 60
    );
    if frac != 0 {
        let f = format!("{frac:09}");
        out.push('.');
        out.push_str(f.trim_end_matches('0'));
    }
    out
}

fn date_text(days: i64) -> String {
    let (y, m, d) = civil_from_days(days);
    format!("{y:04}-{m:02}-{d:02}")
}

/// The JSON text of row `row` of `array` (of Flink type `ty`), as Flink's json format writes it.
pub fn value_text(array: &ArrayRef, row: usize, ty: &LogicalType) -> Result<String, FormatError> {
    value_text_in(array, row, ty, TimestampFormat::Sql)
}

/// Flink's `json.timestamp-format.standard`: `SQL` (`yyyy-MM-dd HH:mm:ss`) or `ISO-8601`
/// (`yyyy-MM-ddTHH:mm:ss`, which the SQL Gateway's JSON results use).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimestampFormat {
    Sql,
    Iso8601,
}

/// The JSON text of row `row` of `array` in the given timestamp format.
pub fn value_text_in(
    array: &ArrayRef,
    row: usize,
    ty: &LogicalType,
    format: TimestampFormat,
) -> Result<String, FormatError> {
    use arrow::array::AsArray;
    use arrow::datatypes::*;
    if array.is_null(row) {
        return Ok("null".into());
    }
    let mut out = String::new();
    match &ty.kind {
        LogicalKind::Boolean => out.push_str(if array.as_boolean().value(row) {
            "true"
        } else {
            "false"
        }),
        LogicalKind::TinyInt => {
            out.push_str(&array.as_primitive::<Int8Type>().value(row).to_string())
        }
        LogicalKind::SmallInt => {
            out.push_str(&array.as_primitive::<Int16Type>().value(row).to_string())
        }
        LogicalKind::Int => out.push_str(&array.as_primitive::<Int32Type>().value(row).to_string()),
        LogicalKind::BigInt => {
            out.push_str(&array.as_primitive::<Int64Type>().value(row).to_string())
        }
        LogicalKind::Float => out.push_str(&java_float(
            array.as_primitive::<Float32Type>().value(row) as f64,
            true,
        )),
        LogicalKind::Double => out.push_str(&java_float(
            array.as_primitive::<Float64Type>().value(row),
            false,
        )),
        LogicalKind::Decimal { .. } => {
            let DataType::Decimal128(_, scale) = array.data_type() else {
                return Err(err(format!("DECIMAL column of {}", array.data_type())));
            };
            out.push_str(&java_decimal(
                array.as_primitive::<Decimal128Type>().value(row),
                *scale,
            ))
        }
        LogicalKind::Char(_) | LogicalKind::Varchar(_) => {
            let s = match array.data_type() {
                DataType::Utf8 => array.as_string::<i32>().value(row),
                DataType::LargeUtf8 => array.as_string::<i64>().value(row),
                DataType::Utf8View => array.as_string_view().value(row),
                other => return Err(err(format!("STRING column of {other}"))),
            };
            write_json_string(&mut out, s)
        }
        LogicalKind::Binary(_) | LogicalKind::Varbinary(_) => {
            let b = array.as_binary::<i32>().value(row);
            write_json_string(
                &mut out,
                &base64::engine::general_purpose::STANDARD.encode(b),
            )
        }
        LogicalKind::Date => write_json_string(
            &mut out,
            &date_text(array.as_primitive::<Date32Type>().value(row) as i64),
        ),
        LogicalKind::Time(_) => write_json_string(
            &mut out,
            &time_text(
                array.as_primitive::<Time32MillisecondType>().value(row) as i64,
                0,
            ),
        ),
        LogicalKind::Timestamp { .. } | LogicalKind::TimestampLtz { .. } => {
            let s = array.as_struct();
            let millis = s.column(0).as_primitive::<Int64Type>().value(row);
            let nanos = s.column(1).as_primitive::<Int32Type>().value(row) as i64;
            let days = millis.div_euclid(86_400_000);
            let ms = millis.rem_euclid(86_400_000);
            let separator = match format {
                TimestampFormat::Sql => ' ',
                TimestampFormat::Iso8601 => 'T',
            };
            let mut text = format!("{}{separator}{}", date_text(days), time_text(ms, nanos));
            if matches!(ty.kind, LogicalKind::TimestampLtz { .. }) {
                text.push('Z');
            }
            write_json_string(&mut out, &text)
        }
        LogicalKind::IntervalYearMonth { .. } => {
            out.push_str(&array.as_primitive::<Int32Type>().value(row).to_string())
        }
        LogicalKind::IntervalDayTime { .. } => {
            out.push_str(&array.as_primitive::<Int64Type>().value(row).to_string())
        }
        LogicalKind::Row(fields) => {
            let s = array.as_struct();
            out.push('{');
            for (i, f) in fields.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_json_string(&mut out, &f.name);
                out.push(':');
                out.push_str(&value_text_in(s.column(i), row, &f.ty, format)?);
            }
            out.push('}');
        }
        LogicalKind::Array(element) => {
            let list = array.as_list::<i32>();
            let items = list.value(row);
            out.push('[');
            for i in 0..items.len() {
                if i > 0 {
                    out.push(',');
                }
                out.push_str(&value_text_in(&items, i, element, format)?);
            }
            out.push(']');
        }
        LogicalKind::Map(..) | LogicalKind::Multiset(_) => {
            let (key_ty, value_ty) = match &ty.kind {
                LogicalKind::Map(k, v) => ((**k).clone(), (**v).clone()),
                LogicalKind::Multiset(e) => ((**e).clone(), LogicalType::int().not_null()),
                _ => unreachable!(),
            };
            let map = array.as_map();
            let entries = map.value(row);
            out.push('{');
            for i in 0..entries.len() {
                if i > 0 {
                    out.push(',');
                }
                let key = value_text_in(entries.column(0), i, &key_ty, format)?;
                // Object keys are strings: a string key as is, any other key as its text.
                if key.starts_with('"') {
                    out.push_str(&key);
                } else {
                    write_json_string(&mut out, &key);
                }
                out.push(':');
                out.push_str(&value_text_in(entries.column(1), i, &value_ty, format)?);
            }
            out.push('}');
        }
        _ => {
            return Err(err(format!(
                "the json format of Loams Stream does not write {ty} columns yet"
            )));
        }
    }
    Ok(out)
}

/// One row as a JSON object, fields in column order.
pub fn row_json(
    batch: &RecordBatch,
    row: usize,
    columns: &[Column],
) -> Result<String, FormatError> {
    let mut out = String::from("{");
    for (i, (name, ty)) in columns.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        write_json_string(&mut out, name);
        out.push(':');
        out.push_str(&value_text(batch.column(i), row, ty)?);
    }
    out.push('}');
    Ok(out)
}

/// The row kinds of `batch` (all inserts when it has no row-kind column).
pub fn row_kinds(batch: &RecordBatch) -> Vec<RowKind> {
    use arrow::array::AsArray;
    match batch.column_by_name(ROW_KIND_COLUMN) {
        Some(c) => c
            .as_primitive::<arrow::datatypes::Int8Type>()
            .values()
            .iter()
            .map(|k| RowKind::from_i8(*k))
            .collect(),
        None => vec![RowKind::Insert; batch.num_rows()],
    }
}

/// Encodes a batch as `json` lines; the stream must be insert-only.
pub fn encode_json(batch: &RecordBatch, columns: &[Column]) -> Result<Vec<String>, FormatError> {
    let kinds = row_kinds(batch);
    (0..batch.num_rows())
        .map(|r| {
            if kinds[r] != RowKind::Insert {
                return Err(err(format!(
                    "The json format only supports insert-only changes, got {}",
                    kinds[r].short()
                )));
            }
            row_json(batch, r, columns)
        })
        .collect()
}

/// Encodes a batch as `debezium-json` lines: `-U` and `-D` as `d` events, `+I` and `+U` as `c`
/// (Flink's `DebeziumJsonSerializationSchema`).
pub fn encode_debezium(
    batch: &RecordBatch,
    columns: &[Column],
) -> Result<Vec<String>, FormatError> {
    let kinds = row_kinds(batch);
    (0..batch.num_rows())
        .map(|r| {
            let row = row_json(batch, r, columns)?;
            Ok(match kinds[r] {
                RowKind::Insert | RowKind::UpdateAfter => {
                    format!("{{\"before\":null,\"after\":{row},\"op\":\"c\"}}")
                }
                RowKind::UpdateBefore | RowKind::Delete => {
                    format!("{{\"before\":{row},\"after\":null,\"op\":\"d\"}}")
                }
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cols() -> Vec<Column> {
        vec![
            ("a".into(), LogicalType::bigint()),
            ("p".into(), LogicalType::decimal(23, 3)),
            ("ts".into(), LogicalType::timestamp(3)),
            ("s".into(), LogicalType::string()),
        ]
    }

    #[test]
    fn json_round_trips_in_flinks_text() {
        let text = "{\"a\":1,\"p\":29537240.908,\"ts\":\"2026-01-01 07:59:30.060\",\"s\":\"x\\\\y\"}\n\
                    {\"a\":null,\"p\":2951E+1,\"ts\":\"2023-12-16 01:01:00\"}\n";
        let batch = decode_json(text, &cols()).unwrap();
        assert_eq!(batch.num_rows(), 2);
        let lines = encode_json(&batch, &cols()).unwrap();
        assert_eq!(
            lines[0],
            "{\"a\":1,\"p\":29537240.908,\"ts\":\"2026-01-01 07:59:30.06\",\"s\":\"x\\\\y\"}"
        );
        assert_eq!(
            lines[1],
            "{\"a\":null,\"p\":2.951E+4,\"ts\":\"2023-12-16 01:01:00\",\"s\":null}"
        );
    }

    #[test]
    fn nested_columns_round_trip() {
        use loams_stream_sql::types::LogicalRowField;
        let c = vec![
            (
                "r".to_owned(),
                LogicalType::row(vec![
                    LogicalRowField::new("id", LogicalType::bigint().not_null()),
                    LogicalRowField::new("tags", LogicalType::array(LogicalType::string())),
                ]),
            ),
            (
                "m".to_owned(),
                LogicalType::new(LogicalKind::Map(
                    Box::new(LogicalType::string()),
                    Box::new(LogicalType::int()),
                )),
            ),
        ];
        let text =
            "{\"r\":{\"id\":7,\"tags\":[\"a\",null]},\"m\":{\"k\":1}}\n{\"r\":null,\"m\":null}\n";
        let batch = decode_json(text, &c).unwrap();
        let lines = encode_json(&batch, &c).unwrap();
        assert_eq!(
            lines[0],
            "{\"r\":{\"id\":7,\"tags\":[\"a\",null]},\"m\":{\"k\":1}}"
        );
        assert_eq!(lines[1], "{\"r\":null,\"m\":null}");
    }

    #[test]
    fn debezium_updates_become_retractions() {
        let c = vec![("k".to_owned(), LogicalType::int())];
        let text = "{\"before\":null,\"after\":{\"k\":1},\"op\":\"c\"}\n\
                    {\"before\":{\"k\":1},\"after\":{\"k\":2},\"op\":\"u\"}\n\
                    {\"before\":{\"k\":2},\"after\":null,\"op\":\"d\"}\n";
        let batch = decode_debezium(text, &c).unwrap();
        let kinds: Vec<&str> = row_kinds(&batch).iter().map(|k| k.short()).collect();
        assert_eq!(kinds, ["+I", "-U", "+U", "-D"]);
        let out = encode_debezium(&batch, &c).unwrap();
        assert_eq!(out[1], "{\"before\":{\"k\":1},\"after\":null,\"op\":\"d\"}");
        assert_eq!(out[2], "{\"before\":null,\"after\":{\"k\":2},\"op\":\"c\"}");
    }

    #[test]
    fn numbers_render_as_java_does() {
        assert_eq!(java_float(1.0, false), "1.0");
        assert_eq!(java_float(0.001, false), "0.001");
        assert_eq!(java_float(0.0001, false), "1.0E-4");
        assert_eq!(java_float(1e7, false), "1.0E7");
        assert_eq!(java_float(1234567.0, false), "1234567.0");
        assert_eq!(java_float(-2.5, false), "-2.5");
        assert_eq!(java_float(0.1f32 as f64, true), "0.1");
        assert_eq!(java_decimal(425_593_220, 3), "425593.22");
        assert_eq!(java_decimal(29_510_000, 3), "2.951E+4");
        assert_eq!(java_decimal(908_000, 3), "908");
        assert_eq!(java_decimal(5, 9), "5E-9");
        assert_eq!(java_decimal(-15, 4), "-0.0015");
        assert_eq!(java_decimal(0, 2), "0");
    }

    #[test]
    fn decimals_round_half_up_and_overflow_to_null() {
        assert_eq!(parse_decimal("1.2345", 10, 3), Some(Some(1235)));
        assert_eq!(parse_decimal("-1.2345", 10, 3), Some(Some(-1235)));
        assert_eq!(parse_decimal("1E+3", 10, 0), Some(Some(1000)));
        assert_eq!(parse_decimal("123456", 5, 0), Some(None));
        assert_eq!(parse_decimal("abc", 5, 0), None);
    }

    #[test]
    fn dates_and_times_round_trip() {
        assert_eq!(parse_date("1970-01-01"), Some(0));
        assert_eq!(
            date_text(parse_date("2024-02-29").unwrap() as i64),
            "2024-02-29"
        );
        assert_eq!(parse_date("2023-02-29"), None);
        assert_eq!(
            parse_timestamp("2023-12-16 01:01:01.000000123"),
            Some((1_702_688_461_000, 123))
        );
        assert_eq!(time_text(3_661_000, 0), "01:01:01");
        assert_eq!(time_text(3_661_500, 0), "01:01:01.5");
    }
}
