//! Flink's data types as the parser sees them.
//!
//! Mirrors `ExtendedDataType`, `ExtendedSqlBasicTypeName`, `CustomizedCollectionsTypeName`,
//! `ExtendedCollectionsTypeName`, `SqlMapTypeName`, `SqlRawTypeName`, `ExtendedSqlRowTypeName` and
//! `SqlStructuredTypeName` in Flink 2.2.1's `parserImpls.ftl`, plus Calcite's standard `TypeName`.
//! Types keep the spelling the binder needs (`STRING` is not `VARCHAR(n)`); Task 4 maps them onto
//! Flink's logical types.

use std::fmt;

use sqlparser::ast::{DataType, ObjectName, ObjectNamePart};
use sqlparser::parser::Parser;
use sqlparser::tokenizer::{Span, Token};

use crate::cursor::{
    PResult, compound_identifier, eat, eat_all, expect_tok, next_span, peek_is, peek_tok,
    peek_word, quote, raw_ident, simple_identifier, span_from, string_literal,
};

/// A Flink data type with its nullability and source span.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlinkType {
    pub kind: TypeKind,
    /// `false` for `NOT NULL`. Flink's default is nullable.
    pub nullable: bool,
    /// `true` when the source spelled `NULL` explicitly.
    pub explicit_null: bool,
    pub span: Span,
}

/// The type constructors of Flink 2.2.1's SQL type grammar.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TypeKind {
    Char(Option<u64>),
    Varchar(Option<u64>),
    /// `STRING`, Flink's `VARCHAR(2147483647)`.
    String,
    Boolean,
    Binary(Option<u64>),
    Varbinary(Option<u64>),
    /// `BYTES`, Flink's `VARBINARY(2147483647)`.
    Bytes,
    /// `DECIMAL`, `DEC` or `NUMERIC` with optional precision and scale.
    Decimal(Option<u64>, Option<u64>),
    TinyInt,
    SmallInt,
    Int,
    BigInt,
    Float,
    Real,
    Double,
    Date,
    Time(Option<u64>),
    Timestamp(Option<u64>),
    /// `TIMESTAMP_LTZ(p)` or `TIMESTAMP(p) WITH LOCAL TIME ZONE`.
    TimestampLtz {
        precision: Option<u64>,
        ltz_alias: bool,
    },
    /// `TIMESTAMP(p) WITH TIME ZONE` (parsed by Calcite, rejected by Flink's binder).
    TimestampTz(Option<u64>),
    /// `INTERVAL <qualifier>`, e.g. `DAY(2) TO SECOND(3)`, kept as its normalised text.
    Interval(String),
    Array(Box<FlinkType>),
    Multiset(Box<FlinkType>),
    Map(Box<FlinkType>, Box<FlinkType>),
    Row(Vec<RowField>),
    Raw {
        class: String,
        snapshot: String,
    },
    Structured {
        class: String,
        fields: Vec<RowField>,
    },
    /// A user-defined (distinct or structured) type referenced by name.
    UserDefined(ObjectName),
}

/// A `ROW` or `STRUCTURED` field: `name type ['comment']`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowField {
    pub name: sqlparser::ast::Ident,
    pub ty: FlinkType,
    pub comment: Option<String>,
}

/// Parses a type at the parser's position (`ExtendedDataType`).
pub fn parse_type(p: &mut Parser) -> PResult<FlinkType> {
    let mut tp = TypeParser { p, pending_gt: 0 };
    let ty = tp.extended_data_type()?;
    if tp.pending_gt > 0 {
        // A `>>` closed one more bracket than the type opened.
        let found = tp.p.get_current_token().clone();
        return tp.p.expected("end of type", found);
    }
    Ok(ty)
}

/// Re-reads the type of a `CAST`/`TRY_CAST` whose `DataType::Custom` the dialect hook produced.
pub fn type_of_cast(data_type: &DataType) -> Option<FlinkType> {
    match data_type {
        DataType::Custom(ObjectName(parts), modifiers) if modifiers.is_empty() => {
            match &parts[..] {
                [ObjectNamePart::Identifier(ident)] if ident.quote_style.is_none() => {
                    crate::parse_type_str(&ident.value).ok()
                }
                _ => None,
            }
        }
        _ => None,
    }
}

/// The `DataType` that carries a Flink type inside sqlparser's AST (`CAST(x AS <type>)`).
pub(crate) fn as_data_type(ty: &FlinkType) -> DataType {
    DataType::Custom(
        ObjectName(vec![ObjectNamePart::Identifier(raw_ident(
            ty.to_string(),
            ty.span,
        ))]),
        vec![],
    )
}

struct TypeParser<'p, 'a> {
    p: &'p mut Parser<'a>,
    /// `>` tokens owed by a `>>` the tokenizer produced for nested `ARRAY<ARRAY<INT>>`.
    pending_gt: usize,
}

impl TypeParser<'_, '_> {
    fn extended_data_type(&mut self) -> PResult<FlinkType> {
        let start = next_span(self.p);
        let mut ty = self.type_name()?;
        // `T [NULL | NOT NULL] ARRAY` and `... MULTISET` suffixes (ExtendedCollectionsTypeName).
        loop {
            if self.pending_gt > 0 {
                break;
            }
            let (skip, element_nullable) = if peek_is(self.p, 0, "NULL") {
                (1, true)
            } else if peek_is(self.p, 0, "NOT") && peek_is(self.p, 1, "NULL") {
                (2, false)
            } else {
                (0, true)
            };
            let collection = peek_word(self.p, skip);
            if !matches!(collection.as_deref(), Some("ARRAY" | "MULTISET")) {
                break;
            }
            for _ in 0..skip {
                self.p.next_token();
            }
            self.p.next_token();
            ty.nullable = element_nullable;
            ty.explicit_null = skip == 1;
            let element = Box::new(ty);
            let kind = if collection.as_deref() == Some("ARRAY") {
                TypeKind::Array(element)
            } else {
                TypeKind::Multiset(element)
            };
            ty = FlinkType {
                kind,
                nullable: true,
                explicit_null: false,
                span: span_from(self.p, start),
            };
        }
        self.nullability(&mut ty);
        ty.span = span_from(self.p, start);
        Ok(ty)
    }

    /// `NullableOptDefaultTrue`.
    fn nullability(&mut self, ty: &mut FlinkType) {
        if self.pending_gt > 0 {
            return;
        }
        if eat_all(self.p, &["NOT", "NULL"]) {
            ty.nullable = false;
        } else if eat(self.p, "NULL") {
            ty.nullable = true;
            ty.explicit_null = true;
        }
    }

    fn precision(&mut self) -> PResult<Option<u64>> {
        if self.p.consume_token(&Token::LParen) {
            let n = self.p.parse_literal_uint()?;
            expect_tok(self.p, &Token::RParen)?;
            Ok(Some(n))
        } else {
            Ok(None)
        }
    }

    fn type_name(&mut self) -> PResult<FlinkType> {
        let start = next_span(self.p);
        let Some(word) = peek_word(self.p, 0) else {
            let found = self.p.peek_token();
            return self.p.expected("a data type", found);
        };
        let simple = |kind| Some(kind);
        let kind = match word.as_str() {
            "CHAR" | "CHARACTER" => {
                self.p.next_token();
                if eat(self.p, "VARYING") {
                    Some(TypeKind::Varchar(self.precision()?))
                } else {
                    Some(TypeKind::Char(self.precision()?))
                }
            }
            "VARCHAR" => {
                self.p.next_token();
                Some(TypeKind::Varchar(self.precision()?))
            }
            "STRING" => {
                self.p.next_token();
                simple(TypeKind::String)
            }
            "BOOLEAN" => {
                self.p.next_token();
                simple(TypeKind::Boolean)
            }
            "BINARY" => {
                self.p.next_token();
                if eat(self.p, "VARYING") {
                    Some(TypeKind::Varbinary(self.precision()?))
                } else {
                    Some(TypeKind::Binary(self.precision()?))
                }
            }
            "VARBINARY" => {
                self.p.next_token();
                Some(TypeKind::Varbinary(self.precision()?))
            }
            "BYTES" => {
                self.p.next_token();
                simple(TypeKind::Bytes)
            }
            "DECIMAL" | "DEC" | "NUMERIC" => {
                self.p.next_token();
                if self.p.consume_token(&Token::LParen) {
                    let precision = self.p.parse_literal_uint()?;
                    let scale = if self.p.consume_token(&Token::Comma) {
                        Some(self.p.parse_literal_uint()?)
                    } else {
                        None
                    };
                    expect_tok(self.p, &Token::RParen)?;
                    Some(TypeKind::Decimal(Some(precision), scale))
                } else {
                    Some(TypeKind::Decimal(None, None))
                }
            }
            "TINYINT" => {
                self.p.next_token();
                simple(TypeKind::TinyInt)
            }
            "SMALLINT" => {
                self.p.next_token();
                simple(TypeKind::SmallInt)
            }
            "INT" | "INTEGER" => {
                self.p.next_token();
                simple(TypeKind::Int)
            }
            "BIGINT" => {
                self.p.next_token();
                simple(TypeKind::BigInt)
            }
            "FLOAT" => {
                self.p.next_token();
                simple(TypeKind::Float)
            }
            "REAL" => {
                self.p.next_token();
                simple(TypeKind::Real)
            }
            "DOUBLE" => {
                self.p.next_token();
                eat(self.p, "PRECISION");
                simple(TypeKind::Double)
            }
            "DATE" => {
                self.p.next_token();
                simple(TypeKind::Date)
            }
            "TIME" => {
                self.p.next_token();
                let precision = self.precision()?;
                eat_all(self.p, &["WITHOUT", "TIME", "ZONE"]);
                Some(TypeKind::Time(precision))
            }
            "TIMESTAMP" => {
                self.p.next_token();
                let precision = self.precision()?;
                if eat_all(self.p, &["WITH", "LOCAL", "TIME", "ZONE"]) {
                    Some(TypeKind::TimestampLtz {
                        precision,
                        ltz_alias: false,
                    })
                } else if eat_all(self.p, &["WITH", "TIME", "ZONE"]) {
                    Some(TypeKind::TimestampTz(precision))
                } else {
                    eat_all(self.p, &["WITHOUT", "TIME", "ZONE"]);
                    Some(TypeKind::Timestamp(precision))
                }
            }
            "TIMESTAMP_LTZ" => {
                self.p.next_token();
                Some(TypeKind::TimestampLtz {
                    precision: self.precision()?,
                    ltz_alias: true,
                })
            }
            "INTERVAL" => {
                self.p.next_token();
                Some(TypeKind::Interval(self.interval_qualifier()?))
            }
            "ARRAY" | "MULTISET" if peek_tok(self.p, 1) == Token::Lt => {
                self.p.next_token();
                self.p.next_token();
                let element = Box::new(self.extended_data_type()?);
                self.close_angle()?;
                Some(if word == "ARRAY" {
                    TypeKind::Array(element)
                } else {
                    TypeKind::Multiset(element)
                })
            }
            "MAP" if peek_tok(self.p, 1) == Token::Lt => {
                self.p.next_token();
                self.p.next_token();
                let key = Box::new(self.extended_data_type()?);
                expect_tok(self.p, &Token::Comma)?;
                let value = Box::new(self.extended_data_type()?);
                self.close_angle()?;
                Some(TypeKind::Map(key, value))
            }
            "ROW" => {
                self.p.next_token();
                let next = self.p.next_token();
                let fields = match next.token {
                    Token::Neq => vec![],
                    Token::Lt => {
                        let fields = self.fields(true)?;
                        self.close_angle()?;
                        fields
                    }
                    Token::LParen => {
                        let fields = self.fields(false)?;
                        expect_tok(self.p, &Token::RParen)?;
                        fields
                    }
                    _ => return self.p.expected("<, <> or ( after ROW", next),
                };
                Some(TypeKind::Row(fields))
            }
            "RAW" => {
                self.p.next_token();
                expect_tok(self.p, &Token::LParen)?;
                let (class, _) = string_literal(self.p)?;
                expect_tok(self.p, &Token::Comma)?;
                let (snapshot, _) = string_literal(self.p)?;
                expect_tok(self.p, &Token::RParen)?;
                Some(TypeKind::Raw { class, snapshot })
            }
            "STRUCTURED" => {
                self.p.next_token();
                expect_tok(self.p, &Token::Lt)?;
                let (class, _) = string_literal(self.p)?;
                let fields = if self.p.consume_token(&Token::Comma) {
                    self.fields(true)?
                } else {
                    vec![]
                };
                self.close_angle()?;
                Some(TypeKind::Structured { class, fields })
            }
            _ => None,
        };
        let kind = match kind {
            Some(kind) => kind,
            None => TypeKind::UserDefined(compound_identifier(self.p)?),
        };
        Ok(FlinkType {
            kind,
            nullable: true,
            explicit_null: false,
            span: span_from(self.p, start),
        })
    }

    /// `ExtendedFieldNameTypeCommaList`: `name type ['comment'] [, ...]`, possibly empty.
    fn fields(&mut self, angle: bool) -> PResult<Vec<RowField>> {
        let mut fields = vec![];
        let at_end = |tp: &Self| {
            tp.pending_gt > 0
                || if angle {
                    matches!(peek_tok(tp.p, 0), Token::Gt | Token::ShiftRight)
                } else {
                    peek_tok(tp.p, 0) == Token::RParen
                }
        };
        if at_end(self) {
            return Ok(fields);
        }
        loop {
            let name = simple_identifier(self.p)?;
            let ty = self.extended_data_type()?;
            let comment = if self.pending_gt == 0
                && matches!(peek_tok(self.p, 0), Token::SingleQuotedString(_))
            {
                Some(string_literal(self.p)?.0)
            } else {
                None
            };
            fields.push(RowField { name, ty, comment });
            if self.pending_gt > 0 || !self.p.consume_token(&Token::Comma) {
                break;
            }
        }
        Ok(fields)
    }

    /// Consumes one `>`, splitting a `>>` token when needed.
    fn close_angle(&mut self) -> PResult<()> {
        if self.pending_gt > 0 {
            self.pending_gt -= 1;
            return Ok(());
        }
        let next = self.p.next_token();
        match next.token {
            Token::Gt => Ok(()),
            Token::ShiftRight => {
                self.pending_gt += 1;
                Ok(())
            }
            _ => self.p.expected(">", next),
        }
    }

    /// Calcite's `IntervalQualifier`, kept as normalised text.
    fn interval_qualifier(&mut self) -> PResult<String> {
        const UNITS: [&str; 6] = ["YEAR", "MONTH", "DAY", "HOUR", "MINUTE", "SECOND"];
        let mut text = String::new();
        let unit = |tp: &mut Self, text: &mut String| -> PResult<()> {
            match peek_word(tp.p, 0) {
                Some(w) if UNITS.contains(&w.as_str()) => {
                    tp.p.next_token();
                    text.push_str(&w);
                }
                _ => {
                    let found = tp.p.peek_token();
                    return tp.p.expected("an interval unit", found);
                }
            }
            if tp.p.consume_token(&Token::LParen) {
                let a = tp.p.parse_literal_uint()?;
                if tp.p.consume_token(&Token::Comma) {
                    let b = tp.p.parse_literal_uint()?;
                    text.push_str(&format!("({a}, {b})"));
                } else {
                    text.push_str(&format!("({a})"));
                }
                expect_tok(tp.p, &Token::RParen)?;
            }
            Ok(())
        };
        unit(self, &mut text)?;
        if eat(self.p, "TO") {
            text.push_str(" TO ");
            unit(self, &mut text)?;
        }
        Ok(text)
    }
}

fn opt(f: &mut fmt::Formatter<'_>, name: &str, n: &Option<u64>) -> fmt::Result {
    match n {
        Some(n) => write!(f, "{name}({n})"),
        None => f.write_str(name),
    }
}

fn fields(f: &mut fmt::Formatter<'_>, fields: &[RowField]) -> fmt::Result {
    for (i, field) in fields.iter().enumerate() {
        if i > 0 {
            f.write_str(", ")?;
        }
        write!(f, "{} {}", field.name, field.ty)?;
        if let Some(comment) = &field.comment {
            write!(f, " {}", quote(comment))?;
        }
    }
    Ok(())
}

impl fmt::Display for TypeKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TypeKind::Char(n) => opt(f, "CHAR", n),
            TypeKind::Varchar(n) => opt(f, "VARCHAR", n),
            TypeKind::String => f.write_str("STRING"),
            TypeKind::Boolean => f.write_str("BOOLEAN"),
            TypeKind::Binary(n) => opt(f, "BINARY", n),
            TypeKind::Varbinary(n) => opt(f, "VARBINARY", n),
            TypeKind::Bytes => f.write_str("BYTES"),
            TypeKind::Decimal(None, _) => f.write_str("DECIMAL"),
            TypeKind::Decimal(Some(p), None) => write!(f, "DECIMAL({p})"),
            TypeKind::Decimal(Some(p), Some(s)) => write!(f, "DECIMAL({p}, {s})"),
            TypeKind::TinyInt => f.write_str("TINYINT"),
            TypeKind::SmallInt => f.write_str("SMALLINT"),
            TypeKind::Int => f.write_str("INT"),
            TypeKind::BigInt => f.write_str("BIGINT"),
            TypeKind::Float => f.write_str("FLOAT"),
            TypeKind::Real => f.write_str("REAL"),
            TypeKind::Double => f.write_str("DOUBLE"),
            TypeKind::Date => f.write_str("DATE"),
            TypeKind::Time(n) => opt(f, "TIME", n),
            TypeKind::Timestamp(n) => opt(f, "TIMESTAMP", n),
            TypeKind::TimestampLtz {
                precision,
                ltz_alias: true,
            } => opt(f, "TIMESTAMP_LTZ", precision),
            TypeKind::TimestampLtz {
                precision,
                ltz_alias: false,
            } => {
                opt(f, "TIMESTAMP", precision)?;
                f.write_str(" WITH LOCAL TIME ZONE")
            }
            TypeKind::TimestampTz(n) => {
                opt(f, "TIMESTAMP", n)?;
                f.write_str(" WITH TIME ZONE")
            }
            TypeKind::Interval(q) => write!(f, "INTERVAL {q}"),
            TypeKind::Array(t) => write!(f, "ARRAY<{t}>"),
            TypeKind::Multiset(t) => write!(f, "MULTISET<{t}>"),
            TypeKind::Map(k, v) => write!(f, "MAP<{k}, {v}>"),
            TypeKind::Row(fs) => {
                f.write_str("ROW<")?;
                fields(f, fs)?;
                f.write_str(">")
            }
            TypeKind::Raw { class, snapshot } => {
                write!(f, "RAW({}, {})", quote(class), quote(snapshot))
            }
            TypeKind::Structured { class, fields: fs } => {
                write!(f, "STRUCTURED<{}", quote(class))?;
                if !fs.is_empty() {
                    f.write_str(", ")?;
                    fields(f, fs)?;
                }
                f.write_str(">")
            }
            TypeKind::UserDefined(name) => write!(f, "{name}"),
        }
    }
}

impl fmt::Display for FlinkType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.kind)?;
        if !self.nullable {
            f.write_str(" NOT NULL")?;
        } else if self.explicit_null {
            f.write_str(" NULL")?;
        }
        Ok(())
    }
}
