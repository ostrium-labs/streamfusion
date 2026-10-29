//! `FlinkDialect`: Flink 2.2.1's lexical rules and expression forms on sqlparser.
//!
//! Lexical rules follow Flink's Calcite configuration (`FlinkSqlParserImpl`, `Lex.JAVA`-like):
//! identifiers are quoted with backticks, string literals with single quotes (doubled to escape),
//! there is no backslash escape, and double-quoted text is not a token.
//!
//! The expression hooks (`parse_prefix`, `parse_infix`) cover what sqlparser lacks:
//! - `CAST`/`TRY_CAST` to Flink types (`STRING`, `TIMESTAMP_LTZ(3)`, `ROW<...>`, `ARRAY<...>`,
//!   `MAP<...>`, `TIMESTAMP(3) WITH LOCAL TIME ZONE`, ...); see `types`.
//! - table arguments of table-valued functions, `TABLE t [PARTITION BY ...] [ORDER BY ...]`
//!   (Calcite's `TableFunctionArg`/`PartitionedQueryOrQueryOrExpr`), used by
//!   `TABLE(TUMBLE(TABLE t, DESCRIPTOR(ts), INTERVAL '5' MINUTES))` and `SESSION(TABLE t PARTITION
//!   BY k, ...)`.
//! - the `MAP[k, v, ...]` value constructor (Calcite's `MapConstructor`).
//! - SQL/JSON clauses and `IS JSON`, which Loams Stream does not parse yet: they answer a
//!   `ParseError` naming the feature instead of a misleading syntax error.

use sqlparser::ast::{
    CastKind, DuplicateTreatment, Expr, Function, FunctionArg, FunctionArgExpr,
    FunctionArgumentList, FunctionArguments, Ident, ObjectName, ObjectNamePart, OrderByExpr,
};
use sqlparser::dialect::Dialect;
use sqlparser::parser::{Parser, ParserError};
use sqlparser::tokenizer::{Span, Token};

use crate::cursor::{
    PResult, compound_identifier, eat_all, expect, expect_tok, next_span, peek_is, peek_tok,
    peek_word, raw_ident, span_from,
};
use crate::error::Feature;
use crate::types::{as_data_type, parse_type};

/// Flink SQL as Flink 2.2.1's default (non-Hive) dialect accepts it.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct FlinkDialect;

/// Prefix of a sqlparser error message that carries an unsupported feature out of a hook.
pub(crate) const UNSUPPORTED_MARK: &str = "\u{1}unsupported:";

pub(crate) fn unsupported<T>(feature: Feature, span: Span) -> PResult<T> {
    Err(ParserError::ParserError(format!(
        "{UNSUPPORTED_MARK}{} at Line: {}, Column: {}",
        feature.name, span.start.line, span.start.column
    )))
}

const JSON_FUNCTIONS: [&str; 7] = [
    "JSON_VALUE",
    "JSON_QUERY",
    "JSON_EXISTS",
    "JSON_OBJECT",
    "JSON_ARRAY",
    "JSON_OBJECTAGG",
    "JSON_ARRAYAGG",
];

impl Dialect for FlinkDialect {
    fn is_delimited_identifier_start(&self, ch: char) -> bool {
        ch == '`'
    }

    fn identifier_quote_style(&self, _identifier: &str) -> Option<char> {
        Some('`')
    }

    fn is_identifier_start(&self, ch: char) -> bool {
        ch.is_alphabetic() || ch == '_'
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        ch.is_alphanumeric() || ch == '_' || ch == '$'
    }

    // `COUNT(*) FILTER (WHERE ...)`
    fn supports_filter_during_aggregation(&self) -> bool {
        true
    }

    // `MATCH_RECOGNIZE (...)`, Flink's subset (design 82, §4.1).
    fn supports_match_recognize(&self) -> bool {
        true
    }

    // `GROUPING SETS`, `ROLLUP`, `CUBE`.
    fn supports_group_by_expr(&self) -> bool {
        true
    }

    // Query hints: `SELECT /*+ LOOKUP(...), STATE_TTL(...) */ ...`.
    fn supports_comment_optimizer_hint(&self) -> bool {
        true
    }

    // `FOR SYSTEM_TIME AS OF` (temporal and lookup joins).
    fn supports_table_versioning(&self) -> bool {
        true
    }

    // `U&'...'`
    fn supports_unicode_string_literal(&self) -> bool {
        true
    }

    // `WINDOW w AS (...)` referenced by `OVER w`.
    fn supports_window_clause_named_window_reference(&self) -> bool {
        true
    }

    // `FROM (t1 JOIN t2 ON ...)`
    fn supports_parens_around_table_factor(&self) -> bool {
        true
    }

    // `FROM (VALUES (1, 'a')) AS T(id, s)`
    fn supports_values_as_table_factor(&self) -> bool {
        true
    }

    // Calcite requires `INTERVAL '5' SECOND`, never a bare `INTERVAL '5 seconds'`.
    fn require_interval_qualifier(&self) -> bool {
        true
    }

    fn parse_prefix(&self, parser: &mut Parser) -> Option<PResult<Expr>> {
        let word = peek_word(parser, 0)?;
        match word.as_str() {
            "CAST" | "TRY_CAST" if peek_tok(parser, 1) == Token::LParen => Some(parse_cast(parser)),
            "TABLE" if matches!(peek_tok(parser, 1), Token::Word(_)) => {
                Some(parse_table_argument(parser))
            }
            "TABLE" if peek_tok(parser, 1) == Token::LParen => {
                let found = parser.peek_nth_token(1);
                Some(parser.expected("a table name after TABLE", found))
            }
            "MAP" if peek_tok(parser, 1) == Token::LBracket => Some(parse_map_constructor(parser)),
            "MAP" if peek_tok(parser, 1) == Token::LParen => {
                let found = parser.peek_nth_token(1);
                Some(parser.expected(
                    "[ after MAP (the MAP(query) constructor is not supported)",
                    found,
                ))
            }
            w if JSON_FUNCTIONS.contains(&w) && peek_tok(parser, 1) == Token::LParen => {
                json_clauses(parser).map(|span| unsupported(Feature::JSON_FUNCTION_CLAUSES, span))
            }
            _ => None,
        }
    }

    fn parse_infix(
        &self,
        parser: &mut Parser,
        _expr: &Expr,
        _precedence: u8,
    ) -> Option<PResult<Expr>> {
        let is_json = peek_is(parser, 0, "IS")
            && (peek_is(parser, 1, "JSON")
                || (peek_is(parser, 1, "NOT") && peek_is(parser, 2, "JSON")));
        if is_json {
            let span = next_span(parser);
            return Some(unsupported(Feature::JSON_FUNCTION_CLAUSES, span));
        }
        None
    }
}

/// `CAST(expr AS type)` and `TRY_CAST(expr AS type)` with Flink's type grammar.
fn parse_cast(p: &mut Parser) -> PResult<Expr> {
    let kind = if peek_is(p, 0, "TRY_CAST") {
        CastKind::TryCast
    } else {
        CastKind::Cast
    };
    p.next_token();
    expect_tok(p, &Token::LParen)?;
    let expr = p.parse_expr()?;
    expect(p, "AS")?;
    let ty = parse_type(p)?;
    expect_tok(p, &Token::RParen)?;
    Ok(Expr::Cast {
        kind,
        expr: Box::new(expr),
        data_type: as_data_type(&ty),
        array: false,
        format: None,
    })
}

/// The name a table argument is encoded under (see [`table_argument`]).
const TABLE_ARGUMENT: &str = "TABLE";
/// The name a `MAP[...]` constructor is encoded under (see [`map_constructor`]).
const MAP_CONSTRUCTOR: &str = "MAP";

fn marker_function(
    name: &str,
    span: Span,
    args: Vec<Expr>,
    within_group: Vec<OrderByExpr>,
) -> Expr {
    Expr::Function(Function {
        name: ObjectName(vec![ObjectNamePart::Identifier(raw_ident(
            name.to_string(),
            span,
        ))]),
        uses_odbc_syntax: false,
        parameters: FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None::<DuplicateTreatment>,
            args: args
                .into_iter()
                .map(|e| FunctionArg::Unnamed(FunctionArgExpr::Expr(e)))
                .collect(),
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group,
    })
}

/// `TABLE name [PARTITION BY k | PARTITION BY (k1, k2)] [ORDER BY o | ORDER BY (o1, o2)]`.
fn parse_table_argument(p: &mut Parser) -> PResult<Expr> {
    let start = next_span(p);
    p.next_token();
    let name = compound_identifier(p)?;
    let idents: Vec<Ident> = name
        .0
        .into_iter()
        .filter_map(|part| match part {
            ObjectNamePart::Identifier(ident) => Some(ident),
            _ => None,
        })
        .collect();
    let mut args = vec![if idents.len() == 1 {
        Expr::Identifier(idents.into_iter().next().unwrap())
    } else {
        Expr::CompoundIdentifier(idents)
    }];
    if eat_all(p, &["PARTITION", "BY"]) {
        if p.consume_token(&Token::LParen) {
            args.extend(p.parse_comma_separated(Parser::parse_expr)?);
            expect_tok(p, &Token::RParen)?;
        } else {
            args.push(p.parse_expr()?);
        }
    }
    let mut order_by = vec![];
    if eat_all(p, &["ORDER", "BY"]) {
        if p.consume_token(&Token::LParen) {
            order_by = p.parse_comma_separated(Parser::parse_order_by_expr)?;
            expect_tok(p, &Token::RParen)?;
        } else {
            order_by.push(p.parse_order_by_expr()?);
        }
    }
    Ok(marker_function(
        TABLE_ARGUMENT,
        span_from(p, start),
        args,
        order_by,
    ))
}

/// `MAP[k1, v1, k2, v2, ...]`.
fn parse_map_constructor(p: &mut Parser) -> PResult<Expr> {
    let start = next_span(p);
    p.next_token();
    expect_tok(p, &Token::LBracket)?;
    let args = if p.peek_token_ref().token == Token::RBracket {
        vec![]
    } else {
        p.parse_comma_separated(Parser::parse_expr)?
    };
    expect_tok(p, &Token::RBracket)?;
    Ok(marker_function(
        MAP_CONSTRUCTOR,
        span_from(p, start),
        args,
        vec![],
    ))
}

/// When a SQL/JSON call uses one of the clauses sqlparser cannot parse, the clause's span.
fn json_clauses(p: &Parser) -> Option<Span> {
    let mut depth = 0usize;
    let mut i = 1;
    loop {
        let token = p.peek_nth_token(i);
        match &token.token {
            Token::LParen => depth += 1,
            Token::RParen => {
                depth -= 1;
                if depth == 0 {
                    return None;
                }
            }
            Token::EOF | Token::SemiColon => return None,
            Token::Word(w) if w.quote_style.is_none() && depth == 1 => {
                let upper = w.value.to_ascii_uppercase();
                let clause = match upper.as_str() {
                    "RETURNING" | "KEY" | "WRAPPER" | "EMPTY" | "ERROR" | "ABSENT" => true,
                    "ON" => matches!(
                        peek_word(p, i + 1).as_deref(),
                        Some("EMPTY" | "ERROR" | "NULL")
                    ),
                    "NULL" => peek_is(p, i + 1, "ON"),
                    "FORMAT" => peek_is(p, i + 1, "JSON"),
                    _ => false,
                };
                if clause {
                    return Some(token.span);
                }
            }
            _ => {}
        }
        i += 1;
    }
}

/// A table argument of a table-valued function call, `TABLE t [PARTITION BY ...] [ORDER BY ...]`.
#[derive(Debug, Clone, Copy)]
pub struct TableArgument<'a> {
    /// `t`, as an `Identifier` or `CompoundIdentifier`.
    pub table: &'a Expr,
    pub partition_by: &'a [FunctionArg],
    pub order_by: &'a [OrderByExpr],
    pub span: Span,
}

fn marker<'a>(expr: &'a Expr, name: &str) -> Option<(&'a Function, &'a [FunctionArg])> {
    let Expr::Function(function) = expr else {
        return None;
    };
    let [ObjectNamePart::Identifier(ident)] = &function.name.0[..] else {
        return None;
    };
    if ident.quote_style.is_some() || ident.value != name {
        return None;
    }
    if !matches!(function.parameters, FunctionArguments::None) {
        return None;
    }
    match &function.args {
        FunctionArguments::List(list) => Some((function, &list.args)),
        _ => None,
    }
}

/// Reads a table argument the dialect encoded in sqlparser's AST.
pub fn table_argument(expr: &Expr) -> Option<TableArgument<'_>> {
    let (function, args) = marker(expr, TABLE_ARGUMENT)?;
    let (first, rest) = args.split_first()?;
    let FunctionArg::Unnamed(FunctionArgExpr::Expr(table)) = first else {
        return None;
    };
    let span = match &function.name.0[0] {
        ObjectNamePart::Identifier(ident) => ident.span,
        _ => Span::empty(),
    };
    Some(TableArgument {
        table,
        partition_by: rest,
        order_by: &function.within_group,
        span,
    })
}

/// Reads the arguments of a `MAP[...]` constructor the dialect encoded in sqlparser's AST.
pub fn map_constructor(expr: &Expr) -> Option<&[FunctionArg]> {
    marker(expr, MAP_CONSTRUCTOR).map(|(_, args)| args)
}
