//! `loams-stream-sql`: the Flink SQL parser of Loams Stream (preview).
//!
//! A [`FlinkDialect`] on sqlparser 0.62.0 (the version DataFusion 54.1 links), with
//! statement-level hooks for everything sqlparser lacks. The reference grammar is Apache Flink
//! 2.2.1's `flink-table/flink-sql-parser` (`parserImpls.ftl`, `Parser.tdd`) plus the regex
//! strategies of `flink-table-planner`'s `ExtendedParser` (`SET`, `RESET`, `HELP`, `CLEAR`,
//! `QUIT`/`EXIT`). Design 82, §4.1 and §7.1.
//!
//! ```
//! let statements = loams_stream_sql::parse(
//!     "SET 'execution.runtime-mode' = 'streaming';
//!      SELECT window_start, COUNT(*) FROM TABLE(
//!        TUMBLE(TABLE bid, DESCRIPTOR(ts), INTERVAL '10' SECOND)) GROUP BY window_start, window_end;",
//! )
//! .unwrap();
//! assert_eq!(statements.len(), 2);
//! ```
//!
//! Status: preview. The parser accepts the statements of Flink's SQL Client and SQL Gateway `.q`
//! suites and Nexmark q0–q22; the binder (FC1 Task 4) gives them meaning.

mod ast;
mod cursor;
mod dialect;
mod display;
mod error;
mod lexer;
mod parser;
mod render;
pub mod types;

pub use ast::*;
pub use dialect::{FlinkDialect, TableArgument, map_constructor, table_argument};
pub use error::{Feature, FeatureStatus, ParseError};
pub use render::{decode_hints, expr_to_string, query_to_string};
pub use types::{FlinkType, RowField, TypeKind, parse_type as parse_type_at, type_of_cast};

/// Re-export of the sqlparser version the AST wraps.
pub use sqlparser;

use sqlparser::parser::{Parser, ParserError};
use sqlparser::tokenizer::{Location, Span, TokenWithSpan};

use crate::dialect::UNSUPPORTED_MARK;
use crate::lexer::{StatementTokens, extract_hints, mentions_hive_ddl, split, tokenize};
use crate::parser::StatementParser;

/// One statement of a script and its source text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScriptStatement {
    /// The statement's text, without the terminating `;`.
    pub text: String,
    pub span: Span,
    /// The statement ended with `;`.
    pub terminated: bool,
}

/// Parses a script of `;`-separated statements.
pub fn parse(sql: &str) -> Result<Vec<Statement>, ParseError> {
    let index = LineIndex::new(sql);
    let tokens = tokenize(sql)?;
    split(tokens)
        .into_iter()
        .map(|statement| parse_tokens(statement, &index))
        .collect()
}

/// Parses exactly one statement (an optional trailing `;` is allowed).
pub fn parse_statement(sql: &str) -> Result<Statement, ParseError> {
    let mut statements = parse(sql)?;
    match statements.len() {
        1 => Ok(statements.remove(0)),
        0 => Err(ParseError::syntax(
            "Encountered <EOF>: expected a statement",
            Span::empty(),
        )),
        _ => Err(ParseError::syntax(
            "Encountered a second statement: expected one",
            statements[1].span(),
        )),
    }
}

/// Splits a script into statements without parsing them. Fails only on lexical errors (for
/// example an unterminated string or comment).
pub fn split_script(sql: &str) -> Result<Vec<ScriptStatement>, ParseError> {
    let index = LineIndex::new(sql);
    Ok(split(tokenize(sql)?)
        .into_iter()
        .map(|s| ScriptStatement {
            text: index.slice(s.span).to_string(),
            span: s.span,
            terminated: s.terminated,
        })
        .collect())
}

/// Parses a Flink data type, e.g. `ROW<a INT NOT NULL, b ARRAY<STRING>>`.
pub fn parse_type_str(sql: &str) -> Result<FlinkType, ParseError> {
    let tokens = tokenize(sql)?;
    let dialect = FlinkDialect;
    let mut parser = Parser::new(&dialect).with_tokens_with_locations(tokens);
    let ty =
        types::parse_type(&mut parser).map_err(|e| ParseError::from_parser(e, Span::empty()))?;
    let next = parser.peek_token();
    if next.token != sqlparser::tokenizer::Token::EOF {
        return Err(ParseError::syntax(
            format!("Encountered {}: expected end of type", next.token),
            next.span,
        ));
    }
    Ok(ty)
}

fn parse_tokens(statement: StatementTokens, index: &LineIndex) -> Result<Statement, ParseError> {
    let text = index.slice(statement.span);
    if let Some(parsed) = regex_strategies(text, statement.span) {
        return Ok(parsed);
    }
    let mut tokens: Vec<TokenWithSpan> = statement.tokens;
    let sites = extract_hints(&mut tokens)?;
    let hive = mentions_hive_ddl(&tokens);
    let dialect = FlinkDialect;
    let mut parser = Parser::new(&dialect).with_tokens_with_locations(tokens);
    let mut statement_parser = StatementParser {
        p: &mut parser,
        sites,
    };
    let result = statement_parser.statement_to_end();
    let sites = std::mem::take(&mut statement_parser.sites);
    let parsed = result.map_err(|e| ParseError::from_parser(e, statement.span));
    let parsed = match (parsed, hive) {
        (Err(ParseError::Syntax { .. }), Some(span)) => {
            return Err(ParseError::unsupported(Feature::HIVE_DIALECT, span));
        }
        (parsed, _) => parsed?,
    };
    if let Some(site) = sites.iter().find(|s| !s.used) {
        return Err(ParseError::syntax(
            "Encountered \"/*+\": a table hint must follow a table name",
            site.span,
        ));
    }
    Ok(parsed)
}

/// Flink's `ExtendedParser` strategies, matched against the whole statement text before the
/// Calcite parser runs (`SetOperationParseStrategy`, `ResetOperationParseStrategy`,
/// `HelpOperationParseStrategy`, `ClearOperationParseStrategy`, `QuitOperationParseStrategy`).
fn regex_strategies(text: &str, span: Span) -> Option<Statement> {
    let text = text.trim();
    let upper = text.to_ascii_uppercase();
    let rest_is_empty = |rest: &str| rest.trim().trim_end_matches(';').trim().is_empty();
    if upper.starts_with("HELP") && rest_is_empty(&text[4..]) {
        return Some(Statement::Help(span));
    }
    if upper.starts_with("CLEAR") && rest_is_empty(&text[5..]) {
        return Some(Statement::Clear(span));
    }
    for (word, exit) in [("QUIT", false), ("EXIT", true)] {
        if upper.starts_with(word) && rest_is_empty(&text[4..]) {
            return Some(Statement::Quit { exit, span });
        }
    }
    // RESET(\s+(?<key>[^'\s]+)\s*)?\s*;?
    if upper.starts_with("RESET") {
        let rest = &text[5..];
        if rest_is_empty(rest) {
            return Some(Statement::Reset(Reset { key: None, span }));
        }
        if rest.starts_with(char::is_whitespace) {
            let rest = rest.trim();
            let rest = rest.strip_suffix(';').unwrap_or(rest).trim_end();
            if !rest.is_empty() && !rest.contains(|c: char| c == '\'' || c.is_whitespace()) {
                return Some(Statement::Reset(Reset {
                    key: Some(Literal {
                        value: rest.to_string(),
                        span,
                    }),
                    span,
                }));
            }
        }
        return None;
    }
    // SET(\s+(?<key>[^'\s]+)\s*=\s*('(?<quotedVal>[^']*)'|(?<val>[^;\s]+)))?\s*;?
    if upper.starts_with("SET") {
        let rest = &text[3..];
        if rest_is_empty(rest) {
            return Some(Statement::Set(Set {
                assignment: None,
                span,
            }));
        }
        if !rest.starts_with(char::is_whitespace) {
            return None;
        }
        let rest = rest.trim_start();
        // Greedy key, backtracking to each '=' from the right as the regex engine would.
        let candidates: Vec<usize> = rest.match_indices('=').map(|(i, _)| i).rev().collect();
        for eq in candidates {
            let key = rest[..eq].trim_end();
            if key.is_empty() || key.contains(|c: char| c == '\'' || c.is_whitespace()) {
                continue;
            }
            // The key may not be followed by anything but whitespace before '='.
            if rest[..eq].len() != key.len() && !rest[key.len()..eq].trim().is_empty() {
                continue;
            }
            let value_part = rest[eq + 1..].trim_start();
            let end_ok = |tail: &str| {
                let tail = tail.trim_start();
                let tail = tail.strip_prefix(';').unwrap_or(tail);
                tail.is_empty()
            };
            let value = if let Some(quoted) = value_part.strip_prefix('\'') {
                match quoted.find('\'') {
                    Some(close) if end_ok(&quoted[close + 1..]) => quoted[..close].to_string(),
                    _ => continue,
                }
            } else {
                let end = value_part
                    .find(|c: char| c == ';' || c.is_whitespace())
                    .unwrap_or(value_part.len());
                if end == 0 || !end_ok(&value_part[end..]) {
                    continue;
                }
                value_part[..end].to_string()
            };
            if key.eq_ignore_ascii_case("table.sql-dialect") && value.eq_ignore_ascii_case("hive") {
                // Handled by the Calcite path's error for the quoted form; keep one behaviour.
                return None;
            }
            return Some(Statement::Set(Set {
                assignment: Some((
                    Literal {
                        value: key.to_string(),
                        span,
                    },
                    Literal { value, span },
                )),
                span,
            }));
        }
        return None;
    }
    None
}

/// Maps sqlparser locations (1-based line and character column) to byte offsets.
struct LineIndex<'a> {
    text: &'a str,
    line_starts: Vec<usize>,
}

impl<'a> LineIndex<'a> {
    fn new(text: &'a str) -> Self {
        let mut line_starts = vec![0];
        for (i, c) in text.char_indices() {
            if c == '\n' {
                line_starts.push(i + 1);
            }
        }
        LineIndex { text, line_starts }
    }

    fn offset(&self, location: Location) -> usize {
        let line = location.line.max(1) as usize - 1;
        let Some(&start) = self.line_starts.get(line) else {
            return self.text.len();
        };
        let column = location.column.max(1) as usize - 1;
        self.text[start..]
            .char_indices()
            .nth(column)
            .map(|(i, _)| start + i)
            .unwrap_or(self.text.len())
    }

    fn slice(&self, span: Span) -> &'a str {
        let start = self.offset(span.start);
        let end = self.offset(span.end).max(start);
        &self.text[start..end]
    }
}

impl ParseError {
    pub(crate) fn into_parser_error(self) -> ParserError {
        match self {
            ParseError::Syntax { message, span } => ParserError::ParserError(format!(
                "{message} at Line: {}, Column: {}",
                span.start.line, span.start.column
            )),
            ParseError::Unsupported { feature, span } => ParserError::ParserError(format!(
                "{UNSUPPORTED_MARK}{} at Line: {}, Column: {}",
                feature.name, span.start.line, span.start.column
            )),
        }
    }
}
