//! Tokenizing, Flink's lexical checks, hint extraction and statement splitting.

use sqlparser::keywords::Keyword;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::{Location, Span, Token, TokenWithSpan, Tokenizer, Whitespace};

use crate::ast::{Hint, HintKey, HintOptions, Literal};
use crate::cursor::{PResult, expect_tok, next_span, peek_tok, span_from, string_literal};
use crate::dialect::FlinkDialect;
use crate::error::{Feature, ParseError};

/// Table hints found after an object name: `t /*+ OPTIONS('k' = 'v') */`.
#[derive(Debug, Clone)]
pub(crate) struct HintSite {
    /// The end of the token the hint comment follows.
    pub anchor: Location,
    pub hints: Vec<Hint>,
    pub span: Span,
    pub used: bool,
}

/// One statement of a script, as tokens.
#[derive(Debug, Clone)]
pub(crate) struct StatementTokens {
    pub tokens: Vec<TokenWithSpan>,
    pub span: Span,
    /// The statement ended with `;`.
    pub terminated: bool,
}

/// Tokenizes `sql` with Flink's lexical rules.
pub(crate) fn tokenize(sql: &str) -> Result<Vec<TokenWithSpan>, ParseError> {
    let dialect = FlinkDialect;
    let tokens = Tokenizer::new(&dialect, sql)
        .with_unescape(true)
        .tokenize_with_location()
        .map_err(ParseError::from_tokenizer)?;
    for token in &tokens {
        match &token.token {
            // Flink quotes identifiers with backticks; `"` starts no token (a lexical error).
            Token::DoubleQuotedString(_)
            | Token::TripleDoubleQuotedString(_)
            | Token::DoubleQuotedByteStringLiteral(_)
            | Token::NationalStringLiteral(_)
            | Token::DollarQuotedString(_) => {
                return Err(ParseError::syntax(
                    format!("Lexical error: Encountered {}", token.token),
                    token.span,
                ));
            }
            Token::Word(w) if w.quote_style == Some('"') => {
                return Err(ParseError::syntax(
                    "Lexical error: Encountered \"\\\"\"",
                    token.span,
                ));
            }
            _ => {}
        }
    }
    Ok(tokens)
}

fn is_whitespace(token: &Token) -> bool {
    matches!(token, Token::Whitespace(_))
}

fn word_is(token: &Token, word: &str) -> bool {
    matches!(token, Token::Word(w) if w.quote_style.is_none() && w.value.eq_ignore_ascii_case(word))
}

/// Splits a token stream into statements at top-level `;`. A `STATEMENT SET BEGIN ... END` block
/// keeps its inner `;` (`SqlStatementSet`); `CASE ... END` inside it does not close the block.
pub(crate) fn split(tokens: Vec<TokenWithSpan>) -> Vec<StatementTokens> {
    let mut statements = vec![];
    let mut current: Vec<TokenWithSpan> = vec![];
    // The last three significant tokens, to spot `STATEMENT SET BEGIN`.
    let mut recent: Vec<Token> = vec![];
    let mut in_block = false;
    let mut case_depth = 0usize;
    let finish =
        |current: &mut Vec<TokenWithSpan>, terminated: bool, out: &mut Vec<StatementTokens>| {
            let significant: Vec<&TokenWithSpan> = current
                .iter()
                .filter(|t| !is_whitespace(&t.token) && t.token != Token::EOF)
                .collect();
            if let (Some(first), Some(last)) = (significant.first(), significant.last()) {
                let span = Span::new(first.span.start, last.span.end);
                out.push(StatementTokens {
                    tokens: std::mem::take(current),
                    span,
                    terminated,
                });
            } else {
                current.clear();
            }
        };
    for token in tokens {
        if token.token == Token::EOF {
            break;
        }
        if is_whitespace(&token.token) {
            current.push(token);
            continue;
        }
        if token.token == Token::SemiColon && !in_block {
            finish(&mut current, true, &mut statements);
            recent.clear();
            continue;
        }
        if in_block {
            if word_is(&token.token, "CASE") {
                case_depth += 1;
            } else if word_is(&token.token, "END") {
                if case_depth > 0 {
                    case_depth -= 1;
                } else {
                    in_block = false;
                }
            }
        } else {
            recent.push(token.token.clone());
            if recent.len() > 3 {
                recent.remove(0);
            }
            if recent.len() == 3
                && word_is(&recent[0], "STATEMENT")
                && word_is(&recent[1], "SET")
                && word_is(&recent[2], "BEGIN")
            {
                in_block = true;
                case_depth = 0;
            }
        }
        current.push(token);
    }
    finish(&mut current, false, &mut statements);
    statements
}

/// Removes table-hint comments from a statement's tokens and parses them; checks query hints and
/// `MATCH_RECOGNIZE ... WITHIN`.
pub(crate) fn extract_hints(tokens: &mut [TokenWithSpan]) -> Result<Vec<HintSite>, ParseError> {
    let mut sites = vec![];
    let mut previous: Option<usize> = None;
    let mut match_depth: Option<usize> = None;
    let mut depth = 0usize;
    for i in 0..tokens.len() {
        match &tokens[i].token {
            Token::Whitespace(Whitespace::MultiLineComment(text)) if text.starts_with('+') => {
                let span = tokens[i].span;
                let hints = parse_hints(&text[1..], span)?;
                let after_select = previous.is_some_and(|p| {
                    matches!(&tokens[p].token, Token::Word(w) if w.keyword == Keyword::SELECT && w.quote_style.is_none())
                });
                if !after_select {
                    let anchor = previous.map(|p| tokens[p].span.end).unwrap_or(span.start);
                    sites.push(HintSite {
                        anchor,
                        hints,
                        span,
                        used: false,
                    });
                    tokens[i].token = Token::Whitespace(Whitespace::Space);
                }
            }
            Token::Whitespace(_) => {}
            token => {
                match token {
                    Token::LParen => depth += 1,
                    Token::RParen => {
                        depth = depth.saturating_sub(1);
                        if match_depth.is_some_and(|d| depth < d) {
                            match_depth = None;
                        }
                    }
                    t if word_is(t, "MATCH_RECOGNIZE") => match_depth = Some(depth + 1),
                    t if word_is(t, "WITHIN") && match_depth == Some(depth) => {
                        return Err(ParseError::unsupported(
                            Feature::MATCH_RECOGNIZE_WITHIN,
                            tokens[i].span,
                        ));
                    }
                    _ => {}
                }
                previous = Some(i);
            }
        }
    }
    Ok(sites)
}

/// Parses the text of a `/*+ ... */` comment (Calcite's `TableHints`/`SqlHint`).
fn parse_hints(text: &str, comment: Span) -> Result<Vec<Hint>, ParseError> {
    let dialect = FlinkDialect;
    let mut tokens = Tokenizer::new(&dialect, text)
        .with_unescape(true)
        .tokenize_with_location()
        .map_err(|e| shift_error(ParseError::from_tokenizer(e), comment))?;
    // Re-base the spans onto the comment: the text starts after `/*+`.
    for token in &mut tokens {
        token.span = Span::new(
            shift(token.span.start, comment),
            shift(token.span.end, comment),
        );
    }
    let mut parser = Parser::new(&dialect).with_tokens_with_locations(tokens);
    let result = (|| -> PResult<Vec<Hint>> {
        let hints = parser.parse_comma_separated(parse_hint)?;
        let next = parser.peek_token();
        if next.token != Token::EOF {
            return parser.expected("end of hint", next);
        }
        Ok(hints)
    })();
    result.map_err(|e| ParseError::from_parser(e, comment))
}

fn shift(location: Location, comment: Span) -> Location {
    if location.line <= 1 {
        Location::new(
            comment.start.line,
            comment.start.column + 3 + location.column.saturating_sub(1),
        )
    } else {
        Location::new(comment.start.line + location.line - 1, location.column)
    }
}

fn shift_error(error: ParseError, comment: Span) -> ParseError {
    match error {
        ParseError::Syntax { message, span } => ParseError::Syntax {
            message,
            span: Span::new(shift(span.start, comment), shift(span.end, comment)),
        },
        other => other,
    }
}

fn parse_hint(p: &mut Parser) -> PResult<Hint> {
    let start = next_span(p);
    let name = p.parse_identifier()?;
    let mut options = HintOptions::None;
    if p.consume_token(&Token::LParen) {
        if peek_tok(p, 0) != Token::RParen {
            let key_value = matches!(peek_tok(p, 1), Token::Eq);
            options = if key_value {
                HintOptions::KeyValues(p.parse_comma_separated(|p| {
                    let key = match peek_tok(p, 0) {
                        Token::SingleQuotedString(_) => {
                            let (value, span) = string_literal(p)?;
                            HintKey::Literal(Literal { value, span })
                        }
                        _ => HintKey::Identifier(p.parse_identifier()?),
                    };
                    expect_tok(p, &Token::Eq)?;
                    let (value, span) = string_literal(p)?;
                    Ok((key, Literal { value, span }))
                })?)
            } else if matches!(peek_tok(p, 0), Token::SingleQuotedString(_)) {
                HintOptions::Literals(p.parse_comma_separated(|p| {
                    let (value, span) = string_literal(p)?;
                    Ok(Literal { value, span })
                })?)
            } else {
                HintOptions::Identifiers(p.parse_comma_separated(|p| p.parse_identifier())?)
            };
        }
        expect_tok(p, &Token::RParen)?;
    }
    Ok(Hint {
        name,
        options,
        span: span_from(p, start),
    })
}

/// Whether a token stream mentions Hive-only DDL (`TBLPROPERTIES`, `STORED AS`, `ROW FORMAT`).
pub(crate) fn mentions_hive_ddl(tokens: &[TokenWithSpan]) -> Option<Span> {
    let significant: Vec<&TokenWithSpan> =
        tokens.iter().filter(|t| !is_whitespace(&t.token)).collect();
    for (i, token) in significant.iter().enumerate() {
        let next = significant.get(i + 1).map(|t| &t.token);
        if word_is(&token.token, "TBLPROPERTIES")
            || (word_is(&token.token, "STORED") && next.is_some_and(|n| word_is(n, "AS")))
            || (word_is(&token.token, "ROW") && next.is_some_and(|n| word_is(n, "FORMAT")))
        {
            return Some(token.span);
        }
    }
    None
}
