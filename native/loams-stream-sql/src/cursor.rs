//! Small helpers over sqlparser's `Parser` for Flink's keyword-driven productions.
//!
//! Many Flink keywords (`STRING`, `BYTES`, `CATALOGS`, `WATERMARK`, ...) are not sqlparser keywords,
//! so the hooks match unquoted words by their upper-case text. A backquoted word never matches a
//! keyword, as in Flink.

use sqlparser::ast::{Ident, ObjectName, ObjectNamePart};
use sqlparser::parser::{Parser, ParserError};
use sqlparser::tokenizer::{Span, Token};

pub(crate) type PResult<T> = Result<T, ParserError>;

/// The upper-case text of the `n`th next token when it is an unquoted word.
pub(crate) fn peek_word(p: &Parser, n: usize) -> Option<String> {
    match p.peek_nth_token(n).token {
        Token::Word(w) if w.quote_style.is_none() => Some(w.value.to_ascii_uppercase()),
        _ => None,
    }
}

pub(crate) fn peek_is(p: &Parser, n: usize, word: &str) -> bool {
    peek_word(p, n).as_deref() == Some(word)
}

/// True when the next tokens are exactly `words` (unquoted, case-insensitive).
pub(crate) fn peek_words(p: &Parser, words: &[&str]) -> bool {
    words.iter().enumerate().all(|(i, w)| peek_is(p, i, w))
}

/// Consumes `word` when it is next.
pub(crate) fn eat(p: &mut Parser, word: &str) -> bool {
    if peek_is(p, 0, word) {
        p.next_token();
        true
    } else {
        false
    }
}

/// Consumes `words` when they are all next; otherwise consumes nothing.
pub(crate) fn eat_all(p: &mut Parser, words: &[&str]) -> bool {
    if peek_words(p, words) {
        for _ in words {
            p.next_token();
        }
        true
    } else {
        false
    }
}

pub(crate) fn expect(p: &mut Parser, word: &str) -> PResult<Span> {
    if peek_is(p, 0, word) {
        Ok(p.next_token().span)
    } else {
        let found = p.peek_token();
        p.expected(word, found)
    }
}

pub(crate) fn expect_tok(p: &mut Parser, token: &Token) -> PResult<Span> {
    Ok(p.expect_token(token)?.span)
}

pub(crate) fn peek_tok(p: &Parser, n: usize) -> Token {
    p.peek_nth_token(n).token
}

/// The span of the next token.
pub(crate) fn next_span(p: &Parser) -> Span {
    p.peek_token_ref().span
}

/// The span of the last consumed token.
pub(crate) fn last_span(p: &Parser) -> Span {
    p.get_current_token().span
}

/// From `start` to the end of the last consumed token.
pub(crate) fn span_from(p: &Parser, start: Span) -> Span {
    Span::new(start.start, last_span(p).end)
}

/// A single-quoted string literal (Calcite's `QUOTED_STRING`).
pub(crate) fn string_literal(p: &mut Parser) -> PResult<(String, Span)> {
    let token = p.next_token();
    match token.token {
        Token::SingleQuotedString(s) => Ok((s, token.span)),
        _ => p.expected("a string literal", token),
    }
}

/// Calcite's `SimpleIdentifier`: one, possibly backquoted, identifier. Unlike sqlparser's
/// `parse_identifier`, a string literal is not an identifier (`LOAD MODULE 'dummy'` is an error).
pub(crate) fn simple_identifier(p: &mut Parser) -> PResult<Ident> {
    match p.peek_token_ref().token {
        Token::Word(_) => p.parse_identifier(),
        _ => {
            let found = p.peek_token();
            p.expected("an identifier", found)
        }
    }
}

/// Calcite's `CompoundIdentifier`: `a`, `a.b`, `a.b.c`.
pub(crate) fn compound_identifier(p: &mut Parser) -> PResult<ObjectName> {
    let mut parts = vec![ObjectNamePart::Identifier(simple_identifier(p)?)];
    while p.consume_token(&Token::Period) {
        parts.push(ObjectNamePart::Identifier(simple_identifier(p)?));
    }
    Ok(ObjectName(parts))
}

/// The identifiers of an object name.
pub(crate) fn name_parts(name: &ObjectName) -> Vec<&Ident> {
    name.0
        .iter()
        .filter_map(|part| match part {
            ObjectNamePart::Identifier(ident) => Some(ident),
            _ => None,
        })
        .collect()
}

/// The span of an object name.
pub(crate) fn name_span(name: &ObjectName) -> Span {
    Span::union_iter(name_parts(name).iter().map(|ident| ident.span))
}

/// Writes a Flink string literal, doubling single quotes.
pub(crate) fn quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

/// An identifier that renders as `text` verbatim; used to print Flink-only syntax through
/// sqlparser's `Display` (see `render`).
pub(crate) fn raw_ident(text: String, span: Span) -> Ident {
    Ident {
        value: text,
        quote_style: None,
        span,
    }
}
