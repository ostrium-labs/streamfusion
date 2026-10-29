//! Parse errors (D2268: unsupported syntax fails loudly and names the feature).

use std::fmt;

use sqlparser::parser::ParserError;
use sqlparser::tokenizer::{Location, Span, TokenizerError};

/// Where an unsupported feature stands in Loams Stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FeatureStatus {
    /// Planned for the named phase of track FC (design 82, §16.1).
    Planned(&'static str),
    /// Not built (design 82, §16.4, D2274).
    NotBuilt,
}

/// A Flink SQL feature the parser recognises but Loams Stream does not accept yet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Feature {
    /// The feature as the error message names it, e.g. `ADD JAR`.
    pub name: &'static str,
    pub status: FeatureStatus,
}

impl Feature {
    pub const ADD_JAR: Feature = Feature {
        name: "ADD JAR",
        status: FeatureStatus::Planned("FC2"),
    };
    pub const REMOVE_JAR: Feature = Feature {
        name: "REMOVE JAR",
        status: FeatureStatus::Planned("FC2"),
    };
    pub const HIVE_DIALECT: Feature = Feature {
        name: "Hive dialect",
        status: FeatureStatus::NotBuilt,
    };
    pub const MATERIALIZED_TABLE: Feature = Feature {
        name: "materialized tables (FLIP-435)",
        status: FeatureStatus::Planned("FC3"),
    };
    pub const MODEL: Feature = Feature {
        name: "models (CREATE MODEL and ML_PREDICT)",
        status: FeatureStatus::Planned("FC3"),
    };
    pub const MATCH_RECOGNIZE_WITHIN: Feature = Feature {
        name: "MATCH_RECOGNIZE ... WITHIN",
        status: FeatureStatus::Planned("FC2"),
    };
    pub const JSON_FUNCTION_CLAUSES: Feature = Feature {
        name: "SQL/JSON function clauses (RETURNING, ON EMPTY, ON ERROR, KEY ... VALUE, ... ON NULL, WRAPPER)",
        status: FeatureStatus::Planned("FC1"),
    };

    /// Every feature the parser can name.
    pub const ALL: [Feature; 7] = [
        Feature::ADD_JAR,
        Feature::REMOVE_JAR,
        Feature::HIVE_DIALECT,
        Feature::MATERIALIZED_TABLE,
        Feature::MODEL,
        Feature::MATCH_RECOGNIZE_WITHIN,
        Feature::JSON_FUNCTION_CLAUSES,
    ];
}

/// A parse failure. Every error carries the source span it refers to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParseError {
    /// The text is not Flink SQL (Flink's own parser would answer `SqlParseException`).
    Syntax { message: String, span: Span },
    /// The text is Flink SQL that Loams Stream does not accept yet (D2268).
    Unsupported { feature: Feature, span: Span },
}

impl ParseError {
    pub fn syntax(message: impl Into<String>, span: Span) -> Self {
        ParseError::Syntax {
            message: message.into(),
            span,
        }
    }

    pub fn unsupported(feature: Feature, span: Span) -> Self {
        ParseError::Unsupported { feature, span }
    }

    pub fn span(&self) -> Span {
        match self {
            ParseError::Syntax { span, .. } | ParseError::Unsupported { span, .. } => *span,
        }
    }

    /// The feature an `Unsupported` error names.
    pub fn feature(&self) -> Option<&Feature> {
        match self {
            ParseError::Unsupported { feature, .. } => Some(feature),
            ParseError::Syntax { .. } => None,
        }
    }

    pub(crate) fn from_parser(error: ParserError, fallback: Span) -> Self {
        match error {
            ParserError::TokenizerError(message) | ParserError::ParserError(message) => {
                let (message, span) = split_location(&message, fallback);
                if let Some(name) = message.strip_prefix(crate::dialect::UNSUPPORTED_MARK)
                    && let Some(feature) = Feature::ALL.iter().find(|f| f.name == name)
                {
                    return ParseError::Unsupported {
                        feature: *feature,
                        span,
                    };
                }
                ParseError::Syntax { message, span }
            }
            ParserError::RecursionLimitExceeded => {
                ParseError::syntax("statement nests too deeply", fallback)
            }
        }
    }

    pub(crate) fn from_tokenizer(error: TokenizerError) -> Self {
        let span = Span::new(error.location, error.location);
        ParseError::Syntax {
            message: format!("Lexical error: {}", error.message),
            span,
        }
    }
}

/// sqlparser appends " at Line: L, Column: C" to its messages; turn it into a span.
fn split_location(message: &str, fallback: Span) -> (String, Span) {
    if let Some(at) = message.rfind(" at Line: ") {
        let tail = &message[at + " at Line: ".len()..];
        if let Some((line, column)) = tail.split_once(", Column: ")
            && let (Ok(line), Ok(column)) = (line.trim().parse(), column.trim().parse())
        {
            let location = Location::new(line, column);
            let text = message[..at].trim_start_matches("sql parser error: ");
            return (text.to_string(), Span::new(location, location));
        }
    }
    (message.to_string(), fallback)
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParseError::Syntax { message, span } => write!(
                f,
                "SQL parse failed. {message} at line {}, column {}",
                span.start.line, span.start.column
            ),
            ParseError::Unsupported { feature, span } => {
                write!(
                    f,
                    "{} is not supported by Loams Stream (preview)",
                    feature.name
                )?;
                match feature.status {
                    FeatureStatus::Planned(phase) => write!(f, "; planned for phase {phase}")?,
                    FeatureStatus::NotBuilt => write!(f, "; not built (D2274)")?,
                }
                write!(
                    f,
                    ". See the Loams Stream compatibility profile. (line {}, column {})",
                    span.start.line, span.start.column
                )
            }
        }
    }
}

impl std::error::Error for ParseError {}
