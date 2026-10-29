//! Planning errors: Flink's validation errors, and named features Loams Stream does not plan yet
//! (D2268).

use loams_stream_sql::binder::BindError;
use loams_stream_sql::{Feature, FeatureStatus, ParseError};

/// Why a statement did not plan.
#[derive(Debug, Clone, PartialEq)]
pub enum PlanError {
    /// The statement does not parse.
    Parse(String),
    /// The binder rejected the statement: a validation error worded as Flink's, or a feature the
    /// binder names.
    Bind(BindError),
    /// Valid Flink SQL the planner does not support yet. `feature` names the construct; `phase`
    /// is where it is planned (`FC1` means in FC1's scope but not built yet).
    Unsupported {
        feature: String,
        phase: &'static str,
    },
    /// The statement is invalid in a way Flink's planner reports (not the validator).
    Invalid(String),
}

impl PlanError {
    pub fn unsupported(feature: impl Into<String>, phase: &'static str) -> Self {
        PlanError::Unsupported {
            feature: feature.into(),
            phase,
        }
    }

    /// An FC1 construct the planner does not lower yet.
    pub fn not_yet(feature: impl Into<String>) -> Self {
        PlanError::unsupported(feature, "FC1")
    }

    pub fn invalid(message: impl Into<String>) -> Self {
        PlanError::Invalid(message.into())
    }

    /// The named feature, if this error names one.
    pub fn feature(&self) -> Option<(String, &'static str)> {
        match self {
            PlanError::Unsupported { feature, phase } => Some((feature.clone(), phase)),
            PlanError::Bind(BindError {
                feature: Some(f), ..
            }) => Some((f.name.to_owned(), feature_phase(f))),
            _ => None,
        }
    }
}

fn feature_phase(f: &Feature) -> &'static str {
    match f.status {
        FeatureStatus::Planned(phase) => phase,
        FeatureStatus::NotBuilt => "not built",
    }
}

impl std::fmt::Display for PlanError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PlanError::Parse(m) | PlanError::Invalid(m) => f.write_str(m),
            PlanError::Bind(e) => write!(f, "{e}"),
            PlanError::Unsupported { feature, phase } => write!(
                f,
                "{feature} is not supported by Loams Stream yet (planned for {phase})"
            ),
        }
    }
}

impl std::error::Error for PlanError {}

impl From<BindError> for PlanError {
    fn from(e: BindError) -> Self {
        PlanError::Bind(e)
    }
}

impl From<ParseError> for PlanError {
    fn from(e: ParseError) -> Self {
        PlanError::Parse(e.to_string())
    }
}

pub type PlanResult<T> = Result<T, PlanError>;
