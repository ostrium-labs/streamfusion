//! Verdicts, `triage.jsonl` and the summary table.

use std::collections::BTreeMap;
use std::fmt;
use std::path::Path;

use serde::Serialize;

use crate::error::{Error, Result};
use crate::normalise::Difference;

/// What became of a case on a pair of sides.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum Verdict {
    /// Equal under the case's rules.
    Match,
    /// Both answered and the answers differ.
    Mismatch,
    /// Both sides failed.
    ErrorBoth,
    /// One side failed.
    ErrorOne,
    /// Record mode: the oracle's result was written to `expected.jsonl`.
    Recorded,
    /// Nothing to compare with: one target and no `expected.jsonl`.
    NoExpectation,
}

impl Verdict {
    /// The verdict's name as written in `triage.jsonl`.
    pub fn as_str(self) -> &'static str {
        match self {
            Verdict::Match => "match",
            Verdict::Mismatch => "mismatch",
            Verdict::ErrorBoth => "error-both",
            Verdict::ErrorOne => "error-one",
            Verdict::Recorded => "recorded",
            Verdict::NoExpectation => "no-expectation",
        }
    }

    /// Whether the verdict is a pass.
    pub fn is_pass(self) -> bool {
        matches!(self, Verdict::Match | Verdict::Recorded)
    }
}

impl fmt::Display for Verdict {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One line of `triage.jsonl`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Triage {
    /// The case.
    pub case: String,
    /// The two sides compared: the reference (`expected` or a target) and
    /// the target under test.
    pub targets: Vec<String>,
    /// The verdict.
    pub verdict: Verdict,
    /// The first difference, for `mismatch`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub first_difference: Option<Difference>,
    /// Errors by side.
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub errors: BTreeMap<String, String>,
    /// Row counts by side, before normalisation.
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub rows: BTreeMap<String, usize>,
    /// Wall-clock time on the target under test, in ms.
    pub millis: u128,
}

/// A run's triage lines.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Report {
    /// One line per compared pair.
    pub lines: Vec<Triage>,
}

impl Report {
    /// Counts per verdict.
    pub fn counts(&self) -> BTreeMap<Verdict, usize> {
        let mut m = BTreeMap::new();
        for l in &self.lines {
            *m.entry(l.verdict).or_default() += 1;
        }
        m
    }

    /// Whether every line passed.
    pub fn all_passed(&self) -> bool {
        self.lines.iter().all(|l| l.verdict.is_pass())
    }

    /// `triage.jsonl`.
    pub fn to_jsonl(&self) -> String {
        self.lines
            .iter()
            .map(|l| serde_json::to_string(l).expect("triage serializes") + "\n")
            .collect()
    }

    /// The summary as a Markdown table.
    pub fn summary(&self) -> String {
        let mut out =
            String::from("| Case | Sides | Verdict | Rows | Detail |\n|---|---|---|---|---|\n");
        for l in &self.lines {
            let rows = l
                .rows
                .iter()
                .map(|(k, v)| format!("{k}: {v}"))
                .collect::<Vec<_>>()
                .join(", ");
            let detail = match (&l.first_difference, l.errors.is_empty()) {
                (Some(d), _) => d.to_string(),
                (None, false) => l
                    .errors
                    .iter()
                    .map(|(k, v)| format!("{k}: {v}"))
                    .collect::<Vec<_>>()
                    .join("; "),
                (None, true) => String::new(),
            };
            out.push_str(&format!(
                "| {} | {} | {} | {} | {} |\n",
                l.case,
                l.targets.join(" vs "),
                l.verdict,
                rows,
                detail.replace('|', "\\|").replace('\n', " ")
            ));
        }
        let counts = self
            .counts()
            .iter()
            .map(|(k, v)| format!("{v} {k}"))
            .collect::<Vec<_>>()
            .join(", ");
        out.push_str(&format!("\n{} lines: {counts}\n", self.lines.len()));
        out
    }

    /// Writes `triage.jsonl` and `summary.md` into `dir`.
    ///
    /// # Errors
    ///
    /// [`Error::Io`].
    pub fn write(&self, dir: &Path) -> Result<()> {
        std::fs::create_dir_all(dir).map_err(|e| Error::io(dir.display(), e))?;
        let t = dir.join("triage.jsonl");
        std::fs::write(&t, self.to_jsonl()).map_err(|e| Error::io(t.display(), e))?;
        let s = dir.join("summary.md");
        std::fs::write(&s, self.summary()).map_err(|e| Error::io(s.display(), e))
    }
}
