//! Changelog modes: which row kinds a stream carries (Flink's `ChangelogMode`, FC1 Task 10).
//!
//! The planner infers a mode per node bottom-up, the way Flink's
//! `FlinkChangelogModeInferenceProgram` does, and checks it against what each sink accepts.

use std::fmt;

/// A set of row kinds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ChangelogMode {
    pub insert: bool,
    pub update_before: bool,
    pub update_after: bool,
    pub delete: bool,
}

impl ChangelogMode {
    /// `[I]`.
    pub const INSERT_ONLY: ChangelogMode = ChangelogMode {
        insert: true,
        update_before: false,
        update_after: false,
        delete: false,
    };
    /// `[I,UA,D]`: updates without the old row (an upsert stream, keyed).
    pub const UPSERT: ChangelogMode = ChangelogMode {
        insert: true,
        update_before: false,
        update_after: true,
        delete: true,
    };
    /// `[I,UB,UA,D]`: every change.
    pub const ALL: ChangelogMode = ChangelogMode {
        insert: true,
        update_before: true,
        update_after: true,
        delete: true,
    };

    pub fn is_insert_only(&self) -> bool {
        *self == Self::INSERT_ONLY
    }

    /// Whether the stream carries updates or deletes.
    pub fn is_updating(&self) -> bool {
        self.update_after || self.update_before || self.delete
    }

    pub fn union(self, other: ChangelogMode) -> ChangelogMode {
        ChangelogMode {
            insert: self.insert || other.insert,
            update_before: self.update_before || other.update_before,
            update_after: self.update_after || other.update_after,
            delete: self.delete || other.delete,
        }
    }
}

impl fmt::Display for ChangelogMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut kinds = vec![];
        if self.insert {
            kinds.push("I");
        }
        if self.update_before {
            kinds.push("UB");
        }
        if self.update_after {
            kinds.push("UA");
        }
        if self.delete {
            kinds.push("D");
        }
        write!(f, "[{}]", kinds.join(","))
    }
}

/// The changelog mode a source produces for its format, Flink's `DecodingFormat` modes.
pub fn source_mode(connector: &str, format: Option<&str>) -> ChangelogMode {
    match (connector, format) {
        ("upsert-kafka", _) => ChangelogMode::UPSERT,
        (_, Some("debezium-json" | "canal-json" | "maxwell-json" | "ogg-json")) => {
            ChangelogMode::ALL
        }
        _ => ChangelogMode::INSERT_ONLY,
    }
}

/// The changelog modes a sink accepts; `None` means any.
pub fn sink_accepts(connector: &str, format: Option<&str>) -> ChangelogMode {
    match (connector, format) {
        ("upsert-kafka", _) => ChangelogMode::UPSERT,
        ("print" | "blackhole" | "collect", _) => ChangelogMode::ALL,
        (_, Some("debezium-json" | "canal-json" | "maxwell-json" | "ogg-json")) => {
            ChangelogMode::ALL
        }
        _ => ChangelogMode::INSERT_ONLY,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn modes_print_as_flink_explain() {
        assert_eq!(ChangelogMode::INSERT_ONLY.to_string(), "[I]");
        assert_eq!(ChangelogMode::UPSERT.to_string(), "[I,UA,D]");
        assert_eq!(ChangelogMode::ALL.to_string(), "[I,UB,UA,D]");
    }

    #[test]
    fn debezium_sources_are_changelogs() {
        assert_eq!(
            source_mode("filesystem", Some("debezium-json")),
            ChangelogMode::ALL
        );
        assert!(source_mode("filesystem", Some("json")).is_insert_only());
    }
}
