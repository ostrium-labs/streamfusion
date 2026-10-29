//! Loams Stream's differential harness (FC1 Task 5, design 82 §3.2 and §15;
//! preview).
//!
//! It runs a corpus of Flink SQL cases on Apache Flink 2.2.1 (the oracle)
//! and on Loams Stream, both reached only through the SQL Gateway REST API,
//! and compares the results under each case's declared rules. With Loams
//! Stream not yet answering it runs in oracle-only mode and records each
//! case's `expected.jsonl`.
//!
//! - [`client`]: the SQL Gateway REST client, v1 to v4, target-agnostic.
//! - [`case`]: the case format (`case.toml`, `setup.sql`, `query.sql`,
//!   `expected.jsonl`, `data/`).
//! - [`runner`]: one case on one target, results from the gateway's result
//!   stream or from a `filesystem` sink in `debezium-json`.
//! - [`normalise`]: the normaliser's rules and the comparison.
//! - [`harness`]: record and run over a corpus.
//! - [`triage`]: verdicts, `triage.jsonl` and the summary table.
//! - [`compose`]: starting and stopping the oracle with Podman.
//! - [`nexmark`]: a deterministic port of Nexmark's event generator, for
//!   the `dev/corpus/nexmark` cases.
//!
//! The harness depends on no engine crate: no DataFusion, no Arrow, no JNI.

pub mod case;
pub mod client;
pub mod compose;
pub mod conversation;
pub mod error;
pub mod harness;
pub mod nexmark;
pub mod normalise;
pub mod profile;
pub mod result;
pub mod runner;
pub mod selfcheck;
pub mod triage;

pub use error::{Error, Result};
