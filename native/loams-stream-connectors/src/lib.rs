//! Sources and sinks: Kafka, Iggy, Iceberg, Postgres CDC, JDBC, the filesystem, test connectors
//! and formats.
//!
//! Part of Loams Stream, the Apache Flink-compatible engine (Loams design 82). **Preview**: the
//! `json` and `debezium-json` formats and a local `filesystem` reader and writer exist (FC1 Task
//! 10); the exactly-once filesystem sink, Kafka, Iggy and Iceberg come in later tasks.

pub mod filesystem;
pub mod format;
