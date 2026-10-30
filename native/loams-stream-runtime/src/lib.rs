//! The task manager: tasks, operator chains, the network between workers, timers, watermarks and
//! checkpoint barriers.
//!
//! Part of Loams Stream, the Apache Flink-compatible engine (Loams design 82). **Preview**: the
//! operator [`Driver`] (FC1 Task 2) and a single-threaded executor of a planned job over bounded
//! sources ([`local`], FC1 Task 10) exist; task managers, the network and checkpoints come with
//! Tasks 11 and 13.

pub mod driver;
pub mod enforcer;
pub mod local;

pub use driver::{Driver, DriverError};
