//! The task manager: tasks, operator chains, the network between workers, timers, watermarks and
//! checkpoint barriers.
//!
//! Part of Loams Stream, the Apache Flink-compatible engine (Loams design 82). **Preview**: only
//! the operator [`Driver`] exists so far (FC1 Task 2); nothing here has run end to end.

pub mod driver;

pub use driver::{Driver, DriverError};
