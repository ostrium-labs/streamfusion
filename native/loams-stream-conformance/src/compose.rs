//! Starts and stops the oracle (`dev/oracle/compose.yaml`) with Podman.
//!
//! The compose command is `podman compose` unless `LOAMS_ORACLE_COMPOSE`
//! names another (e.g. `podman-compose`). `down` removes the project's
//! containers and network only; it never prunes images or volumes.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

use crate::error::{Error, Result};

/// The compose project name.
pub const PROJECT: &str = "loams-oracle";

/// The oracle's compose stack.
#[derive(Debug, Clone)]
pub struct Oracle {
    /// `dev/oracle/compose.yaml`.
    pub compose_file: PathBuf,
    /// The SQL Gateway, e.g. `http://127.0.0.1:18083`.
    pub gateway_url: String,
    /// The JobManager's REST API, e.g. `http://127.0.0.1:18081`.
    pub rest_url: String,
}

impl Oracle {
    /// The stack in `compose_file` on the default (or `ORACLE_*_PORT`) ports.
    pub fn new(compose_file: impl Into<PathBuf>) -> Self {
        let port = |var: &str, default: u16| {
            std::env::var(var)
                .ok()
                .and_then(|v| v.parse::<u16>().ok())
                .unwrap_or(default)
        };
        Self {
            compose_file: compose_file.into(),
            gateway_url: format!("http://127.0.0.1:{}", port("ORACLE_GATEWAY_PORT", 18083)),
            rest_url: format!("http://127.0.0.1:{}", port("ORACLE_REST_PORT", 18081)),
        }
    }

    /// The directory mounted at `/oracle/work`.
    pub fn work_dir(&self) -> PathBuf {
        self.compose_file
            .parent()
            .unwrap_or(Path::new("."))
            .join("work")
    }

    fn compose(&self, args: &[&str]) -> Result<()> {
        let cmd = std::env::var("LOAMS_ORACLE_COMPOSE").unwrap_or_else(|_| "podman compose".into());
        let mut parts = cmd.split_whitespace();
        let program = parts
            .next()
            .ok_or_else(|| Error::Invalid("LOAMS_ORACLE_COMPOSE is empty".into()))?;
        let file = self.compose_file.display().to_string();
        let status = Command::new(program)
            .args(parts)
            .args(["-p", PROJECT, "-f", &file])
            .args(args)
            .status()
            .map_err(|e| Error::io(program, e))?;
        if status.success() {
            Ok(())
        } else {
            Err(Error::Invalid(format!(
                "{cmd} {} failed: {status}",
                args.join(" ")
            )))
        }
    }

    /// Creates the work directory and starts the stack.
    ///
    /// # Errors
    ///
    /// [`Error::Io`] or a failing compose command.
    pub fn up(&self) -> Result<()> {
        let work = self.work_dir();
        std::fs::create_dir_all(&work).map_err(|e| Error::io(work.display(), e))?;
        // `up -d` is idempotent; a second try starts whatever a transient
        // runtime error left in `Created`.
        let mut last = Ok(());
        for _ in 0..3 {
            last = self.compose(&["up", "-d"]);
            if last.is_ok() {
                break;
            }
        }
        last
    }

    /// Stops and removes the stack's containers and network.
    ///
    /// # Errors
    ///
    /// A failing compose command.
    pub fn down(&self) -> Result<()> {
        self.compose(&["down"])
    }

    /// Whether the gateway answers and a TaskManager has registered.
    pub async fn is_ready(&self) -> bool {
        let Ok(http) = reqwest::Client::builder()
            .timeout(Duration::from_secs(3))
            .build()
        else {
            return false;
        };
        let ok = |r: reqwest::Result<reqwest::Response>| r.ok().filter(|r| r.status().is_success());
        if ok(http
            .get(format!("{}/v1/info", self.gateway_url))
            .send()
            .await)
        .is_none()
        {
            return false;
        }
        let Some(resp) = ok(http
            .get(format!("{}/taskmanagers", self.rest_url))
            .send()
            .await)
        else {
            return false;
        };
        let Ok(body) = resp.bytes().await else {
            return false;
        };
        serde_json::from_slice::<serde_json::Value>(&body)
            .ok()
            .and_then(|v| v.get("taskmanagers")?.as_array().map(|a| !a.is_empty()))
            .unwrap_or(false)
    }

    /// Waits until [`Oracle::is_ready`].
    ///
    /// # Errors
    ///
    /// [`Error::Timeout`].
    pub async fn wait_ready(&self, timeout: Duration) -> Result<()> {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if self.is_ready().await {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        Err(Error::Timeout(format!(
            "the oracle at {} was not ready after {timeout:?}",
            self.gateway_url
        )))
    }
}
