//! `loams-stream`: one binary for the Loams Stream job manager and task manager.
//!
//! Part of Loams Stream, the Apache Flink-compatible engine (Loams design 82). **Preview**: today
//! it only prints its version and the role it was started with, then exits. Nothing in it has run
//! end to end yet.

use std::process::ExitCode;

/// The roles one binary can take, as in Flink's standalone cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Role {
    JobManager,
    TaskManager,
    All,
}

impl Role {
    fn parse(value: &str) -> Option<Self> {
        match value {
            "jobmanager" => Some(Self::JobManager),
            "taskmanager" => Some(Self::TaskManager),
            "all" => Some(Self::All),
            _ => None,
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::JobManager => "jobmanager",
            Self::TaskManager => "taskmanager",
            Self::All => "all",
        }
    }
}

const USAGE: &str = "usage: loams-stream [--role jobmanager|taskmanager|all] [--version] [--help]
  --role   the process role (default: all, a job manager and a task manager in one process)";

/// What the command line asks for.
#[derive(Debug, PartialEq, Eq)]
enum Command {
    Run(Role),
    Version,
    Help,
}

fn parse(args: impl IntoIterator<Item = String>) -> Result<Command, String> {
    let mut role = Role::All;
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        let value = match arg.as_str() {
            "-h" | "--help" => return Ok(Command::Help),
            "-V" | "--version" => return Ok(Command::Version),
            "--role" => args
                .next()
                .ok_or_else(|| "--role needs a value".to_string())?,
            other => match other.strip_prefix("--role=") {
                Some(value) => value.to_string(),
                None => return Err(format!("unknown argument `{other}`")),
            },
        };
        role = Role::parse(&value).ok_or_else(|| format!("unknown role `{value}`"))?;
    }
    Ok(Command::Run(role))
}

fn version() -> String {
    format!(
        "loams-stream {} (preview; engine streamfusion {})",
        env!("CARGO_PKG_VERSION"),
        streamfusion::VERSION
    )
}

fn main() -> ExitCode {
    match parse(std::env::args().skip(1)) {
        Ok(Command::Run(role)) => {
            println!("{} role={}", version(), role.name());
            ExitCode::SUCCESS
        }
        Ok(Command::Version) => {
            println!("{}", version());
            ExitCode::SUCCESS
        }
        Ok(Command::Help) => {
            println!("{}\n{USAGE}", version());
            ExitCode::SUCCESS
        }
        Err(message) => {
            eprintln!("loams-stream: {message}\n{USAGE}");
            ExitCode::from(2)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| value.to_string()).collect()
    }

    #[test]
    fn parses_roles_and_flags() {
        assert_eq!(parse(args(&[])), Ok(Command::Run(Role::All)));
        assert_eq!(
            parse(args(&["--role", "jobmanager"])),
            Ok(Command::Run(Role::JobManager))
        );
        assert_eq!(
            parse(args(&["--role=taskmanager"])),
            Ok(Command::Run(Role::TaskManager))
        );
        assert_eq!(parse(args(&["--version"])), Ok(Command::Version));
        assert_eq!(parse(args(&["-h"])), Ok(Command::Help));
        assert!(parse(args(&["--role"])).is_err());
        assert!(parse(args(&["--role", "worker"])).is_err());
        assert!(parse(args(&["--port", "1"])).is_err());
    }
}
