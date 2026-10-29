//! `loams-stream-oracle`: the differential harness's CLI (preview).
//!
//! ```text
//! loams-stream-oracle up                         start dev/oracle/compose.yaml and wait for it
//! loams-stream-oracle down                       stop it
//! loams-stream-oracle record <corpus>            run every case on the oracle, write expected.jsonl
//! loams-stream-oracle run <corpus>               run every case, compare with expected.jsonl
//! loams-stream-oracle fixtures <dir>             record the gateway fixtures for v1..v4
//! loams-stream-oracle nexmark-gen --events N     write person.json, auction.json, bid.json
//!                                                (Nexmark events, JSON lines) into --out [.]
//!
//! options:
//!   --compose <file>          the oracle's compose file   [dev/oracle/compose.yaml]
//!   --target <name>=<url>     a target (repeatable)       [flink=http://127.0.0.1:18083]
//!   --api <v1..v4>            the API version to speak    [the highest the target offers]
//!   --shared <local>=<remote> the shared directory        [<compose dir>/work=/oracle/work]
//!   --only <substring>        only cases whose name contains it
//!   --out <dir>               where triage.jsonl and summary.md go [<shared>/reports/<run>]
//!   --keep                    keep each case's data and sink files
//!
//! nexmark-gen options:
//!   --events N                events to generate (required)
//!   --seed S                  perturbs every random seed; 0 = Beam's per-event-id seeding [0]
//!   --base-time-ms T          event time of event number 0, ms since the epoch [1767225600000]
//!   --rate R                  first-event.rate, events per second of event time [10000]
//!   --side-input ROWS         also write side_input.csv (q13's side input, ROWS lines)
//! ```
//!
//! Exit status: 0 when every case passed, 1 otherwise, 2 for bad usage.

use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use loams_stream_conformance::case::discover;
use loams_stream_conformance::client::{ApiVersion, GatewayClient, Recorder};
use loams_stream_conformance::compose::Oracle;
use loams_stream_conformance::harness::{self, RunOptions};
use loams_stream_conformance::runner::{Shared, Target};
use loams_stream_conformance::{Error, Result, conversation, nexmark};

#[derive(Default)]
struct Args {
    command: String,
    path: Option<PathBuf>,
    compose: Option<PathBuf>,
    targets: Vec<(String, String)>,
    api: Option<ApiVersion>,
    shared: Option<(PathBuf, String)>,
    only: Option<String>,
    out: Option<PathBuf>,
    keep: bool,
    events: Option<u64>,
    seed: Option<u64>,
    base_time_ms: Option<i64>,
    rate: Option<u32>,
    side_input: Option<u64>,
}

fn usage() -> &'static str {
    "usage: loams-stream-oracle <up|down|record|run|fixtures> [path] [--compose F] [--target N=URL]... \
     [--api vN] [--shared LOCAL=REMOTE] [--only S] [--out DIR] [--keep]\n       \
     loams-stream-oracle nexmark-gen --events N [--seed S] [--out DIR] [--base-time-ms T] [--rate R] \
     [--side-input ROWS]"
}

fn parse(mut it: impl Iterator<Item = String>) -> std::result::Result<Args, String> {
    let mut a = Args {
        command: it.next().ok_or("no command")?,
        ..Args::default()
    };
    while let Some(arg) = it.next() {
        let mut value = |name: &str| it.next().ok_or(format!("{name} needs a value"));
        match arg.as_str() {
            "--compose" => a.compose = Some(value("--compose")?.into()),
            "--target" => {
                let v = value("--target")?;
                let (n, u) = v.split_once('=').ok_or("--target takes NAME=URL")?;
                a.targets.push((n.to_owned(), u.to_owned()));
            }
            "--api" => {
                let v = value("--api")?;
                a.api = Some(ApiVersion::parse(&v).ok_or(format!("unknown API version {v}"))?);
            }
            "--shared" => {
                let v = value("--shared")?;
                let (l, r) = v.split_once('=').ok_or("--shared takes LOCAL=REMOTE")?;
                a.shared = Some((l.into(), r.to_owned()));
            }
            "--only" => a.only = Some(value("--only")?),
            "--out" => a.out = Some(value("--out")?.into()),
            "--keep" => a.keep = true,
            "--events" => a.events = Some(number(&value("--events")?, "--events")?),
            "--seed" => a.seed = Some(number(&value("--seed")?, "--seed")?),
            "--base-time-ms" => {
                a.base_time_ms = Some(number(&value("--base-time-ms")?, "--base-time-ms")?);
            }
            "--rate" => a.rate = Some(number(&value("--rate")?, "--rate")?),
            "--side-input" => a.side_input = Some(number(&value("--side-input")?, "--side-input")?),
            "-h" | "--help" => return Err(String::new()),
            s if s.starts_with('-') => return Err(format!("unknown option {s}")),
            s if a.path.is_none() => a.path = Some(s.into()),
            s => return Err(format!("unexpected argument {s}")),
        }
    }
    Ok(a)
}

fn number<T: std::str::FromStr>(v: &str, name: &str) -> std::result::Result<T, String> {
    v.parse()
        .map_err(|_| format!("{name} takes a number, not {v:?}"))
}

/// `nexmark-gen`: no gateway, no runtime.
fn nexmark_gen(a: &Args) -> Result<bool> {
    let events = a
        .events
        .ok_or_else(|| Error::Invalid("nexmark-gen needs --events N".into()))?;
    let rate = a.rate.unwrap_or(10_000);
    if rate == 0 {
        return Err(Error::Invalid("--rate must be positive".into()));
    }
    let config = nexmark::Config {
        events,
        seed: a.seed.unwrap_or(0),
        base_time_ms: a.base_time_ms.unwrap_or(nexmark::DEFAULT_BASE_TIME_MS),
        first_event_rate: rate,
        ..nexmark::Config::default()
    };
    let out = a.out.clone().unwrap_or_else(|| ".".into());
    let io = |e: std::io::Error| Error::Io {
        path: out.display().to_string(),
        message: e.to_string(),
    };
    let counts = nexmark::write_dataset(config, &out).map_err(io)?;
    if let Some(rows) = a.side_input {
        nexmark::write_side_input(rows, &out).map_err(io)?;
    }
    eprintln!(
        "wrote {} persons, {} auctions, {} bids to {}",
        counts.persons,
        counts.auctions,
        counts.bids,
        out.display()
    );
    Ok(true)
}

async fn client(url: &str, api: Option<ApiVersion>) -> Result<GatewayClient> {
    let c = GatewayClient::new(url, ApiVersion::V1)?;
    match api {
        Some(v) => Ok(c.with_version(v)),
        None => c.negotiate().await,
    }
}

async fn main_inner(a: Args) -> Result<bool> {
    let oracle = Oracle::new(
        a.compose
            .clone()
            .unwrap_or_else(|| "dev/oracle/compose.yaml".into()),
    );
    let (shared_local, shared_remote) = a
        .shared
        .clone()
        .unwrap_or_else(|| (oracle.work_dir(), "/oracle/work".to_owned()));
    let target_specs = if a.targets.is_empty() {
        vec![("flink".to_owned(), oracle.gateway_url.clone())]
    } else {
        a.targets.clone()
    };
    match a.command.as_str() {
        "up" => {
            oracle.up()?;
            oracle.wait_ready(Duration::from_secs(180)).await?;
            eprintln!("oracle ready: {}", oracle.gateway_url);
            Ok(true)
        }
        "down" => {
            oracle.down()?;
            Ok(true)
        }
        "fixtures" => {
            let out = a
                .path
                .clone()
                .ok_or_else(|| Error::Invalid("fixtures needs a directory".into()))?;
            std::fs::create_dir_all(&out).map_err(|e| Error::Io {
                path: out.display().to_string(),
                message: e.to_string(),
            })?;
            for v in ApiVersion::ALL {
                let rec = Recorder::default();
                let c = GatewayClient::new(&target_specs[0].1, v)?.with_recorder(rec.clone());
                conversation::run(&c, Duration::from_millis(1000)).await?;
                let file = out.join(format!("{v}.json"));
                let exchanges: Vec<_> = rec
                    .take()
                    .into_iter()
                    .map(conversation::trim_stack_frames)
                    .collect();
                let text =
                    serde_json::to_string_pretty(&exchanges).expect("exchanges serialize") + "\n";
                std::fs::write(&file, text).map_err(|e| Error::Io {
                    path: file.display().to_string(),
                    message: e.to_string(),
                })?;
                eprintln!("wrote {}", file.display());
            }
            Ok(true)
        }
        "record" | "run" => {
            let corpus = a
                .path
                .clone()
                .ok_or_else(|| Error::Invalid(format!("{} needs a corpus directory", a.command)))?;
            let mut cases = discover(&corpus)?;
            if let Some(only) = &a.only {
                cases.retain(|c| c.name.contains(only.as_str()));
            }
            let mut targets = Vec::new();
            for (name, url) in &target_specs {
                targets.push(Target {
                    name: name.clone(),
                    client: client(url, a.api).await?,
                    shared: Shared {
                        local: shared_local.clone(),
                        remote: shared_remote.clone(),
                    },
                });
            }
            let opts = RunOptions {
                keep: a.keep,
                run_id: harness::new_run_id(),
                verbose: true,
            };
            let report = if a.command == "record" {
                if targets.len() != 1 {
                    return Err(Error::Invalid(
                        "record takes one target (the oracle)".into(),
                    ));
                }
                harness::record(&cases, &targets[0], &opts).await
            } else {
                harness::run(&cases, &targets, &opts).await
            };
            let out = a
                .out
                .clone()
                .unwrap_or_else(|| shared_local.join("reports").join(&opts.run_id));
            report.write(&out)?;
            print!("{}", report.summary());
            eprintln!("triage: {}", out.join("triage.jsonl").display());
            Ok(report.all_passed())
        }
        other => Err(Error::Invalid(format!(
            "unknown command {other}; {}",
            usage()
        ))),
    }
}

fn main() -> ExitCode {
    let args = match parse(std::env::args().skip(1)) {
        Ok(a) => a,
        Err(m) => {
            if !m.is_empty() {
                eprintln!("{m}");
            }
            eprintln!("{}", usage());
            return ExitCode::from(2);
        }
    };
    if args.command == "nexmark-gen" {
        return match nexmark_gen(&args) {
            Ok(_) => ExitCode::SUCCESS,
            Err(e) => {
                eprintln!("loams-stream-oracle: {e}");
                ExitCode::from(1)
            }
        };
    }
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("a tokio runtime");
    match rt.block_on(main_inner(args)) {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => ExitCode::from(1),
        Err(e) => {
            eprintln!("loams-stream-oracle: {e}");
            ExitCode::from(1)
        }
    }
}
