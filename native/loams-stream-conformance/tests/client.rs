//! `client_speaks_v1_to_v4`: the client against SQL Gateway responses
//! recorded from Apache Flink 2.2.1 (`tests/fixtures/gateway/v<N>.json`,
//! written by `loams-stream-oracle fixtures`).
//!
//! A replay server answers each request with the next recorded response,
//! after checking that the request is the recorded one (method, path with
//! query, JSON body). The scripted conversation then runs against it, so
//! the client must send exactly what the live gateway answered, and must
//! decode what it answered. Every path is also checked against the
//! vendored OpenAPI file of its version.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use loams_stream_conformance::client::{ApiVersion, Exchange, GatewayClient, OperationStatus};
use loams_stream_conformance::conversation;
use loams_stream_conformance::result::RowKind;

fn fixtures(v: ApiVersion) -> Vec<Exchange> {
    let p = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/gateway")
        .join(format!("{v}.json"));
    serde_json::from_str(&std::fs::read_to_string(&p).unwrap()).unwrap()
}

fn openapi(v: ApiVersion) -> String {
    let p = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../conformance/vendor/flink-2.2.1/openapi")
        .join(format!("rest_{v}_sql_gateway.yml"));
    std::fs::read_to_string(p).unwrap()
}

/// The OpenAPI path template a concrete path matches, e.g.
/// `/sessions/{session_handle}/operations/{operation_handle}/status`.
fn template(path: &str, v: ApiVersion) -> String {
    let path = path.split('?').next().unwrap();
    let rest = path
        .strip_prefix(&format!("/{v}"))
        .unwrap_or(path)
        .to_owned();
    let segs: Vec<&str> = rest.split('/').collect();
    let mut out = Vec::new();
    for (i, s) in segs.iter().enumerate() {
        let prev = if i > 0 { segs[i - 1] } else { "" };
        out.push(match prev {
            "sessions" => "{session_handle}",
            "operations" => "{operation_handle}",
            "result" => "{token}",
            _ => s,
        });
    }
    out.join("/")
}

struct Replay {
    script: Vec<Exchange>,
    next: usize,
    failures: Vec<String>,
}

fn read_request(
    r: &mut BufReader<TcpStream>,
) -> Option<(String, String, Option<serde_json::Value>)> {
    let mut line = String::new();
    if r.read_line(&mut line).ok()? == 0 {
        return None;
    }
    let mut parts = line.split_whitespace();
    let method = parts.next()?.to_owned();
    let path = parts.next()?.to_owned();
    let mut len = 0usize;
    loop {
        let mut h = String::new();
        r.read_line(&mut h).ok()?;
        let h = h.trim_end();
        if h.is_empty() {
            break;
        }
        if let Some((k, val)) = h.split_once(':')
            && k.eq_ignore_ascii_case("content-length")
        {
            len = val.trim().parse().ok()?;
        }
    }
    let body = if len > 0 {
        let mut buf = vec![0; len];
        r.read_exact(&mut buf).ok()?;
        Some(serde_json::from_slice(&buf).ok()?)
    } else {
        None
    };
    Some((method, path, body))
}

fn serve(listener: TcpListener, state: Arc<Mutex<Replay>>) {
    for stream in listener.incoming() {
        let Ok(stream) = stream else { return };
        let state = state.clone();
        std::thread::spawn(move || {
            let mut writer = stream.try_clone().unwrap();
            let mut reader = BufReader::new(stream);
            while let Some((method, path, body)) = read_request(&mut reader) {
                let (status, response) = {
                    let mut s = state.lock().unwrap();
                    let i = s.next;
                    s.next += 1;
                    match s.script.get(i).cloned() {
                        Some(e) if e.method == method && e.path == path && e.body == body => {
                            (e.status, e.response)
                        }
                        Some(e) => {
                            s.failures.push(format!(
                                "exchange {i}: expected {} {} {:?}, got {method} {path} {body:?}",
                                e.method, e.path, e.body
                            ));
                            (599, "{\"errors\":[\"replay mismatch\"]}".to_owned())
                        }
                        None => {
                            s.failures
                                .push(format!("exchange {i}: unexpected {method} {path}"));
                            (599, "{\"errors\":[\"replay exhausted\"]}".to_owned())
                        }
                    }
                };
                let head = format!(
                    "HTTP/1.1 {status} X\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n",
                    response.len()
                );
                if writer.write_all(head.as_bytes()).is_err()
                    || writer.write_all(response.as_bytes()).is_err()
                {
                    return;
                }
            }
        });
    }
}

#[tokio::test]
async fn client_speaks_v1_to_v4() {
    for v in ApiVersion::ALL {
        let script = fixtures(v);
        assert!(!script.is_empty(), "{v}: no fixtures");

        // Every recorded path is an endpoint of this version's OpenAPI file.
        let spec = openapi(v);
        for e in &script {
            let t = template(&e.path, v);
            assert!(
                spec.contains(&format!("\n  {t}:\n")),
                "{v}: {} is not in rest_{v}_sql_gateway.yml",
                t
            );
        }

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let state = Arc::new(Mutex::new(Replay {
            script: script.clone(),
            next: 0,
            failures: Vec::new(),
        }));
        let st = state.clone();
        std::thread::spawn(move || serve(listener, st));

        let client = GatewayClient::new(&format!("http://{addr}"), v).unwrap();
        let t = conversation::run(&client, Duration::from_millis(1)).await;
        let s = state.lock().unwrap();
        assert!(s.failures.is_empty(), "{v}: {:#?}", s.failures);
        let t = t.unwrap_or_else(|e| panic!("{v}: {e}"));
        assert_eq!(
            s.next,
            script.len(),
            "{v}: not every recorded exchange was replayed"
        );

        assert_eq!(t.versions, ApiVersion::ALL.to_vec());
        assert_eq!(t.info, ("Apache Flink".to_owned(), "2.2.1".to_owned()));
        assert_eq!(t.session_time_zone.as_deref(), Some("UTC"));
        assert_eq!(t.candidates.is_some(), v >= ApiVersion::V2, "{v}");
        assert!(!t.first_status.is_terminal() || t.first_status == OperationStatus::Finished);
        assert!(t.had_job);
        assert_eq!(t.closed, "CLOSED");
        assert!(t.failure.contains("no_such_table"), "{v}: {}", t.failure);

        // The typed result: names, types, row kinds and the exact field text.
        let cols: Vec<(&str, &str)> = t
            .result
            .columns
            .iter()
            .map(|c| (c.name.as_str(), c.ty.as_str()))
            .collect();
        assert_eq!(
            cols,
            [
                ("a", "INTEGER NOT NULL"),
                ("d", "DOUBLE NOT NULL"),
                ("f", "FLOAT NOT NULL"),
                ("dc", "DECIMAL(3, 2) NOT NULL"),
                ("big", "DOUBLE NOT NULL"),
                ("dt", "DATE NOT NULL"),
                ("ts", "TIMESTAMP(1) NOT NULL"),
                ("ltz", "TIMESTAMP_LTZ(3)"),
                ("nn", "VARCHAR(2147483647)"),
                ("arr", "ARRAY<INTEGER NOT NULL> NOT NULL"),
                ("m", "MAP<CHAR(1) NOT NULL, INTEGER NOT NULL> NOT NULL"),
                (
                    "r",
                    "ROW<`EXPR$0` INTEGER NOT NULL, `EXPR$1` CHAR(1) NOT NULL> NOT NULL"
                ),
            ],
            "{v}"
        );
        let mut rows = t.result.rows.clone();
        rows.sort();
        assert_eq!(rows.len(), 2, "{v}");
        assert!(rows.iter().all(|r| r.kind == RowKind::Insert));
        assert_eq!(
            rows[0].fields,
            [
                "1",
                "0.3333333333333333",
                "1.5",
                "1.1",
                "1.0E20",
                "\"2024-01-01\"",
                "\"2024-01-01T00:00:01.5\"",
                "\"1970-01-01T00:00:01Z\"",
                "null",
                "[1,2]",
                "{\"k\":1}",
                "{\"EXPR$0\":1,\"EXPR$1\":\"x\"}",
            ],
            "{v}"
        );
    }
}
