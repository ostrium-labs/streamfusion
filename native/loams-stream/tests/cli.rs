//! The `loams-stream` binary's command line and its dependency graph.

use std::path::Path;
use std::process::Command;

const BINARY: &str = env!("CARGO_BIN_EXE_loams-stream");

fn run(args: &[&str]) -> std::process::Output {
    Command::new(BINARY)
        .args(args)
        .output()
        .expect("run loams-stream")
}

#[test]
fn loams_stream_prints_version_and_roles() {
    let version = format!("loams-stream {} (preview", env!("CARGO_PKG_VERSION"));
    let cases = [
        (vec!["--role", "jobmanager"], "jobmanager"),
        (vec!["--role", "taskmanager"], "taskmanager"),
        (vec!["--role", "all"], "all"),
        (vec!["--role=taskmanager"], "taskmanager"),
        (vec![], "all"),
    ];
    for (args, role) in cases {
        let output = run(&args);
        let stdout = String::from_utf8(output.stdout).unwrap();
        assert!(output.status.success(), "{args:?}: {stdout}");
        assert!(stdout.starts_with(&version), "{args:?}: {stdout}");
        assert!(
            stdout.trim_end().ends_with(&format!(" role={role}")),
            "{args:?}: {stdout}"
        );
    }

    let output = run(&["--version"]);
    assert!(output.status.success());
    assert!(
        String::from_utf8(output.stdout)
            .unwrap()
            .starts_with(&version)
    );

    let output = run(&["--role", "worker"]);
    assert_eq!(output.status.code(), Some(2));
    assert!(
        String::from_utf8(output.stderr)
            .unwrap()
            .contains("unknown role `worker`")
    );
}

/// The standalone binary needs no JVM: its normal dependency graph contains no `jni` crate
/// (design 82, review focus 4).
#[test]
fn standalone_graph_has_no_jni() {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR")).join("../Cargo.toml");
    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_string());
    let output = Command::new(cargo)
        .args(["tree", "--locked", "-p", "loams-stream", "-e", "normal"])
        .args(["--prefix", "none", "--format", "{p}", "--manifest-path"])
        .arg(&manifest)
        .output()
        .expect("run cargo tree");
    let tree = String::from_utf8(output.stdout).unwrap();
    assert!(
        output.status.success(),
        "cargo tree failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        tree.lines().any(|line| line.starts_with("streamfusion v")),
        "the engine is missing from the graph:\n{tree}"
    );
    let jni: Vec<&str> = tree
        .lines()
        .filter(|line| {
            let name = line.split_whitespace().next().unwrap_or_default();
            name == "jni" || name.starts_with("jni-") || name.starts_with("jni_")
        })
        .collect();
    assert!(jni.is_empty(), "loams-stream links {jni:?}");
}
