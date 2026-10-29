//! The parser against Flink 2.2.1's SQL Client and SQL Gateway `.q` suites and Nexmark.
//!
//! Run with `-- --nocapture` to print the statement counts by kind.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use loams_stream_sql::{Feature, ParseError, Statement, parse, split_script};

fn vendor() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/vendor")
}

fn files(dir: &Path, extension: &str) -> Vec<PathBuf> {
    let mut files: Vec<PathBuf> = fs::read_dir(dir)
        .unwrap_or_else(|e| panic!("{}: {e}", dir.display()))
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|e| e == extension))
        .collect();
    files.sort();
    files
}

/// The statements Flink accepts that Loams Stream rejects by name (FC1 Task 3 ruling).
const NAMED_REJECTIONS: [Feature; 2] = [Feature::ADD_JAR, Feature::REMOVE_JAR];

#[derive(Debug)]
struct Case {
    file: String,
    line: usize,
    sql: String,
    /// The `.q` file expects Flink's parser to reject the statement.
    parse_error: bool,
    /// The `.q` file expects Flink to reject the statement (parse or validation).
    error: bool,
    /// The output the `.q` file records for the statement.
    output: String,
}

fn is_parse_error(output: &str) -> bool {
    output.contains("ParseException") || output.contains("SQL parse failed")
}

/// The SQL of a block: drops `#` comment lines.
fn sql_lines(block: &[&str]) -> Vec<String> {
    block
        .iter()
        .filter(|l| !l.starts_with('#'))
        .map(|l| l.to_string())
        .collect()
}

/// Reads a `.q` file into cases. SQL Gateway files separate statement and output with `!output`;
/// SQL Client files print the output after the statement, before the `!ok`/`!info`/`!error` tag.
fn read_q(path: &Path) -> Vec<Case> {
    let text = fs::read_to_string(path).unwrap();
    let file = format!(
        "{}/{}",
        path.parent()
            .unwrap()
            .file_name()
            .unwrap()
            .to_string_lossy(),
        path.file_name().unwrap().to_string_lossy()
    );
    let lines: Vec<&str> = text.lines().collect();
    let mut cases = vec![];
    let mut block_start = 0;
    let mut pending: Option<(usize, String)> = None;
    for (i, line) in lines.iter().enumerate() {
        let Some(tag) = line.strip_prefix('!') else {
            continue;
        };
        let block = &lines[block_start..i];
        let start_line = block_start + 1;
        block_start = i + 1;
        if tag.trim() == "output" {
            let sql = sql_lines(block).join("\n");
            pending = Some((start_line, sql));
            continue;
        }
        let error = tag.trim() == "error";
        if let Some((line, sql)) = pending.take() {
            // Gateway format: `block` is the output.
            cases.push(Case {
                file: file.clone(),
                line,
                sql,
                parse_error: error && is_parse_error(&block.join("\n")),
                error,
                output: block.join("\n"),
            });
            continue;
        }
        // Client format: the statement is the shortest prefix that splits into one terminated
        // statement; the rest is output.
        let body = sql_lines(block);
        let mut sql = String::new();
        let mut taken = 0;
        for (n, line) in body.iter().enumerate() {
            if !sql.is_empty() {
                sql.push('\n');
            }
            sql.push_str(line);
            if line.trim_end().ends_with(';')
                && let Ok(parts) = split_script(&sql)
                && parts.len() == 1
                && parts[0].terminated
            {
                taken = n + 1;
                break;
            }
        }
        assert!(taken > 0, "{file}:{start_line}: no statement before !{tag}");
        let output = body[taken..].join("\n");
        cases.push(Case {
            file: file.clone(),
            line: start_line,
            sql,
            parse_error: error && is_parse_error(&output),
            error,
            output,
        });
    }
    cases
}

fn q_cases() -> Vec<Case> {
    let root = vendor().join("flink-2.2.1/sql");
    let mut cases = vec![];
    let mut count = 0;
    for dir in ["sql-client", "sql-gateway"] {
        for path in files(&root.join(dir), "q") {
            count += 1;
            cases.extend(read_q(&path));
        }
    }
    assert_eq!(count, 23, "the 23 vendored .q files");
    cases
}

fn nexmark_scripts() -> Vec<(String, String)> {
    files(&vendor().join("nexmark"), "sql")
        .into_iter()
        .map(|path| {
            let sql = fs::read_to_string(&path)
                .unwrap()
                // ddl_views.sql selects from the source table chosen at run time.
                .replace("${NEXMARK_TABLE}", "datagen");
            (path.file_name().unwrap().to_string_lossy().to_string(), sql)
        })
        .collect()
}

fn count(counts: &mut BTreeMap<String, usize>, statements: &[Statement]) {
    for statement in statements {
        *counts.entry(statement.kind().to_string()).or_default() += 1;
        if let Statement::Explain(e) = statement {
            *counts
                .entry(format!("EXPLAIN > {}", e.statement.kind()))
                .or_default() += 1;
        }
        if let Statement::Execute(e) = statement {
            *counts
                .entry(format!("EXECUTE > {}", e.statement.kind()))
                .or_default() += 1;
        }
    }
}

fn print_counts(title: &str, counts: &BTreeMap<String, usize>) {
    println!("{title}");
    for (kind, n) in counts {
        println!("  {kind:<32} {n}");
    }
}

#[test]
fn parses_every_q_file_statement() {
    let cases = q_cases();
    let mut failures = vec![];
    let mut counts = BTreeMap::new();
    let mut rejected_as_flink_does = 0;
    let mut named = BTreeMap::<&str, usize>::new();
    for case in &cases {
        let result = parse(&case.sql);
        match (&result, case.parse_error) {
            (Ok(statements), false) => count(&mut counts, statements),
            (Err(e), true) => {
                rejected_as_flink_does += 1;
                println!("rejected as in Flink: {}:{}: {e}", case.file, case.line);
            }
            (Err(ParseError::Unsupported { feature, .. }), false)
                if NAMED_REJECTIONS.contains(feature) =>
            {
                *named.entry(feature.name).or_default() += 1;
            }
            (Ok(_), true) => failures.push(format!(
                "{}:{}: Flink rejects this statement but it parsed:\n{}",
                case.file, case.line, case.sql
            )),
            (Err(e), false) => {
                failures.push(format!("{}:{}: {e}\n{}", case.file, case.line, case.sql))
            }
        }
    }
    let parsed: usize = counts.values().sum::<usize>()
        - counts
            .iter()
            .filter(|(k, _)| k.contains(" > "))
            .map(|(_, n)| n)
            .sum::<usize>();
    println!(
        ".q files: {} cases; {parsed} statements parsed; {rejected_as_flink_does} rejected as Flink rejects them; named rejections {named:?}",
        cases.len()
    );
    print_counts(".q statements by kind:", &counts);
    assert!(failures.is_empty(), "{}", failures.join("\n\n"));
    assert!(rejected_as_flink_does >= 9, "the .q files' parse errors");
}

#[test]
fn parses_nexmark_ddl_and_queries() {
    let mut counts = BTreeMap::new();
    let mut queries = 0;
    for (file, sql) in nexmark_scripts() {
        let statements = parse(&sql).unwrap_or_else(|e| panic!("{file}: {e}"));
        assert!(!statements.is_empty(), "{file}");
        if file.starts_with('q') {
            queries += 1;
            assert!(
                statements.iter().any(|s| matches!(s, Statement::Insert(_))),
                "{file} has its INSERT"
            );
        }
        count(&mut counts, &statements);
    }
    assert_eq!(queries, 23, "q0-q22");
    print_counts("Nexmark statements by kind:", &counts);
}

/// Flink SQL forms the corpus does not exercise, for the round trip.
const EXTRA: &str = r#"
CREATE TABLE orders (
  `user` BIGINT NOT NULL,
  product VARCHAR(32),
  amount INT,
  price DECIMAL(10, 2),
  tags ARRAY<STRING NOT NULL>,
  attrs MAP<STRING, ARRAY<ROW<k STRING, v INT>>>,
  nested ROW<a INT 'the a', b ROW<c TIMESTAMP_LTZ(3)>>,
  raw_bytes BYTES,
  ts TIMESTAMP(3),
  ts_ltz TIMESTAMP(3) WITH LOCAL TIME ZONE,
  `offset` BIGINT METADATA VIRTUAL,
  part INT METADATA FROM 'partition' VIRTUAL COMMENT 'kafka partition',
  ptime AS PROCTIME(),
  total AS amount * price COMMENT 'computed',
  CONSTRAINT pk PRIMARY KEY (`user`) NOT ENFORCED,
  WATERMARK FOR ts AS ts - INTERVAL '1' SECONDS
) COMMENT 'orders' DISTRIBUTED BY HASH(`user`) INTO 4 BUCKETS PARTITIONED BY (product) WITH (
  'connector' = 'kafka',
  'topic' = 'it''s'
);
CREATE TEMPORARY TABLE derived WITH ('connector' = 'print') LIKE orders (EXCLUDING ALL INCLUDING WATERMARKS);
CREATE TABLE ctas WITH ('connector' = 'blackhole') AS SELECT * FROM orders;
CREATE OR REPLACE TABLE rtas (a, b) WITH ('connector' = 'blackhole') AS SELECT 1, 2;
REPLACE TABLE rtas2 AS SELECT 1;
INSERT INTO sink /*+ OPTIONS('sink.parallelism' = '2') */ PARTITION (dt = '2026-10-29') (a, b) SELECT a, b FROM src;
INSERT OVERWRITE sink SELECT * FROM src /*+ OPTIONS('scan.startup.mode' = 'earliest-offset') */ AS s;
SELECT /*+ LOOKUP('table' = 'D', 'retry-predicate' = 'lookup_miss'), STATE_TTL('o' = '1d') */ o.id, d.name
FROM orders AS o JOIN dim FOR SYSTEM_TIME AS OF o.ptime AS d ON o.id = d.id;
SELECT window_start, window_end, COUNT(*) FROM TABLE(
  SESSION(TABLE bid PARTITION BY bidder, DESCRIPTOR(`dateTime`), INTERVAL '5' MINUTES))
GROUP BY window_start, window_end;
SELECT * FROM TABLE(CUMULATE(DATA => TABLE bid, TIMECOL => DESCRIPTOR(ts), STEP => INTERVAL '1' MINUTES, SIZE => INTERVAL '10' MINUTES));
SELECT CAST(a AS STRING), TRY_CAST(b AS INT NOT NULL), CAST(c AS MAP<STRING, BIGINT>), MAP['a', 1, 'b', 2], ARRAY[1, 2][1] FROM t;
SELECT a, s FROM t, LATERAL TABLE(split(a)) AS T(s);
SELECT * FROM t MATCH_RECOGNIZE (PARTITION BY k ORDER BY rt MEASURES A.p AS ap ONE ROW PER MATCH AFTER MATCH SKIP PAST LAST ROW PATTERN (A B+) DEFINE B AS B.p < A.p) AS m;
EXECUTE STATEMENT SET BEGIN
INSERT INTO a SELECT CASE WHEN x > 1 THEN 'y' ELSE 'n' END FROM t;
INSERT INTO b SELECT * FROM t;
END;
COMPILE PLAN 'file:///tmp/plan.json' IF NOT EXISTS FOR INSERT INTO a SELECT * FROM t;
COMPILE AND EXECUTE PLAN 'file:///tmp/plan.json' FOR STATEMENT SET BEGIN INSERT INTO a SELECT * FROM t; END;
EXPLAIN CHANGELOG_MODE, JSON_EXECUTION_PLAN CREATE TABLE x AS SELECT 1;
ALTER TABLE orders ADD (c1 INT COMMENT 'c' AFTER amount, WATERMARK FOR ts AS ts - INTERVAL '5' SECOND, DISTRIBUTION BY HASH(a) INTO 3 BUCKETS);
ALTER TABLE orders MODIFY c1 BIGINT FIRST;
ALTER TABLE orders DROP (c1, c2);
ALTER TABLE orders DROP PRIMARY KEY;
ALTER TABLE orders ADD IF NOT EXISTS PARTITION (dt = '1') WITH ('k' = 'v') PARTITION (dt = '2');
ALTER TABLE IF EXISTS orders DROP IF EXISTS PARTITION (dt = '1'), PARTITION (dt = '2');
ALTER VIEW v AS SELECT 1;
CREATE TEMPORARY SYSTEM FUNCTION f AS 'com.example.F' LANGUAGE JAVA USING JAR 'a.jar', JAR 'b.jar';
DROP TEMPORARY SYSTEM FUNCTION IF EXISTS f;
DROP DATABASE IF EXISTS c.d CASCADE;
ANALYZE TABLE t PARTITION (a = 1, b) COMPUTE STATISTICS FOR COLUMNS a, b;
STOP JOB '228d70913eab60dda85c5e7f78b5782c' WITH SAVEPOINT WITH DRAIN;
DESCRIBE JOB '228d70913eab60dda85c5e7f78b5782c';
SHOW PARTITIONS t PARTITION (a = 1);
DELETE FROM t WHERE a = 1;
UPDATE t SET a = 1 WHERE b = 2;
SET 'table.local-time-zone' = 'UTC';
SET table.exec.mini-batch.enabled = true;
RESET table.exec.mini-batch.enabled;
"#;

#[test]
fn round_trips_display() {
    let mut sources: Vec<(String, String)> = vec![];
    for case in q_cases().into_iter().filter(|c| !c.parse_error) {
        sources.push((format!("{}:{}", case.file, case.line), case.sql));
    }
    sources.extend(nexmark_scripts());
    sources.push(("extra".to_string(), EXTRA.to_string()));
    let mut checked = 0;
    for (origin, sql) in sources {
        let Ok(first) = parse(&sql) else {
            continue;
        };
        for statement in first {
            let printed = statement.to_string();
            let reparsed = parse(&printed)
                .unwrap_or_else(|e| panic!("{origin}: printed SQL does not parse: {e}\n{printed}"));
            assert_eq!(reparsed.len(), 1, "{origin}: {printed}");
            let again = reparsed[0].to_string();
            assert_eq!(printed, again, "{origin}: print is not stable");
            assert_eq!(statement.kind(), reparsed[0].kind(), "{origin}: {printed}");
            checked += 1;
        }
    }
    println!("round trip: {checked} statements");
    assert!(checked > 400, "{checked}");
}

#[test]
fn rejects_with_named_feature() {
    let cases = [
        ("ADD JAR '/tmp/udf.jar'", Feature::ADD_JAR),
        ("REMOVE JAR '/tmp/udf.jar'", Feature::REMOVE_JAR),
        ("SET 'table.sql-dialect' = 'hive'", Feature::HIVE_DIALECT),
        (
            "CREATE TABLE h (a STRING) PARTITIONED BY (dt STRING) TBLPROPERTIES ('k' = 'v')",
            Feature::HIVE_DIALECT,
        ),
        (
            "CREATE MATERIALIZED TABLE m FRESHNESS = INTERVAL '1' MINUTE AS SELECT 1",
            Feature::MATERIALIZED_TABLE,
        ),
        ("DROP MATERIALIZED TABLE m", Feature::MATERIALIZED_TABLE),
        ("SHOW MATERIALIZED TABLES", Feature::MATERIALIZED_TABLE),
        (
            "CREATE MODEL m INPUT (a STRING) OUTPUT (b STRING) WITH ('provider' = 'openai')",
            Feature::MODEL,
        ),
        ("SHOW MODELS", Feature::MODEL),
        (
            "SELECT * FROM t MATCH_RECOGNIZE (ORDER BY rt MEASURES A.p AS p PATTERN (A B) WITHIN INTERVAL '1' HOUR DEFINE A AS A.p > 0) AS m",
            Feature::MATCH_RECOGNIZE_WITHIN,
        ),
        (
            "SELECT JSON_VALUE(j, '$.a' RETURNING INT DEFAULT 0 ON EMPTY) FROM t",
            Feature::JSON_FUNCTION_CLAUSES,
        ),
        (
            "SELECT JSON_OBJECT(KEY 'a' VALUE b) FROM t",
            Feature::JSON_FUNCTION_CLAUSES,
        ),
        ("SELECT j IS JSON FROM t", Feature::JSON_FUNCTION_CLAUSES),
    ];
    for (sql, feature) in cases {
        match parse(sql) {
            Err(ParseError::Unsupported { feature: got, span }) => {
                assert_eq!(got, feature, "{sql}");
                assert!(span.start.line >= 1, "{sql}: the error has a span");
                let message = ParseError::Unsupported { feature: got, span }.to_string();
                assert!(message.contains(feature.name), "{message}");
                assert!(message.contains("Loams Stream"), "{message}");
            }
            other => panic!(
                "{sql}: expected {} to be named, got {other:?}",
                feature.name
            ),
        }
    }
}

#[test]
fn syntax_errors_carry_spans() {
    let error = parse("create catalog invalid.cat with ('type'='generic_in_memory')").unwrap_err();
    assert!(matches!(error, ParseError::Syntax { .. }), "{error:?}");
    assert_eq!(error.span().start.line, 1);
    assert_eq!(error.span().start.column, 23, "{error}");
    let error = parse("SELECT \"x\" FROM t").unwrap_err();
    assert!(error.to_string().contains("Lexical error"), "{error}");
}

/// The binder against the `.q` files: each file is one session, bound statement by statement.
/// A statement Flink accepts must bind or be refused with a named feature (D2268); a statement
/// Flink rejects should be rejected too (the binder is permissive where Flink's coercion is wide,
/// so those are reported, not failed).
#[test]
fn binder_errors_match_q_files() {
    use loams_stream_sql::binder::Binder;
    let cases = q_cases();
    let mut sessions: BTreeMap<String, Binder> = BTreeMap::new();
    let (mut bound, mut both_reject, mut accepted_flink_rejects) = (0, 0, 0);
    let mut named = BTreeMap::<&str, usize>::new();
    let mut failures = vec![];
    for case in &cases {
        if case.parse_error {
            continue;
        }
        let Ok(statements) = parse(&case.sql) else {
            continue;
        };
        let binder = sessions.entry(case.file.clone()).or_default();
        for statement in &statements {
            match (binder.bind(statement), case.error) {
                (Ok(_), false) => bound += 1,
                (Err(e), _) if e.feature.is_some() => {
                    *named.entry(e.feature.unwrap().name).or_default() += 1;
                }
                (Err(_), true) => both_reject += 1,
                (Ok(_), true) => {
                    accepted_flink_rejects += 1;
                    println!(
                        "accepted, Flink rejects: {}:{}: {}\n  Flink: {}",
                        case.file,
                        case.line,
                        case.sql.lines().next().unwrap_or(""),
                        case.output
                            .lines()
                            .find(|l| l.contains("Exception") || l.contains("ERROR"))
                            .unwrap_or("")
                    );
                }
                (Err(e), false) => failures.push(format!(
                    "{}:{}: {}\n{}",
                    case.file, case.line, e.message, case.sql
                )),
            }
        }
    }
    println!(
        "binder over .q files: {bound} bound; {both_reject} rejected as Flink rejects them; {accepted_flink_rejects} accepted that Flink rejects; named {named:?}"
    );
    assert!(
        failures.is_empty(),
        "{} failures:\n{}",
        failures.len(),
        failures.join("\n\n")
    );
}

/// Every Nexmark query binds after the DDL, or is refused with a named feature.
#[test]
fn binder_types_nexmark() {
    use loams_stream_sql::binder::Binder;
    let scripts: BTreeMap<String, String> = nexmark_scripts().into_iter().collect();
    let mut named = vec![];
    for (file, sql) in &scripts {
        if !file.starts_with('q') {
            continue;
        }
        let mut binder = Binder::new();
        for setup in ["ddl_gen.sql", "ddl_views.sql"] {
            for s in parse(&scripts[setup]).unwrap() {
                binder
                    .bind(&s)
                    .unwrap_or_else(|e| panic!("{setup}: {e}\n{s}"));
            }
        }
        for s in parse(sql).unwrap() {
            match binder.bind(&s) {
                Ok(_) => {}
                Err(e) if e.feature.is_some() => named.push(format!("{file}: {}", e.message)),
                // Nexmark marks q6 unsupported: its WHERE refers to a SELECT alias, which Flink's
                // validator rejects too (Task 6's corpus records it as an oracle xfail).
                Err(e) if file == "q6.sql" && e.message.contains("rownum") => {}
                Err(e) => panic!("{file}: {e}\n{s}"),
            }
        }
    }
    println!("Nexmark named refusals: {named:?}");
}
