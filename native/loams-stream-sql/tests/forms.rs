//! Targeted checks of the forms the hooks add: spans, encoded table arguments, hints and types.

use loams_stream_sql::sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, FunctionArguments, SelectItem, SetExpr, TableFactor,
    TableVersion,
};
use loams_stream_sql::{
    ColumnKind, HintOptions, Statement, TableElement, TypeKind, decode_hints, parse,
    parse_statement, parse_type_str, table_argument, type_of_cast,
};

fn select(sql: &str) -> loams_stream_sql::sqlparser::ast::Select {
    match parse_statement(sql).unwrap() {
        Statement::Query(q) => match *q.query.body {
            SetExpr::Select(s) => *s,
            other => panic!("{other:?}"),
        },
        other => panic!("{other:?}"),
    }
}

#[test]
fn statements_and_columns_keep_spans() {
    let statements =
        parse("SET 'a' = 'b';\n  CREATE TABLE t (\n  a INT,\n  b AS a + 1\n) WITH ()").unwrap();
    assert_eq!(statements[0].span().start.line, 1);
    let Statement::CreateTable(table) = &statements[1] else {
        panic!()
    };
    assert_eq!((table.span.start.line, table.span.start.column), (2, 3));
    let TableElement::Column(b) = &table.elements[1] else {
        panic!()
    };
    assert_eq!((b.span.start.line, b.span.start.column), (4, 3));
    assert!(matches!(b.kind, ColumnKind::Computed { .. }));
    let TableElement::Column(a) = &table.elements[0] else {
        panic!()
    };
    let ColumnKind::Physical { ty, .. } = &a.kind else {
        panic!()
    };
    assert_eq!((ty.span.start.line, ty.span.start.column), (3, 5));
}

#[test]
fn window_tvf_table_argument_is_structured() {
    let s = select(
        "SELECT * FROM TABLE(SESSION(TABLE bid PARTITION BY (bidder, auction), DESCRIPTOR(ts), INTERVAL '5' MINUTES))",
    );
    let TableFactor::TableFunction { expr, .. } = &s.from[0].relation else {
        panic!("{:?}", s.from[0].relation)
    };
    let Expr::Function(session) = expr else {
        panic!()
    };
    let FunctionArguments::List(list) = &session.args else {
        panic!()
    };
    let FunctionArg::Unnamed(FunctionArgExpr::Expr(first)) = &list.args[0] else {
        panic!()
    };
    let argument = table_argument(first).expect("a table argument");
    assert_eq!(argument.table.to_string(), "bid");
    assert_eq!(argument.partition_by.len(), 2);
    assert!(argument.span.start.column > 1);
}

#[test]
fn table_hints_attach_to_their_table() {
    let s =
        select("SELECT * FROM t /*+ OPTIONS('k' = 'v', 'x' = 'y') */ AS a JOIN u ON a.id = u.id");
    let TableFactor::Table { with_hints, .. } = &s.from[0].relation else {
        panic!()
    };
    let hints = decode_hints(with_hints);
    assert_eq!(hints.len(), 1);
    assert_eq!(hints[0].name.value, "OPTIONS");
    let HintOptions::KeyValues(pairs) = &hints[0].options else {
        panic!()
    };
    assert_eq!(pairs.len(), 2);
    assert!(
        parse("SELECT * FROM t AS a /*+ OPTIONS('k' = 'v') */").is_err(),
        "hint after alias"
    );
    assert!(
        parse("SELECT * FROM t WITH (NOLOCK)").is_err(),
        "no MSSQL hints"
    );
}

#[test]
fn temporal_join_keeps_flink_order() {
    let sql = "SELECT * FROM o JOIN r FOR SYSTEM_TIME AS OF o.ts AS r2 ON o.k = r2.k";
    let s = select(sql);
    let TableFactor::Table { version, .. } = &s.from[0].joins[0].relation else {
        panic!()
    };
    assert!(matches!(version, Some(TableVersion::ForSystemTimeAsOf(_))));
    assert_eq!(parse_statement(sql).unwrap().to_string(), sql);
}

#[test]
fn casts_carry_flink_types() {
    let s = select("SELECT CAST(a AS ROW<x INT NOT NULL, y ARRAY<MAP<STRING, BYTES>>>) FROM t");
    let SelectItem::UnnamedExpr(Expr::Cast { data_type, .. }) = &s.projection[0] else {
        panic!()
    };
    let ty = type_of_cast(data_type).expect("a Flink type");
    let TypeKind::Row(fields) = &ty.kind else {
        panic!()
    };
    assert!(!fields[0].ty.nullable);
    assert!(matches!(fields[1].ty.kind, TypeKind::Array(_)));
}

#[test]
fn types_parse_and_print() {
    for (sql, printed) in [
        ("STRING", "STRING"),
        ("INT ARRAY", "ARRAY<INT>"),
        (
            "INT NOT NULL MULTISET NOT NULL",
            "MULTISET<INT NOT NULL> NOT NULL",
        ),
        ("TIMESTAMP_LTZ(3)", "TIMESTAMP_LTZ(3)"),
        ("TIMESTAMP(9) WITHOUT TIME ZONE", "TIMESTAMP(9)"),
        ("ROW(a INT, b STRING 'c')", "ROW<a INT, b STRING 'c'>"),
        ("ROW<>", "ROW<>"),
        (
            "INTERVAL DAY(2) TO SECOND(3)",
            "INTERVAL DAY(2) TO SECOND(3)",
        ),
        (
            "RAW('java.lang.Object', 'AAA=')",
            "RAW('java.lang.Object', 'AAA=')",
        ),
        ("DOUBLE PRECISION", "DOUBLE"),
        ("CHARACTER VARYING(3)", "VARCHAR(3)"),
        ("ARRAY<ARRAY<ARRAY<INT>>>", "ARRAY<ARRAY<ARRAY<INT>>>"),
    ] {
        let ty = parse_type_str(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
        assert_eq!(ty.to_string(), printed, "{sql}");
    }
    assert!(parse_type_str("ARRAY<INT>>").is_err());
    assert!(parse_type_str("MAP<INT>").is_err());
}

#[test]
fn flink_extended_parser_strategies() {
    let set = parse_statement("SET table.exec.state.ttl = 1 h").unwrap_err();
    // `[^;\s]+` stops at the space, and the trailing `h` fails the regex; the Calcite form needs
    // quoted strings.
    assert!(matches!(set, loams_stream_sql::ParseError::Syntax { .. }));
    let Statement::Set(set) = parse_statement("set a.b-c = 'x y';").unwrap() else {
        panic!()
    };
    let (k, v) = set.assignment.unwrap();
    assert_eq!((k.value.as_str(), v.value.as_str()), ("a.b-c", "x y"));
    assert!(matches!(
        parse_statement("quit").unwrap(),
        Statement::Quit { exit: false, .. }
    ));
    assert!(matches!(
        parse_statement("EXIT;").unwrap(),
        Statement::Quit { exit: true, .. }
    ));
    assert!(matches!(
        parse_statement("RESET").unwrap(),
        Statement::Reset(_)
    ));
}

#[test]
fn flink_parser_messages() {
    for (sql, message) in [
        (
            "CREATE SYSTEM FUNCTION f AS 'c'",
            "CREATE SYSTEM FUNCTION is not supported",
        ),
        (
            "EXPLAIN CHANGELOG_MODE, CHANGELOG_MODE SELECT 1",
            "Duplicate EXPLAIN DETAIL",
        ),
        (
            "EXPLAIN CREATE TABLE t (a INT)",
            "Unsupported CREATE OR REPLACE statement for EXPLAIN",
        ),
        (
            "CREATE TABLE t (a, b) WITH ()",
            "Columns identifiers without types",
        ),
        (
            "UPSERT OVERWRITE t SELECT 1",
            "OVERWRITE expression is only used with INSERT",
        ),
        (
            "STOP JOB 'x' WITH DRAIN",
            "WITH DRAIN could only be used after WITH SAVEPOINT",
        ),
        (
            "CREATE FUNCTION f AS 'c' LANGUAGE PYTHON USING JAR 'x'",
            "not applicable to PYTHON",
        ),
        (
            "CREATE TABLE t (a INT, WATERMARK FOR a AS a, WATERMARK FOR a AS a)",
            "Multiple WATERMARK statements",
        ),
        (
            "EXECUTE STATEMENT SET BEGIN END",
            "Expected: INSERT or UPSERT",
        ),
    ] {
        let error = parse(sql).unwrap_err().to_string();
        assert!(error.contains(message), "{sql}: {error}");
    }
}
