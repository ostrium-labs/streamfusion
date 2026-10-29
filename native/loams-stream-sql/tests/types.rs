//! Flink's type rules as the binder applies them (FC1 Task 4).

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Decimal128Array};
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs};
use loams_stream_sql::binder::{Binder, Bound};
use loams_stream_sql::parse_statement;
use loams_stream_sql::types::casts::{CastVerdict, cast_verdict};
use loams_stream_sql::types::{LogicalType, LogicalTypeFamily};
use streamfusion::semantics::{DecimalArithmetic, decimal_arithmetic};

/// The types of the cast matrix: one per logical type root, at Flink's defaults.
fn matrix_types() -> Vec<(&'static str, LogicalType)> {
    let parse = |s: &str| {
        let ty = loams_stream_sql::parse_type_str(s).unwrap();
        LogicalType::from_syntax(&ty).unwrap()
    };
    [
        "CHAR(1)",
        "VARCHAR(10)",
        "STRING",
        "BOOLEAN",
        "BINARY(1)",
        "BYTES",
        "DECIMAL(10, 2)",
        "TINYINT",
        "SMALLINT",
        "INT",
        "BIGINT",
        "FLOAT",
        "DOUBLE",
        "DATE",
        "TIME(0)",
        "TIMESTAMP(3)",
        "TIMESTAMP_LTZ(3)",
        "INTERVAL YEAR TO MONTH",
        "INTERVAL DAY TO SECOND(3)",
        "ARRAY<INT>",
        "MAP<STRING, INT>",
        "ROW<a INT>",
    ]
    .into_iter()
    .map(|s| (s, parse(s)))
    .collect()
}

fn render_matrix() -> String {
    let types = matrix_types();
    let mut out = String::from("source\\target");
    for (name, _) in &types {
        out.push('\t');
        out.push_str(name);
    }
    out.push('\n');
    for (source_name, source) in &types {
        out.push_str(source_name);
        for (_, target) in &types {
            out.push('\t');
            out.push_str(match cast_verdict(source, target) {
                CastVerdict::Implicit => "I",
                CastVerdict::Explicit => "E",
                CastVerdict::Refused => "-",
            });
        }
        out.push('\n');
    }
    out
}

/// The full matrix is a golden file generated from the ported rules (`LogicalTypeCasts`, the SQL
/// validator's cast check, the runtime cast rules); regenerate with `UPDATE_GOLDEN=1`. The spot
/// checks below are facts from Flink 2.2.1's documented cast table and `LogicalTypeCastsTest`.
#[test]
fn cast_matrix_matches_flink() {
    let path =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/golden/cast_matrix.tsv");
    let rendered = render_matrix();
    if std::env::var_os("UPDATE_GOLDEN").is_some() {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, &rendered).unwrap();
    }
    let golden =
        std::fs::read_to_string(&path).expect("tests/golden/cast_matrix.tsv (UPDATE_GOLDEN=1)");
    assert_eq!(
        rendered, golden,
        "the cast matrix changed; review and regenerate the golden file"
    );

    let t = |s: &str| {
        let ty = loams_stream_sql::parse_type_str(s).unwrap();
        LogicalType::from_syntax(&ty).unwrap()
    };
    let check = |from: &str, to: &str, want: CastVerdict| {
        assert_eq!(cast_verdict(&t(from), &t(to)), want, "{from} -> {to}");
    };
    // Widening numeric casts are implicit; narrowing ones need CAST.
    check("INT", "BIGINT", CastVerdict::Implicit);
    check("TINYINT", "DOUBLE", CastVerdict::Implicit);
    check("BIGINT", "INT", CastVerdict::Explicit);
    check("DOUBLE", "INT", CastVerdict::Explicit);
    // Character strings parse into numbers and datetimes only explicitly.
    check("STRING", "INT", CastVerdict::Explicit);
    check("STRING", "DATE", CastVerdict::Explicit);
    check("STRING", "TIMESTAMP(3)", CastVerdict::Explicit);
    // Every predefined type casts to a character string.
    check("INT", "STRING", CastVerdict::Explicit);
    check("TIMESTAMP(3)", "STRING", CastVerdict::Explicit);
    // Unrelated families are refused.
    check("BOOLEAN", "DATE", CastVerdict::Refused);
    check("DATE", "BOOLEAN", CastVerdict::Refused);
    check("ARRAY<INT>", "INT", CastVerdict::Refused);
}

fn bind_select(sql: &str) -> Vec<(String, LogicalType)> {
    let mut binder = Binder::new();
    let statement = parse_statement(sql).unwrap();
    match binder.bind(&statement).unwrap() {
        Bound::Query(cols) => cols.into_iter().map(|c| (c.name, c.ty)).collect(),
        other => panic!("{other:?}"),
    }
}

fn decimal_column(values: &[i128], precision: u8, scale: i8) -> ArrayRef {
    Arc::new(
        Decimal128Array::from(values.to_vec())
            .with_precision_and_scale(precision, scale)
            .unwrap(),
    )
}

/// The binder derives Flink's result type for decimal arithmetic, and the engine's kernel,
/// bound to that type, computes Flink's value (`DecimalDataUtils`, HALF_UP).
#[test]
fn decimal_arithmetic_matches_flink() {
    let cases = [
        // (sql operator, op, lhs (unscaled, p, s), rhs, expected type, expected unscaled result).
        // Flink multiplies to DECIMAL(p1 + p2 + 1, s1 + s2) (`findMultiplicationDecimalType`).
        (
            "*",
            DecimalArithmetic::Multiply,
            (110, 10, 2),
            (25, 5, 1),
            "DECIMAL(16, 3)",
            2750,
        ),
        (
            "+",
            DecimalArithmetic::Add,
            (110, 10, 2),
            (25, 5, 1),
            "DECIMAL(11, 2)",
            360,
        ),
        (
            "-",
            DecimalArithmetic::Subtract,
            (110, 10, 2),
            (25, 5, 1),
            "DECIMAL(11, 2)",
            -140,
        ),
        (
            "/",
            DecimalArithmetic::Divide,
            (100, 10, 2),
            (3, 5, 0),
            "DECIMAL(16, 8)",
            33333333,
        ),
    ];
    for (sql_op, op, (lv, lp, ls), (rv, rp, rs), want_ty, want_value) in cases {
        let sql = format!(
            "SELECT CAST(a AS DECIMAL({lp}, {ls})) {sql_op} CAST(b AS DECIMAL({rp}, {rs})) FROM (VALUES (1.0, 1.0)) AS t(a, b)"
        );
        let cols = bind_select(&sql);
        let ty = &cols[0].1;
        assert!(ty.is(LogicalTypeFamily::ExactNumeric), "{sql}: {ty}");
        assert_eq!(
            ty.clone().nullable().to_string().to_uppercase(),
            want_ty,
            "{sql}"
        );
        let (precision, scale) = (ty.precision().unwrap(), ty.scale().unwrap() as i8);
        let udf = decimal_arithmetic(op, precision, scale);
        let args = vec![
            ColumnarValue::Array(decimal_column(&[lv], lp, ls as i8)),
            ColumnarValue::Array(decimal_column(&[rv], rp, rs as i8)),
        ];
        let return_field = Arc::new(arrow::datatypes::Field::new(
            "r",
            arrow::datatypes::DataType::Decimal128(precision, scale),
            true,
        ));
        let arg_fields = vec![
            Arc::new(arrow::datatypes::Field::new("a", args[0].data_type(), true)),
            Arc::new(arrow::datatypes::Field::new("b", args[1].data_type(), true)),
        ];
        let out = udf
            .invoke_with_args(ScalarFunctionArgs {
                args,
                arg_fields,
                number_rows: 1,
                return_field,
                config_options: Arc::new(Default::default()),
            })
            .unwrap();
        let ColumnarValue::Array(out) = out else {
            panic!("array result")
        };
        let out = out.as_any().downcast_ref::<Decimal128Array>().unwrap();
        assert_eq!(out.value(0), want_value, "{sql}");
    }
}
