//! Return types of Flink's built-in functions (the binder's part of the function matrix,
//! FC1 Task 7). Each rule follows the function's `TypeStrategy` in Flink 2.2.1's
//! `BuiltInFunctionDefinitions` or Calcite's `SqlStdOperatorTable`; functions not listed here are
//! reported as unknown (`No match found for function signature ...`).

use crate::types::merging::{avg_agg_type, find_common_type, sum_agg_type};
use crate::types::{LogicalKind, LogicalRowField, LogicalType, LogicalTypeFamily, TimestampKind};

use super::expr::materialize;

/// Calcite's family name of a type, as `No match found for function signature` prints it.
pub fn sql_family(t: &LogicalType) -> &'static str {
    if t.is(LogicalTypeFamily::CharacterString) {
        "CHARACTER"
    } else if t.is(LogicalTypeFamily::Numeric) {
        "NUMERIC"
    } else if t.is(LogicalTypeFamily::BinaryString) {
        "BINARY"
    } else if matches!(t.kind, LogicalKind::Boolean) {
        "BOOLEAN"
    } else if matches!(t.kind, LogicalKind::Date) {
        "DATE"
    } else if matches!(t.kind, LogicalKind::Time(_)) {
        "TIME"
    } else if t.is(LogicalTypeFamily::Timestamp) {
        "TIMESTAMP"
    } else if t.is(LogicalTypeFamily::Interval) {
        "INTERVAL"
    } else if matches!(t.kind, LogicalKind::Array(_)) {
        "ARRAY"
    } else if matches!(t.kind, LogicalKind::Map(..)) {
        "MAP"
    } else if matches!(t.kind, LogicalKind::Row(_)) {
        "ROW"
    } else {
        "ANY"
    }
}

fn any_nullable(args: &[LogicalType]) -> bool {
    args.iter().any(|a| a.nullable)
}

fn first(args: &[LogicalType]) -> Option<LogicalType> {
    args.first().map(materialize)
}

/// The return type of built-in `name` (upper case) for `args`; `None` when `name` is not a
/// built-in function.
pub fn builtin(
    name: &str,
    args: &[LogicalType],
    star: bool,
    over: bool,
) -> Option<Result<LogicalType, String>> {
    let n = any_nullable(args);
    let ok = |t: LogicalType| Some(Ok(t));
    let arity = |min: usize, max: usize| -> Result<(), String> {
        if args.len() < min || args.len() > max {
            Err(format!(
                "Invalid number of arguments to function '{name}'. Was expecting {min} arguments"
            ))
        } else {
            Ok(())
        }
    };
    macro_rules! check {
        ($min:expr, $max:expr) => {
            if let Err(e) = arity($min, $max) {
                return Some(Err(e));
            }
        };
    }
    if args.is_empty()
        && matches!(
            name,
            "COALESCE" | "GREATEST" | "LEAST" | "ARRAY" | "MAP" | "IFNULL" | "NULLIF" | "IF"
        )
    {
        return Some(Err(format!(
            "Invalid number of arguments to function '{name}'. Was expecting at least 1 argument"
        )));
    }
    match name {
        // Aggregates.
        "COUNT" => ok(LogicalType::bigint().not_null()),
        "SUM" | "SUM0" => {
            check!(1, 1);
            let t = materialize(&args[0]);
            ok(sum_agg_type(&t).nullable())
        }
        "AVG" => {
            check!(1, 1);
            ok(avg_agg_type(&materialize(&args[0])).nullable())
        }
        "MIN" | "MAX" | "FIRST_VALUE" | "LAST_VALUE" | "ANY_VALUE" => {
            check!(1, 2);
            ok(first(args)?.nullable())
        }
        "LAG" | "LEAD" => {
            check!(1, 3);
            ok(first(args)?.nullable())
        }
        "LISTAGG" => {
            check!(1, 2);
            ok(LogicalType::string())
        }
        "COLLECT" => {
            check!(1, 1);
            ok(LogicalType::multiset(first(args)?).not_null())
        }
        "ARRAY_AGG" => {
            check!(1, 1);
            ok(LogicalType::array(first(args)?))
        }
        "STDDEV" | "STDDEV_POP" | "STDDEV_SAMP" | "VARIANCE" | "VAR_POP" | "VAR_SAMP" => {
            check!(1, 1);
            ok(avg_agg_type(&materialize(&args[0])).nullable())
        }
        "BOOL_AND" | "BOOL_OR" | "EVERY" | "SOME" => ok(LogicalType::boolean()),
        "ROW_NUMBER" | "RANK" | "DENSE_RANK" | "NTILE" => {
            if !over {
                return Some(Err(
                    "OVER clause is necessary for window functions".to_string()
                ));
            }
            ok(LogicalType::bigint().not_null())
        }
        "PERCENT_RANK" | "CUME_DIST" => ok(LogicalType::double().not_null()),
        "JSON_OBJECTAGG" | "JSON_ARRAYAGG" => ok(LogicalType::string().not_null()),

        // Conditional.
        "COALESCE" => {
            let tys: Vec<_> = args.iter().map(materialize).collect();
            let common = find_common_type(&tys)?;
            ok(common.with_nullable(args.iter().all(|a| a.nullable)))
        }
        "IFNULL" => {
            check!(2, 2);
            let common = find_common_type(&[materialize(&args[0]), materialize(&args[1])])?;
            ok(common.with_nullable(args[1].nullable))
        }
        "NULLIF" => {
            check!(2, 2);
            ok(first(args)?.nullable())
        }
        "IF" => {
            check!(3, 3);
            let common = find_common_type(&[materialize(&args[1]), materialize(&args[2])])?;
            ok(common)
        }
        "GREATEST" | "LEAST" => {
            let tys: Vec<_> = args.iter().map(materialize).collect();
            ok(find_common_type(&tys)?.with_nullable(n))
        }
        "IS_ALPHA" | "IS_DECIMAL" | "IS_DIGIT" => ok(LogicalType::boolean().not_null()),

        // Strings.
        "UPPER" | "LOWER" | "INITCAP" | "TRIM" | "LTRIM" | "RTRIM" | "BTRIM" | "REVERSE"
        | "LPAD" | "RPAD" | "REPLACE" | "REPEAT" | "REGEXP_REPLACE" | "REGEXP_EXTRACT"
        | "TRANSLATE" | "OVERLAY" | "SUBSTR" | "SUBSTRING" | "LEFT" | "RIGHT" | "CONCAT"
        | "CONCAT_WS" | "SPLIT_INDEX" | "MD5" | "SHA1" | "SHA2" | "SHA224" | "SHA256"
        | "SHA384" | "SHA512" | "TO_BASE64" | "FROM_BASE64" | "HEX" | "UNHEX" | "CHR"
        | "DECODE" | "ENCODE" | "STR_TO_MAP_VALUE" | "UUID" | "PARSE_URL" | "ELT" | "PRINTF"
        | "TRANSLATE3" | "JSON_QUOTE" | "JSON_UNQUOTE" | "URL_DECODE" | "URL_ENCODE"
        | "REGEXP_EXTRACT_ALL" | "SPLIT" => {
            let t = match name {
                "MD5" | "SHA1" | "SHA2" | "SHA224" | "SHA256" | "SHA384" | "SHA512" => {
                    LogicalType::char(match name {
                        "MD5" => 32,
                        "SHA1" => 40,
                        "SHA224" => 56,
                        "SHA256" | "SHA2" => 64,
                        "SHA384" => 96,
                        _ => 128,
                    })
                }
                "UUID" => return ok(LogicalType::char(36).not_null()),
                "REGEXP_EXTRACT_ALL" | "SPLIT" => {
                    return ok(LogicalType::array(LogicalType::string()).with_nullable(n));
                }
                "UPPER" | "LOWER" | "INITCAP" | "TRIM" | "LTRIM" | "RTRIM" | "REVERSE" => {
                    match first(args).map(|t| t.kind) {
                        Some(LogicalKind::Char(k)) => LogicalType::char(k),
                        Some(LogicalKind::Varchar(k)) => LogicalType::varchar(k),
                        _ => LogicalType::string(),
                    }
                }
                _ => LogicalType::string(),
            };
            let nullable = n
                || matches!(
                    name,
                    "SPLIT_INDEX"
                        | "REGEXP_EXTRACT"
                        | "FROM_BASE64"
                        | "PARSE_URL"
                        | "SUBSTR"
                        | "SUBSTRING"
                        | "DECODE"
                        | "ELT"
                        | "STR_TO_MAP_VALUE"
                        | "URL_DECODE"
                        | "URL_ENCODE"
                );
            ok(t.with_nullable(nullable))
        }
        "CHAR_LENGTH" | "CHARACTER_LENGTH" | "LENGTH" | "POSITION" | "LOCATE" | "INSTR"
        | "ASCII" | "BIT_LENGTH" | "OCTET_LENGTH" => ok(LogicalType::int().with_nullable(n)),
        "REGEXP" | "STARTSWITH" | "ENDSWITH" | "REGEXP_COUNT" => {
            ok(LogicalType::boolean().with_nullable(n))
        }
        "STR_TO_MAP" => {
            ok(LogicalType::map(LogicalType::string(), LogicalType::string()).with_nullable(n))
        }
        "JSON_VALUE" => ok(LogicalType::string()),
        "JSON_QUERY" => ok(LogicalType::string()),
        "JSON_EXISTS" | "IS_JSON" => ok(LogicalType::boolean().with_nullable(n)),
        "JSON_STRING" | "JSON_OBJECT" | "JSON_ARRAY" => ok(LogicalType::string().not_null()),

        // Numeric.
        "ABS" | "NEGATIVE" | "POSITIVE" | "SIGN" | "TRUNCATE" => {
            check!(1, 2);
            ok(first(args)?.with_nullable(n))
        }
        "ROUND" => {
            check!(1, 2);
            let t = first(args)?;
            match t.kind {
                LogicalKind::Decimal { precision, scale } => {
                    let r = 0i32;
                    let _ = r;
                    ok(LogicalType::decimal(precision, scale).with_nullable(n))
                }
                _ => ok(t.with_nullable(n)),
            }
        }
        "CEIL" | "CEILING" | "FLOOR" => ok(first(args)?.with_nullable(n)),
        "MOD" => {
            check!(2, 2);
            ok(materialize(&args[1]).with_nullable(n))
        }
        "POWER" | "POW" | "SQRT" | "EXP" | "LN" | "LOG" | "LOG10" | "LOG2" | "SIN" | "COS"
        | "TAN" | "COT" | "ASIN" | "ACOS" | "ATAN" | "ATAN2" | "SINH" | "COSH" | "TANH"
        | "DEGREES" | "RADIANS" | "CBRT" | "E" | "PI" => ok(LogicalType::double().with_nullable(n)),
        "RAND" | "RANDOM" => ok(LogicalType::double().not_null()),
        "RAND_INTEGER" => ok(LogicalType::int().not_null()),
        "BIN" => ok(LogicalType::string().with_nullable(n)),
        "HASH_CODE" => ok(LogicalType::int().not_null()),
        "PERCENTILE" => ok(LogicalType::double()),

        // Temporal.
        "NOW" | "CURRENT_TIMESTAMP" | "LOCALTIMESTAMP" | "CURRENT_ROW_TIMESTAMP" => {
            ok(match name {
                "LOCALTIMESTAMP" => LogicalType::timestamp(3),
                _ => LogicalType::timestamp_ltz(3),
            }
            .not_null())
        }
        "CURRENT_DATE" => ok(LogicalType::date().not_null()),
        "CURRENT_TIME" | "LOCALTIME" => ok(LogicalType::time(0).not_null()),
        "PROCTIME" => ok(LogicalType::proctime()),
        "CURRENT_WATERMARK" => {
            check!(1, 1);
            let t = first(args)?;
            ok(t.nullable())
        }
        "TO_TIMESTAMP" => ok(LogicalType::timestamp(3).with_nullable(true)),
        "TO_TIMESTAMP_LTZ" => ok(LogicalType::timestamp_ltz(3).with_nullable(n)),
        "TO_DATE" => ok(LogicalType::date().with_nullable(true)),
        "DATE_FORMAT" | "FROM_UNIXTIME" | "CONVERT_TZ" => {
            ok(LogicalType::string().with_nullable(true))
        }
        "UNIX_TIMESTAMP" => ok(LogicalType::bigint().with_nullable(n)),
        "TIMESTAMPADD" => ok(args.get(2).map(materialize)?.with_nullable(n)),
        "TIMESTAMPDIFF" => ok(LogicalType::int().with_nullable(n)),
        "DATEDIFF" => ok(LogicalType::int().with_nullable(n)),
        "YEAR" | "QUARTER" | "MONTH" | "WEEK" | "DAYOFYEAR" | "DAYOFMONTH" | "DAYOFWEEK"
        | "HOUR" | "MINUTE" | "SECOND" => ok(LogicalType::bigint().with_nullable(n)),
        "TUMBLE_START" | "TUMBLE_END" | "HOP_START" | "HOP_END" | "SESSION_START"
        | "SESSION_END" => ok(LogicalType::timestamp(3).not_null()),
        "TUMBLE_ROWTIME" | "HOP_ROWTIME" | "SESSION_ROWTIME" | "TUMBLE_PROCTIME"
        | "HOP_PROCTIME" | "SESSION_PROCTIME" => {
            let mut t = first(args)?;
            t.kind = match t.kind {
                LogicalKind::TimestampLtz { precision, .. } => LogicalKind::TimestampLtz {
                    precision,
                    kind: if name.ends_with("PROCTIME") {
                        TimestampKind::Proctime
                    } else {
                        TimestampKind::Rowtime
                    },
                },
                _ => LogicalKind::Timestamp {
                    precision: 3,
                    kind: if name.ends_with("PROCTIME") {
                        TimestampKind::Proctime
                    } else {
                        TimestampKind::Rowtime
                    },
                },
            };
            ok(t.not_null())
        }
        // Group-window functions in `GROUP BY` (legacy group windows).
        "TUMBLE" | "HOP" | "SESSION" => ok(LogicalType::timestamp(3).not_null()),

        // Collections and rows.
        "CARDINALITY" | "ARRAY_POSITION" => ok(LogicalType::int().with_nullable(n)),
        "ELEMENT" => ok(match first(args)?.kind {
            LogicalKind::Array(e) | LogicalKind::Multiset(e) => (*e).nullable(),
            _ => return Some(Err("ELEMENT requires a collection".into())),
        }),
        "ARRAY" => {
            let tys: Vec<_> = args.iter().map(materialize).collect();
            ok(LogicalType::array(find_common_type(&tys)?).not_null())
        }
        "ARRAY_CONTAINS" => ok(LogicalType::boolean().with_nullable(n)),
        "ARRAY_DISTINCT" | "ARRAY_REMOVE" | "ARRAY_REVERSE" | "ARRAY_SORT" | "ARRAY_SLICE"
        | "ARRAY_UNION" | "ARRAY_CONCAT" | "ARRAY_EXCEPT" | "ARRAY_INTERSECT" | "ARRAY_APPEND"
        | "ARRAY_PREPEND" | "ARRAY_COMPACT" => ok(first(args)?.with_nullable(n)),
        "ARRAY_MAX" | "ARRAY_MIN" => ok(match first(args)?.kind {
            LogicalKind::Array(e) => (*e).nullable(),
            _ => LogicalType::null(),
        }),
        "ARRAY_JOIN" => ok(LogicalType::string().with_nullable(n)),
        "MAP_KEYS" => ok(match first(args)?.kind {
            LogicalKind::Map(k, _) => LogicalType::array(*k).with_nullable(n),
            _ => LogicalType::null(),
        }),
        "MAP_VALUES" => ok(match first(args)?.kind {
            LogicalKind::Map(_, v) => LogicalType::array(*v).with_nullable(n),
            _ => LogicalType::null(),
        }),
        "MAP_ENTRIES" | "MAP_UNION" | "MAP_FROM_ARRAYS" => ok(first(args)?.with_nullable(n)),
        "ROW" => ok(LogicalType::row(
            args.iter()
                .enumerate()
                .map(|(i, t)| LogicalRowField::new(format!("EXPR${i}"), materialize(t)))
                .collect(),
        )
        .not_null()),
        "MAP" => {
            let keys: Vec<_> = args.iter().step_by(2).map(materialize).collect();
            let values: Vec<_> = args.iter().skip(1).step_by(2).map(materialize).collect();
            ok(LogicalType::map(find_common_type(&keys)?, find_common_type(&values)?).not_null())
        }
        "TYPEOF" => ok(LogicalType::string().not_null()),
        "SOURCE_WATERMARK" => ok(LogicalType::timestamp_ltz(3)),
        "GROUPING" | "GROUPING_ID" | "GROUP_ID" => ok(LogicalType::bigint().not_null()),
        _ => {
            let _ = star;
            None
        }
    }
}
