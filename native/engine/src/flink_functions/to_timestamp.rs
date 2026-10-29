//! `TO_TIMESTAMP(string)`: Flink's `DateTimeUtils.parseTimestampData` with its default format
//! `yyyy-[MM][M]-[dd][d][ [HH][H]:[mm][m]:[ss][s][.f…]]`, truncated to millisecond precision.
//! An unparsable string is NULL, as Flink's function returns.

use arrow::array::{Array, ArrayRef};
use arrow::datatypes::DataType;
use datafusion::common::{exec_err, Result};
use datafusion::logical_expr::ScalarUDF;
use std::sync::Arc;
use streamfusion_bridge::timestamp::{timestamp_array, timestamp_type, TimestampValue};

pub(crate) fn function() -> ScalarUDF {
    super::udf(
        "flink_to_timestamp",
        vec![DataType::Utf8],
        timestamp_type(),
        to_timestamp,
    )
}

fn to_timestamp(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [input] = args else {
        return exec_err!("TO_TIMESTAMP expects one argument");
    };
    let input = arrow::compute::cast(input, &DataType::Utf8)?;
    let input = datafusion::common::cast::as_string_array(&input)?;
    let values: Vec<Option<TimestampValue>> = (0..input.len())
        .map(|i| {
            if input.is_null(i) {
                return Ok(None);
            }
            parse(input.value(i))
                .map(|millis| TimestampValue::new(millis, 0))
                .transpose()
        })
        .collect::<std::result::Result<_, _>>()?;
    Ok(Arc::new(timestamp_array(values)))
}

/// Digits of a field `[XX][X]`: one or two (`max` 2), or exactly `exact`.
fn digits(s: &str, min: usize, max: usize) -> Option<(i64, &str)> {
    let n = s.bytes().take_while(u8::is_ascii_digit).count();
    if n < min || n > max {
        return None;
    }
    Some((s[..n].parse().ok()?, &s[n..]))
}

fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

fn month_days(y: i64, m: i64) -> i64 {
    match m {
        2 if (y % 4 == 0 && y % 100 != 0) || y % 400 == 0 => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    }
}

/// Epoch milliseconds of `s`, or `None` when it does not match the default format.
pub(crate) fn parse(s: &str) -> Option<i64> {
    let (year, rest) = digits(s, 4, 4)?;
    let rest = rest.strip_prefix('-')?;
    let (month, rest) = digits(rest, 1, 2)?;
    let rest = rest.strip_prefix('-')?;
    let (day, rest) = digits(rest, 1, 2)?;
    if !(1..=12).contains(&month) || day < 1 || day > month_days(year, month) {
        return None;
    }
    let mut millis_of_day = 0;
    if !rest.is_empty() {
        let rest = rest.strip_prefix(' ')?;
        let (h, rest) = digits(rest, 1, 2)?;
        let rest = rest.strip_prefix(':')?;
        let (m, rest) = digits(rest, 1, 2)?;
        let rest = rest.strip_prefix(':')?;
        let (sec, rest) = digits(rest, 1, 2)?;
        if h > 23 || m > 59 || sec > 59 {
            return None;
        }
        let fraction = match rest.strip_prefix('.') {
            Some(f) => {
                if f.len() > 9 || !f.bytes().all(|b| b.is_ascii_digit()) {
                    return None;
                }
                let mut ms = f.chars().take(3).collect::<String>();
                while ms.len() < 3 {
                    ms.push('0');
                }
                ms.parse::<i64>().ok()?
            }
            None if rest.is_empty() => 0,
            None => return None,
        };
        millis_of_day = ((h * 60 + m) * 60 + sec) * 1000 + fraction;
    }
    Some(days_from_civil(year, month, day) * 86_400_000 + millis_of_day)
}

#[cfg(test)]
mod tests {
    use super::parse;

    #[test]
    fn parses_flinks_default_timestamp_format() {
        assert_eq!(parse("1970-01-01 00:00:00"), Some(0));
        assert_eq!(parse("1970-01-01"), Some(0));
        assert_eq!(parse("1970-1-2 0:0:1.5"), Some(86_401_500));
        assert_eq!(parse("2020-04-15 08:00:28.123456"), Some(1_586_937_628_123));
        assert_eq!(parse("2020-02-30 00:00:00"), None);
        assert_eq!(parse("2020-04-15T08:00:28"), None);
        assert_eq!(parse("2020-04-15 08:00"), None);
        assert_eq!(parse("abc"), None);
    }
}
