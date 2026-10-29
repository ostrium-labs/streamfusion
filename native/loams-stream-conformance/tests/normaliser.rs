//! `normaliser_never_hides_rows_types_or_rowkinds` (FC1 Review Focus 1):
//! property tests over generated result pairs. A result is never accepted
//! as equal when a row, a column type or name, a significant `RowKind` or a
//! float's text form differs; and the normaliser only forgives what the
//! case declared (order, and the changelog when it is not significant).

use loams_stream_conformance::normalise::{Rules, compare, normalise};
use loams_stream_conformance::result::{Column, ResultTable, Row, RowKind};
use proptest::prelude::*;

const TYPES: [&str; 6] = [
    "INTEGER",
    "BIGINT NOT NULL",
    "DOUBLE",
    "DECIMAL(10, 2)",
    "VARCHAR(2147483647)",
    "TIMESTAMP(3)",
];

/// Field texts, including numbers that are equal as values but not as text.
const FIELDS: [&str; 16] = [
    "1",
    "1.0",
    "1.00",
    "1E0",
    "1.0E20",
    "1E20",
    "0.1",
    "0.10",
    "-0.0",
    "0.0",
    "null",
    "\"a\"",
    "\"A\"",
    "\"\"",
    "[1,2]",
    "{\"k\":1}",
];

/// A numerically equal but textually different spelling of `f`, if any.
fn respell(f: &str) -> Option<&'static str> {
    Some(match f {
        "1" => "1.0",
        "1.0" => "1.00",
        "1.00" => "1E0",
        "1E0" => "1",
        "1.0E20" => "1E20",
        "1E20" => "1.0E20",
        "0.1" => "0.10",
        "0.10" => "0.1",
        "-0.0" => "0.0",
        "0.0" => "-0.0",
        _ => return None,
    })
}

fn kind() -> impl Strategy<Value = RowKind> {
    prop::sample::select(RowKind::ALL.to_vec())
}

fn table(width: usize, max_rows: usize) -> impl Strategy<Value = ResultTable> {
    let cols = prop::collection::vec(prop::sample::select(TYPES.to_vec()), width);
    let rows = prop::collection::vec(
        (
            kind(),
            prop::collection::vec(prop::sample::select(FIELDS.to_vec()), width),
        ),
        0..max_rows,
    );
    (cols, rows).prop_map(|(types, rows)| ResultTable {
        columns: types
            .iter()
            .enumerate()
            .map(|(i, t)| Column::new(format!("c{i}"), *t))
            .collect(),
        rows: rows.into_iter().map(|(k, f)| Row::new(k, f)).collect(),
    })
}

fn insert_only(mut t: ResultTable) -> ResultTable {
    for r in &mut t.rows {
        r.kind = RowKind::Insert;
    }
    t
}

/// The three rule sets a case can declare without an upsert key.
fn rule_sets() -> [Rules; 3] {
    [
        Rules {
            order_sensitive: true,
            changelog_significant: true,
            upsert_key: None,
        },
        Rules {
            order_sensitive: false,
            changelog_significant: true,
            upsert_key: None,
        },
        Rules {
            order_sensitive: false,
            changelog_significant: false,
            upsert_key: None,
        },
    ]
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 512, ..ProptestConfig::default() })]

    /// A result always equals itself, and a permutation of an
    /// order-insensitive result equals the original.
    #[test]
    fn equal_results_match(t in table(3, 12), seed in any::<u64>()) {
        for rules in rule_sets() {
            prop_assert_eq!(compare(&t, &t, &rules), None);
        }
        let mut shuffled = t.clone();
        let n = shuffled.rows.len();
        if n > 1 {
            let mut s = seed;
            for i in (1..n).rev() {
                s = s.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                shuffled.rows.swap(i, (s >> 33) as usize % (i + 1));
            }
        }
        for rules in rule_sets().into_iter().filter(|r| !r.order_sensitive) {
            prop_assert_eq!(compare(&t, &shuffled, &rules), None);
        }
    }

    /// Rows: one row more or one row fewer is a mismatch under every rule.
    #[test]
    fn a_missing_or_extra_row_is_never_hidden(t in table(3, 12), pick in any::<prop::sample::Index>()) {
        prop_assume!(!t.rows.is_empty());
        let i = pick.index(t.rows.len());
        let t = insert_only(t);
        let mut fewer = t.clone();
        fewer.rows.remove(i);
        let mut more = t.clone();
        more.rows.push(t.rows[i].clone());
        for rules in rule_sets() {
            prop_assert!(compare(&t, &fewer, &rules).is_some(), "{:?} hid a missing row", rules);
            prop_assert!(compare(&t, &more, &rules).is_some(), "{:?} hid an extra row", rules);
        }
    }

    /// Types and names: any change to a column is a mismatch, whatever the
    /// rows and the rules.
    #[test]
    fn a_column_change_is_never_hidden(t in table(3, 6), col in 0usize..3, ty in prop::sample::select(TYPES.to_vec())) {
        let mut retyped = t.clone();
        prop_assume!(retyped.columns[col].ty != ty);
        retyped.columns[col].ty = ty.to_owned();
        let mut renamed = t.clone();
        renamed.columns[col].name.push('x');
        let mut narrower = t.clone();
        narrower.columns.pop();
        for rules in rule_sets() {
            prop_assert!(compare(&t, &retyped, &rules).is_some());
            prop_assert!(compare(&t, &renamed, &rules).is_some());
            prop_assert!(compare(&t, &narrower, &rules).is_some());
        }
        let keyed = Rules { upsert_key: Some(vec![0]), ..rule_sets()[2].clone() };
        prop_assert!(compare(&t, &retyped, &keyed).is_some());
    }

    /// Float text: respelling one number (`1.0` as `1.00`, `0.1` as `0.10`,
    /// `1.0E20` as `1E20`) is a mismatch under every rule.
    #[test]
    fn a_float_respelling_is_never_hidden(t in table(3, 12), pick in any::<prop::sample::Index>(), col in 0usize..3) {
        prop_assume!(!t.rows.is_empty());
        let t = insert_only(t);
        let i = pick.index(t.rows.len());
        let Some(other) = respell(&t.rows[i].fields[col]) else { return Ok(()); };
        let mut respelt = t.clone();
        respelt.rows[i].fields[col] = other.to_owned();
        for rules in rule_sets() {
            prop_assert!(compare(&t, &respelt, &rules).is_some(), "{:?} hid {} -> {}", rules, t.rows[i].fields[col], other);
        }
    }

    /// RowKinds: changing one row's kind is a mismatch whenever the case
    /// marks the changelog significant; when it does not, a change that
    /// flips a row between adding and retracting still shows.
    #[test]
    fn a_rowkind_change_is_never_hidden(t in table(2, 12), pick in any::<prop::sample::Index>(), k in kind()) {
        prop_assume!(!t.rows.is_empty());
        let i = pick.index(t.rows.len());
        prop_assume!(t.rows[i].kind != k);
        let mut changed = t.clone();
        changed.rows[i].kind = k;
        for rules in rule_sets() {
            if rules.changelog_significant {
                prop_assert!(compare(&t, &changed, &rules).is_some(), "{:?} hid {} -> {}", rules, t.rows[i].kind, k);
            } else if t.rows[i].kind.is_addition() != k.is_addition() {
                prop_assert!(compare(&t, &changed, &rules).is_some(), "fold hid {} -> {}", t.rows[i].kind, k);
            }
        }
    }

    /// Order: when the case is order-sensitive, swapping two different rows
    /// is a mismatch.
    #[test]
    fn a_reordering_is_seen_when_order_matters(t in table(2, 12), a in any::<prop::sample::Index>(), b in any::<prop::sample::Index>()) {
        prop_assume!(t.rows.len() > 1);
        let (i, j) = (a.index(t.rows.len()), b.index(t.rows.len()));
        prop_assume!(t.rows[i] != t.rows[j]);
        let mut swapped = t.clone();
        swapped.rows.swap(i, j);
        prop_assert!(compare(&t, &swapped, &rule_sets()[0]).is_some());
    }

    /// Folding never invents or drops rows: the folded table's signed row
    /// count equals the changelog's additions minus its retractions.
    #[test]
    fn folding_keeps_the_net_row_count(t in table(2, 16)) {
        let folded = normalise(&t, &rule_sets()[2]);
        let net: i64 = t.rows.iter().map(|r| if r.kind.is_addition() { 1 } else { -1 }).sum();
        let folded_net: i64 = folded.rows.iter().map(|r| if r.kind.is_addition() { 1 } else { -1 }).sum();
        prop_assert_eq!(net, folded_net);
        prop_assert_eq!(&folded.columns, &t.columns);
    }

    /// Upsert folding: the last write of every key is visible; changing
    /// it, or dropping it, is a mismatch.
    #[test]
    fn upsert_folding_shows_the_last_write(t in table(2, 12), pick in any::<prop::sample::Index>(), other in prop::sample::select(FIELDS.to_vec())) {
        prop_assume!(!t.rows.is_empty());
        let mut t = t;
        // Upsert streams carry additions and deletions only.
        for r in &mut t.rows {
            if r.kind == RowKind::UpdateBefore { r.kind = RowKind::Delete; }
        }
        let rules = Rules { upsert_key: Some(vec![0]), ..rule_sets()[2].clone() };
        let i = pick.index(t.rows.len());
        let key = t.rows[i].fields[0].clone();
        // The last row with this key decides the final table.
        let last = t.rows.iter().rposition(|r| r.fields[0] == key).unwrap();
        let mut changed = t.clone();
        prop_assume!(changed.rows[last].fields[1] != other);
        changed.rows[last].fields[1] = other.to_owned();
        if t.rows[last].kind.is_addition() {
            prop_assert!(compare(&t, &changed, &rules).is_some());
            // Dropping the only write of a key removes the key.
            if t.rows.iter().filter(|r| r.fields[0] == key).count() == 1 {
                let mut dropped = t.clone();
                dropped.rows.remove(last);
                prop_assert!(compare(&t, &dropped, &rules).is_some());
            }
        }
    }

    /// Normalising is idempotent: applying it to its own output changes
    /// nothing (so the reference and the target are treated alike).
    #[test]
    fn normalising_is_idempotent(t in table(2, 12)) {
        for rules in rule_sets() {
            let once = normalise(&t, &rules);
            prop_assert_eq!(normalise(&once, &rules), once.clone());
        }
    }
}

/// The named test of FC1 Review Focus 1: runs every property above once
/// more as one gate, so the name appears in the test output.
#[test]
fn normaliser_never_hides_rows_types_or_rowkinds() {
    use RowKind::*;
    let cols = vec![
        Column::new("k", "BIGINT NOT NULL"),
        Column::new("v", "DOUBLE"),
    ];
    let t = |rows: &[(RowKind, &str, &str)]| ResultTable {
        columns: cols.clone(),
        rows: rows
            .iter()
            .map(|(k, a, b)| Row::new(*k, [*a, *b]))
            .collect(),
    };
    let base = t(&[
        (Insert, "1", "1.0"),
        (Insert, "2", "0.1"),
        (UpdateBefore, "2", "0.1"),
        (UpdateAfter, "2", "0.2"),
    ]);
    for rules in rule_sets() {
        // Rows.
        assert!(compare(&base, &t(&[(Insert, "1", "1.0")]), &rules).is_some());
        // Types.
        let mut retyped = base.clone();
        retyped.columns[1].ty = "FLOAT".into();
        assert!(compare(&base, &retyped, &rules).is_some());
        // Float text.
        let mut respelt = base.clone();
        respelt.rows[0].fields[1] = "1.00".into();
        assert!(compare(&base, &respelt, &rules).is_some());
        // RowKinds, when significant.
        let mut rekinded = base.clone();
        rekinded.rows[3].kind = Insert;
        assert_eq!(
            compare(&base, &rekinded, &rules).is_some(),
            rules.changelog_significant
        );
    }
    // The folded form equals the final table, and only when declared so.
    let final_table = t(&[(Insert, "1", "1.0"), (Insert, "2", "0.2")]);
    assert_eq!(compare(&final_table, &base, &rule_sets()[2]), None);
    assert!(compare(&final_table, &base, &rule_sets()[1]).is_some());
}
