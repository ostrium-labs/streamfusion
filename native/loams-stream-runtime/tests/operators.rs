//! StreamFusion's operator tests, re-run through the [`Driver`] from [`OpSpec`]s, with the engine
//! built without JNI (FC1 Task 2, `ported_operator_tests_pass_without_jni`). Each test names the
//! engine test it mirrors; inputs and expected outputs are that test's.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int8Array, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use loams_stream_plan::{
    CalcSpec, EncodedExpr, GroupAggSpec, KeepFirstSpec, OpSpec, WindowAggSpec, WindowShape,
};
use loams_stream_runtime::Driver;

fn values(batch: &RecordBatch, column: usize) -> Vec<i64> {
    batch
        .column(column)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap_or_else(|| panic!("column {column} is Int64: {:?}", batch.schema()))
        .values()
        .to_vec()
}

fn row_kinds(batch: &RecordBatch) -> Vec<i8> {
    let column = batch.column_by_name("$row_kind$").expect("row kinds");
    column
        .as_any()
        .downcast_ref::<Int8Array>()
        .unwrap()
        .values()
        .to_vec()
}

fn concat(batches: Vec<RecordBatch>) -> RecordBatch {
    assert!(!batches.is_empty(), "the operator emitted rows");
    arrow::compute::concat_batches(&batches[0].schema(), &batches).unwrap()
}

/// A `[ts, value0, key0]` batch (bigint value and key), as the engine's `keyed_window_batch`.
fn keyed_window_batch(ts_millis: i64, keys: Vec<i64>) -> RecordBatch {
    let n = keys.len();
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("ts", DataType::Int64, false),
            Field::new("value0", DataType::Int64, true),
            Field::new("key0", DataType::Int64, false),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![ts_millis; n])),
            Arc::new(Int64Array::from(vec![1i64; n])),
            Arc::new(Int64Array::from(keys)),
        ],
    )
    .unwrap()
}

fn window_spec(shape: WindowShape) -> OpSpec {
    // SUM (kind 0) over a bigint value (type 0).
    OpSpec::WindowAgg(WindowAggSpec {
        shape,
        value_types: vec![0],
        aggregate_kinds: vec![0],
        memory_budget_bytes: -1,
    })
}

// Mirrors `window_state_reserves_and_releases_memory`: two tumbling windows, the first closed by
// the watermark at 1000, the rest at 2000; output columns are key0, window_start, window_end, SUM.
#[test]
fn tumbling_window_closes_on_watermark() {
    let mut d = Driver::new(&window_spec(WindowShape::Tumble { size_ms: 1000 })).unwrap();
    assert!(d.push(0, keyed_window_batch(0, vec![1, 2])).unwrap().is_empty());
    assert!(d.push(0, keyed_window_batch(1500, vec![1])).unwrap().is_empty());
    let first = concat(d.watermark(1000).unwrap());
    let mut keys = values(&first, 0);
    keys.sort();
    assert_eq!(keys, vec![1, 2]);
    assert_eq!(values(&first, 1), vec![0, 0]); // window_start
    assert_eq!(values(&first, 2), vec![1000, 1000]); // window_end
    assert_eq!(values(&first, 3), vec![1, 1]); // SUM
    let second = concat(d.watermark(2000).unwrap());
    assert_eq!(values(&second, 0), vec![1]);
    assert_eq!(values(&second, 1), vec![1000]);
    assert!(d.watermark(3000).unwrap().is_empty());
}

// A hopping window of 2000 sliding by 1000: a row at 1500 falls into (0,2000] and (1000,3000].
#[test]
fn hopping_window_assigns_each_row_to_every_overlapping_window() {
    let mut d = Driver::new(&window_spec(WindowShape::Hop {
        size_ms: 2000,
        slide_ms: 1000,
    }))
    .unwrap();
    d.push(0, keyed_window_batch(1500, vec![7])).unwrap();
    let out = concat(d.finish().unwrap());
    let mut ends = values(&out, 2);
    ends.sort();
    assert_eq!(ends, vec![2000, 3000]);
    assert_eq!(values(&out, 3), vec![1, 1]);
}

// Cumulative windows of up to 3000 by 1000 steps: a row at 500 is in every nested window of the
// bucket, (0,1000], (0,2000], (0,3000].
#[test]
fn cumulative_window_emits_every_nested_window() {
    let mut d = Driver::new(&window_spec(WindowShape::Cumulate {
        max_size_ms: 3000,
        step_ms: 1000,
    }))
    .unwrap();
    d.push(0, keyed_window_batch(500, vec![1])).unwrap();
    let out = concat(d.finish().unwrap());
    assert_eq!(values(&out, 1), vec![0, 0, 0]);
    assert_eq!(values(&out, 2), vec![1000, 2000, 3000]);
    assert_eq!(values(&out, 3), vec![1, 1, 1]);
}

fn group_changelog(keys: Vec<i64>, values: Vec<i64>, kinds: Vec<i8>) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("key0", DataType::Int64, false),
            Field::new("value0", DataType::Int64, true),
            Field::new("$row_kind$", DataType::Int8, false),
        ])),
        vec![
            Arc::new(Int64Array::from(keys)),
            Arc::new(Int64Array::from(values)),
            Arc::new(Int8Array::from(kinds)),
        ],
    )
    .unwrap()
}

fn group_spec(kinds: Vec<i64>, value_columns: Vec<i64>) -> OpSpec {
    OpSpec::GroupAgg(GroupAggSpec {
        value_types: vec![0; kinds.len()],
        aggregate_kinds: kinds,
        value_columns,
        key_columns: vec![0],
        key_timestamp_precisions: vec![],
        filter_columns: vec![],
        count_columns: vec![],
        distinct_view_columns: vec![],
        record_count_column: -1,
        generate_update_before: true,
        mini_batch: false,
        state_ttl_ms: 0,
        memory_budget_bytes: -1,
        emit_unchanged_with_ttl: false,
    })
}

// Mirrors `group_by_emits_insert_then_update_changelog`.
#[test]
fn group_by_emits_insert_then_update_changelog() {
    let mut d = Driver::new(&group_spec(vec![0], vec![1])).unwrap();
    let out = concat(
        d.push(0, group_changelog(vec![1, 1, 2, 1], vec![1, 2, 5, 0], vec![0; 4]))
            .unwrap(),
    );
    assert_eq!(row_kinds(&out), vec![0, 1, 2, 0]);
    assert_eq!(values(&out, 0), vec![1, 1, 1, 2]);
    assert_eq!(values(&out, 1), vec![1, 1, 3, 5]);
}

// Mirrors `group_by_counts_every_row_for_count_star`.
#[test]
fn group_by_counts_every_row_for_count_star() {
    let mut d = Driver::new(&group_spec(vec![3, 0], vec![-1, 1])).unwrap();
    let out = concat(
        d.push(0, group_changelog(vec![1, 1], vec![10, 5], vec![0, 0]))
            .unwrap(),
    );
    assert_eq!(row_kinds(&out), vec![0, 1, 2]);
    assert_eq!(values(&out, 1), vec![1, 1, 2]);
    assert_eq!(values(&out, 2), vec![10, 10, 15]);
}

fn dedup_batch(keys: Vec<i64>, times: Vec<i64>, vals: Vec<i64>) -> RecordBatch {
    RecordBatch::try_from_iter(vec![
        ("k", Arc::new(Int64Array::from(keys)) as ArrayRef),
        ("rt", Arc::new(Int64Array::from(times)) as ArrayRef),
        ("v", Arc::new(Int64Array::from(vals)) as ArrayRef),
    ])
    .unwrap()
}

// Mirrors `replacements_preserve_winning_arrival_order_and_incumbent_ties` (without the snapshot,
// which the driver does not cover yet): first row per key by event time, released by the watermark.
#[test]
fn keep_first_by_event_time_releases_on_watermark() {
    let mut d = Driver::new(&OpSpec::KeepFirst(KeepFirstSpec {
        partition_columns: vec![0],
        key_timestamp_precisions: vec![],
        rowtime_column: 1,
        state_ttl_ms: 0,
        memory_budget_bytes: -1,
    }))
    .unwrap();
    d.push(0, dedup_batch(vec![1, 2, 3], vec![30, 40, 50], vec![10, 20, 30]))
        .unwrap();
    d.push(
        0,
        dedup_batch(
            vec![2, 1, 4, 2, 3],
            vec![20, 30, 60, 20, 40],
            vec![200, 100, 40, 201, 300],
        ),
    )
    .unwrap();
    let early = concat(d.watermark(35).unwrap());
    assert_eq!(values(&early, 0), vec![1, 2]);
    assert_eq!(values(&early, 2), vec![10, 200]);
    d.push(0, dedup_batch(vec![4, 3], vec![60, 40], vec![400, 301]))
        .unwrap();
    let rest = concat(d.finish().unwrap());
    assert_eq!(values(&rest, 0), vec![4, 3]);
    assert_eq!(values(&rest, 2), vec![40, 300]);
}

// A Calc projecting a column and a bigint literal, with a filter on a boolean literal: the engine's
// prefix encoding (kind 0 column, 1 bigint literal, 4 boolean literal).
#[test]
fn calc_projects_columns_and_literals() {
    let spec = OpSpec::Calc(CalcSpec {
        expr: EncodedExpr {
            kinds: vec![0, 1, 4],
            payload: vec![1, 0, 1],
            child_counts: vec![0, 0, 0],
            longs: vec![7, 1],
            doubles: vec![],
            strings: vec![],
        },
        projection_roots: vec![0, 1],
        condition_root: 2,
        output_names: vec!["b".into(), "seven".into()],
    });
    let mut d = Driver::new(&spec).unwrap();
    let input = RecordBatch::try_from_iter(vec![
        ("a", Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef),
        ("b", Arc::new(Int64Array::from(vec![10, 20])) as ArrayRef),
    ])
    .unwrap();
    let out = concat(d.push(0, input).unwrap());
    assert_eq!(out.schema().field(0).name(), "b");
    assert_eq!(out.schema().field(1).name(), "seven");
    assert_eq!(values(&out, 0), vec![10, 20]);
    assert_eq!(values(&out, 1), vec![7, 7]);
}

#[test]
fn watermarks_never_go_back_and_inputs_are_checked() {
    let mut d = Driver::new(&window_spec(WindowShape::Tumble { size_ms: 1000 })).unwrap();
    d.watermark(100).unwrap();
    assert!(d.watermark(50).is_err());
    assert!(d.push(1, keyed_window_batch(0, vec![1])).is_err());
}

/// Nexmark q0 (pass-through) and a q5-like tumbling count per auction, driven from hand-written
/// specs over generated bids (FC1 Task 2 exit).
#[test]
fn nexmark_q0_and_q5_like_run_in_the_driver() {
    // 1000 bids: auction = i % 10, event time i * 10 ms.
    let n = 1000i64;
    let auction: Vec<i64> = (0..n).map(|i| i % 10).collect();
    let price: Vec<i64> = (0..n).map(|i| 100 + i).collect();
    let time: Vec<i64> = (0..n).map(|i| i * 10).collect();
    let bids = RecordBatch::try_from_iter(vec![
        ("auction", Arc::new(Int64Array::from(auction.clone())) as ArrayRef),
        ("price", Arc::new(Int64Array::from(price)) as ArrayRef),
        ("dateTime", Arc::new(Int64Array::from(time.clone())) as ArrayRef),
    ])
    .unwrap();
    // q0: SELECT auction, price, dateTime FROM bid.
    let q0 = OpSpec::Calc(CalcSpec {
        expr: EncodedExpr {
            kinds: vec![0, 0, 0],
            payload: vec![0, 1, 2],
            child_counts: vec![0, 0, 0],
            ..Default::default()
        },
        projection_roots: vec![0, 1, 2],
        condition_root: -1,
        output_names: vec!["auction".into(), "price".into(), "dateTime".into()],
    });
    let mut d = Driver::new(&q0).unwrap();
    let out = concat(d.push(0, bids).unwrap());
    assert_eq!(out.num_rows(), n as usize);
    assert_eq!(values(&out, 0), auction);

    // q5-like: COUNT(*) per auction over 2-second tumbling windows (window aggregate input
    // layout: ts, value0, key0).
    let window_input = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("ts", DataType::Int64, false),
            Field::new("value0", DataType::Int64, true),
            Field::new("key0", DataType::Int64, false),
        ])),
        vec![
            Arc::new(Int64Array::from(time)),
            Arc::new(Int64Array::from(vec![1i64; n as usize])),
            Arc::new(Int64Array::from(auction)),
        ],
    )
    .unwrap();
    let count = OpSpec::WindowAgg(WindowAggSpec {
        shape: WindowShape::Tumble { size_ms: 2000 },
        value_types: vec![0],
        aggregate_kinds: vec![3],
        memory_budget_bytes: -1,
    });
    let mut d = Driver::new(&count).unwrap();
    d.push(0, window_input).unwrap();
    let out = concat(d.finish().unwrap());
    // 10 seconds of bids: 5 windows x 10 auctions, 20 bids each.
    assert_eq!(out.num_rows(), 50);
    assert!(values(&out, 3).iter().all(|c| *c == 20), "{:?}", values(&out, 3));
}
