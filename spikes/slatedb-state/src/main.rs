//! FC1 Task 8 spike: keyed state on slatedb (one database per subtask, keys prefixed by key group)
//! against RocksDB with checkpoint copies, on the local file system as the object store.
//! Prints one line per measurement. Report: loams docs/plans/fc1-rulings/t8.md.

use std::ops::Bound;
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use slatedb::admin::AdminBuilder;
use slatedb::config::{CheckpointOptions, CheckpointScope};
use slatedb::object_store::{ObjectStore, local::LocalFileSystem};
use slatedb::{CloneSourceSpec, Db};

const KEY_GROUPS: u16 = 128;
const N: u64 = 200_000;
const VALUE: [u8; 64] = [7; 64];

fn key(i: u64) -> Vec<u8> {
    // Flink assigns a key to a key group by its hash; any spread will do for the spike.
    let kg = (i.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 57) as u16 % KEY_GROUPS;
    let mut k = Vec::with_capacity(10);
    k.extend_from_slice(&kg.to_be_bytes());
    k.extend_from_slice(&i.to_be_bytes());
    k
}

fn pct(mut v: Vec<u128>, p: f64) -> u128 {
    v.sort_unstable();
    v[((v.len() as f64 - 1.0) * p) as usize]
}

async fn slate(work: &std::path::Path) {
    std::fs::create_dir_all(work).unwrap();
    let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(work).unwrap());
    let db = Db::open("subtask-0", Arc::clone(&store)).await.unwrap();
    let t = Instant::now();
    // In slatedb 0.17 a put returns without waiting for durability (WriteHandle::await_durable).
    for i in 0..N {
        db.put(key(i), VALUE).await.unwrap();
    }
    let put = t.elapsed();
    let t = Instant::now();
    db.flush().await.unwrap();
    println!("slatedb put {N} keys: {:?} ({:.0} puts/s), flush {:?}", put, N as f64 / put.as_secs_f64(), t.elapsed());
    let mut lat = vec![];
    for j in 0..10_000u64 {
        let i = (j * 7919) % N;
        let t = Instant::now();
        assert!(db.get(key(i)).await.unwrap().is_some());
        lat.push(t.elapsed().as_micros());
    }
    println!("slatedb get (warm, no extra cache): p50 {} us, p99 {} us", pct(lat.clone(), 0.5), pct(lat, 0.99));
    let t = Instant::now();
    let cp = db
        .create_checkpoint(CheckpointScope::All, &CheckpointOptions::default())
        .await
        .unwrap();
    println!("slatedb checkpoint: {:?} (a manifest pointer; no data copied)", t.elapsed());
    db.close().await.unwrap();

    // Rescale: a new subtask owns key groups 0..64, cloned from the checkpoint with a projection.
    let t = Instant::now();
    let lo = Bytes::from(0u16.to_be_bytes().to_vec());
    let hi = Bytes::from(64u16.to_be_bytes().to_vec());
    let admin = AdminBuilder::new("subtask-new", Arc::clone(&store)).build();
    admin
        .create_clone_builder_from_source(
            CloneSourceSpec::with_checkpoint("subtask-0", cp.id)
                .with_projection_range((Bound::Included(lo.clone()), Bound::Excluded(hi.clone()))),
        )
        .build()
        .await
        .unwrap();
    let cloned = t.elapsed();
    let db2 = Db::open("subtask-new", Arc::clone(&store)).await.unwrap();
    let t = Instant::now();
    let mut it = db2.scan::<std::ops::RangeFull>(..).await.unwrap();
    let mut n = 0u64;
    while let Some(kv) = it.next().await.unwrap() {
        assert!(kv.key.as_ref() < hi.as_ref());
        n += 1;
    }
    println!("slatedb clone of key groups 0..64 from the checkpoint: {cloned:?}; scan of the clone: {n} keys in {:?}", t.elapsed());
    db2.close().await.unwrap();
}

fn rocks(work: &std::path::Path) {
    use rocksdb::{DB, Options, checkpoint::Checkpoint};
    std::fs::create_dir_all(work).unwrap();
    let mut opts = Options::default();
    opts.create_if_missing(true);
    let db = DB::open(&opts, work.join("subtask-0")).unwrap();
    let t = Instant::now();
    for i in 0..N {
        db.put(key(i), VALUE).unwrap();
    }
    let put = t.elapsed();
    let t = Instant::now();
    db.flush().unwrap();
    println!("rocksdb put {N} keys: {:?} ({:.0} puts/s), flush {:?}", put, N as f64 / put.as_secs_f64(), t.elapsed());
    let mut lat = vec![];
    for j in 0..10_000u64 {
        let i = (j * 7919) % N;
        let t = Instant::now();
        assert!(db.get(key(i)).unwrap().is_some());
        lat.push(t.elapsed().as_micros());
    }
    println!("rocksdb get: p50 {} us, p99 {} us", pct(lat.clone(), 0.5), pct(lat, 0.99));
    let t = Instant::now();
    let cp_dir = work.join("checkpoint");
    Checkpoint::new(&db).unwrap().create_checkpoint(&cp_dir).unwrap();
    let local = t.elapsed();
    // "Upload": copy the checkpoint's files to the object-store directory, as an incremental
    // checkpoint uploads new SSTs (all are new on the first checkpoint).
    let t = Instant::now();
    let remote = work.join("remote");
    std::fs::create_dir_all(&remote).unwrap();
    let mut bytes = 0u64;
    for e in std::fs::read_dir(&cp_dir).unwrap().flatten() {
        bytes += std::fs::copy(e.path(), remote.join(e.file_name())).unwrap();
    }
    println!("rocksdb checkpoint: {local:?} local, then {bytes} bytes copied out in {:?}", t.elapsed());
    // Rescale: a new subtask reads key groups 0..64 from the checkpoint and writes them.
    let t = Instant::now();
    let src = DB::open_for_read_only(&Options::default(), &remote, false).unwrap();
    let dst = DB::open(&opts, work.join("subtask-new")).unwrap();
    let mut n = 0u64;
    for kv in src.iterator(rocksdb::IteratorMode::Start) {
        let (k, v) = kv.unwrap();
        if k[..2] < 64u16.to_be_bytes()[..] {
            dst.put(k, v).unwrap();
            n += 1;
        }
    }
    dst.flush().unwrap();
    println!("rocksdb restore of key groups 0..64: {n} keys in {:?}", t.elapsed());
}

#[tokio::main]
async fn main() {
    let work = std::path::PathBuf::from(std::env::args().nth(1).expect("work directory"));
    let _ = std::fs::remove_dir_all(&work);
    slate(&work.join("slatedb")).await;
    rocks(&work.join("rocksdb"));
    let _ = std::fs::remove_dir_all(&work);
}
