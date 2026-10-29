//! The Nexmark generator: determinism, proportions, pinned seed-0 values,
//! the CLI, and the `dev/corpus/nexmark` cases and their shared dataset.

use std::path::{Path, PathBuf};
use std::process::Command;

use loams_stream_conformance::case::discover;
use loams_stream_conformance::nexmark::{
    Config, DEFAULT_BASE_TIME_MS, Event, Generator, format_timestamp, write_dataset,
    write_side_input,
};

/// A scratch directory under the crate (never /tmp), emptied first.
fn scratch(name: &str) -> PathBuf {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join(format!("target-test-{name}"));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn read_all(dir: &Path) -> Vec<Vec<u8>> {
    ["person.json", "auction.json", "bid.json"]
        .iter()
        .map(|f| std::fs::read(dir.join(f)).unwrap())
        .collect()
}

fn config(events: u64, seed: u64) -> Config {
    Config {
        events,
        seed,
        ..Config::default()
    }
}

#[test]
fn nexmark_gen_is_deterministic() {
    let dir = scratch("nexmark-determinism");
    let a = write_dataset(config(1000, 0), &dir.join("a")).unwrap();
    let b = write_dataset(config(1000, 0), &dir.join("b")).unwrap();
    let c = write_dataset(config(1000, 7), &dir.join("c")).unwrap();
    let (ra, rb, rc) = (
        read_all(&dir.join("a")),
        read_all(&dir.join("b")),
        read_all(&dir.join("c")),
    );
    std::fs::remove_dir_all(&dir).unwrap();
    assert_eq!(a, b);
    assert_eq!(
        ra, rb,
        "same (events, seed) must give byte-identical output"
    );
    // Ids and timestamps do not depend on the seed; the counts match too.
    assert_eq!(a, c);
    for (x, y) in ra.iter().zip(&rc) {
        assert_ne!(x, y, "a different seed must change every file");
    }
    assert!(ra.iter().all(|f| !f.is_empty()));
}

#[test]
fn proportions_are_one_person_three_auctions_46_bids_per_50_events() {
    // Event ids run from 2 (firstEventId 1 + firstEventNumber 1) to N + 1;
    // id % 50 == 0 is a person, 1..=3 an auction, the rest bids.
    let mut counts = [0u64; 3];
    for e in Generator::new(config(5000, 0)) {
        counts[match e {
            Event::Person(_) => 0,
            Event::Auction(_) => 1,
            Event::Bid(_) => 2,
        }] += 1;
    }
    // Ids 2..=5001: persons 50, 100, ..., 5000; auctions 2, 3, then
    // 51..=53, ..., 4951..=4953, then 5001.
    assert_eq!(counts, [100, 300, 4600]);
    // Every whole block of 50 consecutive event ids holds 1 : 3 : 46.
    let kinds: Vec<u8> = Generator::new(config(5000, 3))
        .map(|e| match e {
            Event::Person(_) => 0,
            Event::Auction(_) => 1,
            Event::Bid(_) => 2,
        })
        .collect();
    // Event id 50 is at index 48.
    for block in kinds[48..48 + 4950].chunks(50) {
        let n = |k| block.iter().filter(|&&x| x == k).count();
        assert_eq!((n(0), n(1), n(2)), (1, 3, 46));
    }
}

/// Values derived by hand from the upstream Java logic (and an independent
/// Python transcription of `java.util.Random`), for seed 0 and the
/// defaults (rate 10 000/s: 100 us between events).
#[test]
fn seed_zero_values_match_the_upstream_logic() {
    let events: Vec<Event> = Generator::new(config(60, 0)).collect();
    let base = DEFAULT_BASE_TIME_MS;

    // Index 0 is event id 2: rem 2 is an auction; lastBase0AuctionId(2) = 1.
    // new Random(2): nextInt(4) = 2 > 0 so the hot seller (base0 0) = 1000;
    // category 10 + nextInt(5) = 12; initialBid round(10^(6 nextDouble) * 100).
    let Event::Auction(a) = &events[0] else {
        panic!("event id 2 is an auction: {:?}", events[0])
    };
    assert_eq!(a.id, 1001);
    assert_eq!(a.seller, 1000);
    assert_eq!(a.category, 12);
    assert_eq!(a.initial_bid, 25_626_290);
    // Event number 1 at 100 us: (long)(1 * 100.0) / 1000 = 0 ms.
    assert_eq!(a.date_time, base);
    assert!(a.reserve > a.initial_bid);
    assert!(a.expires > a.date_time);

    // Index 2 is event id 4, a bid. new Random(4): nextInt(2) = 1 (hot
    // auction: lastBase0AuctionId(4) = 2, / 100 * 100 = 0 -> 1000);
    // nextInt(4) = 3 (hot bidder: 0 / 100 * 100 + 1, then + 1000 twice).
    let Event::Bid(b) = &events[2] else {
        panic!("event id 4 is a bid: {:?}", events[2])
    };
    assert_eq!((b.auction, b.bidder, b.price), (1000, 2001, 32_530_001));
    assert!(b.url.starts_with("https://www.nexmark.com/"));
    assert!(b.url.contains("/item.htm?query=1"));

    // Index 48 is event id 50, the first person: lastBase0PersonId(50) = 1.
    let Event::Person(p) = &events[48] else {
        panic!("event id 50 is a person: {:?}", events[48])
    };
    assert_eq!(p.id, 1001);
    assert_eq!(p.name, "Sarah Smith");
    assert_eq!(p.email_address, "f  y@yumv.com");
    assert_eq!(p.credit_card, "0892 2755 1923 0591");
    assert_eq!((p.city.as_str(), p.state.as_str()), ("Los Angeles", "ID"));
    // Event number 49: 4900 us -> 4 ms.
    assert_eq!(p.date_time, base + 4);
    assert_eq!(format_timestamp(p.date_time), "2026-01-01 00:00:00.004");
    // nextExtra: currentSize = 8 + 11 + 13 + 19 + 11 + 2 = 64, so 136 +- 27.
    assert!((109..163).contains(&p.extra.len()), "{}", p.extra.len());
}

#[test]
fn the_cli_writes_the_same_dataset() {
    let dir = scratch("nexmark-cli");
    let status = Command::new(env!("CARGO_BIN_EXE_loams-stream-oracle"))
        .args([
            "nexmark-gen",
            "--events",
            "500",
            "--seed",
            "3",
            "--rate",
            "50",
        ])
        .args(["--side-input", "5", "--out"])
        .arg(dir.join("cli"))
        .status()
        .unwrap();
    let lib = write_dataset(
        Config {
            first_event_rate: 50,
            ..config(500, 3)
        },
        &dir.join("lib"),
    );
    let (cli, libr) = (read_all(&dir.join("cli")), read_all(&dir.join("lib")));
    let side = std::fs::read_to_string(dir.join("cli/side_input.csv"));
    let bad = Command::new(env!("CARGO_BIN_EXE_loams-stream-oracle"))
        .args(["nexmark-gen", "--seed", "1"])
        .status()
        .unwrap();
    std::fs::remove_dir_all(&dir).unwrap();
    assert!(status.success());
    lib.unwrap();
    assert_eq!(cli, libr);
    assert_eq!(side.unwrap(), "0,0\n1,1\n2,2\n3,3\n4,4\n");
    assert!(!bad.success(), "--events is required");
}

fn corpus() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../dev/corpus/nexmark")
}

/// The command in `dev/corpus/nexmark/README.md` regenerates the shared
/// dataset byte for byte.
#[test]
fn the_shared_nexmark_dataset_is_reproducible() {
    let dir = scratch("nexmark-corpus");
    write_dataset(
        Config {
            first_event_rate: 50,
            base_time_ms: 1_767_254_370_000,
            ..config(3000, 0)
        },
        &dir,
    )
    .unwrap();
    write_side_input(10_000, &dir).unwrap();
    let fresh = read_all(&dir);
    let fresh_side = std::fs::read(dir.join("side_input.csv")).unwrap();
    std::fs::remove_dir_all(&dir).unwrap();
    let data = corpus().join("data");
    let side = std::fs::read(data.join("side_input/side_input.csv")).unwrap();
    assert!(
        fresh_side == side,
        "dev/corpus/nexmark/data/side_input is stale"
    );
    let shipped: Vec<Vec<u8>> = ["person", "auction", "bid"]
        .iter()
        .map(|t| std::fs::read(data.join(t).join(format!("{t}.json"))).unwrap())
        .collect();
    assert!(fresh == shipped, "dev/corpus/nexmark/data is stale");
}

/// Every case q0..q22 loads (format and source rules) and sees the shared
/// dataset through its `data` link.
#[test]
fn the_nexmark_cases_load() {
    let cases = discover(&corpus()).unwrap();
    let mut names: Vec<String> = cases.iter().map(|c| c.name.clone()).collect();
    names.sort();
    let mut want: Vec<String> = (0..=22).map(|i| format!("q{i}")).collect();
    want.sort();
    assert_eq!(names, want);
    for c in &cases {
        assert!(
            c.dir.join("data/bid/bid.json").is_file(),
            "{}: data/ does not reach the shared dataset",
            c.name
        );
        assert_eq!(
            c.spec.sink.as_deref(),
            Some(format!("nexmark_{}", c.name).as_str())
        );
    }
}
