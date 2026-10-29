//! A deterministic Nexmark event generator (FC1 Task 6, design 82 §15;
//! preview).
//!
//! Ported from nexmark/nexmark at commit
//! 6b3646c3baec701f1fa74baf938d235f742e5d3c, files
//! `nexmark-flink/src/main/java/com/github/nexmark/flink/generator/`
//! (`GeneratorConfig.java`, `NexmarkGenerator.java`, `SideInputGenerator.java`,
//! `model/PersonGenerator.java`, `model/AuctionGenerator.java`,
//! `model/BidGenerator.java`, `model/PriceGenerator.java`,
//! `model/StringsGenerator.java`, `model/LongGenerator.java`) and the defaults of
//! `NexmarkConfiguration.java` / `source/NexmarkSourceOptions.java`.
//! Licensed under the Apache License, Version 2.0; the upstream LICENSE is
//! vendored at `dev/corpus/nexmark/vendor/LICENSE`, the provenance in
//! `dev/corpus/nexmark/NOTICE`.
//!
//! What is the same as upstream: the person/auction/bid proportions
//! (1:3:46), event ids (`firstEventId = 1`, `firstEventNumber = 1`, one
//! generator, as the `nexmark` table source at parallelism 1), the id
//! arithmetic (`lastBase0PersonId`, `lastBase0AuctionId`, hot sellers,
//! bidders and auctions, the `+ FIRST_PERSON_ID` that upstream applies to a
//! bidder twice), each field's derivation, the order and bounds of every
//! random draw, the 10 000-entry channel/URL cache and the 1 MiB reusable
//! extra string, and event timestamps from `baseTime` and the inter-event
//! delay `1e6 / first-event.rate` microseconds.
//!
//! What differs, on purpose: upstream draws from an unseeded
//! `java.util.SplittableRandom` (one per generator, plus unseeded ones for
//! its static caches) and takes `baseTime` from the wall clock, so no two
//! upstream runs agree. Here every event gets its own `java.util.Random`
//! (ported bit for bit: [`JavaRandom`]) seeded with its event id, as Apache
//! Beam's Nexmark generator (which nexmark/nexmark derives from) does, the
//! static caches come from `java.util.Random`s with fixed seeds, and
//! `baseTime` is an option with a fixed default. A `seed` S perturbs all
//! of these seeds (event seed = event id XOR S·0x9E3779B97F4A7C15), and S =
//! 0 is the Beam scheme. The output is a pure function of (events, seed,
//! base time, rate).
//!
//! Prices use `f64::powf`, as upstream uses `Math.pow`; both may differ
//! from the exact power by one ulp, which changes a price only on an exact
//! `.5` tie after scaling (not observed).

use std::fmt::Write as _;
use std::io::Write as _;
use std::path::Path;

/// `java.util.Random`, bit for bit: the 48-bit LCG and its `nextInt`,
/// `nextInt(bound)`, `nextLong` and `nextDouble`.
#[derive(Debug, Clone)]
pub struct JavaRandom {
    seed: i64,
}

const LCG_MULT: i64 = 0x5_DEEC_E66D;
const LCG_ADD: i64 = 0xB;
const LCG_MASK: i64 = (1 << 48) - 1;

impl JavaRandom {
    /// `new Random(seed)`.
    pub fn new(seed: i64) -> Self {
        Self {
            seed: (seed ^ LCG_MULT) & LCG_MASK,
        }
    }

    fn next(&mut self, bits: u32) -> i32 {
        self.seed = self.seed.wrapping_mul(LCG_MULT).wrapping_add(LCG_ADD) & LCG_MASK;
        (self.seed >> (48 - bits)) as i32
    }

    /// `nextInt()`.
    pub fn next_int(&mut self) -> i32 {
        self.next(32)
    }

    /// `nextInt(bound)`; `bound` must be positive.
    pub fn next_int_bounded(&mut self, bound: i32) -> i32 {
        assert!(bound > 0, "bound must be positive");
        let mut r = self.next(31);
        let m = bound - 1;
        if bound & m == 0 {
            return ((i64::from(bound) * i64::from(r)) >> 31) as i32;
        }
        let mut u = r;
        loop {
            r = u % bound;
            if u.wrapping_sub(r).wrapping_add(m) >= 0 {
                return r;
            }
            u = self.next(31);
        }
    }

    /// `nextLong()`.
    pub fn next_long(&mut self) -> i64 {
        (i64::from(self.next(32)) << 32).wrapping_add(i64::from(self.next(32)))
    }

    /// `nextDouble()`.
    pub fn next_double(&mut self) -> f64 {
        let hi = i64::from(self.next(26)) << 27;
        let lo = i64::from(self.next(27));
        (hi + lo) as f64 * (1.0 / (1u64 << 53) as f64)
    }
}

/// The generator's configuration (upstream `NexmarkConfiguration` defaults).
#[derive(Debug, Clone, PartialEq)]
pub struct Config {
    /// Events to generate.
    pub events: u64,
    /// Perturbs every random seed; 0 is the Beam scheme.
    pub seed: u64,
    /// Event time of event number 0, ms since the epoch (upstream: the wall clock).
    pub base_time_ms: i64,
    /// `first-event.rate`, events per second (upstream default 10 000).
    pub first_event_rate: u32,
    /// `person.proportion`.
    pub person_proportion: u32,
    /// `auction.proportion`.
    pub auction_proportion: u32,
    /// `bid.proportion`.
    pub bid_proportion: u32,
    /// `person.avg-size`, bytes.
    pub avg_person_byte_size: i32,
    /// `auction.avg-size`, bytes.
    pub avg_auction_byte_size: i32,
    /// `bid.avg-size`, bytes.
    pub avg_bid_byte_size: i32,
    /// `bid.hot-ratio.auctions`.
    pub hot_auction_ratio: i32,
    /// `auction.hot-ratio.sellers`.
    pub hot_sellers_ratio: i32,
    /// `bid.hot-ratio.bidders`.
    pub hot_bidders_ratio: i32,
    /// `numInFlightAuctions`.
    pub num_in_flight_auctions: i64,
    /// `numActivePeople`.
    pub num_active_people: i64,
    /// `outOfOrderGroupSize` (1: in order).
    pub out_of_order_group_size: i64,
}

/// 2026-01-01T00:00:00Z, the default base time.
pub const DEFAULT_BASE_TIME_MS: i64 = 1_767_225_600_000;

impl Default for Config {
    fn default() -> Self {
        Self {
            events: 0,
            seed: 0,
            base_time_ms: DEFAULT_BASE_TIME_MS,
            first_event_rate: 10_000,
            person_proportion: 1,
            auction_proportion: 3,
            bid_proportion: 46,
            avg_person_byte_size: 200,
            avg_auction_byte_size: 500,
            avg_bid_byte_size: 100,
            hot_auction_ratio: 2,
            hot_sellers_ratio: 4,
            hot_bidders_ratio: 4,
            num_in_flight_auctions: 100,
            num_active_people: 1000,
            out_of_order_group_size: 1,
        }
    }
}

/// A `Person` event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Person {
    pub id: i64,
    pub name: String,
    pub email_address: String,
    pub credit_card: String,
    pub city: String,
    pub state: String,
    pub date_time: i64,
    pub extra: String,
}

/// An `Auction` event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Auction {
    pub id: i64,
    pub item_name: String,
    pub description: String,
    pub initial_bid: i64,
    pub reserve: i64,
    pub date_time: i64,
    pub expires: i64,
    pub seller: i64,
    pub category: i64,
    pub extra: String,
}

/// A `Bid` event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Bid {
    pub auction: i64,
    pub bidder: i64,
    pub price: i64,
    pub channel: String,
    pub url: String,
    pub date_time: i64,
    pub extra: String,
}

/// One generated event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    Person(Person),
    Auction(Auction),
    Bid(Bid),
}

const FIRST_AUCTION_ID: i64 = 1000;
const FIRST_PERSON_ID: i64 = 1000;
const FIRST_CATEGORY_ID: i64 = 10;
const FIRST_EVENT_ID: i64 = 1;
const FIRST_EVENT_NUMBER: i64 = 1;

const NUM_CATEGORIES: i32 = 5;
const AUCTION_ID_LEAD: i64 = 10;
const PERSON_ID_LEAD: i64 = 10;
const HOT_SELLER_RATIO: i64 = 100;
const HOT_AUCTION_RATIO: i64 = 100;
const HOT_BIDDER_RATIO: i64 = 100;
const HOT_CHANNELS_RATIO: i32 = 2;
const CHANNELS_NUMBER: i32 = 10_000;
const HOT_CHANNELS: [&str; 4] = ["Google", "Facebook", "Baidu", "Apple"];

const US_STATES: [&str; 6] = ["AZ", "CA", "ID", "OR", "WA", "WY"];
const US_CITIES: [&str; 10] = [
    "Phoenix",
    "Los Angeles",
    "San Francisco",
    "Boise",
    "Portland",
    "Bend",
    "Redmond",
    "Seattle",
    "Kent",
    "Cheyenne",
];
const FIRST_NAMES: [&str; 11] = [
    "Peter", "Paul", "Luke", "John", "Saul", "Vicky", "Kate", "Julie", "Sarah", "Deiter", "Walter",
];
const LAST_NAMES: [&str; 9] = [
    "Shultz", "Abrams", "Spencer", "White", "Bartels", "Walton", "Smith", "Jones", "Noris",
];

const MIN_STRING_LENGTH: i32 = 3;
const REUSABLE_EXTRA_LEN: usize = 1024 * 1024;

/// Fixed seeds for the static caches upstream fills from unseeded randoms.
const EXTRA_STRING_SEED: i64 = 0x4578_7472_6173; // "Extras"
const BID_STATICS_SEED: i64 = 0x4269_6447_656E; // "BidGen"

/// The perturbation every seed is XORed with.
fn seed_mix(seed: u64) -> i64 {
    seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) as i64
}

/// `StringsGenerator.nextString(random, maxLength, special)`.
fn next_string(rng: &mut JavaRandom, max_length: i32, special: char) -> String {
    let mut len = MIN_STRING_LENGTH + rng.next_int_bounded(max_length - MIN_STRING_LENGTH);
    let mut s = String::with_capacity(len as usize);
    while len > 0 {
        len -= 1;
        if rng.next_int_bounded(13) == 0 {
            s.push(special);
        } else {
            s.push((b'a' + rng.next_int_bounded(26) as u8) as char);
        }
    }
    // Java's String.trim() strips every char <= U+0020 at both ends.
    s.trim_matches(|c: char| c <= ' ').to_owned()
}

/// `StringsGenerator.nextExactString` without the reusable string (used to
/// build it). Java's `rnd % 26` is negative for a negative `rnd`, so
/// characters run from 'H' to 'z', as upstream.
fn exact_string_slow(rng: &mut JavaRandom, length: usize) -> String {
    let mut s = String::with_capacity(length);
    let mut rnd = 0i32;
    let mut n = 0;
    for _ in 0..length {
        if n == 0 {
            rnd = rng.next_int();
            n = 6;
        }
        s.push((i32::from(b'a') + rnd % 26) as u8 as char);
        rnd /= 26;
        n -= 1;
    }
    s
}

/// The static state upstream keeps in class fields.
struct Statics {
    reusable_extra: String,
    hot_urls: [String; 4],
    channel_urls: Vec<(String, String)>,
}

impl Statics {
    fn new(seed: u64) -> Self {
        let mix = seed_mix(seed);
        let mut rng = JavaRandom::new(EXTRA_STRING_SEED ^ mix);
        let reusable_extra = exact_string_slow(&mut rng, REUSABLE_EXTRA_LEN);
        let mut rng = JavaRandom::new(BID_STATICS_SEED ^ mix);
        let hot_urls = std::array::from_fn(|_| base_url(&mut rng));
        let channel_urls = (0..CHANNELS_NUMBER)
            .map(|i| {
                let mut url = base_url(&mut rng);
                if rng.next_int_bounded(10) > 0 {
                    // Math.abs(Integer.reverse(i)).
                    let _ = write!(url, "&channel_id={}", i.reverse_bits().wrapping_abs());
                }
                (format!("channel-{i}"), url)
            })
            .collect();
        Self {
            reusable_extra,
            hot_urls,
            channel_urls,
        }
    }

    /// `StringsGenerator.nextExactString`.
    fn exact_string(&self, rng: &mut JavaRandom, length: usize) -> String {
        let total = self.reusable_extra.len();
        if length < total / 2 {
            let offset = rng.next_int_bounded((total - length) as i32) as usize;
            return self.reusable_extra[offset..offset + length].to_owned();
        }
        exact_string_slow(rng, length)
    }

    /// `StringsGenerator.nextExtra`.
    fn next_extra(&self, rng: &mut JavaRandom, current_size: i32, desired_average: i32) -> String {
        if current_size > desired_average {
            return String::new();
        }
        let desired = desired_average - current_size;
        let delta = (f64::from(desired) * 0.2).round() as i32;
        let min_size = desired - delta;
        let size = min_size
            + if delta == 0 {
                0
            } else {
                rng.next_int_bounded(2 * delta)
            };
        self.exact_string(rng, size as usize)
    }
}

/// `BidGenerator.getBaseUrl`.
fn base_url(rng: &mut JavaRandom) -> String {
    let a = next_string(rng, 5, '_');
    let b = next_string(rng, 5, '_');
    let c = next_string(rng, 5, '_');
    format!("https://www.nexmark.com/{a}/{b}/{c}/item.htm?query=1")
}

/// `LongGenerator.nextLong`.
fn next_long_below(rng: &mut JavaRandom, n: i64) -> i64 {
    if n < i64::from(i32::MAX) {
        i64::from(rng.next_int_bounded(n as i32))
    } else {
        (rng.next_long() % n).wrapping_abs()
    }
}

/// `PriceGenerator.nextPrice`.
fn next_price(rng: &mut JavaRandom) -> i64 {
    (10f64.powf(rng.next_double() * 6.0) * 100.0).round() as i64
}

/// The Nexmark event generator; an iterator over `config.events` events.
pub struct Generator {
    config: Config,
    statics: Statics,
    total_proportion: i64,
    inter_event_delay_us: f64,
    count: u64,
}

impl Generator {
    /// A generator for `config`.
    ///
    /// # Panics
    ///
    /// For a zero rate or zero total proportion.
    pub fn new(config: Config) -> Self {
        assert!(
            config.first_event_rate > 0,
            "first-event.rate must be positive"
        );
        let total = i64::from(config.person_proportion)
            + i64::from(config.auction_proportion)
            + i64::from(config.bid_proportion);
        assert!(total > 0, "the proportions must not all be zero");
        // GeneratorConfig: 1000000.0 / firstEventRate * numEventGenerators (one generator).
        let inter_event_delay_us = 1_000_000.0 / f64::from(config.first_event_rate) * 1.0;
        Self {
            statics: Statics::new(config.seed),
            total_proportion: total,
            inter_event_delay_us,
            count: 0,
            config,
        }
    }

    fn person_proportion(&self) -> i64 {
        i64::from(self.config.person_proportion)
    }

    fn auction_proportion(&self) -> i64 {
        i64::from(self.config.auction_proportion)
    }

    /// `GeneratorConfig.timestampForEvent`.
    fn timestamp_for_event(&self, event_number: i64) -> i64 {
        self.config.base_time_ms + ((event_number as f64 * self.inter_event_delay_us) as i64) / 1000
    }

    /// `GeneratorConfig.nextAdjustedEventNumber`.
    fn next_adjusted_event_number(&self, num_events: i64) -> i64 {
        let n = self.config.out_of_order_group_size;
        let event_number = FIRST_EVENT_NUMBER + num_events;
        let base = (event_number / n) * n;
        let offset = (event_number.wrapping_mul(953)) % n;
        base + offset
    }

    /// `PersonGenerator.lastBase0PersonId`.
    fn last_base0_person_id(&self, event_id: i64) -> i64 {
        let epoch = event_id / self.total_proportion;
        let mut offset = event_id % self.total_proportion;
        if offset >= self.person_proportion() {
            offset = self.person_proportion() - 1;
        }
        epoch * self.person_proportion() + offset
    }

    /// `PersonGenerator.nextBase0PersonId`.
    fn next_base0_person_id(&self, event_id: i64, rng: &mut JavaRandom) -> i64 {
        let num_people = self.last_base0_person_id(event_id) + 1;
        let active = num_people.min(self.config.num_active_people);
        let n = next_long_below(rng, active + PERSON_ID_LEAD);
        num_people - active + n
    }

    /// `AuctionGenerator.lastBase0AuctionId`.
    fn last_base0_auction_id(&self, event_id: i64) -> i64 {
        let mut epoch = event_id / self.total_proportion;
        let mut offset = event_id % self.total_proportion;
        if offset < self.person_proportion() {
            epoch -= 1;
            offset = self.auction_proportion() - 1;
        } else if offset >= self.person_proportion() + self.auction_proportion() {
            offset = self.auction_proportion() - 1;
        } else {
            offset -= self.person_proportion();
        }
        epoch * self.auction_proportion() + offset
    }

    /// `AuctionGenerator.nextBase0AuctionId`.
    fn next_base0_auction_id(&self, event_id: i64, rng: &mut JavaRandom) -> i64 {
        let max = self.last_base0_auction_id(event_id);
        let min = (max - self.config.num_in_flight_auctions).max(0);
        min + next_long_below(rng, max - min + 1 + AUCTION_ID_LEAD)
    }

    /// `AuctionGenerator.nextAuctionLengthMs`.
    fn next_auction_length_ms(&self, count: i64, rng: &mut JavaRandom, timestamp: i64) -> i64 {
        let current = self.next_adjusted_event_number(count);
        let events_for_auctions = (self.config.num_in_flight_auctions * self.total_proportion)
            / self.auction_proportion();
        let future_auction = self.timestamp_for_event(current + events_for_auctions);
        let horizon_ms = future_auction - timestamp;
        1 + next_long_below(rng, (horizon_ms * 2).max(1))
    }

    fn next_person(&self, event_id: i64, rng: &mut JavaRandom, timestamp: i64) -> Person {
        let id = self.last_base0_person_id(event_id) + FIRST_PERSON_ID;
        let first = FIRST_NAMES[rng.next_int_bounded(FIRST_NAMES.len() as i32) as usize];
        let last = LAST_NAMES[rng.next_int_bounded(LAST_NAMES.len() as i32) as usize];
        let name = format!("{first} {last}");
        let email = format!(
            "{}@{}.com",
            next_string(rng, 7, ' '),
            next_string(rng, 5, ' ')
        );
        let credit_card = (0..4)
            .map(|_| format!("{:04}", rng.next_int_bounded(10_000)))
            .collect::<Vec<_>>()
            .join(" ");
        let city = US_CITIES[rng.next_int_bounded(US_CITIES.len() as i32) as usize].to_owned();
        let state = US_STATES[rng.next_int_bounded(US_STATES.len() as i32) as usize].to_owned();
        let size = 8 + name.len() + email.len() + credit_card.len() + city.len() + state.len();
        let extra = self
            .statics
            .next_extra(rng, size as i32, self.config.avg_person_byte_size);
        Person {
            id,
            name,
            email_address: email,
            credit_card,
            city,
            state,
            date_time: timestamp,
            extra,
        }
    }

    fn next_auction(
        &self,
        count: i64,
        event_id: i64,
        rng: &mut JavaRandom,
        timestamp: i64,
    ) -> Auction {
        let id = self.last_base0_auction_id(event_id) + FIRST_AUCTION_ID;
        let mut seller = if rng.next_int_bounded(self.config.hot_sellers_ratio) > 0 {
            (self.last_base0_person_id(event_id) / HOT_SELLER_RATIO) * HOT_SELLER_RATIO
        } else {
            self.next_base0_person_id(event_id, rng)
        };
        seller += FIRST_PERSON_ID;
        let category = FIRST_CATEGORY_ID + i64::from(rng.next_int_bounded(NUM_CATEGORIES));
        let initial_bid = next_price(rng);
        let expires = timestamp + self.next_auction_length_ms(count, rng, timestamp);
        let item_name = next_string(rng, 20, ' ');
        let description = next_string(rng, 100, ' ');
        let reserve = initial_bid + next_price(rng);
        let size = 8 + item_name.len() + description.len() + 8 + 8 + 8 + 8 + 8;
        let extra = self
            .statics
            .next_extra(rng, size as i32, self.config.avg_auction_byte_size);
        Auction {
            id,
            item_name,
            description,
            initial_bid,
            reserve,
            date_time: timestamp,
            expires,
            seller,
            category,
            extra,
        }
    }

    fn next_bid(&self, event_id: i64, rng: &mut JavaRandom, timestamp: i64) -> Bid {
        let mut auction = if rng.next_int_bounded(self.config.hot_auction_ratio) > 0 {
            (self.last_base0_auction_id(event_id) / HOT_AUCTION_RATIO) * HOT_AUCTION_RATIO
        } else {
            self.next_base0_auction_id(event_id, rng)
        };
        auction += FIRST_AUCTION_ID;
        let mut bidder = if rng.next_int_bounded(self.config.hot_bidders_ratio) > 0 {
            (self.last_base0_person_id(event_id) / HOT_BIDDER_RATIO) * HOT_BIDDER_RATIO + 1
        } else {
            self.next_base0_person_id(event_id, rng)
        };
        bidder += FIRST_PERSON_ID;
        let price = next_price(rng);
        let (channel, url) = if rng.next_int_bounded(HOT_CHANNELS_RATIO) > 0 {
            let i = rng.next_int_bounded(HOT_CHANNELS.len() as i32) as usize;
            (HOT_CHANNELS[i].to_owned(), self.statics.hot_urls[i].clone())
        } else {
            let n = rng.next_int_bounded(CHANNELS_NUMBER) as usize;
            self.statics.channel_urls[n].clone()
        };
        // Upstream adds FIRST_PERSON_ID to the bidder a second time.
        bidder += FIRST_PERSON_ID;
        let extra = self
            .statics
            .next_extra(rng, 8 + 8 + 8 + 8, self.config.avg_bid_byte_size);
        Bid {
            auction,
            bidder,
            price,
            channel,
            url,
            date_time: timestamp,
            extra,
        }
    }
}

impl Iterator for Generator {
    type Item = Event;

    /// `NexmarkGenerator.nextEvent`.
    fn next(&mut self) -> Option<Event> {
        if self.count >= self.config.events {
            return None;
        }
        let count = self.count as i64;
        let adjusted = self.next_adjusted_event_number(count);
        let timestamp = self.timestamp_for_event(adjusted);
        let event_id = FIRST_EVENT_ID + adjusted;
        let mut rng = JavaRandom::new(event_id ^ seed_mix(self.config.seed));
        let rem = event_id % self.total_proportion;
        let event = if rem < self.person_proportion() {
            Event::Person(self.next_person(event_id, &mut rng, timestamp))
        } else if rem < self.person_proportion() + self.auction_proportion() {
            Event::Auction(self.next_auction(count, event_id, &mut rng, timestamp))
        } else {
            Event::Bid(self.next_bid(event_id, &mut rng, timestamp))
        };
        self.count += 1;
        Some(event)
    }
}

/// A timestamp in Flink `json`'s default SQL format, `yyyy-MM-dd HH:mm:ss.SSS` (UTC).
pub fn format_timestamp(ms: i64) -> String {
    let days = ms.div_euclid(86_400_000);
    let rem = ms.rem_euclid(86_400_000);
    // Howard Hinnant's civil_from_days.
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    format!(
        "{y:04}-{m:02}-{d:02} {:02}:{:02}:{:02}.{:03}",
        rem / 3_600_000,
        rem / 60_000 % 60,
        rem / 1000 % 60,
        rem % 1000
    )
}

fn js(s: &str) -> String {
    serde_json::to_string(s).expect("a string serializes")
}

impl Person {
    /// One JSON line, columns as the `person` table.
    pub fn to_json(&self) -> String {
        format!(
            "{{\"id\":{},\"name\":{},\"emailAddress\":{},\"creditCard\":{},\"city\":{},\"state\":{},\"dateTime\":{},\"extra\":{}}}",
            self.id,
            js(&self.name),
            js(&self.email_address),
            js(&self.credit_card),
            js(&self.city),
            js(&self.state),
            js(&format_timestamp(self.date_time)),
            js(&self.extra)
        )
    }
}

impl Auction {
    /// One JSON line, columns as the `auction` table.
    pub fn to_json(&self) -> String {
        format!(
            "{{\"id\":{},\"itemName\":{},\"description\":{},\"initialBid\":{},\"reserve\":{},\"dateTime\":{},\"expires\":{},\"seller\":{},\"category\":{},\"extra\":{}}}",
            self.id,
            js(&self.item_name),
            js(&self.description),
            self.initial_bid,
            self.reserve,
            js(&format_timestamp(self.date_time)),
            js(&format_timestamp(self.expires)),
            self.seller,
            self.category,
            js(&self.extra)
        )
    }
}

impl Bid {
    /// One JSON line, columns as the `bid` table.
    pub fn to_json(&self) -> String {
        format!(
            "{{\"auction\":{},\"bidder\":{},\"price\":{},\"channel\":{},\"url\":{},\"dateTime\":{},\"extra\":{}}}",
            self.auction,
            self.bidder,
            self.price,
            js(&self.channel),
            js(&self.url),
            js(&format_timestamp(self.date_time)),
            js(&self.extra)
        )
    }
}

/// How many events of each kind were written.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Counts {
    pub persons: u64,
    pub auctions: u64,
    pub bids: u64,
}

/// Generates `config.events` events into `dir/person.json`,
/// `dir/auction.json` and `dir/bid.json` (JSON lines, in event order).
///
/// # Errors
///
/// Any I/O error.
pub fn write_dataset(config: Config, dir: &Path) -> std::io::Result<Counts> {
    std::fs::create_dir_all(dir)?;
    let open = |name: &str| -> std::io::Result<std::io::BufWriter<std::fs::File>> {
        Ok(std::io::BufWriter::new(std::fs::File::create(
            dir.join(name),
        )?))
    };
    let mut person = open("person.json")?;
    let mut auction = open("auction.json")?;
    let mut bid = open("bid.json")?;
    let mut counts = Counts::default();
    for event in Generator::new(config) {
        match event {
            Event::Person(p) => {
                writeln!(person, "{}", p.to_json())?;
                counts.persons += 1;
            }
            Event::Auction(a) => {
                writeln!(auction, "{}", a.to_json())?;
                counts.auctions += 1;
            }
            Event::Bid(b) => {
                writeln!(bid, "{}", b.to_json())?;
                counts.bids += 1;
            }
        }
    }
    person.flush()?;
    auction.flush()?;
    bid.flush()?;
    Ok(counts)
}

/// `SideInputGenerator.prepareSideInput`: `rows` lines `i,i` (q13's side
/// input), written to `dir/side_input.csv`.
///
/// # Errors
///
/// Any I/O error.
pub fn write_side_input(rows: u64, dir: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(dir)?;
    let mut out = String::new();
    for i in 0..rows {
        let _ = writeln!(out, "{i},{i}");
    }
    std::fs::write(dir.join("side_input.csv"), out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn java_random_matches_known_jdk_values() {
        // Well-known outputs of java.util.Random.
        assert_eq!(JavaRandom::new(42).next_int(), -1_170_105_035);
        assert_eq!(JavaRandom::new(0).next_int(), -1_155_484_576);
        assert_eq!(JavaRandom::new(42).next_int_bounded(10), 0);
        assert_eq!(JavaRandom::new(0).next_long(), -4_962_768_465_676_381_896);
        assert_eq!(JavaRandom::new(0).next_double(), 0.730_967_787_376_657);
        assert_eq!(JavaRandom::new(42).next_double(), 0.727_563_680_032_868_1);
    }

    #[test]
    fn timestamps_format_as_flink_sql() {
        assert_eq!(format_timestamp(0), "1970-01-01 00:00:00.000");
        assert_eq!(
            format_timestamp(DEFAULT_BASE_TIME_MS),
            "2026-01-01 00:00:00.000"
        );
        assert_eq!(
            format_timestamp(951_782_400_000 + 3_723_004),
            "2000-02-29 01:02:03.004"
        );
        assert_eq!(format_timestamp(-1), "1969-12-31 23:59:59.999");
    }

    #[test]
    fn strings_trim_like_java() {
        // A run that ends in the special character is trimmed only for ' '.
        for seed in 0..200 {
            let s = next_string(&mut JavaRandom::new(seed), 20, ' ');
            assert_eq!(s, s.trim());
            assert!(s.len() < 20);
        }
    }
}
