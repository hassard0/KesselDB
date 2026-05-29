//! TPC-H lineitem data generator — deterministic, per-trial-seeded.
//!
//! Shared by every driver so KesselDB, Postgres, and SQLite all load
//! byte-identical row payloads. The generator is faithful to the per-
//! column distributions Q1 + Q6 actually filter on (`l_shipdate`,
//! `l_discount`, `l_quantity`, `l_returnflag`, `l_linestatus`); the
//! remaining columns are filled from a deterministic PRNG for layout
//! parity with the canonical dbgen row width.
//!
//! Schema (matches `docs/superpowers/specs/2026-05-28-kesseldb-bench-suite-design.md` §3):
//! ```sql
//! CREATE TABLE lineitem (
//!   l_orderkey BIGINT, l_partkey BIGINT, l_suppkey BIGINT, l_linenumber INT,
//!   l_quantity DOUBLE, l_extendedprice DOUBLE, l_discount DOUBLE, l_tax DOUBLE,
//!   l_returnflag CHAR(1), l_linestatus CHAR(1),
//!   l_shipdate INT,    -- YYYYMMDD as integer for V1
//!   l_commitdate INT, l_receiptdate INT,
//!   l_shipinstruct CHAR(25), l_shipmode CHAR(10), l_comment CHAR(44)
//! );
//! ```
//!
//! Numeric columns the queries touch are stored as **fixed scale-2
//! integers** internally (e.g. `l_quantity = 23.50` -> raw `2350`,
//! `l_discount = 0.06` -> raw `6`). This lets KesselDB's
//! `FieldKind::Fixed { scale: 2 }` carry the value exactly through
//! `Op::Aggregate` (which returns SUMs as i128). Postgres + SQLite
//! receive the same raw integers in `DOUBLE`/`REAL` columns (cast back
//! to the canonical decimal form via the SQL queries) — the integer
//! representation is the byte-identical wire shape across all three
//! drivers.

use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};

/// One generated lineitem row. Numeric columns the queries filter on are
/// scale-2 fixed integers (see module doc); the integer carries 2 decimal
/// digits so 1234 = 12.34.
#[derive(Clone, Debug)]
pub struct LineItem {
    pub l_orderkey: i64,
    pub l_partkey: i64,
    pub l_suppkey: i64,
    pub l_linenumber: i32,
    /// Scale-2 fixed: 23.50 -> 2350. l_quantity range: 1.00 .. 50.00.
    pub l_quantity_raw: i32,
    /// Scale-2 fixed: 1234.56 -> 123456. l_extendedprice range:
    /// (l_partkey % 200000)/100 * l_quantity in canonical dbgen; here we
    /// generate uniform 90000..=10000000 raw (≈ 900 .. 100,000).
    pub l_extendedprice_raw: i64,
    /// Scale-2 fixed: 0.06 -> 6. l_discount range: 0..=10.
    pub l_discount_raw: i32,
    /// Scale-2 fixed: 0.08 -> 8. l_tax range: 0..=8.
    pub l_tax_raw: i32,
    /// 'N', 'R', or 'A' — the column Q1 groups by.
    pub l_returnflag: u8,
    /// 'O' or 'F' — the column Q1 also groups by.
    pub l_linestatus: u8,
    /// YYYYMMDD as INT (1992-01-01 .. 1998-12-31 range).
    pub l_shipdate: i32,
    pub l_commitdate: i32,
    pub l_receiptdate: i32,
    /// `CHAR(25)` filler.
    pub l_shipinstruct: [u8; 25],
    /// `CHAR(10)` filler.
    pub l_shipmode: [u8; 10],
    /// `CHAR(44)` filler.
    pub l_comment: [u8; 44],
}

/// Generate `n` deterministic `lineitem` rows from `seed`. Same seed →
/// byte-identical rows across all drivers and trial repeats.
pub fn gen_lineitem(n: usize, seed: u64) -> Vec<LineItem> {
    let mut rng = SmallRng::seed_from_u64(seed);
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        // shipdate distribution: uniform across 1992-01-01 .. 1998-12-31.
        // We pick a day-of-year as a uniform offset and reconstruct
        // YYYYMMDD; this matches the spec property that l_shipdate is
        // approximately uniform across 7 years for the published
        // 6M-row scale.
        let day_offset: i32 = rng.gen_range(0..7 * 365);
        let shipdate = yyyymmdd_from_day_offset(day_offset, 1992);
        // commitdate = shipdate +- ~30 days; receiptdate = shipdate +
        // ~10 days. We keep the offsets small + bounded so the date
        // range stays plausibly TPC-H-shaped.
        let commitdate = yyyymmdd_from_day_offset(day_offset + rng.gen_range(-30..30), 1992);
        let receiptdate = yyyymmdd_from_day_offset(day_offset + rng.gen_range(1..40), 1992);

        // Quantity: uniform 1.00 .. 50.00.
        let l_quantity_raw: i32 = rng.gen_range(100..=5000);
        // Discount: uniform 0.00 .. 0.10 (scale-2 = 0..10).
        let l_discount_raw: i32 = rng.gen_range(0..=10);
        // Tax: uniform 0.00 .. 0.08 (scale-2 = 0..8).
        let l_tax_raw: i32 = rng.gen_range(0..=8);
        // Extended price: uniform 90000 .. 10_000_000 raw (~900..100,000).
        let l_extendedprice_raw: i64 = rng.gen_range(90_000..10_000_000);

        // returnflag: 'N', 'R', 'A' with rough 50/25/25 split.
        let rflag_pick: u8 = rng.gen_range(0..4);
        let l_returnflag: u8 = match rflag_pick {
            0 => b'R',
            1 => b'A',
            _ => b'N',
        };
        // linestatus: 'O' for in-flight (shipdate after 1995-06-17),
        // 'F' for finished. We emulate the canonical dbgen rule.
        let l_linestatus: u8 = if shipdate >= 19950617 { b'O' } else { b'F' };

        // Filler char columns: random ASCII printable bytes.
        let mut l_shipinstruct = [b' '; 25];
        rng_fill_ascii(&mut rng, &mut l_shipinstruct);
        let mut l_shipmode = [b' '; 10];
        rng_fill_ascii(&mut rng, &mut l_shipmode);
        let mut l_comment = [b' '; 44];
        rng_fill_ascii(&mut rng, &mut l_comment);

        out.push(LineItem {
            l_orderkey: i as i64 + 1,
            l_partkey: rng.gen_range(1..=2_000_000),
            l_suppkey: rng.gen_range(1..=100_000),
            l_linenumber: ((i % 7) as i32) + 1,
            l_quantity_raw,
            l_extendedprice_raw,
            l_discount_raw,
            l_tax_raw,
            l_returnflag,
            l_linestatus,
            l_shipdate: shipdate,
            l_commitdate: commitdate,
            l_receiptdate: receiptdate,
            l_shipinstruct,
            l_shipmode,
            l_comment,
        });
    }
    out
}

/// YYYYMMDD from a day offset since `base_year`-01-01. We don't need
/// strict Gregorian correctness here — the queries only test "before /
/// after" ordering and the canonical YYYYMMDD compare is monotone with
/// real dates for the ranges we use (1992..1998).
fn yyyymmdd_from_day_offset(day_offset: i32, base_year: i32) -> i32 {
    // Simple 365-day "calendar year" with 12 30/31-day months. Good
    // enough for the queries' inclusive-range filters; canonical TPC-H
    // dbgen uses Julian-day math, which we don't need to reproduce.
    let total_days = day_offset.max(0); // clamp negatives to 1992-01-01
    let year = base_year + (total_days / 365);
    let day_of_year = total_days % 365; // 0..364
    let (month, day) = day_of_year_to_md(day_of_year);
    year * 10000 + (month as i32) * 100 + day as i32
}

fn day_of_year_to_md(doy: i32) -> (u32, u32) {
    // Cumulative day counts per month for a 365-day "non-leap" year.
    const CUM: [i32; 13] = [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 365];
    let mut m = 1u32;
    while m < 12 && CUM[m as usize] <= doy {
        m += 1;
    }
    let d = (doy - CUM[(m - 1) as usize] + 1) as u32;
    (m, d.max(1))
}

fn rng_fill_ascii(rng: &mut SmallRng, buf: &mut [u8]) {
    for b in buf.iter_mut() {
        // Uniform printable ASCII (33..=126). Stay deterministic.
        *b = rng.gen_range(33u8..=126);
    }
}

// ---------------------------------------------------------------------------
// Field IDs — shared by KesselDB driver so the planner+predicate builders
// stay in lock-step with the data generator.
// ---------------------------------------------------------------------------

/// Field IDs for the KesselDB `lineitem` catalog type (parallel to the
/// `Vec<Field>` passed to `encode_type_def`). Used both by the loader and
/// the predicate-program builders so a column rename only touches this
/// list. Field IDs are 1-based — the SM `Op::CreateType` handler
/// renumbers fields to `(position + 1)` at registration time
/// (`kessel-sm/src/lib.rs` SP-Bench-Suite T4 audit).
pub mod field_id {
    pub const L_ORDERKEY: u16 = 1;
    pub const L_PARTKEY: u16 = 2;
    pub const L_SUPPKEY: u16 = 3;
    pub const L_LINENUMBER: u16 = 4;
    pub const L_QUANTITY: u16 = 5;
    pub const L_EXTENDEDPRICE: u16 = 6;
    pub const L_DISCOUNT: u16 = 7;
    pub const L_TAX: u16 = 8;
    pub const L_RETURNFLAG: u16 = 9;
    pub const L_LINESTATUS: u16 = 10;
    pub const L_SHIPDATE: u16 = 11;
    pub const L_COMMITDATE: u16 = 12;
    pub const L_RECEIPTDATE: u16 = 13;
    pub const L_SHIPINSTRUCT: u16 = 14;
    pub const L_SHIPMODE: u16 = 15;
    pub const L_COMMENT: u16 = 16;
    /// Composite grouping key (returnflag<<8|linestatus) — Q1 needs to
    /// group on the *pair* but KesselDB's GROUP BY surface is single-
    /// field. We synthesize a Char(2) key column at load time so the
    /// engine groups on it directly. Field IDs 17+ are derived columns.
    pub const L_GROUPKEY: u16 = 17;
    /// Precomputed `l_extendedprice * l_discount` (i64). KesselDB has no
    /// `SUM(expr)` aggregate primitive; storing the product lets
    /// `Op::Aggregate { kind=SUM, field_id=L_Q6_REVENUE }` answer Q6
    /// directly. Honest gap recorded in driver header.
    pub const L_Q6_REVENUE: u16 = 18;
}
