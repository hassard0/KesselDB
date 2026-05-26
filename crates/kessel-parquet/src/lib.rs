//! Minimal pure-Rust Parquet reader (OBJ-2a): PLAIN encoding,
//! UNCOMPRESSED, flat REQUIRED columns, V1 data pages, multi
//! row-group, recipe-mapped leaf-column subset. Zero external
//! dependencies. Never compiled by the default KesselDB build
//! (only `kessel-fetch`'s `object-store` feature pulls it), so the
//! deterministic kernel + seed-7 corpus are untouched.
#![forbid(unsafe_code)]

mod thrift;
mod footer;
mod meta;
mod plain;
mod rle;
mod dict;
mod snappy;
mod gzip;
mod zstd;
mod lz4;
mod zstd_fse;
mod zstd_literals;
mod zstd_huffman;
mod zstd_huffstream;
mod zstd_sequences;
mod zstd_seqexec;
pub(crate) mod assembly;

/// One decoded Parquet physical value, format-agnostic. The caller
/// (`kessel-fetch`) maps this to its own `Cell` so the existing
/// coerce path is reused unchanged.
#[derive(Clone, Debug, PartialEq)]
pub enum PqValue {
    Null,
    Bool(bool),
    I64(i64),
    F64(f64),
    Bytes(Vec<u8>),
    /// INT96 → nanoseconds since the Unix epoch (Julian day
    /// 2_440_588 == 1970-01-01 UTC). i64 to match the catalog's
    /// `FieldKind::Timestamp` 8-byte storage. Negative for pre-1970
    /// timestamps; the fetch-boundary coerce currently surfaces the
    /// nanos as decimal text (the typed `FieldKind::Timestamp`
    /// mapping is the next SP108 follow-up).
    Timestamp(i64),
    /// DECIMAL → unscaled i128 + scale. Logical value =
    /// `unscaled / 10^scale`. i128 covers Parquet's max precision (38).
    Decimal { unscaled: i128, scale: i32 },
    /// SP143: a LIST<primitive> column's value. Each element is itself
    /// a PqValue (typically Null + scalar primitives in V1; nested
    /// List/struct/Map come in SP144/SP145).
    List(Vec<PqValue>),
    /// SP144: Map<K, V> — key/value pairs preserving Parquet wire order.
    /// Each pair is (key, value). Per Parquet spec, MAP keys are REQUIRED
    /// (never null); values may be Null when the schema marks value
    /// OPTIONAL. SP144 supports primitive K and V only (Map of struct or
    /// Map of List is deferred to SP145).
    Map(Vec<(PqValue, PqValue)>),
    /// SP144: Struct — named fields in schema-declared order. An OPTIONAL
    /// struct group whose def-level says "null" produces PqValue::Null at
    /// the column position, NOT PqValue::Struct with all-Null fields.
    /// SP144 supports struct of primitive fields only (struct of nested
    /// LIST/MAP/struct deferred to SP145).
    Struct(Vec<(String, PqValue)>),
}

/// SP143 T2: minimal JSON serialization of a `PqValue::List`'s element
/// vector — sufficient for round-trip display at the fetch boundary.
/// Format: `[item1,item2,...]` with each element rendered per its type
/// (null/true/false, decimal integers/floats, JSON-escaped strings,
/// nested `{"unscaled":"...","scale":N}` for DECIMAL, and recursive
/// `[...]` for nested lists). Non-printable bytes inside `Bytes(_)`
/// are hex-escaped as `\uXXXX` for safe ASCII output. Used by
/// `kessel-fetch`'s `pq_to_cell` to surface List values as
/// `Cell::Blob(json)` without adding a new `Cell` variant (keeps the
/// binary protocol UNCHANGED in this slice — a typed `Cell::List` is
/// the SP144 follow-up).
pub fn pqvalue_list_to_json(items: &[PqValue]) -> Vec<u8> {
    let mut s = String::from("[");
    for (i, v) in items.iter().enumerate() {
        if i > 0 {
            s.push(',');
        }
        s.push_str(&pqvalue_to_json(v));
    }
    s.push(']');
    s.into_bytes()
}

/// SP144 T1: centralized JSON serialization for any `PqValue` (scalar
/// or nested). All three nesting variants (`List`, `Map`, `Struct`)
/// share this single implementation so the wire shape stays consistent
/// regardless of where in a value tree the variant appears. Returns a
/// UTF-8 `String` (ASCII-safe — non-printable bytes inside `Bytes(_)`
/// are hex-escaped as `\uXXXX`).
///
/// Output shapes:
/// - `Map(pairs)` → `[[k,v],[k,v],...]` (array of pair-arrays so any
///   non-string key type round-trips — the alternative of a JSON object
///   would require coercing the key to a string at serialize time).
/// - `Struct(fields)` → `{"name":value,...}` with field names
///   JSON-escaped conservatively.
/// - All scalars + `List` render exactly as the SP143 helper did.
pub fn pqvalue_to_json(v: &PqValue) -> String {
    match v {
        PqValue::Null => "null".to_string(),
        PqValue::Bool(b) => if *b { "true" } else { "false" }.to_string(),
        PqValue::I64(n) => n.to_string(),
        PqValue::F64(x) => x.to_string(),
        PqValue::Bytes(b) => {
            let mut s = String::from("\"");
            for &byte in b {
                if byte == b'"' {
                    s.push_str("\\\"");
                } else if byte == b'\\' {
                    s.push_str("\\\\");
                } else if (0x20..=0x7e).contains(&byte) {
                    s.push(byte as char);
                } else {
                    s.push_str(&format!("\\u{:04x}", byte));
                }
            }
            s.push('"');
            s
        }
        PqValue::Timestamp(n) => n.to_string(),
        PqValue::Decimal { unscaled, scale } => {
            format!(r#"{{"unscaled":"{unscaled}","scale":{scale}}}"#)
        }
        PqValue::List(inner) => {
            // Nested list — recurse via the list helper, which itself
            // recurses through this function for each element.
            String::from_utf8(pqvalue_list_to_json(inner))
                .unwrap_or_else(|_| "\"<binary>\"".to_string())
        }
        PqValue::Map(pairs) => {
            // SP144: emit as array-of-pair-arrays so non-string keys
            // (Bytes, I64, etc.) round-trip without lossy coercion.
            let mut s = String::from("[");
            for (i, (k, val)) in pairs.iter().enumerate() {
                if i > 0 {
                    s.push(',');
                }
                s.push('[');
                s.push_str(&pqvalue_to_json(k));
                s.push(',');
                s.push_str(&pqvalue_to_json(val));
                s.push(']');
            }
            s.push(']');
            s
        }
        PqValue::Struct(fields) => {
            // SP144: emit as a JSON object with string-quoted field
            // names. Field names come from the Parquet schema (UTF-8);
            // escape conservatively for JSON safety.
            let mut s = String::from("{");
            for (i, (name, val)) in fields.iter().enumerate() {
                if i > 0 {
                    s.push(',');
                }
                s.push('"');
                for c in name.chars() {
                    match c {
                        '"' => s.push_str("\\\""),
                        '\\' => s.push_str("\\\\"),
                        c if (c as u32) < 0x20 => {
                            s.push_str(&format!("\\u{:04x}", c as u32))
                        }
                        c => s.push(c),
                    }
                }
                s.push('"');
                s.push(':');
                s.push_str(&pqvalue_to_json(val));
            }
            s.push('}');
            s
        }
    }
}

#[cfg(test)]
mod pqvalue_list_tests {
    use super::*;

    #[test]
    fn list_variant_constructs_and_compares() {
        let v = PqValue::List(vec![
            PqValue::I64(1),
            PqValue::I64(2),
            PqValue::Null,
        ]);
        let v2 = v.clone();
        assert_eq!(v, v2);
    }

    #[test]
    fn list_variant_nested_clone() {
        let v = PqValue::List(vec![
            PqValue::List(vec![PqValue::I64(1)]),
            PqValue::List(vec![PqValue::Null]),
        ]);
        assert_eq!(v.clone(), v);
    }

    #[test]
    fn list_variant_empty() {
        let v = PqValue::List(Vec::new());
        match &v {
            PqValue::List(items) => assert!(items.is_empty()),
            _ => panic!("expected List"),
        }
    }

    #[test]
    fn list_to_json_primitives() {
        // Mixed primitive scalars + Null render as a JSON-shaped array.
        let v = vec![
            PqValue::Null,
            PqValue::Bool(true),
            PqValue::Bool(false),
            PqValue::I64(-7),
            PqValue::I64(0),
            PqValue::I64(42),
        ];
        assert_eq!(
            pqvalue_list_to_json(&v),
            br#"[null,true,false,-7,0,42]"#.to_vec()
        );
    }

    #[test]
    fn list_to_json_bytes_escaping() {
        // ASCII printable passes through; quote/backslash escaped;
        // non-printables hex-escaped as \uXXXX (4-hex JSON escape).
        let v = vec![PqValue::Bytes(vec![b'a', b'"', b'b', b'\\', b'c', 0x01, 0xff])];
        let got = pqvalue_list_to_json(&v);
        let s = std::str::from_utf8(&got).expect("ASCII-only output");
        assert_eq!(s, "[\"a\\\"b\\\\c\\u0001\\u00ff\"]");
    }

    #[test]
    fn list_to_json_decimal_and_timestamp() {
        let v = vec![
            PqValue::Timestamp(1_500_000_000),
            PqValue::Decimal { unscaled: -12345, scale: 2 },
        ];
        assert_eq!(
            pqvalue_list_to_json(&v),
            br#"[1500000000,{"unscaled":"-12345","scale":2}]"#.to_vec()
        );
    }

    #[test]
    fn list_to_json_nested() {
        let v = vec![
            PqValue::List(vec![PqValue::I64(1), PqValue::I64(2)]),
            PqValue::List(vec![]),
            PqValue::List(vec![PqValue::Null]),
        ];
        assert_eq!(
            pqvalue_list_to_json(&v),
            br#"[[1,2],[],[null]]"#.to_vec()
        );
    }

    #[test]
    fn list_to_json_empty() {
        let v: Vec<PqValue> = Vec::new();
        assert_eq!(pqvalue_list_to_json(&v), b"[]".to_vec());
    }
}

#[cfg(test)]
mod sp144_pqvalue_tests {
    use super::*;

    #[test]
    fn map_variant_constructs_and_compares() {
        let v = PqValue::Map(vec![
            (PqValue::Bytes(b"a".to_vec()), PqValue::I64(1)),
            (PqValue::Bytes(b"b".to_vec()), PqValue::I64(2)),
        ]);
        assert_eq!(v.clone(), v);
    }

    #[test]
    fn struct_variant_constructs_and_compares() {
        let v = PqValue::Struct(vec![
            ("id".into(), PqValue::I64(42)),
            ("name".into(), PqValue::Bytes(b"alice".to_vec())),
        ]);
        assert_eq!(v.clone(), v);
    }

    #[test]
    fn map_json_serialization() {
        let v = PqValue::Map(vec![
            (PqValue::Bytes(b"x".to_vec()), PqValue::I64(10)),
            (PqValue::Bytes(b"y".to_vec()), PqValue::Null),
        ]);
        let s = pqvalue_to_json(&v);
        assert_eq!(s, r#"[["x",10],["y",null]]"#);
    }

    #[test]
    fn struct_json_serialization() {
        let v = PqValue::Struct(vec![
            ("a".into(), PqValue::I64(1)),
            ("b".into(), PqValue::Bool(true)),
            ("c".into(), PqValue::Null),
        ]);
        let s = pqvalue_to_json(&v);
        assert_eq!(s, r#"{"a":1,"b":true,"c":null}"#);
    }

    #[test]
    fn nested_list_in_struct_json() {
        let v = PqValue::Struct(vec![
            ("items".into(), PqValue::List(vec![
                PqValue::I64(1), PqValue::I64(2),
            ])),
        ]);
        let s = pqvalue_to_json(&v);
        assert_eq!(s, r#"{"items":[1,2]}"#);
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PqError {
    /// Malformed / truncated / out-of-bounds Parquet bytes.
    Bad(String),
    /// Well-formed but uses a feature outside OBJ-2a (names the
    /// OBJ-2b/2c follow-on).
    Unsupported(String),
}

/// SP151 (OBJ-2c-4 follow-up): default per-page (compressed OR
/// uncompressed) size cap enforced by `extract`. 256 MiB — 4× the
/// historical 64 MiB cap that rejected pyarrow files with
/// high-cardinality dictionary pages or large value pages on
/// many-row row groups. Users who need a different cap (tighter for
/// memory-constrained ingest, looser for known-trusted producers) use
/// `extract_with_cap` directly.
///
/// Per-codec module ceilings (`snappy::SNAPPY_MAX_DECOMP`,
/// `gzip::GZIP_MAX_DECOMP`, `zstd::ZSTD_MAX_DECOMP`) match this
/// value and act as defense-in-depth: even if a caller passes
/// `usize::MAX` to `extract_with_cap`, the per-codec const blocks
/// the allocation before any OOM risk.
pub const DEFAULT_MAX_PAGE_SIZE: usize = 256 * 1024 * 1024;

// Thread-local: per-call cap honored by every page-size check inside
// the extract path. Set on entry to `extract_with_cap`, restored on
// return. Defaults to `DEFAULT_MAX_PAGE_SIZE` outside an extract.
//
// V1 plumbing rationale: the cap needs to reach >10 internal
// helpers (`page_payload`, the V2 flat/nested decompression sites,
// every `read_chunk_*` page-loop) without adding a `max_page_size`
// param to each. A thread-local is the smallest-blast-radius idiom
// — one set+restore at the public boundary, one read at each
// allocation site. Not Send across threads (each `extract*` call
// initialises its own thread's local) and never observed outside
// an extract (the default is the same 256 MiB ceiling).
thread_local! {
    static MAX_PAGE_SIZE: std::cell::Cell<usize> =
        const { std::cell::Cell::new(DEFAULT_MAX_PAGE_SIZE) };
}

/// Read the current per-call page-size cap (thread-local set by
/// `extract_with_cap`). Used at every site that decodes a
/// `uncompressed_page_size` / `compressed_page_size` from a page
/// header BEFORE allocating a buffer of that size.
#[inline]
pub(crate) fn current_max_page_size() -> usize {
    MAX_PAGE_SIZE.with(|c| c.get())
}

/// RAII guard: installs `max_page_size` as the thread-local cap on
/// construction; restores the previous value on drop. Drop runs on
/// any return path (Ok, Err, panic-unwind) so the cap state is
/// always restored.
struct MaxPageSizeGuard(usize);

impl MaxPageSizeGuard {
    fn new(new_cap: usize) -> Self {
        let prev = MAX_PAGE_SIZE.with(|c| {
            let p = c.get();
            c.set(new_cap);
            p
        });
        MaxPageSizeGuard(prev)
    }
}

impl Drop for MaxPageSizeGuard {
    fn drop(&mut self) {
        let prev = self.0;
        MAX_PAGE_SIZE.with(|c| c.set(prev));
    }
}

/// SP151 (OBJ-2c-4 follow-up): assert a page-header-derived size is
/// within the per-call cap BEFORE any allocation of that size.
/// Returns typed `Unsupported` naming the cap and the operator knob
/// (`extract_with_cap`) when the page exceeds it; otherwise Ok.
///
/// `what` is the per-site label (e.g. `"v1 page"`, `"dict page"`,
/// `"v2 values"`) so an operator hitting the cap sees which page
/// kind tripped it.
#[inline]
pub(crate) fn check_page_size(what: &str, size: usize) -> Result<(), PqError> {
    let cap = current_max_page_size();
    if size > cap {
        return Err(PqError::Unsupported(format!(
            "{what} size {size} exceeds max_page_size cap {cap}: SP151 \
             (raise via kessel_parquet::extract_with_cap)"
        )));
    }
    Ok(())
}

impl std::fmt::Display for PqError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PqError::Bad(s) => write!(f, "parquet: {s}"),
            PqError::Unsupported(s) => write!(f, "parquet unsupported: {s}"),
        }
    }
}

/// The on-disk page payload, decompressed if needed. Slices the
/// `comp`-byte on-disk region at `dstart`; Uncompressed → borrowed
/// (zero-copy), Snappy → owned decompressed (length `uncomp`).
fn page_payload<'a>(
    file: &'a [u8],
    dstart: usize,
    comp: usize,
    uncomp: usize,
    codec: meta::Codec,
) -> Result<std::borrow::Cow<'a, [u8]>, PqError> {
    let end = dstart
        .checked_add(comp)
        .ok_or_else(|| PqError::Bad("page region ovf".into()))?;
    let on_disk = file
        .get(dstart..end)
        .ok_or_else(|| PqError::Bad("page data truncated".into()))?;
    match codec {
        meta::Codec::Uncompressed => Ok(std::borrow::Cow::Borrowed(on_disk)),
        meta::Codec::Snappy => {
            Ok(std::borrow::Cow::Owned(snappy::decompress(on_disk, uncomp)?))
        }
        meta::Codec::Gzip => Ok(std::borrow::Cow::Owned(
            gzip::decompress(on_disk, uncomp)?
        )),
        meta::Codec::Zstd => {
            let decoded = zstd::decompress(on_disk)
                .map_err(|e| PqError::Bad(format!("zstd decode: {e:?}")))?;
            if decoded.len() != uncomp {
                return Err(PqError::Bad(format!(
                    "zstd page decompressed size {} != declared {}",
                    decoded.len(),
                    uncomp
                )));
            }
            Ok(std::borrow::Cow::Owned(decoded))
        }
        meta::Codec::Lz4Raw => Ok(std::borrow::Cow::Owned(
            lz4::decompress(on_disk, uncomp)?
        )),
        // SP150: Brotli (codec id 4) recognized at meta-decode time but
        // decompression is deferred to a dedicated SP-arc (zero-dep RFC
        // 7932 decoder is multi-week scope, ~10-15 tasks like SP125-SP140
        // zstd). Named error directs users at the shipped workarounds.
        meta::Codec::Brotli => Err(PqError::Unsupported(
            "Brotli decode: zero-dep decoder is a dedicated multi-week SP-arc \
             (~10-15 tasks like SP125-SP140 zstd); workaround — ask the writer to use \
             compression='zstd' or compression='lz4' instead".into(),
        )),
        // Codec id 5 = legacy LZ4 (deprecated Hadoop framing). Pyarrow
        // stopped writing this in v8; we don't support it in V1 — named
        // separately so a user file that needs it gets a clear pointer
        // at the SP149 follow-up rather than the generic OBJ-2c message.
        meta::Codec::Other(5) => Err(PqError::Unsupported(
            "LZ4 (deprecated Hadoop framing) — use LZ4_RAW; SP149 follow-up if needed".into(),
        )),
        meta::Codec::Other(_) => Err(PqError::Unsupported(
            "compression codec: OBJ-2c".into(),
        )),
    }
}

/// Scatter `vals` (the decoded *present* values) into `n` output slots
/// per the def-level vector `defs` (`d==1` → next value, `d==0` → Null).
/// This is the EXACT OPTIONAL null-scatter logic relocated verbatim out
/// of `decode_page`'s `max_def_level == 1` arm — shared with the V2 path.
/// Same count-mismatch `Bad`, same `PqValue::Null` placement.
fn scatter_nulls(
    defs: &[u64],
    vals: Vec<PqValue>,
    n: usize,
) -> Result<Vec<PqValue>, PqError> {
    let mut out = Vec::with_capacity(n);
    let mut it = vals.into_iter();
    for &d in defs {
        if d == 1 {
            out.push(it.next().ok_or_else(|| {
                PqError::Bad("value/def-level count mismatch".into())
            })?);
        } else {
            out.push(PqValue::Null);
        }
    }
    Ok(out)
}

/// Decode one V1 data-page payload, returning exactly `n` `PqValue`s.
///
/// `max_def_level == 0` (REQUIRED): no level bytes — directly decode `n`
/// values from `payload` by `dp_encoding`. This arm is byte-identical to
/// the prior per-page inline `match ph.dp_encoding { ... }`.
///
/// `max_def_level == 1` (flat OPTIONAL): payload starts with a
/// 4-byte-u32-LE-length-prefixed RLE/bit-packing-hybrid def-level stream
/// (bit_width=1), followed by exactly the present-count values.
fn decode_page(
    payload: &[u8],
    dp_encoding: i32,
    spec: plain::PlainSpec,
    n: usize,
    max_def_level: u32,
    dict: &[PqValue],
) -> Result<Vec<PqValue>, PqError> {
    if max_def_level == 0 {
        return match dp_encoding {
            0 => plain::decode_plain(payload, spec, n),
            2 | 8 => dict::resolve_dict_indices(payload, dict, n),
            _ => Err(PqError::Unsupported(
                "data page encoding (DELTA/BYTE_STREAM_SPLIT): OBJ-2c".into(),
            )),
        };
    }
    // max_def_level == 1: flat OPTIONAL — def-level prefix + present values.
    let (defs, consumed) = rle::decode_level_v1(payload, 1, n)?;
    if defs.len() != n {
        return Err(PqError::Bad("def-level count != num_values".into()));
    }
    // bit_width=1 does not structurally prevent an RLE run whose
    // repeated-value byte has high bits set; a value > max_def_level (1)
    // is malformed input — reject as Bad, never silently treat as present.
    for &d in &defs {
        if d > 1 {
            return Err(PqError::Bad("definition level exceeds max".into()));
        }
    }
    let present = defs.iter().filter(|&&d| d == 1).count();
    let body = payload
        .get(consumed..)
        .ok_or_else(|| PqError::Bad("def-level consumed past payload".into()))?;
    let vals = match dp_encoding {
        0 => plain::decode_plain(body, spec, present)?,
        2 | 8 => dict::resolve_dict_indices(body, dict, present)?,
        _ => {
            return Err(PqError::Unsupported(
                "data page encoding (DELTA/BYTE_STREAM_SPLIT): OBJ-2c".into(),
            ))
        }
    };
    if vals.len() != present {
        return Err(PqError::Bad("value/def-level count mismatch".into()));
    }
    scatter_nulls(&defs, vals, n)
}

/// Decode one DATA_PAGE_V2 page (`page_type == 3`).
///
/// `region` is the raw on-disk page body `file[dstart..dstart+compressed]`.
/// V2 layout: `[rep levels (rep_len)][def levels (def_len)][values]`.
/// Repetition levels (nested/REPEATED) are out of scope ⇒ Unsupported.
/// Flat OPTIONAL def levels are an RLE/bit-packing hybrid (bit_width=1)
/// of exactly `def_len` bytes — NOT 4-byte length-prefixed (that's V1).
/// The values section is independently (de)compressed per `is_compressed`.
fn decode_data_page_v2(
    region: &[u8],
    ph: &meta::PageHeader,
    codec: meta::Codec,
    spec: plain::PlainSpec,
    max_def_level: u32,
    dict: &[PqValue],
) -> Result<Vec<PqValue>, PqError> {
    let rep_len = usize::try_from(ph.v2_rep_len)
        .map_err(|_| PqError::Bad("v2 rep_len range".into()))?;
    if rep_len > 0 {
        return Err(PqError::Unsupported(
            "REPEATED/nested V2 (repetition levels): OBJ-2c-5".into(),
        ));
    }
    let def_len = usize::try_from(ph.v2_def_len)
        .map_err(|_| PqError::Bad("v2 def_len range".into()))?;
    let n = usize::try_from(ph.v2_num_values)
        .map_err(|_| PqError::Bad("v2 num_values range".into()))?;
    let lvl_end = rep_len
        .checked_add(def_len)
        .ok_or_else(|| PqError::Bad("v2 level len ovf".into()))?;
    if lvl_end > region.len() {
        return Err(PqError::Bad("v2 levels exceed page".into()));
    }
    let def_bytes = region
        .get(rep_len..lvl_end)
        .ok_or_else(|| PqError::Bad("v2 def slice".into()))?;
    let values_section = region
        .get(lvl_end..)
        .ok_or_else(|| PqError::Bad("v2 values slice".into()))?;
    // def-levels
    let (defs, present): (Option<Vec<u64>>, usize) = if max_def_level == 1 {
        let d = rle::decode_hybrid(def_bytes, 1, n)?;
        if d.len() != n {
            return Err(PqError::Bad("v2 def-level count != num_values".into()));
        }
        if d.iter().any(|&x| x > 1) {
            return Err(PqError::Bad("v2 def-level exceeds max".into()));
        }
        let p = d.iter().filter(|&&x| x == 1).count();
        // defense-in-depth: cross-check vs declared num_nulls
        let nn = usize::try_from(ph.v2_num_nulls)
            .map_err(|_| PqError::Bad("v2 num_nulls range".into()))?;
        if n.checked_sub(nn) != Some(p) {
            return Err(PqError::Bad(
                "v2 num_nulls vs def-levels mismatch".into(),
            ));
        }
        (Some(d), p)
    } else {
        if def_len != 0 {
            return Err(PqError::Bad(
                "v2 def_len non-zero for REQUIRED".into(),
            ));
        }
        (None, n)
    };
    // values: target uncompressed length
    let uncomp = usize::try_from(ph.uncompressed_size)
        .map_err(|_| PqError::Bad("v2 uncompressed size range".into()))?;
    // SP151: cap-check BEFORE the snappy/gzip/zstd/lz4 decompression
    // path allocates a vt-byte buffer.
    check_page_size("v2 page uncompressed", uncomp)?;
    let vt = uncomp
        .checked_sub(lvl_end)
        .ok_or_else(|| PqError::Bad("v2 values target underflow".into()))?;
    let values_raw: std::borrow::Cow<[u8]> = match codec {
        meta::Codec::Uncompressed => std::borrow::Cow::Borrowed(values_section),
        // Per-page is_compressed=false overrides the column codec:
        // values are raw even when the codec is not Uncompressed.
        // This arm MUST stay above the concrete codec arms (Snappy/
        // Gzip/…future zstd) so they only fire when is_compressed.
        _ if !ph.v2_is_compressed => std::borrow::Cow::Borrowed(values_section),
        meta::Codec::Snappy => {
            std::borrow::Cow::Owned(snappy::decompress(values_section, vt)?)
        }
        meta::Codec::Gzip => {
            std::borrow::Cow::Owned(gzip::decompress(values_section, vt)?)
        }
        meta::Codec::Zstd => {
            let decoded = zstd::decompress(values_section)
                .map_err(|e| PqError::Bad(format!("v2 zstd values decode: {e:?}")))?;
            if decoded.len() != vt {
                return Err(PqError::Bad(format!(
                    "v2 zstd values decompressed size {} != target {}",
                    decoded.len(),
                    vt
                )));
            }
            std::borrow::Cow::Owned(decoded)
        }
        meta::Codec::Lz4Raw => {
            std::borrow::Cow::Owned(lz4::decompress(values_section, vt)?)
        }
        meta::Codec::Brotli => {
            return Err(PqError::Unsupported(
                "Brotli decode: zero-dep decoder is a dedicated multi-week SP-arc \
                 (~10-15 tasks like SP125-SP140 zstd); workaround — ask the writer to use \
                 compression='zstd' or compression='lz4' instead".into(),
            ))
        }
        meta::Codec::Other(5) => {
            return Err(PqError::Unsupported(
                "LZ4 (deprecated Hadoop framing) — use LZ4_RAW; SP149 follow-up if needed".into(),
            ))
        }
        meta::Codec::Other(_) => {
            return Err(PqError::Unsupported(
                "compression codec: OBJ-2c".into(),
            ))
        }
    };
    // values section was NOT decompressed (codec is Uncompressed,
    // or this page's is_compressed override is false) — the on-disk
    // bytes must therefore equal the target uncompressed length vt.
    if (matches!(codec, meta::Codec::Uncompressed) || !ph.v2_is_compressed)
        && values_section.len() != vt
    {
        return Err(PqError::Bad("v2 raw values length mismatch".into()));
    }
    let vals = match ph.v2_encoding {
        0 => plain::decode_plain(&values_raw, spec, present)?,
        2 | 8 => dict::resolve_dict_indices(&values_raw, dict, present)?,
        _ => {
            return Err(PqError::Unsupported(
                "data page encoding (DELTA/BYTE_STREAM_SPLIT): OBJ-2c".into(),
            ))
        }
    };
    match defs {
        Some(d) => scatter_nulls(&d, vals, n),
        None => {
            if vals.len() != n {
                return Err(PqError::Bad("v2 value count".into()));
            }
            Ok(vals)
        }
    }
}

/// SP143 T4: minimum bits needed to represent `max_level` values in
/// `0..=max_level`. `max_level == 0` → 0 (level stream is absent /
/// degenerate). Matches the Parquet/Dremel definition: `bit_width =
/// ceil(log2(max_level + 1))`, computed as `32 - leading_zeros`.
fn bit_width_for_max(max_level: u32) -> u32 {
    if max_level == 0 {
        0
    } else {
        32 - max_level.leading_zeros()
    }
}

/// SP143 T4: dispatch the values-section bytes of a NESTED page to the
/// right encoding. Mirrors the inline `match dp_encoding { 0|2|8|_ }`
/// of the flat `decode_page`/`decode_data_page_v2`, but takes the
/// `present`-count rather than `n`.  PLAIN takes a raw value payload;
/// `2|8` (PLAIN_DICTIONARY / RLE_DICTIONARY) take the standard
/// `<1-byte bit_width><hybrid-stream>` layout — i.e. the WHOLE
/// dictionary-data-page body. The `Unsupported(...)` message uses the
/// same "data page encoding (...): OBJ-2c" phrasing as the flat path so
/// the dispatch is uniform.
fn decode_values_by_encoding(
    payload: &[u8],
    encoding: i32,
    spec: plain::PlainSpec,
    n: usize,
    dict: &[PqValue],
) -> Result<Vec<PqValue>, PqError> {
    match encoding {
        0 => plain::decode_plain(payload, spec, n),
        2 | 8 => dict::resolve_dict_indices(payload, dict, n),
        _ => Err(PqError::Unsupported(
            "data page encoding (DELTA/BYTE_STREAM_SPLIT): OBJ-2c".into(),
        )),
    }
}

/// SP143 T4: V1 page decode for NESTED columns
/// (`max_rep_level > 0` OR `max_def_level > 1`). Returns the parallel
/// `(rep_levels, def_levels, values)` triple the upcoming Dremel
/// assembler (T5/T6) will fold into `PqValue::List(...)`.
///
/// V1 page layout for a NESTED column:
/// ```text
///   [4-byte LE u32 rep_len][rep_data: hybrid, bit_width=ceil(log2(max_rep+1))]
///   [4-byte LE u32 def_len][def_data: hybrid, bit_width=ceil(log2(max_def+1))]
///   [value section: dp_encoding bytes, COUNT = number of def==max_def slots]
/// ```
/// `max_rep_level == 0` SKIPS the rep section entirely (no length prefix
/// either — same convention as `decode_page`'s `max_def_level == 0`
/// arm for the def section). Rep/def levels are range-validated against
/// their max (Bad on overrun) to defeat malformed hybrid headers whose
/// repeated-value byte has high bits set (same defense the flat path
/// applies for `max_def_level==1`, generalized here).
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn decode_page_v1_nested(
    payload: &[u8],
    dp_encoding: i32,
    spec: plain::PlainSpec,
    n: usize,
    max_rep_level: u32,
    max_def_level: u32,
    dict: &[PqValue],
) -> Result<(Vec<u32>, Vec<u32>, Vec<PqValue>), PqError> {
    let mut cursor: &[u8] = payload;
    // Rep section (V1: 4-byte LE length prefix + hybrid).
    let rep_levels: Vec<u32> = if max_rep_level == 0 {
        vec![0u32; n]
    } else {
        let bw = bit_width_for_max(max_rep_level);
        let (levels_u64, consumed) = rle::decode_level_v1(cursor, bw, n)?;
        if levels_u64.len() != n {
            return Err(PqError::Bad("nested v1 rep-level count != num_values".into()));
        }
        cursor = cursor
            .get(consumed..)
            .ok_or_else(|| PqError::Bad("nested v1 rep section consumed past payload".into()))?;
        let mut out = Vec::with_capacity(levels_u64.len());
        for l in levels_u64 {
            if l > max_rep_level as u64 {
                return Err(PqError::Bad(format!(
                    "nested v1 rep level {l} > max {max_rep_level}"
                )));
            }
            out.push(l as u32);
        }
        out
    };
    // Def section (V1: 4-byte LE length prefix + hybrid).
    let def_levels: Vec<u32> = if max_def_level == 0 {
        vec![0u32; n]
    } else {
        let bw = bit_width_for_max(max_def_level);
        let (levels_u64, consumed) = rle::decode_level_v1(cursor, bw, n)?;
        if levels_u64.len() != n {
            return Err(PqError::Bad("nested v1 def-level count != num_values".into()));
        }
        cursor = cursor
            .get(consumed..)
            .ok_or_else(|| PqError::Bad("nested v1 def section consumed past payload".into()))?;
        let mut out = Vec::with_capacity(levels_u64.len());
        for l in levels_u64 {
            if l > max_def_level as u64 {
                return Err(PqError::Bad(format!(
                    "nested v1 def level {l} > max {max_def_level}"
                )));
            }
            out.push(l as u32);
        }
        out
    };
    let present_count = def_levels.iter().filter(|&&d| d == max_def_level).count();
    let values = decode_values_by_encoding(cursor, dp_encoding, spec, present_count, dict)?;
    if values.len() != present_count {
        return Err(PqError::Bad(
            "nested v1 value count != present (def==max_def) count".into(),
        ));
    }
    Ok((rep_levels, def_levels, values))
}

/// SP143 T4: V2 page decode for NESTED columns.
///
/// V2 page layout (NO 4-byte length prefix on the level sections — the
/// `rep_levels_byte_length`/`def_levels_byte_length` page-header fields
/// give the byte lengths directly):
/// ```text
///   [rep_data: rep_len bytes RAW hybrid]
///   [def_data: def_len bytes RAW hybrid]
///   [values: (uncompressed_page_size - rep_len - def_len) bytes,
///    independently compressed iff ph.v2_is_compressed (per the V2
///    flat-path convention in decode_data_page_v2)]
/// ```
/// `max_rep_level == 0` REQUIRES `rep_len == 0` (Bad otherwise); same
/// for def. Rep/def levels are range-validated. The values section is
/// decompressed the same way as the flat V2 path (Snappy/Gzip/Zstd/
/// Uncompressed) — `is_compressed=false` overrides the column codec to
/// raw (matches `decode_data_page_v2`'s `_ if !ph.v2_is_compressed`
/// override).
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn decode_data_page_v2_nested(
    payload: &[u8],
    dp_encoding: i32,
    spec: plain::PlainSpec,
    n: usize,
    max_rep_level: u32,
    max_def_level: u32,
    rep_levels_byte_length: u32,
    def_levels_byte_length: u32,
    is_compressed: bool,
    codec: meta::Codec,
    uncompressed_page_size: u32,
    dict: &[PqValue],
) -> Result<(Vec<u32>, Vec<u32>, Vec<PqValue>), PqError> {
    let rep_len = rep_levels_byte_length as usize;
    let def_len = def_levels_byte_length as usize;
    let lvl_end = rep_len
        .checked_add(def_len)
        .ok_or_else(|| PqError::Bad("nested v2 level len ovf".into()))?;
    if lvl_end > payload.len() {
        return Err(PqError::Bad("nested v2 level sections exceed page".into()));
    }
    // Rep section: raw hybrid, NO length prefix.
    let rep_bytes = payload
        .get(..rep_len)
        .ok_or_else(|| PqError::Bad("nested v2 rep slice".into()))?;
    let rep_levels: Vec<u32> = if max_rep_level == 0 {
        if rep_len != 0 {
            return Err(PqError::Bad("nested v2 rep_len non-zero for max_rep_level=0".into()));
        }
        vec![0u32; n]
    } else {
        let bw = bit_width_for_max(max_rep_level);
        let levels_u64 = rle::decode_hybrid(rep_bytes, bw, n)?;
        if levels_u64.len() != n {
            return Err(PqError::Bad("nested v2 rep-level count != num_values".into()));
        }
        let mut out = Vec::with_capacity(levels_u64.len());
        for l in levels_u64 {
            if l > max_rep_level as u64 {
                return Err(PqError::Bad(format!(
                    "nested v2 rep level {l} > max {max_rep_level}"
                )));
            }
            out.push(l as u32);
        }
        out
    };
    // Def section: raw hybrid, NO length prefix.
    let def_bytes = payload
        .get(rep_len..lvl_end)
        .ok_or_else(|| PqError::Bad("nested v2 def slice".into()))?;
    let def_levels: Vec<u32> = if max_def_level == 0 {
        if def_len != 0 {
            return Err(PqError::Bad("nested v2 def_len non-zero for max_def_level=0".into()));
        }
        vec![0u32; n]
    } else {
        let bw = bit_width_for_max(max_def_level);
        let levels_u64 = rle::decode_hybrid(def_bytes, bw, n)?;
        if levels_u64.len() != n {
            return Err(PqError::Bad("nested v2 def-level count != num_values".into()));
        }
        let mut out = Vec::with_capacity(levels_u64.len());
        for l in levels_u64 {
            if l > max_def_level as u64 {
                return Err(PqError::Bad(format!(
                    "nested v2 def level {l} > max {max_def_level}"
                )));
            }
            out.push(l as u32);
        }
        out
    };
    // Values section: decompressed per V2 convention (mirrors
    // decode_data_page_v2's codec arm, including the per-page
    // is_compressed=false override).
    let values_section = payload
        .get(lvl_end..)
        .ok_or_else(|| PqError::Bad("nested v2 values slice".into()))?;
    let uncomp = uncompressed_page_size as usize;
    let vt = uncomp
        .checked_sub(lvl_end)
        .ok_or_else(|| PqError::Bad("nested v2 values target underflow".into()))?;
    let values_raw: std::borrow::Cow<[u8]> = match codec {
        meta::Codec::Uncompressed => std::borrow::Cow::Borrowed(values_section),
        _ if !is_compressed => std::borrow::Cow::Borrowed(values_section),
        meta::Codec::Snappy => std::borrow::Cow::Owned(snappy::decompress(values_section, vt)?),
        meta::Codec::Gzip => std::borrow::Cow::Owned(gzip::decompress(values_section, vt)?),
        meta::Codec::Zstd => {
            let decoded = zstd::decompress(values_section)
                .map_err(|e| PqError::Bad(format!("nested v2 zstd values decode: {e:?}")))?;
            if decoded.len() != vt {
                return Err(PqError::Bad(format!(
                    "nested v2 zstd values decompressed size {} != target {}",
                    decoded.len(),
                    vt
                )));
            }
            std::borrow::Cow::Owned(decoded)
        }
        meta::Codec::Lz4Raw => {
            std::borrow::Cow::Owned(lz4::decompress(values_section, vt)?)
        }
        meta::Codec::Brotli => {
            return Err(PqError::Unsupported(
                "Brotli decode: zero-dep decoder is a dedicated multi-week SP-arc \
                 (~10-15 tasks like SP125-SP140 zstd); workaround — ask the writer to use \
                 compression='zstd' or compression='lz4' instead".into(),
            ))
        }
        meta::Codec::Other(5) => {
            return Err(PqError::Unsupported(
                "LZ4 (deprecated Hadoop framing) — use LZ4_RAW; SP149 follow-up if needed".into(),
            ))
        }
        meta::Codec::Other(_) => {
            return Err(PqError::Unsupported(
                "compression codec: OBJ-2c".into(),
            ))
        }
    };
    if (matches!(codec, meta::Codec::Uncompressed) || !is_compressed)
        && values_section.len() != vt
    {
        return Err(PqError::Bad("nested v2 raw values length mismatch".into()));
    }
    let present_count = def_levels.iter().filter(|&&d| d == max_def_level).count();
    let values = decode_values_by_encoding(&values_raw, dp_encoding, spec, present_count, dict)?;
    if values.len() != present_count {
        return Err(PqError::Bad(
            "nested v2 value count != present (def==max_def) count".into(),
        ));
    }
    Ok((rep_levels, def_levels, values))
}

/// Read one column chunk's values across all its pages.
/// Flat REQUIRED or OPTIONAL, UNCOMPRESSED or SNAPPY, V1. Supports: an
/// optional leading DICTIONARY_PAGE then zero-or-more DATA_PAGEs; each data
/// page is PLAIN (dictionary-fallback) or PLAIN_DICTIONARY/RLE_DICTIONARY.
fn read_chunk_values(
    file: &[u8],
    cc: &meta::ColumnChunk,
    spec: plain::PlainSpec,
    max_def_level: u32,
) -> Result<Vec<PqValue>, PqError> {
    match cc.codec {
        meta::Codec::Uncompressed
        | meta::Codec::Snappy
        | meta::Codec::Gzip
        | meta::Codec::Zstd
        | meta::Codec::Lz4Raw => {}
        meta::Codec::Brotli => {
            return Err(PqError::Unsupported(
                "Brotli decode: zero-dep decoder is a dedicated multi-week SP-arc \
                 (~10-15 tasks like SP125-SP140 zstd); workaround — ask the writer to use \
                 compression='zstd' or compression='lz4' instead".into(),
            ))
        }
        meta::Codec::Other(5) => {
            return Err(PqError::Unsupported(
                "LZ4 (deprecated Hadoop framing) — use LZ4_RAW; SP149 follow-up if needed".into(),
            ))
        }
        meta::Codec::Other(_) => {
            return Err(PqError::Unsupported(
                "compression codec: OBJ-2c".into(),
            ))
        }
    }
    if cc.encodings.iter().any(|e| {
        !matches!(
            e,
            meta::Encoding::Plain
                | meta::Encoding::Rle
                | meta::Encoding::PlainDictionary
                | meta::Encoding::RleDictionary
        )
    }) {
        return Err(PqError::Unsupported(
            "non-PLAIN/dictionary encoding (DELTA/BYTE_STREAM_SPLIT): OBJ-2c"
                .into(),
        ));
    }

    let dict: Vec<PqValue> = if let Some(dpo) = cc.dictionary_page_offset {
        let off = usize::try_from(dpo)
            .map_err(|_| PqError::Bad("dict page offset range".into()))?;
        let region = file
            .get(off..)
            .ok_or_else(|| PqError::Bad("dict page offset past EOF".into()))?;
        let (ph, hlen) = meta::decode_page_header(region)?;
        if ph.page_type != 2 {
            return Err(PqError::Bad(
                "dictionary_page_offset does not point at a DICTIONARY_PAGE"
                    .into(),
            ));
        }
        if ph.dict_encoding != 0 && ph.dict_encoding != 2 {
            return Err(PqError::Unsupported(
                "dictionary page encoding (not PLAIN/PLAIN_DICTIONARY): OBJ-2c"
                    .into(),
            ));
        }
        let dn = usize::try_from(ph.dict_num_values)
            .map_err(|_| PqError::Bad("dict num_values range".into()))?;
        let dstart = off
            .checked_add(hlen)
            .ok_or_else(|| PqError::Bad("dict page hdr len ovf".into()))?;
        let comp = usize::try_from(ph.compressed_size)
            .map_err(|_| PqError::Bad("dict page comp size range".into()))?;
        let uncomp = usize::try_from(ph.uncompressed_size)
            .map_err(|_| PqError::Bad("dict page size range".into()))?;
        // SP151: cap-check BEFORE page_payload reserves a buffer.
        // Both comp and uncomp matter — Snappy/Gzip/Zstd allocate
        // uncomp bytes; the on-disk slice is comp bytes (capped
        // against the file separately, but a hostile comp value
        // could still trigger pathological work in the decoder).
        check_page_size("dict page compressed", comp)?;
        check_page_size("dict page uncompressed", uncomp)?;
        let payload = page_payload(file, dstart, comp, uncomp, cc.codec)?;
        plain::decode_plain(&payload, spec, dn)?
    } else {
        Vec::new()
    };

    let want_rows = usize::try_from(cc.num_values)
        .map_err(|_| PqError::Bad("chunk num_values range".into()))?;
    let mut out: Vec<PqValue> = Vec::with_capacity(want_rows);
    let mut off = usize::try_from(cc.data_page_offset)
        .map_err(|_| PqError::Bad("data page offset range".into()))?;
    while out.len() < want_rows {
        let region = file
            .get(off..)
            .ok_or_else(|| PqError::Bad("data page offset past EOF".into()))?;
        let (ph, hlen) = meta::decode_page_header(region)?;
        // V1 byte-identity is the CRITICAL bar: the `page_type == 0`
        // arm must reproduce the pre-OBJ-2c-3 fallible-check sequence
        // token-for-token — `dp_num_values` ("num_values range") FIRST,
        // then dstart → comp → uncomp, then page_payload, dict-guard,
        // decode_page. Nothing fallible derived from `ph` is hoisted
        // before this match (a hoist would let a malformed comp/uncomp
        // surface ahead of a malformed `dp_num_values`, a V1 observable
        // change for hostile multi-malformed input). Each arm returns
        // `(vals, dstart, comp)` so the loop's post-match `off` advance
        // is derived identically to pre-T3 (`off_next = dstart + comp =
        // off + hlen + comp`); the V2 arm computes its OWN dstart/comp
        // independently from `ph`, never sharing V1 bindings.
        let (vals, dstart, comp) = match ph.page_type {
            0 => {
                // ── existing V1 path — byte-identical (pre-T3 order) ──
                let n = usize::try_from(ph.dp_num_values)
                    .map_err(|_| PqError::Bad("num_values range".into()))?;
                let dstart = off
                    .checked_add(hlen)
                    .ok_or_else(|| PqError::Bad("page hdr len ovf".into()))?;
                let comp = usize::try_from(ph.compressed_size)
                    .map_err(|_| PqError::Bad("page comp size range".into()))?;
                let uncomp = usize::try_from(ph.uncompressed_size)
                    .map_err(|_| PqError::Bad("page size range".into()))?;
                // SP151 cap-check (flat V1 data page).
                check_page_size("v1 page compressed", comp)?;
                check_page_size("v1 page uncompressed", uncomp)?;
                let payload = page_payload(file, dstart, comp, uncomp, cc.codec)?;
                // decode_page's dict arms (dp_encoding 2|8, both REQUIRED and
                // OPTIONAL) call dict::resolve_dict_indices, which needs a populated
                // `dict`; `dict` is only populated when dictionary_page_offset is
                // present. Guard here so the failure is a precise typed Bad rather
                // than an empty-dict OOB inside decode_page.
                if matches!(ph.dp_encoding, 2 | 8)
                    && cc.dictionary_page_offset.is_none()
                {
                    return Err(PqError::Bad(
                        "dictionary-encoded data page without dictionary_page_offset"
                            .into(),
                    ));
                }
                let vals = decode_page(
                    &payload,
                    ph.dp_encoding,
                    spec,
                    n,
                    max_def_level,
                    &dict,
                )?;
                (vals, dstart, comp)
            }
            3 => {
                // V2 computes its own region bounds independently from
                // `ph` — duplicated offset bookkeeping, never the V1
                // bindings (distinct error strings keep the paths apart).
                let dstart = off
                    .checked_add(hlen)
                    .ok_or_else(|| PqError::Bad("v2 page hdr len ovf".into()))?;
                let comp = usize::try_from(ph.compressed_size)
                    .map_err(|_| PqError::Bad("v2 comp size range".into()))?;
                // SP151 cap-check (flat V2 data page). The internal
                // uncompressed cap-check fires inside decode_data_page_v2.
                check_page_size("v2 page compressed", comp)?;
                let v2_region = file
                    .get(
                        dstart
                            ..dstart.checked_add(comp).ok_or_else(|| {
                                PqError::Bad("v2 region ovf".into())
                            })?,
                    )
                    .ok_or_else(|| PqError::Bad("v2 page truncated".into()))?;
                let vals = decode_data_page_v2(
                    v2_region,
                    &ph,
                    cc.codec,
                    spec,
                    max_def_level,
                    &dict,
                )?;
                (vals, dstart, comp)
            }
            _ => {
                return Err(PqError::Unsupported(
                    "non-V1/V2 data page (index): OBJ-2c".into(),
                ))
            }
        };
        if out.len().checked_add(vals.len()).map(|t| t > want_rows).unwrap_or(true) {
            return Err(PqError::Bad(
                "data page values exceed chunk num_values".into(),
            ));
        }
        out.extend(vals);
        // off strictly advances: off_next = dstart + comp = off + hlen + comp,
        // and hlen >= 1 (decode_page_header always consumes at least the
        // STOP byte), so even a zero-comp page makes forward progress —
        // the loop cannot spin; it terminates at want_rows or an EOF-bounds
        // PqError::Bad.
        off = dstart.checked_add(comp).ok_or_else(|| PqError::Bad("page advance ovf".into()))?;
    }
    if out.len() != want_rows {
        return Err(PqError::Bad(
            "data page values do not sum to chunk num_values".into(),
        ));
    }
    Ok(out)
}

/// Build the per-leaf `PlainSpec` from a schema element. Upfront
/// validation of DECIMAL precision/scale ranges, FLBA width, and
/// physical-vs-logical compatibility — so the decode hot loop is
/// already on validated metadata.
///
/// Errors:
/// - `PqError::Unsupported`: precision outside `1..=38` (i128 cap).
/// - `PqError::Bad`: malformed/incompatible spec (negative or
///   excess scale, FLBA width out of range, DECIMAL on FLOAT/DOUBLE,
///   INT96 + DECIMAL combination, missing FLBA type_length).
fn build_plain_spec(leaf: &meta::SchemaLeaf) -> Result<plain::PlainSpec, PqError> {
    use meta::Type::*;
    // Detect DECIMAL: either ConvertedType=DECIMAL(5) or LogicalType
    // DecimalType arm (`logical_type_decimal: Some((scale, precision))`).
    // T2's `decode_schema_element` agreement check guarantees both sides
    // agree when both are populated; we read whichever has the values.
    let decimal_meta: Option<(i32, i32)> = if leaf.converted_type == Some(5) {
        // ConvertedType DECIMAL: scale/precision are SchemaElement
        // fields 7/8 (default 0 when absent — caught by precision==0
        // < 1 check below as Unsupported).
        let scale = leaf.scale.unwrap_or(0);
        let precision = leaf.precision.unwrap_or(0);
        Some((scale, precision))
    } else if let Some((s, p)) = leaf.logical_type_decimal {
        Some((s, p))
    } else {
        None
    };

    if let Some((scale, precision)) = decimal_meta {
        // Precision range: parquet spec allows 1..=38 (i128 holds the
        // unscaled value; > 38 needs arbitrary precision, out of scope).
        if precision < 1 || precision > 38 {
            return Err(PqError::Unsupported(format!(
                "DECIMAL precision {precision} (must be 1..=38): OBJ-2c-4"
            )));
        }
        if scale < 0 || scale > precision {
            return Err(PqError::Bad(format!(
                "DECIMAL scale {scale} out of range for precision {precision}"
            )));
        }
        match leaf.ptype {
            Int32 if precision > 9 => {
                return Err(PqError::Bad(
                    "DECIMAL precision > 9 on INT32 physical type".into(),
                ))
            }
            Int64 if precision > 18 => {
                return Err(PqError::Bad(
                    "DECIMAL precision > 18 on INT64 physical type".into(),
                ))
            }
            FixedLenByteArray | ByteArray | Int32 | Int64 => {}
            Int96 => {
                return Err(PqError::Bad(
                    "DECIMAL on INT96 physical type: not supported".into(),
                ))
            }
            _ => {
                return Err(PqError::Bad(
                    "DECIMAL on incompatible physical type".into(),
                ))
            }
        }
        // Build the per-physical DECIMAL spec.
        return match leaf.ptype {
            FixedLenByteArray => {
                let n = leaf
                    .type_length
                    .ok_or_else(|| {
                        PqError::Bad(
                            "FLBA DECIMAL missing type_length".into(),
                        )
                    })?;
                let n = usize::try_from(n).map_err(|_| {
                    PqError::Bad("FLBA type_length range".into())
                })?;
                if n == 0 || n > 16 {
                    return Err(PqError::Bad(
                        "DECIMAL FLBA width out of range (1..=16, i128)".into(),
                    ));
                }
                Ok(plain::PlainSpec::flba_decimal(
                    n,
                    precision as u32,
                    scale as u32,
                ))
            }
            Int32 | Int64 => Ok(plain::PlainSpec::int_decimal(
                leaf.ptype,
                precision as u32,
                scale as u32,
            )),
            ByteArray => Ok(plain::PlainSpec::byte_array_decimal(
                precision as u32,
                scale as u32,
            )),
            _ => unreachable!("guarded above"),
        };
    }

    // Non-DECIMAL leaves. FLBA must carry a positive type_length.
    match leaf.ptype {
        FixedLenByteArray => {
            let n = leaf
                .type_length
                .ok_or_else(|| PqError::Bad("FLBA missing type_length".into()))?;
            let n = usize::try_from(n)
                .map_err(|_| PqError::Bad("FLBA type_length range".into()))?;
            if n == 0 || n > 65_536 {
                return Err(PqError::Bad(
                    "FLBA type_length out of range (1..=65_536)".into(),
                ));
            }
            Ok(plain::PlainSpec::flba(n))
        }
        _ => Ok(plain::PlainSpec::plain(leaf.ptype)),
    }
}

/// SP143 T6: dispatch plan classification for one wanted column —
/// either a flat physical leaf (existing path) or a recognized
/// canonical LIST<primitive> column (new path). SP144 T5 extends this
/// with `NestedMapKV` (canonical 3-node MAP<K, V>) and `NestedStruct`
/// (struct-of-primitives) variants.
enum ColumnKind {
    Flat {
        spec: plain::PlainSpec,
        ptype: meta::Type,
        max_def_level: u32,
    },
    NestedListPrimitive {
        spec: plain::PlainSpec,
        ptype: meta::Type,
        max_def_level: u32,
        max_rep_level: u32,
        outer_optional: bool,
        element_optional: bool,
    },
    /// SP144 T5: canonical 3-node MAP<K, V> where K is a REQUIRED leaf
    /// and V is a REQUIRED-or-OPTIONAL leaf. The chunk paths point at
    /// the key and value column chunks; `max_def_level` is V's def
    /// (= K's def + value_optional).
    NestedMapKV {
        key_spec: plain::PlainSpec,
        value_spec: plain::PlainSpec,
        key_ptype: meta::Type,
        value_ptype: meta::Type,
        key_chunk_path: Vec<String>,
        value_chunk_path: Vec<String>,
        /// V's max_def (authoritative for `assemble_map_kv`).
        max_def_level: u32,
        outer_optional: bool,
        value_optional: bool,
    },
    /// SP144 T5: struct column with N children. Each field is decoded
    /// (possibly recursively for SP145 nested-shape fields) then zipped
    /// via `assemble_struct`. SP145 extends this from "primitive fields
    /// only" to "any shape" by storing per-field nested plans.
    NestedStruct {
        outer_optional: bool,
        fields: Vec<StructField>,
    },
    /// SP145 T5: `List<List<primitive>>` — outer LIST with a nested
    /// LIST<primitive> as its element. The leaf path points at the
    /// innermost primitive leaf (the data column). Both LIST groups can
    /// be REQUIRED or OPTIONAL independently; the leaf itself can also
    /// be REQ or OPT.
    NestedListOfListPrimitive {
        spec: plain::PlainSpec,
        ptype: meta::Type,
        max_def_level: u32,
        max_rep_level: u32,
        outer_optional: bool,
        inner_optional: bool,
        item_optional: bool,
    },
    /// SP145 T5: `List<struct<...>>` — outer LIST whose element is a
    /// REQUIRED struct with N primitive fields. Each field is its own
    /// column chunk; all share the same REPEATED outer-LIST ancestor
    /// (so all field columns have identical rep streams at max_rep=1).
    /// The first field is used as the rep/def authority.
    NestedListOfStruct {
        outer_optional: bool,
        /// Authoritative max_def_level at the LIST-of-struct level
        /// (outer_optional + 1 /*REP*/ + 0 — struct REQ inside list).
        list_max_def_level: u32,
        fields: Vec<StructField>,
    },
    /// SP145 T5: `Map<K, struct<...>>` — outer MAP whose V is a
    /// REQUIRED struct with N primitive fields. K is a REQUIRED leaf.
    /// All V field columns share the same REPEATED middle ancestor as K,
    /// so all have identical rep streams (max_rep=1).
    NestedMapOfStruct {
        outer_optional: bool,
        key_spec: plain::PlainSpec,
        key_ptype: meta::Type,
        key_chunk_path: Vec<String>,
        /// Authoritative max_def_level at the MAP-of-struct level
        /// (outer_optional + 1 /*REP*/ + 0 — struct V is REQ).
        map_max_def_level: u32,
        value_fields: Vec<StructField>,
    },
    /// SP145 T5 (BOLD cross-product): `Map<K, List<T>>` — outer MAP
    /// whose V is itself a LIST<primitive>. The V leaf's max_rep_level=2
    /// (MAP REP + LIST REP). K's max_rep_level=1.
    NestedMapOfList {
        outer_optional: bool,
        key_spec: plain::PlainSpec,
        key_ptype: meta::Type,
        key_chunk_path: Vec<String>,
        value_spec: plain::PlainSpec,
        value_ptype: meta::Type,
        value_chunk_path: Vec<String>,
        /// V leaf's max_def_level
        /// (outer_optional + 1 /*MAP REP*/ + 1 /*LIST REP*/ + value_item_optional).
        max_def_level: u32,
        value_item_optional: bool,
    },
    /// SP146: `List<List<List<T>>>` — 3-deep LIST nesting (max_rep_level=3).
    /// One primitive leaf below three nested LISTs.
    NestedListOfListOfListPrimitive {
        spec: plain::PlainSpec,
        ptype: meta::Type,
        max_def_level: u32,
        max_rep_level: u32,
        outer_optional: bool,
        middle_optional: bool,
        inner_optional: bool,
        item_optional: bool,
    },
    /// SP146: `List<Map<K, V>>` — outer LIST whose element is itself a
    /// Map<K, V> with primitive leaves. Both K and V leaves have
    /// max_rep_level=2 (outer LIST REP + MAP key_value REP) — they share
    /// the same rep stream.
    NestedListOfMap {
        outer_optional: bool,
        key_spec: plain::PlainSpec,
        key_ptype: meta::Type,
        key_chunk_path: Vec<String>,
        value_spec: plain::PlainSpec,
        value_ptype: meta::Type,
        value_chunk_path: Vec<String>,
        /// V leaf's max_def_level
        /// (outer_optional + 1 /*LIST REP*/ + 1 /*MAP REP*/ + value_optional).
        max_def_level: u32,
        value_optional: bool,
    },
    /// SP146: `Map<K1, Map<K2, V>>` — outer MAP whose V is itself a
    /// Map<K2, V> with primitive leaves. Inner K and V leaves have
    /// max_rep_level=2 (outer MAP REP + inner MAP REP) — they share the
    /// same rep stream. Outer K has max_rep_level=1.
    NestedMapOfMap {
        outer_optional: bool,
        outer_key_spec: plain::PlainSpec,
        outer_key_ptype: meta::Type,
        outer_key_chunk_path: Vec<String>,
        inner_key_spec: plain::PlainSpec,
        inner_key_ptype: meta::Type,
        inner_key_chunk_path: Vec<String>,
        inner_value_spec: plain::PlainSpec,
        inner_value_ptype: meta::Type,
        inner_value_chunk_path: Vec<String>,
        /// V leaf's max_def_level
        /// (outer_optional + 1 /*MAP REP*/ + 1 /*MAP REP*/ + inner_value_optional).
        max_def_level: u32,
        inner_value_optional: bool,
    },
}

/// SP144 T5: single field of a NestedStruct. SP145 enriches with an
/// optional `nested` — when Some(kind), the field is itself a nested
/// shape (LIST or struct) that requires recursive assembly. When None,
/// the field is a flat primitive decoded via `read_chunk_values`.
struct StructField {
    name: String,
    chunk_path: Vec<String>,
    spec: plain::PlainSpec,
    ptype: meta::Type,
    max_def_level: u32,
    /// SP145: when Some, this field is a nested shape (recursively
    /// classified). The recursion lets `struct<List<T>>` and
    /// `struct<struct<...>>` work compositionally: the per-field
    /// decode for a nested field calls the appropriate nested decoder
    /// (which itself may recurse). When None (SP144 V1), this field
    /// is a flat primitive — same code path as before.
    nested: Option<Box<ColumnKind>>,
}

struct ColumnPlan {
    /// Full leaf path WITHOUT the schema root group name — matches the
    /// shape stored in `ColumnChunk::path` (parquet-format
    /// `path_in_schema`). For `NestedMapKV` this is the VALUE leaf's
    /// chunk path (primary lookup); for `NestedStruct` it is the first
    /// field's chunk path. The per-leaf paths inside the kind variant
    /// are the authoritative source for the per-chunk lookups.
    chunk_path: Vec<String>,
    kind: ColumnKind,
}

/// SP143 T6: walk the SchemaTree to classify a wanted column. For the
/// V1 scope we accept:
///   - a flat physical leaf directly under root (existing path), OR
///   - a canonical LIST<primitive> group with the 3-node pattern
///     `outer{ repeated middle { primitive_leaf } }`.
/// Everything else surfaces a typed `Unsupported` error naming the
/// next slice (SP144 for Map/struct, SP145 for deep nesting / List<group>).
fn classify_column_plan(
    root: &meta::SchemaNode,
    col_name: &str,
    leaves: &[meta::SchemaLeaf],
) -> Result<ColumnPlan, PqError> {
    let root_children = match root {
        meta::SchemaNode::Group { children, .. } => children,
        _ => return Err(PqError::Bad("schema root is not a group".into())),
    };
    let node = root_children
        .iter()
        .find(|n| match n {
            meta::SchemaNode::Group { name, .. } => name == col_name,
            meta::SchemaNode::Leaf { name, .. } => name == col_name,
        })
        .ok_or_else(|| {
            PqError::Bad(format!(
                "column `{col_name}` not found in Parquet schema"
            ))
        })?;

    match node {
        meta::SchemaNode::Leaf { name, ptype, repetition, max_def_level, path, .. } => {
            // Flat column under a non-flat schema (mixed file): preserve
            // the existing flat-path semantics — REQUIRED/OPTIONAL OK,
            // REPEATED and unknown rejected with the same OBJ-2c phrasing
            // so the test corpus stays stable across the flat-vs-nested
            // dispatch split.
            let mdl: u32 = match repetition {
                meta::Repetition::Required => 0,
                meta::Repetition::Optional => 1,
                meta::Repetition::Repeated => {
                    return Err(PqError::Unsupported(
                        "REPEATED columns: OBJ-2c".into(),
                    ))
                }
                meta::Repetition::Other(_) => {
                    return Err(PqError::Unsupported(
                        "unknown repetition: OBJ-2c".into(),
                    ))
                }
            };
            // Defense-in-depth: the schema-tree's computed max_def_level
            // for a flat leaf must agree with the simple 0/1 derivation
            // above. Disagreement = malformed schema the flat-path math
            // would otherwise silently mis-decode.
            if *max_def_level != mdl {
                return Err(PqError::Bad(format!(
                    "flat leaf `{name}` schema-tree max_def_level={max_def_level} \
                     disagrees with repetition-derived {mdl}"
                )));
            }
            match ptype {
                meta::Type::Boolean
                | meta::Type::Int32
                | meta::Type::Int64
                | meta::Type::Float
                | meta::Type::Double
                | meta::Type::ByteArray
                | meta::Type::Int96
                | meta::Type::FixedLenByteArray => {}
                t => {
                    return Err(PqError::Unsupported(format!(
                        "physical type {t:?}: OBJ-2c"
                    )))
                }
            }
            // Build the PlainSpec via the flat-list lookup so DECIMAL /
            // FLBA width validation runs identically to the flat path
            // (SchemaTree::Leaf doesn't yet carry the DECIMAL metadata).
            let leaf_struct = leaves
                .iter()
                .find(|l| &l.name == name)
                .ok_or_else(|| {
                    PqError::Bad(format!(
                        "tree leaf `{name}` missing from flat leaves list"
                    ))
                })?;
            let spec = build_plain_spec(leaf_struct)?;
            // path on a tree Leaf includes the root group name as path[0];
            // strip it to match ColumnChunk::path (`path_in_schema`).
            let chunk_path = path.iter().skip(1).cloned().collect();
            Ok(ColumnPlan {
                chunk_path,
                kind: ColumnKind::Flat {
                    spec,
                    ptype: *ptype,
                    max_def_level: mdl,
                },
            })
        }
        meta::SchemaNode::Group { name, repetition, children, logical_type } => {
            // SP144 T5: Map and struct groups now classify-and-decode.
            // SP145 deep-nesting shapes (struct<group>, MAP<group,_>,
            // List<struct>) still reject with named SP145 errors.
            match logical_type {
                Some(meta::LogicalType::Map) => {
                    return classify_map_plan(name, *repetition, children, leaves);
                }
                None => {
                    return classify_struct_plan(name, *repetition, children, leaves);
                }
                Some(meta::LogicalType::List) => {
                    // fall through to the existing LIST<primitive> path
                }
            }
            let outer_optional = matches!(repetition, meta::Repetition::Optional);
            // Canonical LIST: exactly one child, a REPEATED middle group
            // with exactly one primitive-leaf child. Anything else is
            // List<group> / List<List<_>> / non-canonical → SP145.
            if children.len() != 1 {
                return Err(PqError::Unsupported(format!(
                    "non-canonical LIST `{name}` (outer children != 1): SP145 follow-up"
                )));
            }
            let middle_children = match &children[0] {
                meta::SchemaNode::Group {
                    repetition: meta::Repetition::Repeated,
                    children: gc,
                    ..
                } => gc,
                _ => {
                    return Err(PqError::Unsupported(format!(
                        "non-canonical LIST `{name}` (middle not REPEATED group): SP145 follow-up"
                    )))
                }
            };
            if middle_children.len() != 1 {
                return Err(PqError::Unsupported(format!(
                    "non-canonical LIST `{name}` (middle children != 1): SP145 follow-up"
                )));
            }
            let leaf_node = &middle_children[0];
            // SP145 T5: when the LIST's element is itself a Group (not
            // a Leaf), this is one of the SP145 deep-nesting shapes:
            //   - List<List<T>>      → element is a group with LogicalType::List
            //   - List<struct<...>>  → element is a group with LogicalType=None
            //   - List<Map<K,V>>     → element is a group with LogicalType::Map
            // Recursively classify the element group and emit one of
            // the new ColumnKind variants.
            if let meta::SchemaNode::Group {
                name: element_name,
                repetition: element_rep,
                children: element_children,
                logical_type: element_logical,
            } = leaf_node
            {
                return classify_list_of_group(
                    name,
                    outer_optional,
                    element_name,
                    *element_rep,
                    element_children,
                    element_logical.as_ref(),
                    leaves,
                );
            }
            let (leaf_name, leaf_ptype, leaf_rep, leaf_max_def, leaf_max_rep, leaf_path) =
                match leaf_node {
                    meta::SchemaNode::Leaf {
                        name,
                        ptype,
                        repetition,
                        max_def_level,
                        max_rep_level,
                        path,
                    } => (
                        name.clone(),
                        *ptype,
                        *repetition,
                        *max_def_level,
                        *max_rep_level,
                        path.clone(),
                    ),
                    _ => {
                        return Err(PqError::Unsupported(format!(
                            "List<group> / List<List<_>> `{name}`: SP145 follow-up"
                        )))
                    }
                };
            let element_optional = matches!(leaf_rep, meta::Repetition::Optional);
            // For canonical LIST<primitive>: outer contributes
            // (outer_optional as u32), REPEATED middle contributes 1,
            // leaf contributes (element_optional as u32). Schema-tree
            // max_def_level on the leaf must agree.
            let expected_max_def =
                (outer_optional as u32) + 1 + (element_optional as u32);
            if leaf_max_def != expected_max_def {
                return Err(PqError::Bad(format!(
                    "nested LIST `{name}` leaf max_def_level={leaf_max_def} \
                     disagrees with expected {expected_max_def}"
                )));
            }
            // max_rep_level for a single-level LIST is always 1.
            if leaf_max_rep != 1 {
                return Err(PqError::Bad(format!(
                    "nested LIST `{name}` leaf max_rep_level={leaf_max_rep} != 1"
                )));
            }
            match leaf_ptype {
                meta::Type::Boolean
                | meta::Type::Int32
                | meta::Type::Int64
                | meta::Type::Float
                | meta::Type::Double
                | meta::Type::ByteArray
                | meta::Type::Int96
                | meta::Type::FixedLenByteArray => {}
                t => {
                    return Err(PqError::Unsupported(format!(
                        "LIST<{t:?}>: physical type not supported in OBJ-2c"
                    )))
                }
            }
            // Build the PlainSpec via the flat leaves list (the SchemaTree
            // doesn't yet carry DECIMAL / FLBA metadata on its Leaf
            // variant; the flat list does).
            let leaf_struct = leaves
                .iter()
                .find(|l| l.name == leaf_name)
                .ok_or_else(|| {
                    PqError::Bad(format!(
                        "LIST leaf `{leaf_name}` missing from flat leaves list"
                    ))
                })?;
            let spec = build_plain_spec(leaf_struct)?;
            // Strip root from the tree-recorded path so it matches the
            // ColumnChunk path_in_schema convention.
            let chunk_path = leaf_path.iter().skip(1).cloned().collect();
            Ok(ColumnPlan {
                chunk_path,
                kind: ColumnKind::NestedListPrimitive {
                    spec,
                    ptype: leaf_ptype,
                    max_def_level: leaf_max_def,
                    max_rep_level: leaf_max_rep,
                    outer_optional,
                    element_optional,
                },
            })
        }
    }
}

/// SP144 T5: classify a canonical 3-node MAP<K, V> column.
///
/// Accepts: outer{group, REQ|OPT, MAP} → middle{group, REPEATED,
/// 2 children} → key{leaf, REQUIRED, primitive}, value{leaf, REQ|OPT,
/// primitive}. Anything deeper (group key, group value, missing
/// REPEATED middle) rejects with a named SP145 error.
fn classify_map_plan(
    name: &str,
    repetition: meta::Repetition,
    children: &[meta::SchemaNode],
    leaves: &[meta::SchemaLeaf],
) -> Result<ColumnPlan, PqError> {
    let outer_optional = matches!(repetition, meta::Repetition::Optional);
    // Outer must have exactly 1 child (the REPEATED middle group).
    if children.len() != 1 {
        return Err(PqError::Unsupported(format!(
            "non-canonical MAP `{name}` (outer children != 1): SP145 follow-up"
        )));
    }
    let middle_children = match &children[0] {
        meta::SchemaNode::Group {
            repetition: meta::Repetition::Repeated,
            children: gc,
            ..
        } => gc,
        _ => {
            return Err(PqError::Unsupported(format!(
                "non-canonical MAP `{name}` (middle not REPEATED group): SP145 follow-up"
            )))
        }
    };
    // Middle must have exactly 2 children (key + value).
    if middle_children.len() != 2 {
        return Err(PqError::Unsupported(format!(
            "non-canonical MAP `{name}` (key_value children != 2): SP145 follow-up"
        )));
    }
    // Key: MUST be REQUIRED Leaf (Parquet spec) — reject group keys + OPT keys.
    let (key_name, key_ptype, key_max_def, key_path) = match &middle_children[0] {
        meta::SchemaNode::Leaf {
            name: kn, repetition: kr, ptype, max_def_level, path, ..
        } => {
            if !matches!(kr, meta::Repetition::Required) {
                return Err(PqError::Bad(format!(
                    "MAP key `{name}` must be REQUIRED per Parquet spec"
                )));
            }
            (kn.clone(), *ptype, *max_def_level, path.clone())
        }
        _ => {
            return Err(PqError::Unsupported(format!(
                "MAP<group, _> `{name}` (key is a group): SP145 follow-up"
            )))
        }
    };
    // Value: Leaf, REQUIRED or OPTIONAL. SP145 T5: also accept Group
    // (struct value or List value), dispatching to the appropriate
    // composed-shape classifier.
    if let meta::SchemaNode::Group {
        name: vname, repetition: vrep, children: vchildren, logical_type: vlogical,
    } = &middle_children[1]
    {
        return classify_map_of_group(
            name, outer_optional, &key_name, key_ptype, key_max_def, &key_path,
            vname, *vrep, vchildren, vlogical.as_ref(), leaves,
        );
    }
    let (value_name, value_ptype, value_repetition, value_max_def, value_path) =
        match &middle_children[1] {
            meta::SchemaNode::Leaf {
                name: vn, repetition: vr, ptype, max_def_level, path, ..
            } => (vn.clone(), *ptype, *vr, *max_def_level, path.clone()),
            _ => {
                return Err(PqError::Unsupported(format!(
                    "MAP<_, group> `{name}` (value is a group): SP145 follow-up"
                )))
            }
        };
    let value_optional = matches!(value_repetition, meta::Repetition::Optional);
    if !matches!(
        value_repetition,
        meta::Repetition::Required | meta::Repetition::Optional
    ) {
        return Err(PqError::Bad(format!(
            "MAP value `{name}` must be REQUIRED or OPTIONAL"
        )));
    }
    // Defense-in-depth: V_max_def = outer_optional + 1 (middle REPEATED) +
    // value_optional; K_max_def = outer_optional + 1 + 0 (key REQUIRED).
    let expected_v_max_def =
        (outer_optional as u32) + 1 + (value_optional as u32);
    let expected_k_max_def = (outer_optional as u32) + 1;
    if value_max_def != expected_v_max_def {
        return Err(PqError::Bad(format!(
            "MAP `{name}` value max_def_level={value_max_def} disagrees with expected {expected_v_max_def}"
        )));
    }
    if key_max_def != expected_k_max_def {
        return Err(PqError::Bad(format!(
            "MAP `{name}` key max_def_level={key_max_def} disagrees with expected {expected_k_max_def}"
        )));
    }
    // Physical-type allow-list mirrors the flat / LIST<primitive> path.
    for (label, t) in [("key", key_ptype), ("value", value_ptype)] {
        match t {
            meta::Type::Boolean
            | meta::Type::Int32
            | meta::Type::Int64
            | meta::Type::Float
            | meta::Type::Double
            | meta::Type::ByteArray
            | meta::Type::Int96
            | meta::Type::FixedLenByteArray => {}
            other => {
                return Err(PqError::Unsupported(format!(
                    "MAP `{name}` {label} physical type {other:?}: OBJ-2c"
                )))
            }
        }
    }
    // Build PlainSpecs via the flat leaves list (DECIMAL / FLBA metadata
    // lives there, not on the schema tree's Leaf variant).
    let key_leaf = leaves
        .iter()
        .find(|l| l.name == key_name)
        .ok_or_else(|| {
            PqError::Bad(format!(
                "MAP `{name}` key leaf `{key_name}` missing from flat leaves list"
            ))
        })?;
    let value_leaf = leaves
        .iter()
        .find(|l| l.name == value_name)
        .ok_or_else(|| {
            PqError::Bad(format!(
                "MAP `{name}` value leaf `{value_name}` missing from flat leaves list"
            ))
        })?;
    let key_spec = build_plain_spec(key_leaf)?;
    let value_spec = build_plain_spec(value_leaf)?;
    // Strip root group name from each tree-recorded path to match
    // ColumnChunk.path (parquet `path_in_schema`).
    let key_chunk_path: Vec<String> = key_path.iter().skip(1).cloned().collect();
    let value_chunk_path: Vec<String> = value_path.iter().skip(1).cloned().collect();
    let plan_chunk_path = value_chunk_path.clone();
    Ok(ColumnPlan {
        chunk_path: plan_chunk_path,
        kind: ColumnKind::NestedMapKV {
            key_spec,
            value_spec,
            key_ptype,
            value_ptype,
            key_chunk_path,
            value_chunk_path,
            max_def_level: value_max_def,
            outer_optional,
            value_optional,
        },
    })
}

/// SP144 T5: classify a struct column (any non-LIST/non-MAP group).
///
/// Accepts: outer{group, REQ|OPT, N primitive-leaf children}. A struct
/// containing a nested group child (struct-of-struct, struct-of-list,
/// struct-of-map) rejects with a named SP145 error. Empty structs (0
/// children) reject with Bad — a schema with no fields is malformed.
fn classify_struct_plan(
    name: &str,
    repetition: meta::Repetition,
    children: &[meta::SchemaNode],
    leaves: &[meta::SchemaLeaf],
) -> Result<ColumnPlan, PqError> {
    let outer_optional = matches!(repetition, meta::Repetition::Optional);
    if children.is_empty() {
        return Err(PqError::Bad(format!(
            "struct `{name}` has no fields"
        )));
    }
    let mut fields = Vec::with_capacity(children.len());
    for child in children {
        match child {
            meta::SchemaNode::Leaf {
                name: child_name,
                repetition: child_rep,
                ptype,
                max_def_level,
                path,
                ..
            } => {
                // Flat fields inside a struct: REQUIRED/OPTIONAL only.
                // REPEATED would mean struct-of-list which T5 doesn't
                // ship (SP145).
                let field_local_def: u32 = match child_rep {
                    meta::Repetition::Required => 0,
                    meta::Repetition::Optional => 1,
                    meta::Repetition::Repeated => {
                        return Err(PqError::Unsupported(format!(
                            "struct `{name}` field `{child_name}` is REPEATED (List): SP145 follow-up"
                        )))
                    }
                    meta::Repetition::Other(_) => {
                        return Err(PqError::Unsupported(format!(
                            "struct `{name}` field `{child_name}` unknown repetition"
                        )))
                    }
                };
                // Physical-type allow-list (mirrors the flat path).
                match ptype {
                    meta::Type::Boolean
                    | meta::Type::Int32
                    | meta::Type::Int64
                    | meta::Type::Float
                    | meta::Type::Double
                    | meta::Type::ByteArray
                    | meta::Type::Int96
                    | meta::Type::FixedLenByteArray => {}
                    other => {
                        return Err(PqError::Unsupported(format!(
                            "struct `{name}` field `{child_name}` physical type {other:?}: OBJ-2c"
                        )))
                    }
                };
                // Defense-in-depth: schema-tree max_def_level on the
                // field equals (outer_optional + field_local_def). This
                // is the V1 limit — struct nested under another
                // OPTIONAL/REPEATED ancestor would break the math, but
                // those land via SP145 paths above.
                let expected_field_max_def =
                    (outer_optional as u32) + field_local_def;
                if *max_def_level != expected_field_max_def {
                    return Err(PqError::Bad(format!(
                        "struct `{name}` field `{child_name}` max_def_level={max_def_level} \
                         disagrees with expected {expected_field_max_def}"
                    )));
                }
                // Build PlainSpec from flat leaves list (DECIMAL/FLBA
                // metadata lives there).
                let leaf_struct = leaves
                    .iter()
                    .find(|l| &l.name == child_name)
                    .ok_or_else(|| {
                        PqError::Bad(format!(
                            "struct `{name}` field leaf `{child_name}` missing from flat leaves list"
                        ))
                    })?;
                let spec = build_plain_spec(leaf_struct)?;
                let chunk_path: Vec<String> = path.iter().skip(1).cloned().collect();
                fields.push(StructField {
                    name: child_name.clone(),
                    chunk_path,
                    spec,
                    ptype: *ptype,
                    max_def_level: *max_def_level,
                    nested: None,
                });
            }
            meta::SchemaNode::Group { name: child_name, .. } => {
                // SP145 T5: lift the rejection — recursively classify
                // the nested group child as its own ColumnPlan, then
                // wrap as a struct field with `nested = Some(kind)`.
                // The recursive classify_column_plan call needs a
                // synthetic root that looks like the parent schema root
                // would, but with this child as the only top-level
                // group. Easier: factor the classification into
                // `classify_group_node` that doesn't require a parent
                // root walk, then wrap.
                let nested_kind = classify_nested_group_child(
                    child, leaves,
                )?;
                // The struct field's chunk_path is a placeholder for
                // nested fields; the nested kind's own per-leaf paths
                // are the authoritative source the decoder uses.
                // We use the first reachable leaf under the child as
                // the placeholder.
                let placeholder = first_leaf_chunk_path(child)?;
                fields.push(StructField {
                    name: child_name.clone(),
                    chunk_path: placeholder,
                    // The spec/ptype/max_def_level are unused for
                    // nested fields (decoder dispatches off `nested`),
                    // but rust requires them populated — use bool i32
                    // placeholders that won't crash if accidentally
                    // read.
                    spec: plain::PlainSpec::plain(meta::Type::Boolean),
                    ptype: meta::Type::Boolean,
                    max_def_level: 0,
                    nested: Some(Box::new(nested_kind)),
                });
            }
        }
    }
    // Use the first field's chunk path as the plan-level primary key
    // (any leaf works; the per-field paths inside `fields` are the
    // authoritative source consumed by `read_chunk_values_nested_struct`).
    let primary_chunk_path = fields[0].chunk_path.clone();
    Ok(ColumnPlan {
        chunk_path: primary_chunk_path,
        kind: ColumnKind::NestedStruct {
            outer_optional,
            fields,
        },
    })
}

/// SP145 T5: classify a List<group> shape (the element under the
/// REPEATED middle is itself a Group, not a primitive Leaf). Dispatches
/// to the appropriate compositional ColumnKind based on the element
/// group's LogicalType + structural fingerprint.
///
/// Supported element shapes (V1):
///   - struct (LogicalType=None, ≥1 primitive child): `List<struct>`
///   - List (LogicalType=List, canonical 3-node):     `List<List<primitive>>`
/// Rejects (V1):
///   - struct with nested-group children (i.e. `List<struct<List>>`) —
///     would need 2+ recursive composition levels. Documented follow-up.
///   - List<Map<K,V>> — same reason.
///   - List<List<List<T>>> (3+ deep) — same reason.
fn classify_list_of_group(
    outer_name: &str,
    outer_optional: bool,
    element_name: &str,
    element_rep: meta::Repetition,
    element_children: &[meta::SchemaNode],
    element_logical: Option<&meta::LogicalType>,
    leaves: &[meta::SchemaLeaf],
) -> Result<ColumnPlan, PqError> {
    // The element group sits inside a REPEATED middle and is itself
    // typically REQUIRED (pyarrow default) or OPTIONAL. We accept both.
    let _element_optional = matches!(element_rep, meta::Repetition::Optional);
    match element_logical {
        // List<List<primitive>> ──────────────────────────────────────────
        Some(meta::LogicalType::List) => {
            // The element group is itself an OPT|REQ outer List. Drill
            // through its REPEATED middle to find the leaf.
            if element_children.len() != 1 {
                return Err(PqError::Unsupported(format!(
                    "List<List> `{outer_name}` inner-list outer children != 1: \
                     non-canonical nested LIST"
                )));
            }
            let inner_middle = match &element_children[0] {
                meta::SchemaNode::Group {
                    repetition: meta::Repetition::Repeated,
                    children: ic, ..
                } => ic,
                _ => return Err(PqError::Unsupported(format!(
                    "List<List> `{outer_name}` inner-list middle not REPEATED"
                ))),
            };
            if inner_middle.len() != 1 {
                return Err(PqError::Unsupported(format!(
                    "List<List> `{outer_name}` inner-list middle children != 1"
                )));
            }
            // SP146: when inner_middle[0] is itself a Group with
            // LogicalType=List, this is List<List<List<T>>> (3-deep);
            // drill one more level. Other group shapes (List<List<struct>>,
            // List<List<Map>>) remain SP147+ follow-ups.
            if let meta::SchemaNode::Group {
                name: inner_element_name,
                repetition: inner_element_rep,
                children: inner_element_children,
                logical_type: inner_element_logical,
            } = &inner_middle[0]
            {
                return classify_list_of_list_of_group(
                    outer_name,
                    outer_optional,
                    matches!(element_rep, meta::Repetition::Optional),
                    inner_element_name,
                    *inner_element_rep,
                    inner_element_children,
                    inner_element_logical.as_ref(),
                    leaves,
                );
            }
            let (leaf_name, leaf_ptype, leaf_rep, leaf_max_def, leaf_max_rep, leaf_path) =
                match &inner_middle[0] {
                    meta::SchemaNode::Leaf {
                        name, ptype, repetition, max_def_level, max_rep_level, path,
                    } => (name.clone(), *ptype, *repetition, *max_def_level, *max_rep_level, path.clone()),
                    _ => return Err(PqError::Unsupported(format!(
                        "List<List<group>> `{outer_name}` non-canonical inner element"
                    ))),
                };
            let inner_optional = matches!(element_rep, meta::Repetition::Optional);
            let item_optional = matches!(leaf_rep, meta::Repetition::Optional);
            // Physical type allow-list (mirrors flat / LIST paths).
            match leaf_ptype {
                meta::Type::Boolean | meta::Type::Int32 | meta::Type::Int64
                | meta::Type::Float | meta::Type::Double | meta::Type::ByteArray
                | meta::Type::Int96 | meta::Type::FixedLenByteArray => {}
                t => return Err(PqError::Unsupported(format!(
                    "List<List<{t:?}>> `{outer_name}`: physical type not in OBJ-2c allow-list"
                ))),
            }
            let expected_max_def = (outer_optional as u32) + 1 + (inner_optional as u32) + 1 + (item_optional as u32);
            if leaf_max_def != expected_max_def {
                return Err(PqError::Bad(format!(
                    "List<List> `{outer_name}` leaf max_def_level={leaf_max_def} \
                     disagrees with expected {expected_max_def} \
                     (outer_opt={outer_optional}, inner_opt={inner_optional}, item_opt={item_optional})"
                )));
            }
            if leaf_max_rep != 2 {
                return Err(PqError::Bad(format!(
                    "List<List> `{outer_name}` leaf max_rep_level={leaf_max_rep} != 2"
                )));
            }
            let leaf_struct = leaves.iter().find(|l| l.name == leaf_name)
                .ok_or_else(|| PqError::Bad(format!(
                    "List<List> `{outer_name}` leaf `{leaf_name}` missing from flat leaves list"
                )))?;
            let spec = build_plain_spec(leaf_struct)?;
            let chunk_path: Vec<String> = leaf_path.iter().skip(1).cloned().collect();
            Ok(ColumnPlan {
                chunk_path,
                kind: ColumnKind::NestedListOfListPrimitive {
                    spec,
                    ptype: leaf_ptype,
                    max_def_level: leaf_max_def,
                    max_rep_level: leaf_max_rep,
                    outer_optional,
                    inner_optional,
                    item_optional,
                },
            })
        }
        // List<Map<K,V>> — SP146 T3 lifts.
        Some(meta::LogicalType::Map) => {
            // The element group is itself a MAP whose REPEATED middle has
            // 2 leaves: key + value. Drill through.
            if element_children.len() != 1 {
                return Err(PqError::Unsupported(format!(
                    "List<Map> `{outer_name}` inner-map outer children != 1: non-canonical"
                )));
            }
            let map_middle = match &element_children[0] {
                meta::SchemaNode::Group {
                    repetition: meta::Repetition::Repeated, children: gc, ..
                } => gc,
                _ => return Err(PqError::Unsupported(format!(
                    "List<Map> `{outer_name}` inner-map middle not REPEATED"
                ))),
            };
            if map_middle.len() != 2 {
                return Err(PqError::Unsupported(format!(
                    "List<Map> `{outer_name}` inner-map middle children != 2"
                )));
            }
            let (k_name, k_ptype, k_rep, k_max_def, k_max_rep, k_path) = match &map_middle[0] {
                meta::SchemaNode::Leaf {
                    name, ptype, repetition, max_def_level, max_rep_level, path,
                } => (name.clone(), *ptype, *repetition, *max_def_level, *max_rep_level, path.clone()),
                _ => return Err(PqError::Unsupported(format!(
                    "List<Map<group, _>> `{outer_name}`: non-primitive key, SP147 follow-up"
                ))),
            };
            let (v_name, v_ptype, v_rep, v_max_def, v_max_rep, v_path) = match &map_middle[1] {
                meta::SchemaNode::Leaf {
                    name, ptype, repetition, max_def_level, max_rep_level, path,
                } => (name.clone(), *ptype, *repetition, *max_def_level, *max_rep_level, path.clone()),
                _ => return Err(PqError::Unsupported(format!(
                    "List<Map<_, group>> `{outer_name}`: non-primitive value, SP147 follow-up"
                ))),
            };
            // Key must be REQUIRED.
            if !matches!(k_rep, meta::Repetition::Required) {
                return Err(PqError::Unsupported(format!(
                    "List<Map<_>> `{outer_name}` key `{k_name}` not REQUIRED"
                )));
            }
            let value_optional = matches!(v_rep, meta::Repetition::Optional);
            // Physical-type allow-list for both K and V.
            for (label, t) in [("key", k_ptype), ("value", v_ptype)] {
                match t {
                    meta::Type::Boolean | meta::Type::Int32 | meta::Type::Int64
                    | meta::Type::Float | meta::Type::Double | meta::Type::ByteArray
                    | meta::Type::Int96 | meta::Type::FixedLenByteArray => {}
                    other => return Err(PqError::Unsupported(format!(
                        "List<Map> `{outer_name}` {label} physical type {other:?}: OBJ-2c"
                    ))),
                }
            }
            // Expected max_def_levels:
            //   K: outer_optional + 1 (LIST REP) + 1 (MAP REP) = outer_optional + 2
            //   V: outer_optional + 1 + 1 + value_optional
            let expected_k_max_def = (outer_optional as u32) + 2;
            let expected_v_max_def = (outer_optional as u32) + 2 + (value_optional as u32);
            if k_max_def != expected_k_max_def {
                return Err(PqError::Bad(format!(
                    "List<Map> `{outer_name}` K max_def_level={k_max_def} \
                     disagrees with expected {expected_k_max_def}"
                )));
            }
            if v_max_def != expected_v_max_def {
                return Err(PqError::Bad(format!(
                    "List<Map> `{outer_name}` V max_def_level={v_max_def} \
                     disagrees with expected {expected_v_max_def}"
                )));
            }
            if k_max_rep != 2 || v_max_rep != 2 {
                return Err(PqError::Bad(format!(
                    "List<Map> `{outer_name}` K/V max_rep_level={k_max_rep}/{v_max_rep} != 2"
                )));
            }
            let k_struct = leaves.iter().find(|l| l.name == k_name)
                .ok_or_else(|| PqError::Bad(format!(
                    "List<Map> `{outer_name}` key leaf `{k_name}` missing from flat leaves"
                )))?;
            let v_struct = leaves.iter().find(|l| l.name == v_name)
                .ok_or_else(|| PqError::Bad(format!(
                    "List<Map> `{outer_name}` value leaf `{v_name}` missing from flat leaves"
                )))?;
            let k_spec = build_plain_spec(k_struct)?;
            let v_spec = build_plain_spec(v_struct)?;
            let k_chunk_path: Vec<String> = k_path.iter().skip(1).cloned().collect();
            let v_chunk_path: Vec<String> = v_path.iter().skip(1).cloned().collect();
            Ok(ColumnPlan {
                chunk_path: v_chunk_path.clone(),
                kind: ColumnKind::NestedListOfMap {
                    outer_optional,
                    key_spec: k_spec,
                    key_ptype: k_ptype,
                    key_chunk_path: k_chunk_path,
                    value_spec: v_spec,
                    value_ptype: v_ptype,
                    value_chunk_path: v_chunk_path,
                    max_def_level: v_max_def,
                    value_optional,
                },
            })
        }
        // List<struct<...>> — element is a struct (no LogicalType).
        None => {
            if element_children.is_empty() {
                return Err(PqError::Bad(format!(
                    "List<struct> `{outer_name}` element `{element_name}` has no fields"
                )));
            }
            // All struct fields must be primitive leaves (V1 — struct of
            // nested-shape inside a list is the 3rd-tier nesting case).
            let mut fields = Vec::with_capacity(element_children.len());
            for child in element_children {
                match child {
                    meta::SchemaNode::Leaf {
                        name: fname, repetition: frep, ptype, max_def_level, path, ..
                    } => {
                        let fopt = match frep {
                            meta::Repetition::Required => false,
                            meta::Repetition::Optional => true,
                            meta::Repetition::Repeated => return Err(PqError::Unsupported(format!(
                                "List<struct> `{outer_name}` field `{fname}` is REPEATED: SP146 follow-up"
                            ))),
                            meta::Repetition::Other(_) => return Err(PqError::Unsupported(format!(
                                "List<struct> `{outer_name}` field `{fname}` unknown repetition"
                            ))),
                        };
                        let _ = fopt; // OPT struct fields don't currently affect List-level def-classify
                        match ptype {
                            meta::Type::Boolean | meta::Type::Int32 | meta::Type::Int64
                            | meta::Type::Float | meta::Type::Double | meta::Type::ByteArray
                            | meta::Type::Int96 | meta::Type::FixedLenByteArray => {}
                            other => return Err(PqError::Unsupported(format!(
                                "List<struct> `{outer_name}` field `{fname}` physical type {other:?}: OBJ-2c"
                            ))),
                        };
                        let leaf_struct = leaves.iter().find(|l| &l.name == fname)
                            .ok_or_else(|| PqError::Bad(format!(
                                "List<struct> `{outer_name}` field leaf `{fname}` missing"
                            )))?;
                        let spec = build_plain_spec(leaf_struct)?;
                        let chunk_path: Vec<String> = path.iter().skip(1).cloned().collect();
                        fields.push(StructField {
                            name: fname.clone(),
                            chunk_path,
                            spec,
                            ptype: *ptype,
                            max_def_level: *max_def_level,
                            nested: None,
                        });
                    }
                    meta::SchemaNode::Group { name: gname, .. } => {
                        return Err(PqError::Unsupported(format!(
                            "List<struct<group>> `{outer_name}` field `{gname}`: \
                             SP146 follow-up (V1 supports List<struct<primitives>>)"
                        )));
                    }
                }
            }
            // List-of-struct max_def_level: outer_optional + 1 (REP outer)
            // + 0 (struct middle is the REPEATED itself / element is REQ).
            // Actually pyarrow's element group is REQ inside the REP list.
            let list_max_def_level = (outer_optional as u32) + 1;
            let primary_chunk_path = fields[0].chunk_path.clone();
            Ok(ColumnPlan {
                chunk_path: primary_chunk_path,
                kind: ColumnKind::NestedListOfStruct {
                    outer_optional,
                    list_max_def_level,
                    fields,
                },
            })
        }
    }
}

/// SP146: classify a `List<List<List<T>>>` shape. The inner-most LIST's
/// element must be a primitive leaf. Deeper (4+) nesting still rejects.
///
/// Inputs are the inner-element group (a 2nd-level LIST sitting inside
/// the outer-list's REPEATED middle), specifically its inner middle's
/// children — which must contain exactly 1 group (the 3rd LIST), whose
/// repeated middle contains exactly 1 leaf.
#[allow(clippy::too_many_arguments)]
fn classify_list_of_list_of_group(
    outer_name: &str,
    outer_optional: bool,
    middle_optional: bool,
    inner_element_name: &str,
    inner_element_rep: meta::Repetition,
    inner_element_children: &[meta::SchemaNode],
    inner_element_logical: Option<&meta::LogicalType>,
    leaves: &[meta::SchemaLeaf],
) -> Result<ColumnPlan, PqError> {
    let _ = inner_element_name;
    // For a 3-deep List<List<List<T>>>, inner_element must itself be a
    // LIST (LogicalType::List). Anything else is a different SP147 case.
    match inner_element_logical {
        Some(meta::LogicalType::List) => {}
        Some(meta::LogicalType::Map) => {
            return Err(PqError::Unsupported(format!(
                "List<List<Map<...>>> `{outer_name}`: SP147 follow-up"
            )));
        }
        None => {
            return Err(PqError::Unsupported(format!(
                "List<List<struct<...>>> `{outer_name}`: SP147 follow-up"
            )));
        }
    }
    let inner_optional = matches!(inner_element_rep, meta::Repetition::Optional);
    if inner_element_children.len() != 1 {
        return Err(PqError::Unsupported(format!(
            "List<List<List>> `{outer_name}` inner-list outer children != 1: non-canonical"
        )));
    }
    let innermost_middle = match &inner_element_children[0] {
        meta::SchemaNode::Group {
            repetition: meta::Repetition::Repeated, children: ic, ..
        } => ic,
        _ => return Err(PqError::Unsupported(format!(
            "List<List<List>> `{outer_name}` innermost-list middle not REPEATED"
        ))),
    };
    if innermost_middle.len() != 1 {
        return Err(PqError::Unsupported(format!(
            "List<List<List>> `{outer_name}` innermost-list middle children != 1"
        )));
    }
    let (leaf_name, leaf_ptype, leaf_rep, leaf_max_def, leaf_max_rep, leaf_path) =
        match &innermost_middle[0] {
            meta::SchemaNode::Leaf {
                name, ptype, repetition, max_def_level, max_rep_level, path,
            } => (name.clone(), *ptype, *repetition, *max_def_level, *max_rep_level, path.clone()),
            _ => return Err(PqError::Unsupported(format!(
                "List<List<List<group>>> `{outer_name}` — 4+ deep LIST nesting: SP147 follow-up"
            ))),
        };
    let item_optional = matches!(leaf_rep, meta::Repetition::Optional);
    match leaf_ptype {
        meta::Type::Boolean | meta::Type::Int32 | meta::Type::Int64
        | meta::Type::Float | meta::Type::Double | meta::Type::ByteArray
        | meta::Type::Int96 | meta::Type::FixedLenByteArray => {}
        t => return Err(PqError::Unsupported(format!(
            "List<List<List<{t:?}>>> `{outer_name}`: physical type not in OBJ-2c allow-list"
        ))),
    }
    let expected_max_def = (outer_optional as u32)
        + 1
        + (middle_optional as u32)
        + 1
        + (inner_optional as u32)
        + 1
        + (item_optional as u32);
    if leaf_max_def != expected_max_def {
        return Err(PqError::Bad(format!(
            "List<List<List>> `{outer_name}` leaf max_def_level={leaf_max_def} \
             disagrees with expected {expected_max_def} (outer_opt={outer_optional}, \
             middle_opt={middle_optional}, inner_opt={inner_optional}, item_opt={item_optional})"
        )));
    }
    if leaf_max_rep != 3 {
        return Err(PqError::Bad(format!(
            "List<List<List>> `{outer_name}` leaf max_rep_level={leaf_max_rep} != 3"
        )));
    }
    let leaf_struct = leaves.iter().find(|l| l.name == leaf_name)
        .ok_or_else(|| PqError::Bad(format!(
            "List<List<List>> `{outer_name}` leaf `{leaf_name}` missing from flat leaves list"
        )))?;
    let spec = build_plain_spec(leaf_struct)?;
    let chunk_path: Vec<String> = leaf_path.iter().skip(1).cloned().collect();
    Ok(ColumnPlan {
        chunk_path,
        kind: ColumnKind::NestedListOfListOfListPrimitive {
            spec,
            ptype: leaf_ptype,
            max_def_level: leaf_max_def,
            max_rep_level: leaf_max_rep,
            outer_optional,
            middle_optional,
            inner_optional,
            item_optional,
        },
    })
}

/// SP145 T5: classify a Map<_, group> shape — V is a Group (struct or
/// List), not a primitive leaf. Dispatches based on V's LogicalType.
#[allow(clippy::too_many_arguments)]
fn classify_map_of_group(
    map_name: &str,
    outer_optional: bool,
    key_name: &str,
    key_ptype: meta::Type,
    key_max_def: u32,
    key_path: &[String],
    value_name: &str,
    value_rep: meta::Repetition,
    value_children: &[meta::SchemaNode],
    value_logical: Option<&meta::LogicalType>,
    leaves: &[meta::SchemaLeaf],
) -> Result<ColumnPlan, PqError> {
    // K validation mirrors classify_map_plan: REQ + primitive + spec.
    let expected_k_max_def = (outer_optional as u32) + 1;
    if key_max_def != expected_k_max_def {
        return Err(PqError::Bad(format!(
            "MAP<_, group> `{map_name}` key max_def_level={key_max_def} disagrees with expected {expected_k_max_def}"
        )));
    }
    match key_ptype {
        meta::Type::Boolean | meta::Type::Int32 | meta::Type::Int64
        | meta::Type::Float | meta::Type::Double | meta::Type::ByteArray
        | meta::Type::Int96 | meta::Type::FixedLenByteArray => {}
        other => return Err(PqError::Unsupported(format!(
            "MAP `{map_name}` key physical type {other:?}: OBJ-2c"
        ))),
    }
    let key_leaf = leaves.iter().find(|l| l.name == key_name)
        .ok_or_else(|| PqError::Bad(format!(
            "MAP<_, group> `{map_name}` key leaf `{key_name}` missing"
        )))?;
    let key_spec = build_plain_spec(key_leaf)?;
    let key_chunk_path: Vec<String> = key_path.iter().skip(1).cloned().collect();

    let value_is_required = matches!(value_rep, meta::Repetition::Required);
    if !value_is_required {
        return Err(PqError::Unsupported(format!(
            "MAP `{map_name}` value group is not REQUIRED — Parquet spec quirk: \
             pyarrow always writes value groups as REQ inside the REP middle"
        )));
    }
    match value_logical {
        // Map<K, struct<...>> ──────────────────────────────────────────
        None => {
            if value_children.is_empty() {
                return Err(PqError::Bad(format!(
                    "MAP<_, struct> `{map_name}` value `{value_name}` has no fields"
                )));
            }
            let mut value_fields = Vec::with_capacity(value_children.len());
            for child in value_children {
                match child {
                    meta::SchemaNode::Leaf {
                        name: fname, repetition: frep, ptype, max_def_level, path, ..
                    } => {
                        if matches!(frep, meta::Repetition::Repeated) {
                            return Err(PqError::Unsupported(format!(
                                "MAP<_, struct> `{map_name}` value field `{fname}` REPEATED: \
                                 SP146 follow-up"
                            )));
                        }
                        match ptype {
                            meta::Type::Boolean | meta::Type::Int32 | meta::Type::Int64
                            | meta::Type::Float | meta::Type::Double | meta::Type::ByteArray
                            | meta::Type::Int96 | meta::Type::FixedLenByteArray => {}
                            other => return Err(PqError::Unsupported(format!(
                                "MAP<_, struct> `{map_name}` value field `{fname}` physical type {other:?}: OBJ-2c"
                            ))),
                        };
                        let leaf_struct = leaves.iter().find(|l| &l.name == fname)
                            .ok_or_else(|| PqError::Bad(format!(
                                "MAP<_, struct> `{map_name}` value field leaf `{fname}` missing"
                            )))?;
                        let spec = build_plain_spec(leaf_struct)?;
                        let chunk_path: Vec<String> = path.iter().skip(1).cloned().collect();
                        value_fields.push(StructField {
                            name: fname.clone(),
                            chunk_path,
                            spec,
                            ptype: *ptype,
                            max_def_level: *max_def_level,
                            nested: None,
                        });
                    }
                    meta::SchemaNode::Group { name: gname, .. } => {
                        return Err(PqError::Unsupported(format!(
                            "MAP<_, struct<group>> `{map_name}` value field `{gname}`: \
                             SP146 follow-up"
                        )));
                    }
                }
            }
            let map_max_def_level = (outer_optional as u32) + 1;
            let primary_chunk_path = value_fields[0].chunk_path.clone();
            Ok(ColumnPlan {
                chunk_path: primary_chunk_path,
                kind: ColumnKind::NestedMapOfStruct {
                    outer_optional,
                    key_spec,
                    key_ptype,
                    key_chunk_path,
                    map_max_def_level,
                    value_fields,
                },
            })
        }
        // Map<K, List<T>> ─────────────────────────────────────────────
        Some(meta::LogicalType::List) => {
            // Drill through the inner List shape: V is the LIST outer
            // group, value_children[0] is REPEATED middle, that middle's
            // child is the leaf.
            if value_children.len() != 1 {
                return Err(PqError::Unsupported(format!(
                    "MAP<_, List> `{map_name}` value-list outer children != 1"
                )));
            }
            let v_middle = match &value_children[0] {
                meta::SchemaNode::Group {
                    repetition: meta::Repetition::Repeated, children: ic, ..
                } => ic,
                _ => return Err(PqError::Unsupported(format!(
                    "MAP<_, List> `{map_name}` value-list middle not REPEATED"
                ))),
            };
            if v_middle.len() != 1 {
                return Err(PqError::Unsupported(format!(
                    "MAP<_, List> `{map_name}` value-list middle children != 1"
                )));
            }
            let (v_leaf_name, v_leaf_ptype, v_leaf_rep, v_leaf_max_def, v_leaf_max_rep, v_leaf_path) =
                match &v_middle[0] {
                    meta::SchemaNode::Leaf {
                        name, ptype, repetition, max_def_level, max_rep_level, path,
                    } => (name.clone(), *ptype, *repetition, *max_def_level, *max_rep_level, path.clone()),
                    _ => return Err(PqError::Unsupported(format!(
                        "MAP<_, List<group>> `{map_name}`: SP146 follow-up"
                    ))),
                };
            let value_item_optional = matches!(v_leaf_rep, meta::Repetition::Optional);
            match v_leaf_ptype {
                meta::Type::Boolean | meta::Type::Int32 | meta::Type::Int64
                | meta::Type::Float | meta::Type::Double | meta::Type::ByteArray
                | meta::Type::Int96 | meta::Type::FixedLenByteArray => {}
                t => return Err(PqError::Unsupported(format!(
                    "MAP<_, List<{t:?}>> `{map_name}`: physical type not in OBJ-2c"
                ))),
            }
            // V leaf's max_def: outer_optional + 1 (MAP REP) + 1 (LIST REP) + item_opt
            // (the value-list outer group is REQ here since pyarrow writes it so).
            let expected_v_max_def = (outer_optional as u32) + 1 + 1 + (value_item_optional as u32);
            if v_leaf_max_def != expected_v_max_def {
                return Err(PqError::Bad(format!(
                    "MAP<_, List> `{map_name}` V leaf max_def_level={v_leaf_max_def} \
                     disagrees with expected {expected_v_max_def}"
                )));
            }
            if v_leaf_max_rep != 2 {
                return Err(PqError::Bad(format!(
                    "MAP<_, List> `{map_name}` V leaf max_rep_level={v_leaf_max_rep} != 2"
                )));
            }
            let v_leaf_struct = leaves.iter().find(|l| l.name == v_leaf_name)
                .ok_or_else(|| PqError::Bad(format!(
                    "MAP<_, List> `{map_name}` V leaf `{v_leaf_name}` missing"
                )))?;
            let v_spec = build_plain_spec(v_leaf_struct)?;
            let v_chunk_path: Vec<String> = v_leaf_path.iter().skip(1).cloned().collect();
            Ok(ColumnPlan {
                chunk_path: v_chunk_path.clone(),
                kind: ColumnKind::NestedMapOfList {
                    outer_optional,
                    key_spec,
                    key_ptype,
                    key_chunk_path,
                    value_spec: v_spec,
                    value_ptype: v_leaf_ptype,
                    value_chunk_path: v_chunk_path,
                    max_def_level: v_leaf_max_def,
                    value_item_optional,
                },
            })
        }
        // Map<K, Map<...>> — SP146 T4 lifts.
        Some(meta::LogicalType::Map) => {
            // Value is the inner MAP outer group. Its child[0] is the
            // inner REPEATED middle (key_value), which has 2 leaves.
            if value_children.len() != 1 {
                return Err(PqError::Unsupported(format!(
                    "MAP<_, Map> `{map_name}` inner-map outer children != 1: non-canonical"
                )));
            }
            let inner_middle = match &value_children[0] {
                meta::SchemaNode::Group {
                    repetition: meta::Repetition::Repeated, children: gc, ..
                } => gc,
                _ => return Err(PqError::Unsupported(format!(
                    "MAP<_, Map> `{map_name}` inner-map middle not REPEATED"
                ))),
            };
            if inner_middle.len() != 2 {
                return Err(PqError::Unsupported(format!(
                    "MAP<_, Map> `{map_name}` inner-map middle children != 2"
                )));
            }
            let (ik_name, ik_ptype, ik_rep, ik_max_def, ik_max_rep, ik_path) = match &inner_middle[0] {
                meta::SchemaNode::Leaf {
                    name, ptype, repetition, max_def_level, max_rep_level, path,
                } => (name.clone(), *ptype, *repetition, *max_def_level, *max_rep_level, path.clone()),
                _ => return Err(PqError::Unsupported(format!(
                    "MAP<_, Map<group, _>> `{map_name}`: non-primitive inner key, SP147 follow-up"
                ))),
            };
            let (iv_name, iv_ptype, iv_rep, iv_max_def, iv_max_rep, iv_path) = match &inner_middle[1] {
                meta::SchemaNode::Leaf {
                    name, ptype, repetition, max_def_level, max_rep_level, path,
                } => (name.clone(), *ptype, *repetition, *max_def_level, *max_rep_level, path.clone()),
                _ => return Err(PqError::Unsupported(format!(
                    "MAP<_, Map<_, group>> `{map_name}`: non-primitive inner value, SP147 follow-up"
                ))),
            };
            if !matches!(ik_rep, meta::Repetition::Required) {
                return Err(PqError::Unsupported(format!(
                    "MAP<_, Map<_>> `{map_name}` inner key `{ik_name}` not REQUIRED"
                )));
            }
            let inner_value_optional = matches!(iv_rep, meta::Repetition::Optional);
            for (label, t) in [("inner key", ik_ptype), ("inner value", iv_ptype)] {
                match t {
                    meta::Type::Boolean | meta::Type::Int32 | meta::Type::Int64
                    | meta::Type::Float | meta::Type::Double | meta::Type::ByteArray
                    | meta::Type::Int96 | meta::Type::FixedLenByteArray => {}
                    other => return Err(PqError::Unsupported(format!(
                        "MAP<_, Map<>> `{map_name}` {label} physical type {other:?}: OBJ-2c"
                    ))),
                }
            }
            // Expected max_def_levels:
            //   inner K: outer_optional + 1 (outer MAP REP) + 1 (inner MAP REP) = outer_optional + 2
            //   inner V: outer_optional + 1 + 1 + inner_value_optional
            let expected_ik_max_def = (outer_optional as u32) + 2;
            let expected_iv_max_def = (outer_optional as u32) + 2 + (inner_value_optional as u32);
            if ik_max_def != expected_ik_max_def {
                return Err(PqError::Bad(format!(
                    "MAP<_, Map> `{map_name}` inner K max_def_level={ik_max_def} \
                     disagrees with expected {expected_ik_max_def}"
                )));
            }
            if iv_max_def != expected_iv_max_def {
                return Err(PqError::Bad(format!(
                    "MAP<_, Map> `{map_name}` inner V max_def_level={iv_max_def} \
                     disagrees with expected {expected_iv_max_def}"
                )));
            }
            if ik_max_rep != 2 || iv_max_rep != 2 {
                return Err(PqError::Bad(format!(
                    "MAP<_, Map> `{map_name}` inner K/V max_rep_level={ik_max_rep}/{iv_max_rep} != 2"
                )));
            }
            let ik_struct = leaves.iter().find(|l| l.name == ik_name)
                .ok_or_else(|| PqError::Bad(format!(
                    "MAP<_, Map> `{map_name}` inner key leaf `{ik_name}` missing from flat leaves"
                )))?;
            let iv_struct = leaves.iter().find(|l| l.name == iv_name)
                .ok_or_else(|| PqError::Bad(format!(
                    "MAP<_, Map> `{map_name}` inner value leaf `{iv_name}` missing from flat leaves"
                )))?;
            let ik_spec = build_plain_spec(ik_struct)?;
            let iv_spec = build_plain_spec(iv_struct)?;
            let ik_chunk_path: Vec<String> = ik_path.iter().skip(1).cloned().collect();
            let iv_chunk_path: Vec<String> = iv_path.iter().skip(1).cloned().collect();
            Ok(ColumnPlan {
                chunk_path: iv_chunk_path.clone(),
                kind: ColumnKind::NestedMapOfMap {
                    outer_optional,
                    outer_key_spec: key_spec,
                    outer_key_ptype: key_ptype,
                    outer_key_chunk_path: key_chunk_path,
                    inner_key_spec: ik_spec,
                    inner_key_ptype: ik_ptype,
                    inner_key_chunk_path: ik_chunk_path,
                    inner_value_spec: iv_spec,
                    inner_value_ptype: iv_ptype,
                    inner_value_chunk_path: iv_chunk_path,
                    max_def_level: iv_max_def,
                    inner_value_optional,
                },
            })
        }
    }
}

/// SP145 T5: classify a nested-group child INSIDE a struct field. The
/// recursive entry point — given a SchemaNode (a Group child of a
/// struct), classify it as one of the new SP145 ColumnKind variants
/// (or an existing one if it happens to be a flat-ish shape).
///
/// Used by `classify_struct_plan` when it encounters a nested group
/// field. The returned `ColumnKind` is wrapped in `StructField.nested`.
fn classify_nested_group_child(
    node: &meta::SchemaNode,
    leaves: &[meta::SchemaLeaf],
) -> Result<ColumnKind, PqError> {
    let (gname, grep, gchildren, glogical) = match node {
        meta::SchemaNode::Group { name, repetition, children, logical_type } =>
            (name, repetition, children, logical_type),
        _ => return Err(PqError::Bad(
            "classify_nested_group_child: expected Group node".into(),
        )),
    };
    let g_optional = matches!(grep, meta::Repetition::Optional);
    match glogical {
        Some(meta::LogicalType::List) => {
            // Recursively classify the LIST. Reuse the LIST<primitive>
            // and List<group> code paths by synthesizing the inputs.
            if gchildren.len() != 1 {
                return Err(PqError::Unsupported(format!(
                    "struct<List> `{gname}` outer children != 1: SP146"
                )));
            }
            let middle_children = match &gchildren[0] {
                meta::SchemaNode::Group {
                    repetition: meta::Repetition::Repeated, children: gc, ..
                } => gc,
                _ => return Err(PqError::Unsupported(format!(
                    "struct<List> `{gname}` middle not REPEATED"
                ))),
            };
            if middle_children.len() != 1 {
                return Err(PqError::Unsupported(format!(
                    "struct<List> `{gname}` middle children != 1"
                )));
            }
            // If middle child is a Leaf, this is List<primitive>; if a
            // Group, this is List<List/struct>.
            match &middle_children[0] {
                meta::SchemaNode::Leaf {
                    name: lname, ptype, repetition: lrep, max_def_level, max_rep_level, path,
                } => {
                    let item_optional = matches!(lrep, meta::Repetition::Optional);
                    match ptype {
                        meta::Type::Boolean | meta::Type::Int32 | meta::Type::Int64
                        | meta::Type::Float | meta::Type::Double | meta::Type::ByteArray
                        | meta::Type::Int96 | meta::Type::FixedLenByteArray => {}
                        t => return Err(PqError::Unsupported(format!(
                            "struct<List<{t:?}>> `{gname}`: physical type not in OBJ-2c"
                        ))),
                    }
                    // Note: max_def_level here is the LEAF's full path level,
                    // including all ancestors up to the schema root. The
                    // List<primitive> assembler expects the LIST-level math.
                    let expected_max_def_relative = (g_optional as u32) + 1 + (item_optional as u32);
                    let _ = expected_max_def_relative; // documentation only
                    let leaf_struct = leaves.iter().find(|l| l.name == *lname)
                        .ok_or_else(|| PqError::Bad(format!(
                            "struct<List> `{gname}` leaf `{lname}` missing"
                        )))?;
                    let spec = build_plain_spec(leaf_struct)?;
                    let _chunk_path: Vec<String> = path.iter().skip(1).cloned().collect();
                    Ok(ColumnKind::NestedListPrimitive {
                        spec,
                        ptype: *ptype,
                        max_def_level: *max_def_level,
                        max_rep_level: *max_rep_level,
                        outer_optional: g_optional,
                        element_optional: item_optional,
                    })
                }
                meta::SchemaNode::Group { name: en, repetition: er, children: ec, logical_type: el } => {
                    let plan = classify_list_of_group(
                        gname, g_optional, en, *er, ec, el.as_ref(), leaves,
                    )?;
                    Ok(plan.kind)
                }
            }
        }
        Some(meta::LogicalType::Map) => {
            // Reuse classify_map_plan via a recursive call to
            // classify_column_plan-like logic. We instead reach into
            // classify_map_plan directly with the children list.
            let plan = classify_map_plan(gname, *grep, gchildren, leaves)?;
            Ok(plan.kind)
        }
        None => {
            // struct — reuse classify_struct_plan.
            let plan = classify_struct_plan(gname, *grep, gchildren, leaves)?;
            Ok(plan.kind)
        }
    }
}

/// SP145 T5: helper — find the first reachable Leaf under a node and
/// return its chunk_path (root-stripped). Used as a placeholder
/// chunk_path for struct fields that are themselves nested shapes
/// (the nested decoder uses its own per-leaf paths, but the outer
/// StructField struct requires a chunk_path slot).
fn first_leaf_chunk_path(
    node: &meta::SchemaNode,
) -> Result<Vec<String>, PqError> {
    match node {
        meta::SchemaNode::Leaf { path, .. } =>
            Ok(path.iter().skip(1).cloned().collect()),
        meta::SchemaNode::Group { children, .. } => {
            for c in children {
                if let Ok(p) = first_leaf_chunk_path(c) {
                    return Ok(p);
                }
            }
            Err(PqError::Bad("first_leaf_chunk_path: group with no leaves".into()))
        }
    }
}

/// SP143 T6: nested-schema extractor. Routes each wanted column either
/// through the existing flat decode (when its plan classifies as Flat)
/// or through the new nested decode + assembler (when LIST<primitive>).
/// SP144 T5 extends the dispatch to MAP and struct columns. Mirrors
/// `extract`'s row-major transpose so the caller's contract
/// (Vec<row> of Vec<PqValue>) is unchanged.
fn extract_nested(
    file: &[u8],
    md: &meta::FileMetaData,
    wanted: &[&str],
) -> Result<Vec<Vec<PqValue>>, PqError> {
    let mut plans: Vec<ColumnPlan> = Vec::with_capacity(wanted.len());
    for w in wanted {
        plans.push(classify_column_plan(&md.schema_tree.root, w, &md.leaves)?);
    }

    let mut cols: Vec<Vec<PqValue>> = wanted.iter().map(|_| Vec::new()).collect();
    for rg in &md.row_groups {
        for (ci, plan) in plans.iter().enumerate() {
            // Flat + List use the plan's `chunk_path` to find their
            // single chunk up front; Map + Struct find their per-leaf
            // chunks inside their decode helpers (multi-leaf columns).
            match &plan.kind {
                ColumnKind::Flat { spec, ptype, max_def_level } => {
                    let cc = rg
                        .columns
                        .iter()
                        .find(|c| c.path == plan.chunk_path)
                        .ok_or_else(|| {
                            PqError::Bad(format!(
                                "row group missing column for path {:?}",
                                plan.chunk_path
                            ))
                        })?;
                    if cc.ptype != *ptype {
                        return Err(PqError::Bad(format!(
                            "column for path {:?} schema/column-chunk physical-type mismatch",
                            plan.chunk_path
                        )));
                    }
                    let vals = read_chunk_values(file, cc, *spec, *max_def_level)?;
                    cols[ci].extend(vals);
                }
                ColumnKind::NestedListPrimitive {
                    spec,
                    ptype,
                    max_def_level,
                    max_rep_level,
                    outer_optional,
                    element_optional,
                } => {
                    let cc = rg
                        .columns
                        .iter()
                        .find(|c| c.path == plan.chunk_path)
                        .ok_or_else(|| {
                            PqError::Bad(format!(
                                "row group missing column for path {:?}",
                                plan.chunk_path
                            ))
                        })?;
                    if cc.ptype != *ptype {
                        return Err(PqError::Bad(format!(
                            "column for path {:?} schema/column-chunk physical-type mismatch",
                            plan.chunk_path
                        )));
                    }
                    let vals = read_chunk_values_nested(
                        file,
                        cc,
                        *spec,
                        *max_def_level,
                        *max_rep_level,
                        *outer_optional,
                        *element_optional,
                    )?;
                    cols[ci].extend(vals);
                }
                ColumnKind::NestedMapKV {
                    key_spec,
                    value_spec,
                    key_ptype,
                    value_ptype,
                    key_chunk_path,
                    value_chunk_path,
                    max_def_level,
                    outer_optional,
                    value_optional,
                } => {
                    let vals = read_chunk_values_nested_map(
                        file,
                        rg,
                        key_chunk_path,
                        value_chunk_path,
                        *key_spec,
                        *value_spec,
                        *key_ptype,
                        *value_ptype,
                        *max_def_level,
                        *outer_optional,
                        *value_optional,
                    )?;
                    cols[ci].extend(vals);
                }
                ColumnKind::NestedStruct { outer_optional, fields } => {
                    let vals = read_chunk_values_nested_struct(
                        file,
                        rg,
                        fields,
                        *outer_optional,
                    )?;
                    cols[ci].extend(vals);
                }
                ColumnKind::NestedListOfListPrimitive {
                    spec, ptype, max_def_level, max_rep_level,
                    outer_optional, inner_optional, item_optional,
                } => {
                    let cc = rg.columns.iter().find(|c| c.path == plan.chunk_path)
                        .ok_or_else(|| PqError::Bad(format!(
                            "List<List> row group missing column for path {:?}", plan.chunk_path
                        )))?;
                    if cc.ptype != *ptype {
                        return Err(PqError::Bad(format!(
                            "List<List> column path {:?} ptype mismatch", plan.chunk_path
                        )));
                    }
                    let (rep, def, vals) = read_chunk_levels_and_values(
                        file, cc, *spec, *max_def_level, *max_rep_level,
                    )?;
                    let assembled = assembly::assemble_list_of_list_primitive(
                        &rep, &def, &vals, *max_def_level,
                        *outer_optional, *inner_optional, *item_optional,
                    )?;
                    cols[ci].extend(assembled);
                }
                ColumnKind::NestedListOfStruct {
                    outer_optional, list_max_def_level, fields,
                } => {
                    let vals = read_chunk_values_nested_list_of_struct(
                        file, rg, fields, *outer_optional, *list_max_def_level,
                    )?;
                    cols[ci].extend(vals);
                }
                ColumnKind::NestedMapOfStruct {
                    outer_optional, key_spec, key_ptype, key_chunk_path,
                    map_max_def_level, value_fields,
                } => {
                    let vals = read_chunk_values_nested_map_of_struct(
                        file, rg, key_chunk_path, *key_spec, *key_ptype,
                        value_fields, *outer_optional, *map_max_def_level,
                    )?;
                    cols[ci].extend(vals);
                }
                ColumnKind::NestedMapOfList {
                    outer_optional, key_spec, key_ptype, key_chunk_path,
                    value_spec, value_ptype, value_chunk_path,
                    max_def_level, value_item_optional,
                } => {
                    let vals = read_chunk_values_nested_map_of_list(
                        file, rg, key_chunk_path, value_chunk_path,
                        *key_spec, *value_spec, *key_ptype, *value_ptype,
                        *max_def_level, *outer_optional, *value_item_optional,
                    )?;
                    cols[ci].extend(vals);
                }
                ColumnKind::NestedListOfListOfListPrimitive {
                    spec, ptype, max_def_level, max_rep_level,
                    outer_optional, middle_optional, inner_optional, item_optional,
                } => {
                    let cc = rg.columns.iter().find(|c| c.path == plan.chunk_path)
                        .ok_or_else(|| PqError::Bad(format!(
                            "List<List<List>> row group missing column for path {:?}", plan.chunk_path
                        )))?;
                    if cc.ptype != *ptype {
                        return Err(PqError::Bad(format!(
                            "List<List<List>> column path {:?} ptype mismatch", plan.chunk_path
                        )));
                    }
                    let (rep, def, vals) = read_chunk_levels_and_values(
                        file, cc, *spec, *max_def_level, *max_rep_level,
                    )?;
                    let assembled = assembly::assemble_list_of_list_of_list_primitive(
                        &rep, &def, &vals, *max_def_level,
                        *outer_optional, *middle_optional, *inner_optional, *item_optional,
                    )?;
                    cols[ci].extend(assembled);
                }
                ColumnKind::NestedListOfMap {
                    outer_optional, key_spec, key_ptype, key_chunk_path,
                    value_spec, value_ptype, value_chunk_path,
                    max_def_level, value_optional,
                } => {
                    let vals = read_chunk_values_nested_list_of_map(
                        file, rg, key_chunk_path, value_chunk_path,
                        *key_spec, *value_spec, *key_ptype, *value_ptype,
                        *max_def_level, *outer_optional, *value_optional,
                    )?;
                    cols[ci].extend(vals);
                }
                ColumnKind::NestedMapOfMap {
                    outer_optional,
                    outer_key_spec, outer_key_ptype, outer_key_chunk_path,
                    inner_key_spec, inner_key_ptype, inner_key_chunk_path,
                    inner_value_spec, inner_value_ptype, inner_value_chunk_path,
                    max_def_level, inner_value_optional,
                } => {
                    let vals = read_chunk_values_nested_map_of_map(
                        file, rg,
                        outer_key_chunk_path, inner_key_chunk_path, inner_value_chunk_path,
                        *outer_key_spec, *inner_key_spec, *inner_value_spec,
                        *outer_key_ptype, *inner_key_ptype, *inner_value_ptype,
                        *max_def_level, *outer_optional, *inner_value_optional,
                    )?;
                    cols[ci].extend(vals);
                }
            } // end match plan.kind
        }
    }

    let nrows = cols.first().map(|c| c.len()).unwrap_or(0);
    if cols.iter().any(|c| c.len() != nrows) {
        return Err(PqError::Bad("column length mismatch".into()));
    }
    let mut rows = Vec::with_capacity(nrows);
    for r in 0..nrows {
        rows.push(cols.iter().map(|c| c[r].clone()).collect());
    }
    Ok(rows)
}

/// SP143 T6: nested sibling of `read_chunk_values`. Iterates the
/// column chunk's pages (V1 / V2 / dictionary), accumulates the
/// (rep_levels, def_levels, values) parallel streams across all data
/// pages, then folds them ONCE through `assembly::assemble_list_primitive`
/// at the end. Dictionary-page / codec / encoding handling mirrors
/// `read_chunk_values` byte-for-byte; only the per-page decode call
/// differs (decode_page_v1_nested / decode_data_page_v2_nested).
///
/// `cc.num_values` for a nested column counts (rep, def) PAIRS, not
/// records — so the page-loop terminates when accumulated rep/def
/// pairs reach `cc.num_values`, not when output records reach it.
///
/// SP144 T5: the page-loop body now lives in
/// `read_chunk_levels_and_values` so the Map decode path can reuse the
/// exact same dictionary / codec / encoding / V1+V2 handling. This
/// function is now a thin wrapper that fetches the three streams then
/// folds them through `assemble_list_primitive`.
fn read_chunk_values_nested(
    file: &[u8],
    cc: &meta::ColumnChunk,
    spec: plain::PlainSpec,
    max_def_level: u32,
    max_rep_level: u32,
    outer_optional: bool,
    element_optional: bool,
) -> Result<Vec<PqValue>, PqError> {
    let (all_rep, all_def, all_values) =
        read_chunk_levels_and_values(file, cc, spec, max_def_level, max_rep_level)?;
    assembly::assemble_list_primitive(
        &all_rep,
        &all_def,
        &all_values,
        max_def_level,
        outer_optional,
        element_optional,
    )
}

/// SP144 T5: shared page-loop helper. Walks a nested column chunk's
/// data pages (V1 + V2 + dictionary handling identical to the
/// pre-T5 `read_chunk_values_nested`) and returns the accumulated
/// `(rep_levels, def_levels, values)` triple BEFORE any assembly
/// pass. List, Map, and (future) deep-nesting paths share this
/// helper; the per-shape assembler is the only thing that differs.
///
/// `cc.num_values` counts (rep, def) PAIRS — the loop terminates when
/// accumulated pairs reach `want_pairs`, with a strict-exact post-check.
fn read_chunk_levels_and_values(
    file: &[u8],
    cc: &meta::ColumnChunk,
    spec: plain::PlainSpec,
    max_def_level: u32,
    max_rep_level: u32,
) -> Result<(Vec<u32>, Vec<u32>, Vec<PqValue>), PqError> {
    match cc.codec {
        meta::Codec::Uncompressed
        | meta::Codec::Snappy
        | meta::Codec::Gzip
        | meta::Codec::Zstd
        | meta::Codec::Lz4Raw => {}
        meta::Codec::Brotli => {
            return Err(PqError::Unsupported(
                "Brotli decode: zero-dep decoder is a dedicated multi-week SP-arc \
                 (~10-15 tasks like SP125-SP140 zstd); workaround — ask the writer to use \
                 compression='zstd' or compression='lz4' instead".into(),
            ))
        }
        meta::Codec::Other(5) => {
            return Err(PqError::Unsupported(
                "LZ4 (deprecated Hadoop framing) — use LZ4_RAW; SP149 follow-up if needed".into(),
            ))
        }
        meta::Codec::Other(_) => {
            return Err(PqError::Unsupported(
                "compression codec: OBJ-2c".into(),
            ))
        }
    }
    if cc.encodings.iter().any(|e| {
        !matches!(
            e,
            meta::Encoding::Plain
                | meta::Encoding::Rle
                | meta::Encoding::PlainDictionary
                | meta::Encoding::RleDictionary
        )
    }) {
        return Err(PqError::Unsupported(
            "non-PLAIN/dictionary encoding (DELTA/BYTE_STREAM_SPLIT): OBJ-2c".into(),
        ));
    }

    let dict: Vec<PqValue> = if let Some(dpo) = cc.dictionary_page_offset {
        let off = usize::try_from(dpo)
            .map_err(|_| PqError::Bad("dict page offset range".into()))?;
        let region = file
            .get(off..)
            .ok_or_else(|| PqError::Bad("dict page offset past EOF".into()))?;
        let (ph, hlen) = meta::decode_page_header(region)?;
        if ph.page_type != 2 {
            return Err(PqError::Bad(
                "dictionary_page_offset does not point at a DICTIONARY_PAGE".into(),
            ));
        }
        if ph.dict_encoding != 0 && ph.dict_encoding != 2 {
            return Err(PqError::Unsupported(
                "dictionary page encoding (not PLAIN/PLAIN_DICTIONARY): OBJ-2c".into(),
            ));
        }
        let dn = usize::try_from(ph.dict_num_values)
            .map_err(|_| PqError::Bad("dict num_values range".into()))?;
        let dstart = off
            .checked_add(hlen)
            .ok_or_else(|| PqError::Bad("dict page hdr len ovf".into()))?;
        let comp = usize::try_from(ph.compressed_size)
            .map_err(|_| PqError::Bad("dict page comp size range".into()))?;
        let uncomp = usize::try_from(ph.uncompressed_size)
            .map_err(|_| PqError::Bad("dict page size range".into()))?;
        // SP151 cap-check (nested dict page).
        check_page_size("nested dict page compressed", comp)?;
        check_page_size("nested dict page uncompressed", uncomp)?;
        let payload = page_payload(file, dstart, comp, uncomp, cc.codec)?;
        plain::decode_plain(&payload, spec, dn)?
    } else {
        Vec::new()
    };

    let want_pairs = usize::try_from(cc.num_values)
        .map_err(|_| PqError::Bad("chunk num_values range".into()))?;
    let mut all_rep: Vec<u32> = Vec::with_capacity(want_pairs);
    let mut all_def: Vec<u32> = Vec::with_capacity(want_pairs);
    let mut all_values: Vec<PqValue> = Vec::new();
    let mut off = usize::try_from(cc.data_page_offset)
        .map_err(|_| PqError::Bad("data page offset range".into()))?;
    while all_rep.len() < want_pairs {
        let region = file
            .get(off..)
            .ok_or_else(|| PqError::Bad("data page offset past EOF".into()))?;
        let (ph, hlen) = meta::decode_page_header(region)?;
        let (rep, def, vals, dstart, comp) = match ph.page_type {
            0 => {
                let n = usize::try_from(ph.dp_num_values)
                    .map_err(|_| PqError::Bad("num_values range".into()))?;
                let dstart = off
                    .checked_add(hlen)
                    .ok_or_else(|| PqError::Bad("page hdr len ovf".into()))?;
                let comp = usize::try_from(ph.compressed_size)
                    .map_err(|_| PqError::Bad("page comp size range".into()))?;
                let uncomp = usize::try_from(ph.uncompressed_size)
                    .map_err(|_| PqError::Bad("page size range".into()))?;
                // SP151 cap-check (nested V1 data page).
                check_page_size("nested v1 page compressed", comp)?;
                check_page_size("nested v1 page uncompressed", uncomp)?;
                let payload = page_payload(file, dstart, comp, uncomp, cc.codec)?;
                if matches!(ph.dp_encoding, 2 | 8)
                    && cc.dictionary_page_offset.is_none()
                {
                    return Err(PqError::Bad(
                        "dictionary-encoded data page without dictionary_page_offset".into(),
                    ));
                }
                let (r, d, v) = decode_page_v1_nested(
                    &payload,
                    ph.dp_encoding,
                    spec,
                    n,
                    max_rep_level,
                    max_def_level,
                    &dict,
                )?;
                (r, d, v, dstart, comp)
            }
            3 => {
                let dstart = off
                    .checked_add(hlen)
                    .ok_or_else(|| PqError::Bad("v2 page hdr len ovf".into()))?;
                let comp = usize::try_from(ph.compressed_size)
                    .map_err(|_| PqError::Bad("v2 comp size range".into()))?;
                // SP151 cap-check (nested V2 data page compressed).
                check_page_size("nested v2 page compressed", comp)?;
                let v2_region = file
                    .get(
                        dstart
                            ..dstart.checked_add(comp).ok_or_else(|| {
                                PqError::Bad("v2 region ovf".into())
                            })?,
                    )
                    .ok_or_else(|| PqError::Bad("v2 page truncated".into()))?;
                let n = usize::try_from(ph.v2_num_values)
                    .map_err(|_| PqError::Bad("v2 num_values range".into()))?;
                let rep_byte_len = u32::try_from(ph.v2_rep_len)
                    .map_err(|_| PqError::Bad("v2 rep_len range".into()))?;
                let def_byte_len = u32::try_from(ph.v2_def_len)
                    .map_err(|_| PqError::Bad("v2 def_len range".into()))?;
                let uncomp = u32::try_from(ph.uncompressed_size)
                    .map_err(|_| PqError::Bad("v2 uncompressed size range".into()))?;
                // SP151 cap-check (nested V2 data page uncompressed).
                check_page_size("nested v2 page uncompressed", uncomp as usize)?;
                let (r, d, v) = decode_data_page_v2_nested(
                    v2_region,
                    ph.v2_encoding,
                    spec,
                    n,
                    max_rep_level,
                    max_def_level,
                    rep_byte_len,
                    def_byte_len,
                    ph.v2_is_compressed,
                    cc.codec,
                    uncomp,
                    &dict,
                )?;
                (r, d, v, dstart, comp)
            }
            _ => {
                return Err(PqError::Unsupported(
                    "non-V1/V2 data page (index): OBJ-2c".into(),
                ))
            }
        };
        if all_rep.len().checked_add(rep.len()).map(|t| t > want_pairs).unwrap_or(true) {
            return Err(PqError::Bad(
                "data page (rep,def) pairs exceed chunk num_values".into(),
            ));
        }
        all_rep.extend(rep);
        all_def.extend(def);
        all_values.extend(vals);
        off = dstart
            .checked_add(comp)
            .ok_or_else(|| PqError::Bad("page advance ovf".into()))?;
    }
    if all_rep.len() != want_pairs {
        return Err(PqError::Bad(
            "data page (rep,def) pairs do not sum to chunk num_values".into(),
        ));
    }
    Ok((all_rep, all_def, all_values))
}

/// SP144 T5: decode a canonical 3-node MAP<K, V> column chunk into a
/// stream of `PqValue::Map` records. Pulls the key and value column
/// chunks separately (each through the shared
/// `read_chunk_levels_and_values` page-loop), validates that the rep
/// streams agree (they must — both leaves share the same REPEATED
/// middle ancestor), then folds through `assembly::assemble_map_kv`
/// using V's def stream + K's values + V's values.
///
/// K's max_def_level differs from V's by exactly `value_optional` (K is
/// always REQUIRED per the Parquet spec). Both K and V share the same
/// max_rep_level = 1 (the REPEATED middle group is the single
/// repeating ancestor).
fn read_chunk_values_nested_map(
    file: &[u8],
    rg: &meta::RowGroup,
    key_chunk_path: &[String],
    value_chunk_path: &[String],
    key_spec: plain::PlainSpec,
    value_spec: plain::PlainSpec,
    key_ptype: meta::Type,
    value_ptype: meta::Type,
    max_def_level: u32,
    outer_optional: bool,
    value_optional: bool,
) -> Result<Vec<PqValue>, PqError> {
    // Find both column chunks in this row group by their leaf paths.
    let key_cc = rg
        .columns
        .iter()
        .find(|c| c.path == key_chunk_path)
        .ok_or_else(|| {
            PqError::Bad(format!(
                "MAP row group missing key column for path {:?}",
                key_chunk_path
            ))
        })?;
    let value_cc = rg
        .columns
        .iter()
        .find(|c| c.path == value_chunk_path)
        .ok_or_else(|| {
            PqError::Bad(format!(
                "MAP row group missing value column for path {:?}",
                value_chunk_path
            ))
        })?;
    if key_cc.ptype != key_ptype {
        return Err(PqError::Bad(format!(
            "MAP key column for path {:?} schema/column-chunk physical-type mismatch",
            key_chunk_path
        )));
    }
    if value_cc.ptype != value_ptype {
        return Err(PqError::Bad(format!(
            "MAP value column for path {:?} schema/column-chunk physical-type mismatch",
            value_chunk_path
        )));
    }
    // K's max_def = V_max_def - value_optional (V's OPTIONAL contribution).
    let key_max_def = max_def_level - (value_optional as u32);
    let (k_rep, _k_def, k_vals) = read_chunk_levels_and_values(
        file, key_cc, key_spec, key_max_def, 1,
    )?;
    let (v_rep, v_def, v_vals) = read_chunk_levels_and_values(
        file, value_cc, value_spec, max_def_level, 1,
    )?;
    // Both leaves share the REPEATED middle ancestor, so their rep
    // streams must be byte-identical. If they aren't, the file is
    // malformed (or our classifier missed a shape).
    if k_rep != v_rep {
        return Err(PqError::Bad(
            "MAP key/value rep streams diverge".into(),
        ));
    }
    // K's def stream is intentionally unused: `assemble_map_kv`
    // consumes K's values at every middle-present position (driven by
    // V's def stream + the value-null encoding), so K's per-position
    // def carries no additional information beyond what V's def +
    // value_optional already determine.
    assembly::assemble_map_kv(
        &v_rep,
        &v_def,
        &k_vals,
        &v_vals,
        max_def_level,
        outer_optional,
        value_optional,
    )
}

/// SP144 T5: decode a struct column by decoding each primitive field
/// as a flat column via `read_chunk_values`, then zipping the
/// per-field columns through `assembly::assemble_struct`.
///
/// Per-field repetition stays REQUIRED/OPTIONAL (REPEATED fields = a
/// nested LIST and reject upstream in `classify_struct_plan`). The
/// outer-OPTIONAL handling lives in `assemble_struct` (see its
/// docstring for the all-fields-Null heuristic V1 trade-off).
fn read_chunk_values_nested_struct(
    file: &[u8],
    rg: &meta::RowGroup,
    fields: &[StructField],
    outer_optional: bool,
) -> Result<Vec<PqValue>, PqError> {
    let mut field_names: Vec<String> = Vec::with_capacity(fields.len());
    let mut field_columns: Vec<Vec<PqValue>> = Vec::with_capacity(fields.len());
    for f in fields {
        if let Some(nested_kind) = &f.nested {
            // SP145: this field is itself a nested shape (List, Map,
            // struct). Recursively decode it as its own column kind.
            let col = decode_field_by_kind(file, rg, nested_kind)?;
            field_names.push(f.name.clone());
            field_columns.push(col);
            continue;
        }
        let cc = rg
            .columns
            .iter()
            .find(|c| c.path == f.chunk_path)
            .ok_or_else(|| {
                PqError::Bad(format!(
                    "struct row group missing column for path {:?}",
                    f.chunk_path
                ))
            })?;
        if cc.ptype != f.ptype {
            return Err(PqError::Bad(format!(
                "struct field column for path {:?} schema/column-chunk physical-type mismatch",
                f.chunk_path
            )));
        }
        // Per-field max_def is the field-local level (outer_optional
        // already folded in during classify) — read_chunk_values is
        // the flat-decode path and treats this as the leaf's level.
        let col = read_chunk_values(file, cc, f.spec, f.max_def_level)?;
        field_names.push(f.name.clone());
        field_columns.push(col);
    }
    assembly::assemble_struct(&field_names, &field_columns, outer_optional)
}

/// SP145 T5: dispatch a nested-field decode by its inner ColumnKind.
/// Used recursively by `read_chunk_values_nested_struct` when a
/// StructField has `nested = Some(kind)`. Mirrors the extract_nested
/// per-kind dispatch but operates on a single row group + returns the
/// inner column directly (no row transpose).
fn decode_field_by_kind(
    file: &[u8],
    rg: &meta::RowGroup,
    kind: &ColumnKind,
) -> Result<Vec<PqValue>, PqError> {
    match kind {
        ColumnKind::Flat { spec, ptype: _, max_def_level: _ } => {
            // Flat fields should not be wrapped in nested — log + error.
            let _ = spec;
            Err(PqError::Bad("decode_field_by_kind: Flat should not be wrapped".into()))
        }
        ColumnKind::NestedListPrimitive {
            spec, ptype, max_def_level, max_rep_level,
            outer_optional, element_optional,
        } => {
            // Find the column chunk by walking down to the first leaf path.
            // The plan stored on the nested kind doesn't carry chunk_path;
            // re-derive from spec/ptype + a unique-match in the row group.
            // Use a leaf-path walk: list all leaves under this kind. For
            // List<primitive>, there's one leaf, identified by matching
            // ptype only — but multiple ptype-matches in the same row
            // group is ambiguous, so we'd need the chunk_path. For SP145
            // V1 this dispatch only fires for struct<List<primitive>>,
            // and the StructField's outer chunk_path placeholder is the
            // leaf path. Surface a clear error if the chunk lookup is
            // ambiguous so we don't silently grab the wrong column.
            return decode_list_primitive_in_rg(
                file, rg, *spec, *ptype, *max_def_level, *max_rep_level,
                *outer_optional, *element_optional,
            );
        }
        ColumnKind::NestedMapKV {
            key_spec, value_spec, key_ptype, value_ptype,
            key_chunk_path, value_chunk_path,
            max_def_level, outer_optional, value_optional,
        } => {
            read_chunk_values_nested_map(
                file, rg, key_chunk_path, value_chunk_path,
                *key_spec, *value_spec, *key_ptype, *value_ptype,
                *max_def_level, *outer_optional, *value_optional,
            )
        }
        ColumnKind::NestedStruct { outer_optional, fields } => {
            read_chunk_values_nested_struct(file, rg, fields, *outer_optional)
        }
        ColumnKind::NestedListOfListPrimitive {
            spec, ptype, max_def_level, max_rep_level,
            outer_optional, inner_optional, item_optional,
        } => {
            decode_list_of_list_primitive_in_rg(
                file, rg, *spec, *ptype, *max_def_level, *max_rep_level,
                *outer_optional, *inner_optional, *item_optional,
            )
        }
        ColumnKind::NestedListOfStruct {
            outer_optional, list_max_def_level, fields,
        } => {
            read_chunk_values_nested_list_of_struct(
                file, rg, fields, *outer_optional, *list_max_def_level,
            )
        }
        ColumnKind::NestedMapOfStruct {
            outer_optional, key_spec, key_ptype, key_chunk_path,
            map_max_def_level, value_fields,
        } => {
            read_chunk_values_nested_map_of_struct(
                file, rg, key_chunk_path, *key_spec, *key_ptype,
                value_fields, *outer_optional, *map_max_def_level,
            )
        }
        ColumnKind::NestedMapOfList {
            outer_optional, key_spec, key_ptype, key_chunk_path,
            value_spec, value_ptype, value_chunk_path,
            max_def_level, value_item_optional,
        } => {
            read_chunk_values_nested_map_of_list(
                file, rg, key_chunk_path, value_chunk_path,
                *key_spec, *value_spec, *key_ptype, *value_ptype,
                *max_def_level, *outer_optional, *value_item_optional,
            )
        }
        ColumnKind::NestedListOfListOfListPrimitive {
            spec, ptype, max_def_level, max_rep_level,
            outer_optional, middle_optional, inner_optional, item_optional,
        } => {
            decode_list_of_list_of_list_primitive_in_rg(
                file, rg, *spec, *ptype, *max_def_level, *max_rep_level,
                *outer_optional, *middle_optional, *inner_optional, *item_optional,
            )
        }
        ColumnKind::NestedListOfMap {
            outer_optional, key_spec, key_ptype, key_chunk_path,
            value_spec, value_ptype, value_chunk_path,
            max_def_level, value_optional,
        } => {
            read_chunk_values_nested_list_of_map(
                file, rg, key_chunk_path, value_chunk_path,
                *key_spec, *value_spec, *key_ptype, *value_ptype,
                *max_def_level, *outer_optional, *value_optional,
            )
        }
        ColumnKind::NestedMapOfMap {
            outer_optional,
            outer_key_spec, outer_key_ptype, outer_key_chunk_path,
            inner_key_spec, inner_key_ptype, inner_key_chunk_path,
            inner_value_spec, inner_value_ptype, inner_value_chunk_path,
            max_def_level, inner_value_optional,
        } => {
            read_chunk_values_nested_map_of_map(
                file, rg,
                outer_key_chunk_path, inner_key_chunk_path, inner_value_chunk_path,
                *outer_key_spec, *inner_key_spec, *inner_value_spec,
                *outer_key_ptype, *inner_key_ptype, *inner_value_ptype,
                *max_def_level, *outer_optional, *inner_value_optional,
            )
        }
    }
}

/// SP145: locate a single primitive-leaf column chunk in the row group
/// by ptype (unique-match), then decode as List<primitive>. For SP145
/// V1 this is only called from struct<List<primitive>> field-decode,
/// where there's typically one matching ptype in the struct's leaf
/// surface — but if not, we error rather than silently picking.
#[allow(clippy::too_many_arguments)]
fn decode_list_primitive_in_rg(
    file: &[u8],
    rg: &meta::RowGroup,
    spec: plain::PlainSpec,
    ptype: meta::Type,
    max_def_level: u32,
    max_rep_level: u32,
    outer_optional: bool,
    element_optional: bool,
) -> Result<Vec<PqValue>, PqError> {
    // Heuristic: find the chunk whose path has max_rep_level matching
    // pattern — but ColumnChunk doesn't carry rep level. Instead, find
    // by ptype + matching the "list" path component. SP145 V1: pick
    // the chunk whose path[-1] is "element" and ptype matches (pyarrow
    // canonical name for LIST leaves).
    let candidates: Vec<_> = rg.columns.iter()
        .filter(|c| c.ptype == ptype && c.path.last().map(|s| s.as_str()) == Some("element"))
        .collect();
    if candidates.len() != 1 {
        return Err(PqError::Bad(format!(
            "decode_list_primitive_in_rg: expected 1 candidate chunk, found {}",
            candidates.len()
        )));
    }
    let cc = candidates[0];
    let (rep, def, vals) = read_chunk_levels_and_values(
        file, cc, spec, max_def_level, max_rep_level,
    )?;
    assembly::assemble_list_primitive(
        &rep, &def, &vals, max_def_level, outer_optional, element_optional,
    )
}

#[allow(clippy::too_many_arguments)]
fn decode_list_of_list_primitive_in_rg(
    file: &[u8],
    rg: &meta::RowGroup,
    spec: plain::PlainSpec,
    ptype: meta::Type,
    max_def_level: u32,
    max_rep_level: u32,
    outer_optional: bool,
    inner_optional: bool,
    item_optional: bool,
) -> Result<Vec<PqValue>, PqError> {
    let candidates: Vec<_> = rg.columns.iter()
        .filter(|c| c.ptype == ptype && c.path.last().map(|s| s.as_str()) == Some("element"))
        .collect();
    if candidates.len() != 1 {
        return Err(PqError::Bad(format!(
            "decode_list_of_list_primitive_in_rg: expected 1 candidate, found {}",
            candidates.len()
        )));
    }
    let cc = candidates[0];
    let (rep, def, vals) = read_chunk_levels_and_values(
        file, cc, spec, max_def_level, max_rep_level,
    )?;
    assembly::assemble_list_of_list_primitive(
        &rep, &def, &vals, max_def_level,
        outer_optional, inner_optional, item_optional,
    )
}

/// SP145 T5: decode a `List<struct<...>>` column. Each struct field is
/// its own column chunk; they all share the same REPEATED outer-LIST
/// ancestor (so their rep streams are byte-identical at max_rep=1).
///
/// Strategy:
/// 1. Decode each field column via `read_chunk_levels_and_values` —
///    each returns parallel (rep, def, vals) at max_rep=1.
/// 2. Validate that all field columns share the same rep stream.
/// 3. Take the first field's (rep, def) as authoritative for the
///    LIST-level boundaries.
/// 4. Each field's `vals` is its per-item-present slot value vec.
/// 5. Fold via `assemble_list_of_struct` with all field columns + the
///    shared rep/def stream + outer LIST level.
fn read_chunk_values_nested_list_of_struct(
    file: &[u8],
    rg: &meta::RowGroup,
    fields: &[StructField],
    outer_optional: bool,
    list_max_def_level: u32,
) -> Result<Vec<PqValue>, PqError> {
    if fields.is_empty() {
        return Err(PqError::Bad("list_of_struct: no fields".into()));
    }
    let mut field_names: Vec<String> = Vec::with_capacity(fields.len());
    let mut field_values: Vec<Vec<PqValue>> = Vec::with_capacity(fields.len());
    let mut shared_rep: Option<Vec<u32>> = None;
    let mut shared_def: Option<Vec<u32>> = None;
    for f in fields {
        let cc = rg.columns.iter().find(|c| c.path == f.chunk_path)
            .ok_or_else(|| PqError::Bad(format!(
                "list_of_struct row group missing column for path {:?}", f.chunk_path
            )))?;
        if cc.ptype != f.ptype {
            return Err(PqError::Bad(format!(
                "list_of_struct field column for path {:?} ptype mismatch", f.chunk_path
            )));
        }
        // Field max_def_level is the FULL path level
        // (outer_optional + 1 REP + 0 struct-REQ + field_local_def).
        let (rep, def, vals) = read_chunk_levels_and_values(
            file, cc, f.spec, f.max_def_level, 1,
        )?;
        if shared_rep.is_none() {
            shared_rep = Some(rep.clone());
            shared_def = Some(def.clone());
        } else if shared_rep.as_ref().unwrap() != &rep {
            return Err(PqError::Bad(format!(
                "list_of_struct: field '{}' rep stream diverges from authoritative field",
                f.name
            )));
        }
        // V1: don't strict-compare def streams across OPT fields — they
        // may differ when one field is OPT and another REQ.
        field_names.push(f.name.clone());
        field_values.push(vals);
    }
    let rep = shared_rep.unwrap();
    let def = shared_def.unwrap();
    assembly::assemble_list_of_struct(
        &rep, &def, &field_names, &field_values, list_max_def_level, outer_optional,
    )
}

/// SP145 T5: decode a `Map<K, struct<...>>` column. K is a single
/// column chunk; each V field is its own column chunk. All share the
/// REPEATED middle ancestor (max_rep=1).
#[allow(clippy::too_many_arguments)]
fn read_chunk_values_nested_map_of_struct(
    file: &[u8],
    rg: &meta::RowGroup,
    key_chunk_path: &[String],
    key_spec: plain::PlainSpec,
    key_ptype: meta::Type,
    value_fields: &[StructField],
    outer_optional: bool,
    map_max_def_level: u32,
) -> Result<Vec<PqValue>, PqError> {
    if value_fields.is_empty() {
        return Err(PqError::Bad("map_of_struct: no value fields".into()));
    }
    let key_cc = rg.columns.iter().find(|c| c.path == key_chunk_path)
        .ok_or_else(|| PqError::Bad(format!(
            "map_of_struct row group missing key column for path {:?}", key_chunk_path
        )))?;
    if key_cc.ptype != key_ptype {
        return Err(PqError::Bad(format!(
            "map_of_struct key column path {:?} ptype mismatch", key_chunk_path
        )));
    }
    // K's max_def is map_max_def_level (key REQ inside REP middle).
    let (k_rep, _k_def, k_vals) = read_chunk_levels_and_values(
        file, key_cc, key_spec, map_max_def_level, 1,
    )?;
    let mut v_field_names: Vec<String> = Vec::with_capacity(value_fields.len());
    let mut v_field_values: Vec<Vec<PqValue>> = Vec::with_capacity(value_fields.len());
    let mut shared_rep: Option<Vec<u32>> = None;
    let mut shared_def: Option<Vec<u32>> = None;
    for vf in value_fields {
        let cc = rg.columns.iter().find(|c| c.path == vf.chunk_path)
            .ok_or_else(|| PqError::Bad(format!(
                "map_of_struct row group missing value-field column for path {:?}", vf.chunk_path
            )))?;
        if cc.ptype != vf.ptype {
            return Err(PqError::Bad(format!(
                "map_of_struct value-field column path {:?} ptype mismatch", vf.chunk_path
            )));
        }
        let (rep, def, vals) = read_chunk_levels_and_values(
            file, cc, vf.spec, vf.max_def_level, 1,
        )?;
        if shared_rep.is_none() {
            shared_rep = Some(rep.clone());
            shared_def = Some(def.clone());
        } else if shared_rep.as_ref().unwrap() != &rep {
            return Err(PqError::Bad(format!(
                "map_of_struct: value field '{}' rep stream diverges from authoritative", vf.name
            )));
        }
        v_field_names.push(vf.name.clone());
        v_field_values.push(vals);
    }
    let rep = shared_rep.unwrap();
    let def = shared_def.unwrap();
    // K rep stream must also match.
    if k_rep != rep {
        return Err(PqError::Bad(
            "map_of_struct: K rep stream diverges from V field rep stream".into(),
        ));
    }
    assembly::assemble_map_of_struct(
        &rep, &def, &k_vals, &v_field_names, &v_field_values,
        map_max_def_level, outer_optional,
    )
}

/// SP145 T5: decode a `Map<K, List<T>>` column.
#[allow(clippy::too_many_arguments)]
fn read_chunk_values_nested_map_of_list(
    file: &[u8],
    rg: &meta::RowGroup,
    key_chunk_path: &[String],
    value_chunk_path: &[String],
    key_spec: plain::PlainSpec,
    value_spec: plain::PlainSpec,
    key_ptype: meta::Type,
    value_ptype: meta::Type,
    max_def_level: u32,
    outer_optional: bool,
    value_item_optional: bool,
) -> Result<Vec<PqValue>, PqError> {
    let key_cc = rg.columns.iter().find(|c| c.path == key_chunk_path)
        .ok_or_else(|| PqError::Bad(format!(
            "map_of_list row group missing key column for path {:?}", key_chunk_path
        )))?;
    let value_cc = rg.columns.iter().find(|c| c.path == value_chunk_path)
        .ok_or_else(|| PqError::Bad(format!(
            "map_of_list row group missing value column for path {:?}", value_chunk_path
        )))?;
    if key_cc.ptype != key_ptype {
        return Err(PqError::Bad(format!(
            "map_of_list key column path {:?} ptype mismatch", key_chunk_path
        )));
    }
    if value_cc.ptype != value_ptype {
        return Err(PqError::Bad(format!(
            "map_of_list value column path {:?} ptype mismatch", value_chunk_path
        )));
    }
    // K's max_def_level = outer_optional + 1 (REP middle).
    let key_max_def = (outer_optional as u32) + 1;
    let (_k_rep, _k_def, k_vals) = read_chunk_levels_and_values(
        file, key_cc, key_spec, key_max_def, 1,
    )?;
    // V leaf has max_rep_level=2 (MAP REP + LIST REP).
    let (v_rep, v_def, v_vals) = read_chunk_levels_and_values(
        file, value_cc, value_spec, max_def_level, 2,
    )?;
    assembly::assemble_map_of_list(
        &v_rep, &v_def, &k_vals, &v_vals, max_def_level,
        outer_optional, value_item_optional,
    )
}

/// SP146: locate the single primitive-leaf for `List<List<List<T>>>` and
/// decode it. The leaf has max_rep_level=3.
#[allow(clippy::too_many_arguments)]
fn decode_list_of_list_of_list_primitive_in_rg(
    file: &[u8],
    rg: &meta::RowGroup,
    spec: plain::PlainSpec,
    ptype: meta::Type,
    max_def_level: u32,
    max_rep_level: u32,
    outer_optional: bool,
    middle_optional: bool,
    inner_optional: bool,
    item_optional: bool,
) -> Result<Vec<PqValue>, PqError> {
    let candidates: Vec<_> = rg.columns.iter()
        .filter(|c| c.ptype == ptype && c.path.last().map(|s| s.as_str()) == Some("element"))
        .collect();
    if candidates.len() != 1 {
        return Err(PqError::Bad(format!(
            "decode_list_of_list_of_list_primitive_in_rg: expected 1 candidate, found {}",
            candidates.len()
        )));
    }
    let cc = candidates[0];
    let (rep, def, vals) = read_chunk_levels_and_values(
        file, cc, spec, max_def_level, max_rep_level,
    )?;
    assembly::assemble_list_of_list_of_list_primitive(
        &rep, &def, &vals, max_def_level,
        outer_optional, middle_optional, inner_optional, item_optional,
    )
}

/// SP146: decode a `List<Map<K, V>>` column. K and V chunks share the
/// same rep stream at max_rep_level=2 (outer LIST REP + MAP REP).
#[allow(clippy::too_many_arguments)]
fn read_chunk_values_nested_list_of_map(
    file: &[u8],
    rg: &meta::RowGroup,
    key_chunk_path: &[String],
    value_chunk_path: &[String],
    key_spec: plain::PlainSpec,
    value_spec: plain::PlainSpec,
    key_ptype: meta::Type,
    value_ptype: meta::Type,
    max_def_level: u32,
    outer_optional: bool,
    value_optional: bool,
) -> Result<Vec<PqValue>, PqError> {
    let key_cc = rg.columns.iter().find(|c| c.path == key_chunk_path)
        .ok_or_else(|| PqError::Bad(format!(
            "list_of_map row group missing key column for path {:?}", key_chunk_path
        )))?;
    let value_cc = rg.columns.iter().find(|c| c.path == value_chunk_path)
        .ok_or_else(|| PqError::Bad(format!(
            "list_of_map row group missing value column for path {:?}", value_chunk_path
        )))?;
    if key_cc.ptype != key_ptype {
        return Err(PqError::Bad(format!(
            "list_of_map key column path {:?} ptype mismatch", key_chunk_path
        )));
    }
    if value_cc.ptype != value_ptype {
        return Err(PqError::Bad(format!(
            "list_of_map value column path {:?} ptype mismatch", value_chunk_path
        )));
    }
    // K has max_def = outer_optional + 1 (LIST REP) + 1 (MAP REP).
    let key_max_def = (outer_optional as u32) + 2;
    let (k_rep, _k_def, k_vals) = read_chunk_levels_and_values(
        file, key_cc, key_spec, key_max_def, 2,
    )?;
    let (v_rep, v_def, v_vals) = read_chunk_levels_and_values(
        file, value_cc, value_spec, max_def_level, 2,
    )?;
    if k_rep != v_rep {
        return Err(PqError::Bad(
            "list_of_map: K rep stream diverges from V rep stream".into(),
        ));
    }
    assembly::assemble_list_of_map_kv(
        &v_rep, &v_def, &k_vals, &v_vals, max_def_level,
        outer_optional, value_optional,
    )
}

/// SP146: decode a `Map<K1, Map<K2, V>>` column. Outer K has
/// max_rep_level=1; inner K and inner V share max_rep_level=2.
#[allow(clippy::too_many_arguments)]
fn read_chunk_values_nested_map_of_map(
    file: &[u8],
    rg: &meta::RowGroup,
    outer_key_chunk_path: &[String],
    inner_key_chunk_path: &[String],
    inner_value_chunk_path: &[String],
    outer_key_spec: plain::PlainSpec,
    inner_key_spec: plain::PlainSpec,
    inner_value_spec: plain::PlainSpec,
    outer_key_ptype: meta::Type,
    inner_key_ptype: meta::Type,
    inner_value_ptype: meta::Type,
    max_def_level: u32,
    outer_optional: bool,
    inner_value_optional: bool,
) -> Result<Vec<PqValue>, PqError> {
    let ok_cc = rg.columns.iter().find(|c| c.path == outer_key_chunk_path)
        .ok_or_else(|| PqError::Bad(format!(
            "map_of_map row group missing outer-key column for path {:?}", outer_key_chunk_path
        )))?;
    let ik_cc = rg.columns.iter().find(|c| c.path == inner_key_chunk_path)
        .ok_or_else(|| PqError::Bad(format!(
            "map_of_map row group missing inner-key column for path {:?}", inner_key_chunk_path
        )))?;
    let iv_cc = rg.columns.iter().find(|c| c.path == inner_value_chunk_path)
        .ok_or_else(|| PqError::Bad(format!(
            "map_of_map row group missing inner-value column for path {:?}", inner_value_chunk_path
        )))?;
    if ok_cc.ptype != outer_key_ptype {
        return Err(PqError::Bad(format!(
            "map_of_map outer-key column path {:?} ptype mismatch", outer_key_chunk_path
        )));
    }
    if ik_cc.ptype != inner_key_ptype {
        return Err(PqError::Bad(format!(
            "map_of_map inner-key column path {:?} ptype mismatch", inner_key_chunk_path
        )));
    }
    if iv_cc.ptype != inner_value_ptype {
        return Err(PqError::Bad(format!(
            "map_of_map inner-value column path {:?} ptype mismatch", inner_value_chunk_path
        )));
    }
    // Outer K's max_def_level = outer_optional + 1 (outer MAP REP).
    let outer_key_max_def = (outer_optional as u32) + 1;
    let (_ok_rep, _ok_def, ok_vals) = read_chunk_levels_and_values(
        file, ok_cc, outer_key_spec, outer_key_max_def, 1,
    )?;
    // Inner K & V have max_rep_level=2.
    let (ik_rep, _ik_def, ik_vals) = read_chunk_levels_and_values(
        file, ik_cc, inner_key_spec, max_def_level, 2,
    )?;
    let (iv_rep, iv_def, iv_vals) = read_chunk_levels_and_values(
        file, iv_cc, inner_value_spec, max_def_level, 2,
    )?;
    if ik_rep != iv_rep {
        return Err(PqError::Bad(
            "map_of_map: inner-K rep stream diverges from inner-V rep stream".into(),
        ));
    }
    assembly::assemble_map_of_map_kv(
        &iv_rep, &iv_def, &ok_vals, &ik_vals, &iv_vals, max_def_level,
        outer_optional, inner_value_optional,
    )
}

/// SP151 (OBJ-2c-4 follow-up): decode with an explicit per-page size
/// cap. Page headers whose declared `uncompressed_page_size` or
/// `compressed_page_size` exceeds `max_page_size` are rejected with
/// `Unsupported` BEFORE any allocation — the cap travels via a
/// thread-local set on entry and restored on return (RAII).
///
/// Operators with known-trusted producers can raise the cap (up to
/// the per-codec module ceiling — currently 256 MiB matching
/// `DEFAULT_MAX_PAGE_SIZE`); memory-constrained ingest can lower it.
/// Setting `max_page_size = 0` rejects every page (useful as a kill
/// switch for hostile inputs).
pub fn extract_with_cap(
    bytes: &[u8],
    wanted: &[&str],
    max_page_size: usize,
) -> Result<Vec<Vec<PqValue>>, PqError> {
    let _guard = MaxPageSizeGuard::new(max_page_size);
    extract_inner(bytes, wanted)
}

/// Decode the `wanted` leaf columns (in that output order) from a
/// whole Parquet object. OBJ-2a: flat REQUIRED columns, PLAIN,
/// UNCOMPRESSED, V1 data pages, all row groups concatenated.
///
/// SP151: uses `DEFAULT_MAX_PAGE_SIZE` (256 MiB) as the per-page cap.
/// For a different cap use `extract_with_cap`.
pub fn extract(
    bytes: &[u8],
    wanted: &[&str],
) -> Result<Vec<Vec<PqValue>>, PqError> {
    extract_with_cap(bytes, wanted, DEFAULT_MAX_PAGE_SIZE)
}

fn extract_inner(
    bytes: &[u8],
    wanted: &[&str],
) -> Result<Vec<Vec<PqValue>>, PqError> {
    let md_bytes = footer::metadata_slice(bytes)?;
    let md = meta::FileMetaData::decode(md_bytes)?;

    // SP143 T6: nested schemas — dispatch to the nested decode path if every
    // wanted column is a recognized LIST<primitive>; otherwise reject with
    // typed errors naming the future slice (SP144 Map/struct, SP145 deep
    // nesting). Flat schemas stay on the legacy path below verbatim.
    if !md.flat_schema {
        return extract_nested(bytes, &md, wanted);
    }

    // Resolve each wanted name to its leaf; enforce known repetition +
    // supported physical type. Build a per-leaf `PlainSpec` once here
    // (validating DECIMAL precision/scale/FLBA width upfront) so the
    // per-row-group hot loop can stay panic-free.
    // Also collect the schema-declared physical type for each wanted column
    // so the per-row-group loop can verify ColumnMetaData type consistency.
    let mut wanted_ptypes: Vec<meta::Type> = Vec::with_capacity(wanted.len());
    let mut wanted_specs: Vec<plain::PlainSpec> = Vec::with_capacity(wanted.len());
    let mut wanted_max_def_levels: Vec<u32> = Vec::with_capacity(wanted.len());
    for w in wanted {
        let leaf = md
            .leaves
            .iter()
            .find(|l| &l.name == w)
            .ok_or_else(|| {
                PqError::Bad(format!("column `{w}` not found in Parquet schema"))
            })?;
        let max_def_level: u32 = match leaf.repetition {
            meta::Repetition::Required => 0,
            meta::Repetition::Optional => 1,
            meta::Repetition::Repeated => {
                return Err(PqError::Unsupported("REPEATED columns: OBJ-2c".into()))
            }
            meta::Repetition::Other(_) => {
                return Err(PqError::Unsupported("unknown repetition: OBJ-2c".into()))
            }
        };
        match leaf.ptype {
            meta::Type::Boolean
            | meta::Type::Int32
            | meta::Type::Int64
            | meta::Type::Float
            | meta::Type::Double
            | meta::Type::ByteArray
            | meta::Type::Int96
            | meta::Type::FixedLenByteArray => {}
            t => {
                return Err(PqError::Unsupported(format!(
                    "physical type {t:?}: OBJ-2c"
                )))
            }
        }
        let spec = build_plain_spec(leaf)?;
        wanted_specs.push(spec);
        wanted_ptypes.push(leaf.ptype);
        wanted_max_def_levels.push(max_def_level);
    }

    // Per wanted column: concatenate its values across all row groups.
    let mut cols: Vec<Vec<PqValue>> =
        wanted.iter().map(|_| Vec::new()).collect();
    for rg in &md.row_groups {
        for (ci, w) in wanted.iter().enumerate() {
            let cc = rg
                .columns
                .iter()
                .find(|c| c.path.last().map(|s| s.as_str()) == Some(*w))
                .ok_or_else(|| {
                    PqError::Bad(format!(
                        "row group missing column `{w}`"
                    ))
                })?;
            // STRICT early guard (Fix 1): schema-declared ptype must equal
            // the ColumnMetaData ptype. A crafted file could pass the schema
            // gate with one type and encode a different type in ColumnMetaData.
            // Reject such divergence immediately as a typed Bad error rather
            // than deferring detection to decode_plain's Unsupported arm.
            if cc.ptype != wanted_ptypes[ci] {
                return Err(PqError::Bad(format!(
                    "column `{}` schema/column-chunk physical-type mismatch",
                    w
                )));
            }
            let vals = read_chunk_values(bytes, cc, wanted_specs[ci], wanted_max_def_levels[ci])?;
            cols[ci].extend(vals);
        }
    }

    // Transpose columns → rows; all columns must have equal length.
    let nrows = cols.first().map(|c| c.len()).unwrap_or(0);
    if cols.iter().any(|c| c.len() != nrows) {
        return Err(PqError::Bad("column length mismatch".into()));
    }
    let mut rows = Vec::with_capacity(nrows);
    for r in 0..nrows {
        rows.push(cols.iter().map(|c| c[r].clone()).collect());
    }
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Real python-gzip member of struct.pack('<qq',7,-2) (16 raw bytes).
    // Regenerate: python -c "import gzip,struct,sys; sys.stdout.buffer.write(gzip.compress(struct.pack('<qq',7,-2)))"
    // (gzip embeds a wall-clock MTIME in header bytes 4-7; the DEFLATE
    // body + CRC32/ISIZE trailer are deterministic — only MTIME varies.)
    const GZ_7_NEG2: &[u8] = &[
        0x1f,0x8b,0x08,0x00,0xb8,0xa3,0x0c,0x6a,0x02,0xff,
        0x63,0x67,0x80,0x80,0x7f,0xff,0x21,0x00,0x00,0xcb,
        0xb3,0x8e,0x99,0x10,0x00,0x00,0x00,
    ];

    // ── helpers (same as Tasks 2–5 hand-encoders) ──────────────────────
    fn uv(out: &mut Vec<u8>, mut v: u64) {
        loop {
            let b = (v & 0x7f) as u8;
            v >>= 7;
            if v == 0 { out.push(b); break; } else { out.push(b | 0x80); }
        }
    }
    fn zz(v: i64) -> u64 { ((v << 1) ^ (v >> 63)) as u64 }

    /// Build a V1 PageHeader (Thrift compact) for a PLAIN INT64 page
    /// with `num_values` values and `data_bytes` uncompressed size.
    /// Returns the header bytes only (not the page data).
    ///
    /// parquet.thrift PageHeader:
    ///   1: PageType type (i32 enum)
    ///   2: i32 uncompressed_page_size
    ///   3: i32 compressed_page_size
    ///   4: optional i32 crc (skipped)
    ///   5: DataPageHeader data_page_header (struct)
    ///
    /// DataPageHeader:
    ///   1: i32 num_values
    ///   2: Encoding encoding (i32 enum)
    fn page_header_bytes(num_values: i32, data_bytes: i32) -> Vec<u8> {
        let mut h = Vec::new();
        // f1 page_type = DATA_PAGE(0): (1<<4)|5=0x15, zz(0)=0
        h.push(0x15); uv(&mut h, zz(0));
        // f2 uncompressed_page_size: delta 1→2=1 → (1<<4)|5=0x15
        h.push(0x15); uv(&mut h, zz(data_bytes as i64));
        // f3 compressed_page_size: delta 2→3=1 → (1<<4)|5=0x15
        h.push(0x15); uv(&mut h, zz(data_bytes as i64));
        // f5 DataPageHeader struct: delta 3→5=2 → (2<<4)|12=0x2c
        h.push(0x2c);
        // (nested struct: reset last_id → 0)
        // g1 num_values: (1<<4)|5=0x15
        h.push(0x15); uv(&mut h, zz(num_values as i64));
        // g2 encoding=PLAIN(0): delta 1→2=1 → (1<<4)|5=0x15, zz(0)=0
        h.push(0x15); uv(&mut h, zz(0));
        h.push(0x00); // stop DataPageHeader
        h.push(0x00); // stop PageHeader
        h
    }

    /// Build a PageHeader where the DataPageHeader encoding field is
    /// set to PLAIN_DICTIONARY(2) instead of PLAIN(0) — used to test
    /// the dp_encoding != 0 gate.
    fn page_header_dict_bytes(num_values: i32, data_bytes: i32) -> Vec<u8> {
        let mut h = Vec::new();
        h.push(0x15); uv(&mut h, zz(0));
        h.push(0x15); uv(&mut h, zz(data_bytes as i64));
        h.push(0x15); uv(&mut h, zz(data_bytes as i64));
        h.push(0x2c);
        h.push(0x15); uv(&mut h, zz(num_values as i64));
        // encoding = PLAIN_DICTIONARY(2): zz(2)=4
        h.push(0x15); uv(&mut h, zz(2));
        h.push(0x00); // stop DataPageHeader
        h.push(0x00); // stop PageHeader
        h
    }

    /// Build the FileMetaData Thrift compact bytes for a one-column
    /// (`id` INT64 REQUIRED) one-row-group file.
    ///
    /// Parameters allow toggling individual fields to test support-matrix
    /// gates:
    ///   - `encoding`:    Encoding enum value (0=PLAIN, 2=PLAIN_DICTIONARY)
    ///   - `codec`:       CompressionCodec enum value (0=UNCOMPRESSED, 2=SNAPPY)
    ///   - `repetition`:  RepetitionType enum value (0=REQUIRED, 1=OPTIONAL)
    ///   - `chunk_ptype`: ColumnMetaData physical type (normally == schema type
    ///                    INT64=2; set to a different value to trigger Fix-1 guard)
    ///   - `data_page_offset`: actual byte offset of the page header in the file
    fn filemetadata_bytes(
        encoding: i64,
        codec: i64,
        repetition: i64,
        chunk_ptype: i64,
        data_page_offset: i64,
    ) -> Vec<u8> {
        let mut b = Vec::new();
        // f1 version=2 (i32): (1<<4)|5=0x15, zz(2)=4
        b.push(0x15); uv(&mut b, zz(2));

        // f2 list<SchemaElement>: (1<<4)|9=0x19 ; list-hdr 2 elems struct
        //   list-hdr byte: (count<<4)|etype = (2<<4)|12 = 0x2c
        b.push(0x19); b.push(0x2c);

        // schema[0] root group: name f4="schema", num_children f5=1
        //   f4 name: (4<<4)|8=0x48, len6 "schema"
        b.push(0x48); uv(&mut b, 6); b.extend_from_slice(b"schema");
        //   f5 num_children=1: delta 4→5=1 → (1<<4)|5=0x15, zz(1)=2
        b.push(0x15); uv(&mut b, zz(1));
        b.push(0x00); // stop schema[0]

        // schema[1] leaf "id": f1 type=INT64(2), f3 repetition, f4 name
        //   f1 type=INT64(2): (1<<4)|5=0x15, zz(2)=4
        b.push(0x15); uv(&mut b, zz(2));
        //   f3 repetition_type: delta 1→3=2 → (2<<4)|5=0x25
        b.push(0x25); uv(&mut b, zz(repetition));
        //   f4 name="id": delta 3→4=1 → (1<<4)|8=0x18, len2 "id"
        b.push(0x18); uv(&mut b, 2); b.extend_from_slice(b"id");
        b.push(0x00); // stop schema[1]

        // f3 num_rows=2: delta f2→f3=1 → (1<<4)|6=0x16, zz(2)=4
        b.push(0x16); uv(&mut b, zz(2));

        // f4 list<RowGroup>: delta f3→f4=1 → (1<<4)|9=0x19
        //   list-hdr 1 elem struct: (1<<4)|12=0x1c
        b.push(0x19); b.push(0x1c);

        // RowGroup: f1 list<ColumnChunk>, f3 num_rows
        //   f1 list<ColumnChunk>: (1<<4)|9=0x19 ; (1<<4)|12=0x1c
        b.push(0x19); b.push(0x1c);

        // ColumnChunk: f3 ColumnMetaData (struct)
        //   delta 0→3=3 → (3<<4)|12=0x3c
        b.push(0x3c);

        // ColumnMetaData:
        //   f1 type=chunk_ptype: (1<<4)|5=0x15, zz(chunk_ptype)
        b.push(0x15); uv(&mut b, zz(chunk_ptype));
        //   f2 encodings list<Encoding> 1 elem:
        //     delta 1→2=1 → (1<<4)|9=0x19
        //     list-hdr 1 elem i32: (1<<4)|5=0x15
        b.push(0x19); b.push(0x15); uv(&mut b, zz(encoding));
        //   f3 path_in_schema list<string> ["id"]:
        //     delta 2→3=1 → (1<<4)|9=0x19
        //     list-hdr 1 elem binary: (1<<4)|8=0x18
        b.push(0x19); b.push(0x18); uv(&mut b, 2); b.extend_from_slice(b"id");
        //   f4 codec: delta 3→4=1 → (1<<4)|5=0x15
        b.push(0x15); uv(&mut b, zz(codec));
        //   f5 num_values=2: delta 4→5=1 → (1<<4)|6=0x16, zz(2)=4
        b.push(0x16); uv(&mut b, zz(2));
        //   f9 data_page_offset: delta 5→9=4 → (4<<4)|6=0x46
        b.push(0x46); uv(&mut b, zz(data_page_offset));
        b.push(0x00); // stop ColumnMetaData
        b.push(0x00); // stop ColumnChunk

        // RowGroup f3 num_rows=2: last id in RG was f1 (columns)
        //   delta 1→3=2 → (2<<4)|6=0x26, zz(2)=4
        b.push(0x26); uv(&mut b, zz(2));
        b.push(0x00); // stop RowGroup

        b.push(0x00); // stop FileMetaData
        b
    }

    /// Assemble a complete spec-faithful Parquet file:
    ///   [PAR1][page_hdr][page_data][FileMetaData][mlen u32 LE][PAR1]
    ///
    /// The page header is placed at offset 4 (right after the PAR1 magic),
    /// so `data_page_offset = 4` in the metadata.
    ///
    /// `encoding`/`codec`/`repetition` are passed through to
    /// `filemetadata_bytes` to allow toggling individual fields for
    /// support-matrix rejection tests. `chunk_ptype` is the physical type
    /// written into ColumnMetaData (normally INT64=2 to match the schema
    /// leaf; set to another value to trigger the Fix-1 schema/chunk guard).
    fn build_parquet_file(
        encoding: i64,
        codec: i64,
        repetition: i64,
        use_dict_page_hdr: bool,
    ) -> Vec<u8> {
        build_parquet_file_inner(encoding, codec, repetition, 2, use_dict_page_hdr)
    }

    /// Like `build_parquet_file` but also overrides the ColumnMetaData
    /// physical type (`chunk_ptype`) independently of the schema leaf type
    /// (always INT64=2). Used by the Fix-1 lock test.
    fn build_parquet_file_with_chunk_type(chunk_ptype: i64) -> Vec<u8> {
        build_parquet_file_inner(0, 0, 0, chunk_ptype, false)
    }

    fn build_parquet_file_inner(
        encoding: i64,
        codec: i64,
        repetition: i64,
        chunk_ptype: i64,
        use_dict_page_hdr: bool,
    ) -> Vec<u8> {
        // PLAIN page data: 7i64 + (-2)i64 in little-endian
        let mut page_data = Vec::new();
        page_data.extend_from_slice(&7i64.to_le_bytes());
        page_data.extend_from_slice(&(-2i64).to_le_bytes());
        let data_bytes = page_data.len() as i32; // 16

        // Page header bytes
        let hdr = if use_dict_page_hdr {
            page_header_dict_bytes(2, data_bytes)
        } else {
            page_header_bytes(2, data_bytes)
        };

        // The page header starts at offset 4 (after PAR1 magic)
        let data_page_offset: i64 = 4;

        // Build FileMetaData
        let meta = filemetadata_bytes(
            encoding, codec, repetition, chunk_ptype, data_page_offset,
        );
        let mlen = meta.len() as u32;

        // Assemble file:
        // [PAR1][page_hdr][page_data][FileMetaData][mlen_le][PAR1]
        let mut file = Vec::new();
        file.extend_from_slice(b"PAR1");
        file.extend_from_slice(&hdr);
        file.extend_from_slice(&page_data);
        file.extend_from_slice(&meta);
        file.extend_from_slice(&mlen.to_le_bytes());
        file.extend_from_slice(b"PAR1");
        file
    }

    // ── Split support-matrix tests (Fix 2) ────────────────────────────

    #[test]
    fn extract_golden_int64_two_rows() {
        // Good file: PLAIN(0), UNCOMPRESSED(0), REQUIRED(0), plain page hdr
        let file = build_parquet_file(0, 0, 0, false);
        let rows = extract(&file, &["id"]).expect("extract");
        assert_eq!(rows, vec![
            vec![PqValue::I64(7)],
            vec![PqValue::I64(-2)],
        ]);
    }

    /// Inner builder for a PLAIN INT64 [7,-2] gzip-compressed file.
    /// `comp_override`: `None` → use gz.len() as compressed_page_size
    /// (correct file, decodes ok); `Some(v)` → use `v` instead (lying
    /// value, triggers page_payload bounds check).
    ///
    /// Analogous to `build_snappy_plain_int64_file_inner(Option)` in SP104:
    /// single shared body; None path is byte-identical to
    /// `build_gzip_plain_int64_file(gz)` prior to this refactor.
    fn build_gzip_plain_int64_file_inner(gz: &[u8], comp_override: Option<i64>) -> Vec<u8> {
        let uncomp: i64 = 16;
        let comp = comp_override.unwrap_or(gz.len() as i64);

        let mut hdr = Vec::new();
        hdr.push(0x15); uv(&mut hdr, zz(0));     // f1 type=DATA_PAGE(0)
        hdr.push(0x15); uv(&mut hdr, zz(uncomp)); // f2 uncompressed_page_size=16
        hdr.push(0x15); uv(&mut hdr, zz(comp));  // f3 compressed_page_size
        hdr.push(0x2c);                           // f5 DataPageHeader struct (delta 3->5=2)
        hdr.push(0x15); uv(&mut hdr, zz(2));     // g1 num_values=2
        hdr.push(0x15); uv(&mut hdr, zz(0));     // g2 encoding=PLAIN(0)
        hdr.push(0x00); hdr.push(0x00);          // stop DPH / PH

        let data_page_offset: i64 = 4;

        let mut m = Vec::new();
        m.push(0x15); uv(&mut m, zz(2));         // f1 version=2
        m.push(0x19); m.push(0x2c);              // f2 list<SchemaElement> 2
        m.push(0x48); uv(&mut m, 6); m.extend_from_slice(b"schema");
        m.push(0x15); uv(&mut m, zz(1));         // schema[0] num_children=1
        m.push(0x00);
        m.push(0x15); uv(&mut m, zz(2));         // schema[1] f1 type=INT64
        m.push(0x25); uv(&mut m, zz(0));         // f3 repetition=REQUIRED
        m.push(0x18); uv(&mut m, 2); m.extend_from_slice(b"id");
        m.push(0x00);
        m.push(0x16); uv(&mut m, zz(2));         // f3 num_rows=2
        m.push(0x19); m.push(0x1c);              // f4 list<RowGroup> 1
        m.push(0x19); m.push(0x1c);              // RG f1 list<ColumnChunk> 1
        m.push(0x3c);                            // ColumnChunk f3 ColumnMetaData
        m.push(0x15); uv(&mut m, zz(2));         // CMD f1 type=INT64
        m.push(0x19); m.push(0x15); uv(&mut m, zz(0)); // f2 encodings [PLAIN]
        m.push(0x19); m.push(0x18); uv(&mut m, 2); m.extend_from_slice(b"id");
        m.push(0x15); uv(&mut m, zz(2));         // f4 codec=GZIP(2)
        m.push(0x16); uv(&mut m, zz(2));         // f5 num_values=2
        m.push(0x46); uv(&mut m, zz(data_page_offset)); // f9 data_page_offset=4
        m.push(0x00); m.push(0x00);              // stop CMD / ColumnChunk
        m.push(0x26); uv(&mut m, zz(2));         // RG f3 num_rows=2
        m.push(0x00); m.push(0x00);              // stop RG / FileMetaData

        let mut f = Vec::new();
        f.extend_from_slice(b"PAR1");
        f.extend_from_slice(&hdr);
        f.extend_from_slice(gz);
        let mlen = m.len() as u32;
        f.extend_from_slice(&m);
        f.extend_from_slice(&mlen.to_le_bytes());
        f.extend_from_slice(b"PAR1");
        f
    }

    /// Build a PLAIN INT64 [7,-2] file gzip-compressed (codec=GZIP=2).
    /// Delegates to build_gzip_plain_int64_file_inner(gz, None) — None
    /// path uses the true on-disk gz length, byte-identical to the prior
    /// implementation. The normal path stays byte-identical so T3 tests
    /// remain green.
    fn build_gzip_plain_int64_file(gz: &[u8]) -> Vec<u8> {
        build_gzip_plain_int64_file_inner(gz, None)
    }

    #[test]
    fn extract_decodes_gzip_plain_int64() {
        // Independent authority (Python stdlib gzip — NOT our code).
        let f = build_gzip_plain_int64_file(GZ_7_NEG2);
        assert_eq!(
            extract(&f, &["id"]).expect("gzip"),
            vec![vec![PqValue::I64(7)], vec![PqValue::I64(-2)]],
        );
    }

    #[test]
    fn extract_gzip_uncompressed_snappy_identical() {
        // Source-format independence: same logical [7,-2] three ways.
        let g = extract(&build_gzip_plain_int64_file(GZ_7_NEG2), &["id"]).unwrap();
        let p = extract(&build_parquet_file(0, 0, 0, false), &["id"]).unwrap();
        let s = extract(&build_snappy_plain_int64_file(), &["id"]).unwrap();
        assert_eq!(g, p);
        assert_eq!(g, s);
        assert_eq!(g, vec![vec![PqValue::I64(7)], vec![PqValue::I64(-2)]]);
    }

    #[test]
    fn extract_rejects_lz4_codec_obj2c() {
        // Repurposed from extract_rejects_zstd_codec_obj2c: ZSTD(6) is now
        // SUPPORTED (SP136 wire); LZ4(4) is still Unsupported (OBJ-2c follow-on).
        let f = build_parquet_file(0, 4, 0, false); // codec=LZ4(4)=Other(4)
        assert!(
            matches!(extract(&f, &["id"]), Err(PqError::Unsupported(_))),
            "LZ4 codec must be Unsupported (OBJ-2c)"
        );
    }

    /// Genuine lying-compressed-size lock for gzip (folded from deferred
    /// GZIP-T3 review Minor): the PageHeader f3 compressed_page_size is
    /// set to 10_000_000 (far larger than the actual gzip on-disk bytes),
    /// while footer/FileMetaData remain fully parseable (same discipline
    /// as SP104's extract_snappy_lying_compressed_size_is_bad).
    ///
    /// Path: extract() → read_chunk_values → page_payload →
    /// `file.get(dstart..dstart+10_000_000)` → None →
    /// Err(Bad("page data truncated")). This exercises the page_payload
    /// bounds check for the GZIP path (NOT the footer-short path).
    ///
    /// build_gzip_plain_int64_file_inner(gz, Some(10_000_000)) sets
    /// compressed_page_size=10_000_000 while keeping the actual gz body
    /// at gz.len() bytes and leaving footer/mlen/PAR1 intact.
    /// build_gzip_plain_int64_file_inner(gz, None) — the normal path —
    /// is byte-identical to build_gzip_plain_int64_file(gz), so T3
    /// gzip tests remain green (confirmed by the full suite passing).
    #[test]
    fn extract_gzip_lying_compressed_size_is_bad() {
        let file = build_gzip_plain_int64_file_inner(GZ_7_NEG2, Some(10_000_000));
        let owned = file.clone();
        let r = std::panic::catch_unwind(move || extract(&owned, &["id"]));
        assert!(r.is_ok(), "must not panic");
        assert!(
            matches!(r.unwrap(), Err(PqError::Bad(_))),
            "lying compressed_page_size must yield PqError::Bad (page data truncated)"
        );
    }

    #[test]
    fn extract_rejects_missing_column() {
        // good file + missing column name → Bad
        let good_file = build_parquet_file(0, 0, 0, false);
        assert!(
            matches!(extract(&good_file, &["missing"]), Err(PqError::Bad(_))),
            "missing column must be Bad"
        );
    }

    /// Lock test for Fix 1: schema leaf says INT64(2) but ColumnMetaData
    /// says INT32(1). The strict ptype guard must fire and return Bad,
    /// NOT Unsupported (which is what decode_plain's `other=>Unsupported`
    /// arm would produce without the guard) and NOT a successful decode.
    #[test]
    fn extract_rejects_schema_chunk_type_mismatch() {
        // Schema leaf type = INT64(2), ColumnMetaData type = INT32(1).
        let mismatch_file = build_parquet_file_with_chunk_type(1); // INT32=1
        assert!(
            matches!(extract(&mismatch_file, &["id"]), Err(PqError::Bad(_))),
            "schema/chunk ptype mismatch must be Bad (Fix-1 gate)"
        );
    }

    /// Build a complete dict-encoded INT64 file for column "id":
    ///   logical rows [7,7,-2]; dictionary [7,-2]; indices [0,0,1].
    /// Layout: [PAR1][dict_hdr][dict_data][data_hdr][data_payload]
    ///         [FileMetaData][mlen u32 LE][PAR1]
    ///
    /// `dict_page_offset_override`: if `None`, the f11 dictionary_page_offset
    /// is the correct byte offset of the dict page (4). If `Some(v)`, the f11
    /// field encodes `v` instead — the rest of the file (dict page bytes, data
    /// page, schema, mlen, PAR1 framing) is byte-identical, so the footer +
    /// FileMetaData still decode cleanly; only the f11 i64 value changes.
    fn build_dict_int64_file_with_dict_offset(dict_page_offset_override: Option<i64>) -> Vec<u8> {
        let mut dict_data = Vec::new();
        dict_data.extend_from_slice(&7i64.to_le_bytes());
        dict_data.extend_from_slice(&(-2i64).to_le_bytes());
        let dbytes = dict_data.len() as i64; // 16
        let mut dict_hdr = Vec::new();
        dict_hdr.push(0x15); uv(&mut dict_hdr, zz(2));        // f1 type=DICTIONARY_PAGE(2)
        dict_hdr.push(0x15); uv(&mut dict_hdr, zz(dbytes));   // f2 uncompressed_page_size
        dict_hdr.push(0x15); uv(&mut dict_hdr, zz(dbytes));   // f3 compressed_page_size
        dict_hdr.push(0x4c);                                  // f7 struct (delta 3->7=4)
        dict_hdr.push(0x15); uv(&mut dict_hdr, zz(2));        // g1 num_values=2
        dict_hdr.push(0x15); uv(&mut dict_hdr, zz(2));        // g2 encoding=PLAIN_DICTIONARY(2)
        dict_hdr.push(0x12);                                  // g3 is_sorted=false
        dict_hdr.push(0x00);                                  // stop DictionaryPageHeader
        dict_hdr.push(0x00);                                  // stop PageHeader

        let data_payload: Vec<u8> = vec![0x01, 0x03, 0x04];
        let pbytes = data_payload.len() as i64; // 3
        let mut data_hdr = Vec::new();
        data_hdr.push(0x15); uv(&mut data_hdr, zz(0));        // f1 type=DATA_PAGE(0)
        data_hdr.push(0x15); uv(&mut data_hdr, zz(pbytes));   // f2 uncompressed_page_size
        data_hdr.push(0x15); uv(&mut data_hdr, zz(pbytes));   // f3 compressed_page_size
        data_hdr.push(0x2c);                                  // f5 struct (delta 3->5=2)
        data_hdr.push(0x15); uv(&mut data_hdr, zz(3));        // g1 num_values=3
        data_hdr.push(0x15); uv(&mut data_hdr, zz(2));        // g2 encoding=PLAIN_DICTIONARY(2)
        data_hdr.push(0x00);                                  // stop DataPageHeader
        data_hdr.push(0x00);                                  // stop PageHeader

        let correct_dict_page_offset: i64 = 4;
        let f11_dict_page_offset = dict_page_offset_override.unwrap_or(correct_dict_page_offset);
        let data_page_offset: i64 =
            4 + dict_hdr.len() as i64 + dict_data.len() as i64;

        let mut m = Vec::new();
        m.push(0x15); uv(&mut m, zz(2));                      // f1 version=2
        m.push(0x19); m.push(0x2c);                           // f2 list<SchemaElement> 2
        m.push(0x48); uv(&mut m, 6); m.extend_from_slice(b"schema"); // schema[0] name
        m.push(0x15); uv(&mut m, zz(1));                      // schema[0] f5 num_children=1
        m.push(0x00);                                         // stop schema[0]
        m.push(0x15); uv(&mut m, zz(2));                      // schema[1] f1 type=INT64(2)
        m.push(0x25); uv(&mut m, zz(0));                      // f3 repetition=REQUIRED
        m.push(0x18); uv(&mut m, 2); m.extend_from_slice(b"id"); // f4 name
        m.push(0x00);                                         // stop schema[1]
        m.push(0x16); uv(&mut m, zz(3));                      // f3 num_rows=3
        m.push(0x19); m.push(0x1c);                           // f4 list<RowGroup> 1
        m.push(0x19); m.push(0x1c);                           // RG f1 list<ColumnChunk> 1
        m.push(0x3c);                                         // ColumnChunk f3 ColumnMetaData
        m.push(0x15); uv(&mut m, zz(2));                      // CMD f1 type=INT64(2)
        m.push(0x19); m.push(0x15); uv(&mut m, zz(2));        // f2 encodings [PLAIN_DICTIONARY(2)]
        m.push(0x19); m.push(0x18); uv(&mut m, 2); m.extend_from_slice(b"id"); // f3 path ["id"]
        m.push(0x15); uv(&mut m, zz(0));                      // f4 codec=UNCOMPRESSED
        m.push(0x16); uv(&mut m, zz(3));                      // f5 num_values=3
        m.push(0x46); uv(&mut m, zz(data_page_offset));       // f9 data_page_offset (delta 5->9=4,i64)
        m.push(0x26); uv(&mut m, zz(f11_dict_page_offset));   // f11 dictionary_page_offset (delta 9->11=2,i64)
        m.push(0x00);                                         // stop ColumnMetaData
        m.push(0x00);                                         // stop ColumnChunk
        m.push(0x26); uv(&mut m, zz(3));                      // RG f3 num_rows=3
        m.push(0x00);                                         // stop RowGroup
        m.push(0x00);                                         // stop FileMetaData

        let mut f = Vec::new();
        f.extend_from_slice(b"PAR1");
        f.extend_from_slice(&dict_hdr);
        f.extend_from_slice(&dict_data);
        f.extend_from_slice(&data_hdr);
        f.extend_from_slice(&data_payload);
        let mlen = m.len() as u32;
        f.extend_from_slice(&m);
        f.extend_from_slice(&mlen.to_le_bytes());
        f.extend_from_slice(b"PAR1");
        f
    }

    /// Convenience wrapper: None path → byte-identical to previous
    /// `build_dict_int64_file()` so Task-3 tests are unaffected.
    fn build_dict_int64_file() -> Vec<u8> {
        build_dict_int64_file_with_dict_offset(None)
    }

    #[test]
    fn extract_decodes_dictionary_int64() {
        let file = build_dict_int64_file();
        let rows = extract(&file, &["id"]).expect("extract");
        assert_eq!(rows, vec![
            vec![PqValue::I64(7)],
            vec![PqValue::I64(7)],
            vec![PqValue::I64(-2)],
        ]);
    }

    #[test]
    fn extract_plain_and_dict_are_identical() {
        let plain = build_parquet_file(0, 0, 0, false);
        let plain_rows = extract(&plain, &["id"]).expect("plain");
        let dict_rows = extract(&build_dict_int64_file(), &["id"])
            .expect("dict");
        assert_eq!(plain_rows, vec![vec![PqValue::I64(7)],
                                    vec![PqValue::I64(-2)]]);
        assert_eq!(dict_rows, vec![vec![PqValue::I64(7)],
                                   vec![PqValue::I64(7)],
                                   vec![PqValue::I64(-2)]]);
        assert_eq!(plain_rows[0], dict_rows[0]);
        assert_eq!(plain_rows[1], dict_rows[2]);
    }

    #[test]
    fn extract_rejects_delta_encoding() {
        let file = build_parquet_file(5, 0, 0, false);
        assert!(
            matches!(extract(&file, &["id"]), Err(PqError::Unsupported(_))),
            "DELTA encoding must be Unsupported"
        );
    }

    #[test]
    fn extract_truncated_file_is_bad() {
        // Exercises the footer-short → Bad path: truncating to 8 bytes makes
        // footer::metadata_slice see file.len()<12 and return Err(Bad).
        // Honestly exercises: no-panic + typed-Bad on truncated input.
        let mut f = build_dict_int64_file();
        f.truncate(8); // footer/dict-page region now unreachable
        let owned = f.clone();
        let r = std::panic::catch_unwind(move || extract(&owned, &["id"]));
        assert!(r.is_ok(), "must not panic");
        assert!(matches!(r.unwrap(), Err(PqError::Bad(_))));
    }

    #[test]
    fn extract_dict_page_offset_past_eof_is_bad() {
        // Structurally valid footer + FileMetaData, but the f11
        // dictionary_page_offset points far past EOF → read_chunk_values
        // dict-page bounds check (`file.get(off..).ok_or_else(|| Bad("dict
        // page offset past EOF"))`) returns typed Bad, no panic. This
        // exercises the read_chunk_values dict-page-offset path specifically
        // (not the footer-short path).
        let file = build_dict_int64_file_with_dict_offset(Some(10_000_000));
        let owned = file.clone();
        let r = std::panic::catch_unwind(move || extract(&owned, &["id"]));
        assert!(r.is_ok(), "must not panic");
        assert!(matches!(r.unwrap(), Err(PqError::Bad(_))));
    }

    /// A Snappy "literal-only" block wrapping `raw` exactly (preamble +
    /// one literal). Valid spec-faithful Snappy (literal-only is the
    /// trivial correct encoding). Used to build Snappy test files.
    fn snappy_literal_block(raw: &[u8]) -> Vec<u8> {
        let mut b = Vec::new();
        // preamble varint = raw.len()
        let mut n = raw.len() as u64;
        loop {
            let byte = (n & 0x7f) as u8;
            n >>= 7;
            if n == 0 { b.push(byte); break; } else { b.push(byte | 0x80); }
        }
        // literal: if len-1 < 60 single tag; raw.len() here is small (16)
        let l1 = (raw.len() - 1) as u8;
        assert!((raw.len() as u64) >= 1 && l1 < 60, "helper: small literals only");
        b.push(l1 << 2); // tag, type 00
        b.extend_from_slice(raw);
        b
    }

    /// Inner builder for a PLAIN INT64 [7,-2] Snappy file.
    /// `comp_override`: `None` → use the true on-disk block length as
    /// compressed_page_size (correct file, decodes ok); `Some(v)` → use
    /// `v` instead (lying value, triggers page_payload bounds check).
    ///
    /// Analogous to `build_dict_int64_file_with_dict_offset(Option)` in
    /// SP103: single shared body, None path byte-identical to the prior
    /// `build_snappy_plain_int64_file()`.
    fn build_snappy_plain_int64_file_inner(comp_override: Option<i64>) -> Vec<u8> {
        let mut raw = Vec::new();
        raw.extend_from_slice(&7i64.to_le_bytes());
        raw.extend_from_slice(&(-2i64).to_le_bytes());
        let block = snappy_literal_block(&raw);     // on-disk page bytes
        let uncomp = raw.len() as i64;              // 16
        let comp = comp_override.unwrap_or(block.len() as i64); // 18 when None

        let mut hdr = Vec::new();
        hdr.push(0x15); uv(&mut hdr, zz(0));        // f1 type=DATA_PAGE(0)
        hdr.push(0x15); uv(&mut hdr, zz(uncomp));   // f2 uncompressed_page_size=16
        hdr.push(0x15); uv(&mut hdr, zz(comp));     // f3 compressed_page_size
        hdr.push(0x2c);                             // f5 DataPageHeader struct (delta 3->5=2)
        hdr.push(0x15); uv(&mut hdr, zz(2));        // g1 num_values=2
        hdr.push(0x15); uv(&mut hdr, zz(0));        // g2 encoding=PLAIN(0)
        hdr.push(0x00); hdr.push(0x00);             // stop DPH / PH

        let data_page_offset: i64 = 4;

        let mut m = Vec::new();
        m.push(0x15); uv(&mut m, zz(2));            // f1 version=2
        m.push(0x19); m.push(0x2c);                 // f2 list<SchemaElement> 2
        m.push(0x48); uv(&mut m, 6); m.extend_from_slice(b"schema");
        m.push(0x15); uv(&mut m, zz(1));            // schema[0] num_children=1
        m.push(0x00);
        m.push(0x15); uv(&mut m, zz(2));            // schema[1] f1 type=INT64
        m.push(0x25); uv(&mut m, zz(0));            // f3 repetition=REQUIRED
        m.push(0x18); uv(&mut m, 2); m.extend_from_slice(b"id");
        m.push(0x00);
        m.push(0x16); uv(&mut m, zz(2));            // f3 num_rows=2
        m.push(0x19); m.push(0x1c);                 // f4 list<RowGroup> 1
        m.push(0x19); m.push(0x1c);                 // RG f1 list<ColumnChunk> 1
        m.push(0x3c);                               // ColumnChunk f3 ColumnMetaData
        m.push(0x15); uv(&mut m, zz(2));            // CMD f1 type=INT64
        m.push(0x19); m.push(0x15); uv(&mut m, zz(0)); // f2 encodings [PLAIN]
        m.push(0x19); m.push(0x18); uv(&mut m, 2); m.extend_from_slice(b"id");
        m.push(0x15); uv(&mut m, zz(1));            // f4 codec=SNAPPY(1)
        m.push(0x16); uv(&mut m, zz(2));            // f5 num_values=2
        m.push(0x46); uv(&mut m, zz(data_page_offset)); // f9 data_page_offset=4
        m.push(0x00); m.push(0x00);                 // stop CMD / ColumnChunk
        m.push(0x26); uv(&mut m, zz(2));            // RG f3 num_rows=2
        m.push(0x00); m.push(0x00);                 // stop RG / FileMetaData

        let mut f = Vec::new();
        f.extend_from_slice(b"PAR1");
        f.extend_from_slice(&hdr);
        f.extend_from_slice(&block);
        let mlen = m.len() as u32;
        f.extend_from_slice(&m);
        f.extend_from_slice(&mlen.to_le_bytes());
        f.extend_from_slice(b"PAR1");
        f
    }

    /// Build a PLAIN INT64 [7,-2] file compressed with Snappy (codec=1).
    /// Delegates to `build_snappy_plain_int64_file_inner(None)` — None
    /// path uses the true on-disk block length, byte-identical to the
    /// prior standalone implementation.
    fn build_snappy_plain_int64_file() -> Vec<u8> {
        build_snappy_plain_int64_file_inner(None)
    }

    /// Genuine lying-compressed-size lock (option b): the PageHeader f3
    /// compressed_page_size is 10_000_000 (far larger than the actual
    /// on-disk Snappy block), while the footer/FileMetaData remain fully
    /// parseable. extract() reaches read_chunk_values → page_payload →
    /// `file.get(dstart..dstart+10_000_000)` → None →
    /// Err(Bad("page data truncated")). Wrapped in catch_unwind
    /// asserting no panic + Err(Bad).
    ///
    /// This exercises the page_payload bounds check, NOT the footer-short
    /// path. Named accurately: "lying_compressed_size".
    #[test]
    fn extract_snappy_lying_compressed_size_is_bad() {
        let file = build_snappy_plain_int64_file_inner(Some(10_000_000));
        let owned = file.clone();
        let r = std::panic::catch_unwind(move || extract(&owned, &["id"]));
        assert!(r.is_ok(), "must not panic");
        assert!(matches!(r.unwrap(), Err(PqError::Bad(_))));
    }

    #[test]
    fn extract_decodes_snappy_plain_int64() {
        let file = build_snappy_plain_int64_file();
        let rows = extract(&file, &["id"]).expect("snappy extract");
        assert_eq!(rows, vec![vec![PqValue::I64(7)], vec![PqValue::I64(-2)]]);
    }

    #[test]
    fn extract_snappy_and_uncompressed_identical() {
        // Same logical [7,-2]: existing build_parquet_file(0,0,0,false)
        // is the UNCOMPRESSED PLAIN baseline.
        let plain = extract(&build_parquet_file(0, 0, 0, false), &["id"])
            .expect("plain");
        let snap = extract(&build_snappy_plain_int64_file(), &["id"])
            .expect("snappy");
        assert_eq!(plain, snap); // source-format independence
        assert_eq!(snap, vec![vec![PqValue::I64(7)], vec![PqValue::I64(-2)]]);
    }

    // ── OBJ-2b-4 T2: flat OPTIONAL tests ─────────────────────────────────

    /// Flat OPTIONAL INT64 "id" file. `defs` (length = rows, values 0/1)
    /// + `present_vals` (the non-null i64s, len == #(defs==1)). PLAIN.
    /// Layout: [PAR1][page_hdr][page_payload][FileMetaData][mlen][PAR1].
    /// page_payload = [u32 LE deflen][def hybrid][PLAIN i64 present_vals].
    fn build_opt_plain_i64(defs: &[u8], present_vals: &[i64]) -> Vec<u8> {
        assert_eq!(
            present_vals.len(),
            defs.iter().filter(|&&d| d == 1).count()
        );
        let n = defs.len();
        // def-level hybrid: one bit-packed group of ceil(n/8)*8 (pad 0),
        // bit_width=1. header = (groups<<1)|1.
        let groups = ((n + 7) / 8).max(1) as u64;
        let mut def_hybrid = Vec::new();
        // varint(header)
        let mut h = (groups << 1) | 1;
        loop {
            let b = (h & 0x7f) as u8;
            h >>= 7;
            if h == 0 {
                def_hybrid.push(b);
                break;
            } else {
                def_hybrid.push(b | 0x80);
            }
        }
        let nbytes = groups as usize; // bit_width 1 → 1 byte/group
        let mut bits = vec![0u8; nbytes];
        for (i, &d) in defs.iter().enumerate() {
            if d == 1 {
                bits[i / 8] |= 1 << (i % 8);
            }
        }
        def_hybrid.extend_from_slice(&bits);
        let mut payload = Vec::new();
        payload.extend_from_slice(&(def_hybrid.len() as u32).to_le_bytes());
        payload.extend_from_slice(&def_hybrid);
        for v in present_vals {
            payload.extend_from_slice(&v.to_le_bytes());
        }
        let psz = payload.len() as i64;

        let mut hdr = Vec::new();
        hdr.push(0x15); uv(&mut hdr, zz(0));        // f1 type=DATA_PAGE(0)
        hdr.push(0x15); uv(&mut hdr, zz(psz));      // f2 uncompressed_page_size
        hdr.push(0x15); uv(&mut hdr, zz(psz));      // f3 compressed_page_size
        hdr.push(0x2c);                             // f5 DataPageHeader (delta 3->5=2, struct)
        hdr.push(0x15); uv(&mut hdr, zz(n as i64)); // g1 num_values = rows (incl nulls)
        hdr.push(0x15); uv(&mut hdr, zz(0));        // g2 encoding=PLAIN(0)
        hdr.push(0x00); hdr.push(0x00);

        let data_page_offset: i64 = 4;
        let mut m = Vec::new();
        m.push(0x15); uv(&mut m, zz(2));            // f1 version=2
        m.push(0x19); m.push(0x2c);                 // f2 list<SchemaElement> 2
        m.push(0x48); uv(&mut m, 6); m.extend_from_slice(b"schema");
        m.push(0x15); uv(&mut m, zz(1));            // root num_children=1
        m.push(0x00);
        m.push(0x15); uv(&mut m, zz(2));            // leaf f1 type=INT64
        m.push(0x25); uv(&mut m, zz(1));            // f3 repetition=OPTIONAL(1)
        m.push(0x18); uv(&mut m, 2); m.extend_from_slice(b"id");
        m.push(0x00);
        m.push(0x16); uv(&mut m, zz(n as i64));     // f3 num_rows = rows
        m.push(0x19); m.push(0x1c);                 // f4 list<RowGroup> 1
        m.push(0x19); m.push(0x1c);                 // RG f1 list<ColumnChunk> 1
        m.push(0x3c);                               // ColumnChunk f3 ColumnMetaData
        m.push(0x15); uv(&mut m, zz(2));            // CMD f1 type=INT64
        m.push(0x19); m.push(0x15); uv(&mut m, zz(0)); // f2 enc [PLAIN]
        m.push(0x19); m.push(0x18); uv(&mut m, 2); m.extend_from_slice(b"id");
        m.push(0x15); uv(&mut m, zz(0));            // f4 codec=UNCOMPRESSED
        m.push(0x16); uv(&mut m, zz(n as i64));     // f5 num_values = rows
        m.push(0x46); uv(&mut m, zz(data_page_offset));
        m.push(0x00); m.push(0x00);
        m.push(0x26); uv(&mut m, zz(n as i64));     // RG f3 num_rows = rows
        m.push(0x00); m.push(0x00);

        let mut f = Vec::new();
        f.extend_from_slice(b"PAR1");
        f.extend_from_slice(&hdr);
        f.extend_from_slice(&payload);
        let mlen = m.len() as u32;
        f.extend_from_slice(&m);
        f.extend_from_slice(&mlen.to_le_bytes());
        f.extend_from_slice(b"PAR1");
        f
    }

    #[test]
    fn extract_decodes_optional_int64_with_nulls() {
        // [7, null, -2]: defs [1,0,1], present [7,-2].
        let f = build_opt_plain_i64(&[1, 0, 1], &[7, -2]);
        let rows = extract(&f, &["id"]).expect("opt");
        assert_eq!(rows, vec![
            vec![PqValue::I64(7)], vec![PqValue::Null], vec![PqValue::I64(-2)],
        ]);
    }

    #[test]
    fn extract_optional_all_null_page() {
        let f = build_opt_plain_i64(&[0, 0], &[]);
        assert_eq!(
            extract(&f, &["id"]).expect("allnull"),
            vec![vec![PqValue::Null], vec![PqValue::Null]]
        );
    }

    #[test]
    fn extract_optional_all_present_page() {
        let f = build_opt_plain_i64(&[1, 1], &[7, -2]);
        assert_eq!(
            extract(&f, &["id"]).expect("allpresent"),
            vec![vec![PqValue::I64(7)], vec![PqValue::I64(-2)]]
        );
    }

    #[test]
    fn extract_rejects_repeated_obj2c() {
        // REPEATED(2) leaf still Unsupported (OBJ-2c).
        let f = build_parquet_file(0, 0, 2, false); // repetition=REPEATED(2)
        assert!(
            matches!(extract(&f, &["id"]), Err(PqError::Unsupported(_))),
            "REPEATED must be Unsupported (OBJ-2c)"
        );
    }

    /// Build a file with a nested (non-flat) schema: root → intermediate group → leaf.
    /// Schema has 3 SchemaElements: root(nc=1), group(nc=1), leaf.
    /// The flat_schema gate must reject this with Unsupported("nested schema: OBJ-2c").
    fn build_nested_schema_file() -> Vec<u8> {
        // Page: one PLAIN INT64 value (7). data_page_offset=4.
        let mut page_data = Vec::new();
        page_data.extend_from_slice(&7i64.to_le_bytes());
        let data_bytes = page_data.len() as i32; // 8

        let mut hdr = Vec::new();
        hdr.push(0x15); uv(&mut hdr, zz(0));               // f1 type=DATA_PAGE(0)
        hdr.push(0x15); uv(&mut hdr, zz(data_bytes as i64)); // f2 uncompressed_page_size
        hdr.push(0x15); uv(&mut hdr, zz(data_bytes as i64)); // f3 compressed_page_size
        hdr.push(0x2c);                                    // f5 DataPageHeader struct
        hdr.push(0x15); uv(&mut hdr, zz(1));               // g1 num_values=1
        hdr.push(0x15); uv(&mut hdr, zz(0));               // g2 encoding=PLAIN(0)
        hdr.push(0x00); hdr.push(0x00);

        let data_page_offset: i64 = 4;
        let mut m = Vec::new();
        m.push(0x15); uv(&mut m, zz(2));                   // f1 version=2
        // f2 list<SchemaElement> 3 structs: (3<<4)|12=0x3c
        m.push(0x19); m.push(0x3c);

        // schema[0] root GROUP: f4 name="schema", f5 num_children=1
        // NO f1 type (group). Field IDs reset to 0 at struct start.
        // f4 name: delta=4, binary=8 → (4<<4)|8=0x48
        m.push(0x48); uv(&mut m, 6); m.extend_from_slice(b"schema");
        // f5 num_children=1: delta f4→f5=1, i32=5 → (1<<4)|5=0x15; zz(1)=2
        m.push(0x15); uv(&mut m, zz(1));
        m.push(0x00); // stop schema[0]

        // schema[1] intermediate GROUP "g": no f1 type, f4 name="g", f5 num_children=1
        // f4 name: delta=4 → 0x48; len=1
        m.push(0x48); uv(&mut m, 1); m.extend_from_slice(b"g");
        // f5 num_children=1: delta f4→f5=1 → 0x15; zz(1)=2
        m.push(0x15); uv(&mut m, zz(1));
        m.push(0x00); // stop schema[1]

        // schema[2] leaf "id": f1 type=INT64(2), f3 rep=REQUIRED(0), f4 name="id"
        m.push(0x15); uv(&mut m, zz(2));                   // f1 type=INT64
        m.push(0x25); uv(&mut m, zz(0));                   // f3 repetition=REQUIRED
        m.push(0x18); uv(&mut m, 2); m.extend_from_slice(b"id"); // f4 name
        m.push(0x00); // stop schema[2]

        m.push(0x16); uv(&mut m, zz(1));                   // f3 num_rows=1
        m.push(0x19); m.push(0x1c);                        // f4 list<RowGroup> 1
        m.push(0x19); m.push(0x1c);                        // RG f1 list<ColumnChunk> 1
        m.push(0x3c);                                      // ColumnChunk f3 ColumnMetaData
        m.push(0x15); uv(&mut m, zz(2));                   // CMD f1 type=INT64
        m.push(0x19); m.push(0x15); uv(&mut m, zz(0));     // f2 encodings [PLAIN]
        // f3 path_in_schema: list<binary> of 2 elements ["g", "id"]
        // (SP144 T5: the chunk path MUST include the struct ancestor
        // for the per-leaf path lookup in extract_nested to succeed.)
        m.push(0x19); m.push(0x28);                        // f3 list header (size=2, binary)
        uv(&mut m, 1); m.extend_from_slice(b"g");
        uv(&mut m, 2); m.extend_from_slice(b"id");
        m.push(0x15); uv(&mut m, zz(0));                   // f4 codec=UNCOMPRESSED
        m.push(0x16); uv(&mut m, zz(1));                   // f5 num_values=1
        m.push(0x46); uv(&mut m, zz(data_page_offset));    // f9 data_page_offset
        m.push(0x00); m.push(0x00);                        // stop CMD / ColumnChunk
        m.push(0x26); uv(&mut m, zz(1));                   // RG f3 num_rows=1
        m.push(0x00); m.push(0x00);                        // stop RG / FileMetaData

        let mut f = Vec::new();
        f.extend_from_slice(b"PAR1");
        f.extend_from_slice(&hdr);
        f.extend_from_slice(&page_data);
        let mlen = m.len() as u32;
        f.extend_from_slice(&m);
        f.extend_from_slice(&mlen.to_le_bytes());
        f.extend_from_slice(b"PAR1");
        f
    }

    #[test]
    fn extract_decodes_nested_struct_schema_sp144() {
        // SP144 T5: the file's schema is `root → g (REQ group) →
        // id (REQ INT64)` — a single-field struct (no LIST annotation).
        // PRE-T5 this rejected as "non-LIST group column ... SP144
        // follow-up". POST-T5 it now classifies as NestedStruct and
        // decodes to a single Struct row with the lone {id: 7} pair.
        //
        // Requesting the inner-leaf name "id" stays rejected — "id"
        // isn't a top-level field — with Bad("column ... not found").
        // The file shape (root → g → id) means a top-level wanted
        // column MUST name "g", not the deep leaf.
        let f = build_nested_schema_file();
        let got = extract(&f, &["g"]).expect("struct schema must decode SP144 T5");
        assert_eq!(
            got,
            vec![vec![PqValue::Struct(vec![("id".into(), PqValue::I64(7))])]],
            "single-field REQ struct decodes to one Struct row"
        );
        assert!(
            matches!(extract(&f, &["id"]), Err(PqError::Bad(_))),
            "deep-leaf name not in top-level fields must be Bad(not found)"
        );
    }

    // ── SP143 T7: end-to-end nested decode inline-roundtrip tests ────
    //
    // These hand-build complete Parquet files (Thrift compact footer
    // including a nested LIST<primitive> schema + a row group + a
    // column chunk + a V1 data page carrying rep/def streams + PLAIN
    // INT64 values) and call `extract()` to prove the entire T1–T6
    // pipeline (footer→FileMetaData→SchemaTree+max_def/max_rep→
    // classify_column_plan→read_chunk_values_nested→decode_page_v1_nested→
    // assembly::assemble_list_primitive) wires up correctly BEFORE
    // SP143 T9 hands us real pyarrow-produced bytes.
    //
    // Three canonical shapes are exercised:
    //   T7a REQ-REP-REQ : List<INT64>          two records [[1,2,3],[10,20]]
    //   T7b REQ-REP-OPT : List<Optional<i64>>  one record [10, null, 20]
    //   T7c OPT-REP-REQ : Optional<List<i64>>  two records [null, [7, 8]]
    //
    // The shared builder `build_list_int64_file` factors out the
    // schema-encoding + footer-assembly so each test only writes its
    // own (rep, def, values) page payload.

    /// Per-shape config for `build_list_int64_file`.
    /// `outer_optional`/`element_optional` map onto the SchemaElement
    /// repetition_type for the my_list group and the element leaf
    /// respectively (the middle "list" group is always REPEATED).
    struct ListShape {
        outer_optional: bool,
        element_optional: bool,
        /// Top-level record count (FileMetaData.num_rows + RG.num_rows).
        num_rows: i64,
        /// (rep, def) pair count for the chunk (cc.num_values + page.num_values).
        num_values: i32,
        /// Raw page payload: rep section (4-byte LE prefix + hybrid)
        /// + def section (4-byte LE prefix + hybrid) + INT64 PLAIN values.
        page_payload: Vec<u8>,
    }

    /// Build a complete Parquet file with the canonical 4-element
    /// LIST<INT64> schema (root REQ → my_list {REQ|OPT, LIST(3)} →
    /// list REP → element {REQ|OPT, INT64}) and a single row group
    /// containing a single uncompressed V1 PLAIN data page whose
    /// payload is provided verbatim by the caller. Path encoding for
    /// the ColumnChunk is `["my_list", "list", "element"]` to match
    /// the parquet.thrift `path_in_schema` convention for nested
    /// columns.
    fn build_list_int64_file(shape: ListShape) -> Vec<u8> {
        let outer_rep: i64 = if shape.outer_optional { 1 } else { 0 }; // OPTIONAL=1 vs REQUIRED=0
        let element_rep: i64 = if shape.element_optional { 1 } else { 0 };

        // Page header: V1 DATA_PAGE, PLAIN encoding, num_values =
        // (rep,def) pair count. Reuses `page_header_bytes` from the
        // flat builders — its layout is shape-agnostic.
        let data_bytes = shape.page_payload.len() as i32;
        let hdr = page_header_bytes(shape.num_values, data_bytes);
        let data_page_offset: i64 = 4; // page header starts right after leading "PAR1"

        // FileMetaData (Thrift compact): version=2, 4 SchemaElements
        // in DFS preorder, num_rows, 1 RowGroup with 1 ColumnChunk.
        let mut m = Vec::new();
        // f1 version=2: (1<<4)|5=0x15, zz(2)=4
        m.push(0x15); uv(&mut m, zz(2));
        // f2 list<SchemaElement> 4 structs: delta 1->2=1 -> (1<<4)|9=0x19;
        // list-hdr (4<<4)|12=0x4c
        m.push(0x19); m.push(0x4c);

        // schema[0] root GROUP: f4 name="schema", f5 num_children=1
        // (REQUIRED is the default; f1 type omitted ⇒ group).
        m.push(0x48); uv(&mut m, 6); m.extend_from_slice(b"schema");
        m.push(0x15); uv(&mut m, zz(1));
        m.push(0x00);

        // schema[1] "my_list" GROUP: f3 repetition, f4 name, f5
        // num_children=1, f6 converted_type=LIST(3).
        // f3 repetition: delta 0->3=3 -> (3<<4)|5=0x35
        m.push(0x35); uv(&mut m, zz(outer_rep));
        // f4 name="my_list": delta 3->4=1 -> (1<<4)|8=0x18; len=7
        m.push(0x18); uv(&mut m, 7); m.extend_from_slice(b"my_list");
        // f5 num_children=1: delta 4->5=1 -> 0x15; zz(1)=2
        m.push(0x15); uv(&mut m, zz(1));
        // f6 converted_type=LIST(3): delta 5->6=1 -> 0x15; zz(3)=6
        m.push(0x15); uv(&mut m, zz(3));
        m.push(0x00);

        // schema[2] "list" GROUP: f3 repetition=REPEATED(2), f4 name,
        // f5 num_children=1. No converted_type.
        m.push(0x35); uv(&mut m, zz(2));
        m.push(0x18); uv(&mut m, 4); m.extend_from_slice(b"list");
        m.push(0x15); uv(&mut m, zz(1));
        m.push(0x00);

        // schema[3] "element" LEAF: f1 type=INT64(2), f3 repetition,
        // f4 name="element". num_children defaults to 0 (leaf).
        m.push(0x15); uv(&mut m, zz(2));
        m.push(0x25); uv(&mut m, zz(element_rep));
        m.push(0x18); uv(&mut m, 7); m.extend_from_slice(b"element");
        m.push(0x00);

        // f3 num_rows: delta 2->3=1 -> (1<<4)|6=0x16
        m.push(0x16); uv(&mut m, zz(shape.num_rows));

        // f4 list<RowGroup> 1: delta 3->4=1 -> (1<<4)|9=0x19;
        // list-hdr (1<<4)|12=0x1c
        m.push(0x19); m.push(0x1c);

        // RowGroup: f1 list<ColumnChunk> 1, f3 num_rows.
        m.push(0x19); m.push(0x1c);

        // ColumnChunk: f3 ColumnMetaData struct. delta 0->3=3 -> 0x3c.
        m.push(0x3c);

        // ColumnMetaData:
        // f1 type=INT64(2)
        m.push(0x15); uv(&mut m, zz(2));
        // f2 encodings list<Encoding> [PLAIN(0)]:
        //   delta 1->2=1 -> 0x19; list-hdr (1<<4)|5=0x15
        m.push(0x19); m.push(0x15); uv(&mut m, zz(0));
        // f3 path_in_schema list<string> ["my_list","list","element"]:
        //   delta 2->3=1 -> 0x19; list-hdr (3<<4)|8=0x38 (3 binary elements)
        m.push(0x19); m.push(0x38);
        uv(&mut m, 7); m.extend_from_slice(b"my_list");
        uv(&mut m, 4); m.extend_from_slice(b"list");
        uv(&mut m, 7); m.extend_from_slice(b"element");
        // f4 codec=UNCOMPRESSED(0)
        m.push(0x15); uv(&mut m, zz(0));
        // f5 num_values: delta 4->5=1 -> 0x16 (i64); = (rep,def) pair count
        m.push(0x16); uv(&mut m, zz(shape.num_values as i64));
        // f9 data_page_offset: delta 5->9=4 -> (4<<4)|6=0x46
        m.push(0x46); uv(&mut m, zz(data_page_offset));
        m.push(0x00); m.push(0x00); // stop ColumnMetaData / ColumnChunk

        // RowGroup f3 num_rows: delta 1->3=2 -> (2<<4)|6=0x26
        m.push(0x26); uv(&mut m, zz(shape.num_rows));
        m.push(0x00); // stop RowGroup
        m.push(0x00); // stop FileMetaData

        // Assemble: [PAR1][page_hdr][page_payload][FileMetaData][mlen u32 LE][PAR1]
        let mut f = Vec::new();
        f.extend_from_slice(b"PAR1");
        f.extend_from_slice(&hdr);
        f.extend_from_slice(&shape.page_payload);
        let mlen = m.len() as u32;
        f.extend_from_slice(&m);
        f.extend_from_slice(&mlen.to_le_bytes());
        f.extend_from_slice(b"PAR1");
        f
    }

    /// Build the V1 page payload for SP143 T7a: REQ-REP-REQ
    /// List<INT64>, two records `[[1,2,3], [10,20]]`. 5 (rep,def)
    /// pairs total, all items present.
    ///
    /// rep = [0,1,1,0,1], bit_width=1 (max_rep=1).
    ///   Bit-packed group of 8: header=(1<<1)|1=0x03;
    ///   LSB-first bits of [0,1,1,0,1,0,0,0] = 0b00010110 = 0x16.
    ///   Stream = [0x03, 0x16]; 4-byte LE length prefix = [0x02,0,0,0].
    /// def = [1,1,1,1,1], bit_width=1 (max_def=1).
    ///   Same bit-packed group; bits 0-4 set = 0b00011111 = 0x1F.
    ///   Stream = [0x03, 0x1F]; prefix = [0x02,0,0,0].
    /// values = 5 × INT64 LE = 40 bytes.
    fn t7a_page_payload() -> Vec<u8> {
        let mut p = Vec::new();
        p.extend_from_slice(&2u32.to_le_bytes()); // rep len prefix
        p.extend_from_slice(&[0x03, 0x16]);       // rep hybrid
        p.extend_from_slice(&2u32.to_le_bytes()); // def len prefix
        p.extend_from_slice(&[0x03, 0x1F]);       // def hybrid
        for v in [1i64, 2, 3, 10, 20] {
            p.extend_from_slice(&v.to_le_bytes());
        }
        p
    }

    #[test]
    fn extract_decodes_list_int64_required_inline_roundtrip() {
        // SP143 T7a: REQ-REP-REQ List<INT64>.
        // Two records `[[1,2,3], [10,20]]`; max_def=1, max_rep=1.
        // Proves the whole nested pipeline ends-to-end on a controlled
        // hand-built file (no pyarrow dependency).
        let file = build_list_int64_file(ListShape {
            outer_optional: false,
            element_optional: false,
            num_rows: 2,
            num_values: 5,
            page_payload: t7a_page_payload(),
        });
        let rows = extract(&file, &["my_list"]).expect("extract list<i64> required");
        assert_eq!(rows.len(), 2, "two top-level records");
        assert_eq!(rows[0], vec![
            PqValue::List(vec![PqValue::I64(1), PqValue::I64(2), PqValue::I64(3)])
        ]);
        assert_eq!(rows[1], vec![
            PqValue::List(vec![PqValue::I64(10), PqValue::I64(20)])
        ]);
    }

    /// SP143 T7b page payload: REQ-REP-OPT List<Optional<i64>>, one
    /// record `[10, null, 20]`. 3 (rep,def) pairs, 2 actual values.
    ///
    /// rep = [0,1,1], bit_width=1:
    ///   header 0x03; bits [0,1,1,0,0,0,0,0] = 0b00000110 = 0x06.
    ///   Stream = [0x03, 0x06]; prefix = [0x02,0,0,0].
    /// def = [2,1,2], bit_width=2 (max_def=2):
    ///   header 0x03; LSB-first 2-bit packing of [2,1,2,0,0,0,0,0]:
    ///     val0=10 (bits0-1), val1=01 (bits2-3), val2=10 (bits4-5),
    ///     padding=00 (bits6-7) -> 0b00 10 01 10 = 0x26; byte1=0x00.
    ///   Stream = [0x03, 0x26, 0x00]; prefix = [0x03,0,0,0].
    /// values = 2 × INT64 LE = [10, 20].
    fn t7b_page_payload() -> Vec<u8> {
        let mut p = Vec::new();
        p.extend_from_slice(&2u32.to_le_bytes());
        p.extend_from_slice(&[0x03, 0x06]);
        p.extend_from_slice(&3u32.to_le_bytes());
        p.extend_from_slice(&[0x03, 0x26, 0x00]);
        for v in [10i64, 20] {
            p.extend_from_slice(&v.to_le_bytes());
        }
        p
    }

    #[test]
    fn extract_decodes_list_optional_int64_inline_roundtrip() {
        // SP143 T7b: REQ-REP-OPT List<Optional<i64>>.
        // One record `[10, null, 20]`; max_def=2 (REP+inner OPT),
        // max_rep=1, outer_optional=false, element_optional=true.
        let file = build_list_int64_file(ListShape {
            outer_optional: false,
            element_optional: true,
            num_rows: 1,
            num_values: 3,
            page_payload: t7b_page_payload(),
        });
        let rows = extract(&file, &["my_list"]).expect("extract list<opt i64>");
        assert_eq!(rows.len(), 1, "one top-level record");
        assert_eq!(rows[0], vec![
            PqValue::List(vec![PqValue::I64(10), PqValue::Null, PqValue::I64(20)])
        ]);
    }

    /// SP143 T7c page payload: OPT-REP-REQ Optional<List<i64>>, two
    /// records `[null, [7, 8]]`. 3 (rep,def) pairs, 2 actual values.
    ///
    /// rep = [0,0,1], bit_width=1:
    ///   header 0x03; bits [0,0,1,0,0,0,0,0] = 0b00000100 = 0x04.
    ///   Stream = [0x03, 0x04]; prefix = [0x02,0,0,0].
    /// def = [0,2,2], bit_width=2 (max_def=2: outer OPT + REP):
    ///   header 0x03; LSB-first 2-bit packing of [0,2,2,0,0,0,0,0]:
    ///     val0=00 (bits0-1), val1=10 (bits2-3), val2=10 (bits4-5),
    ///     padding=00 (bits6-7) -> 0b00 10 10 00 = 0x28; byte1=0x00.
    ///   Stream = [0x03, 0x28, 0x00]; prefix = [0x03,0,0,0].
    /// values = 2 × INT64 LE = [7, 8].
    fn t7c_page_payload() -> Vec<u8> {
        let mut p = Vec::new();
        p.extend_from_slice(&2u32.to_le_bytes());
        p.extend_from_slice(&[0x03, 0x04]);
        p.extend_from_slice(&3u32.to_le_bytes());
        p.extend_from_slice(&[0x03, 0x28, 0x00]);
        for v in [7i64, 8] {
            p.extend_from_slice(&v.to_le_bytes());
        }
        p
    }

    #[test]
    fn extract_decodes_optional_list_int64_inline_roundtrip() {
        // SP143 T7c: OPT-REP-REQ Optional<List<i64>>.
        // Two records `[null, [7, 8]]`; max_def=2 (outer OPT + REP),
        // max_rep=1, outer_optional=true, element_optional=false.
        // Exercises the OuterNull def-case path through assemble_list_primitive.
        let file = build_list_int64_file(ListShape {
            outer_optional: true,
            element_optional: false,
            num_rows: 2,
            num_values: 3,
            page_payload: t7c_page_payload(),
        });
        let rows = extract(&file, &["my_list"]).expect("extract opt list<i64>");
        assert_eq!(rows.len(), 2, "two top-level records");
        assert_eq!(rows[0], vec![PqValue::Null]);
        assert_eq!(rows[1], vec![
            PqValue::List(vec![PqValue::I64(7), PqValue::I64(8)])
        ]);
    }

    // ── SP144 T6: end-to-end Map+struct decode inline-roundtrip tests ─
    //
    // These hand-build complete Parquet files (Thrift compact footer
    // including a Map / struct schema + a row group with TWO column
    // chunks + per-chunk V1 data pages) and call `extract()` to prove
    // the entire T1–T5 pipeline (footer→FileMetaData→SchemaTree+
    // recognize_logical_type→classify_column_plan {NestedMapKV |
    // NestedStruct}→read_chunk_values_nested_map /
    // read_chunk_values_nested_struct→assembly::assemble_map_kv /
    // assemble_struct) wires up correctly BEFORE SP144 T7 hands us real
    // pyarrow-produced bytes.
    //
    // Three canonical shapes are exercised:
    //   T6a REQ struct {id: i64, name: BYTE_ARRAY} 2 rows
    //   T6b OPT struct {id: i64, name: BYTE_ARRAY} 2 rows (1 NULL row)
    //   T6c REQ-REP-REQ-REQ Map<String, i64>       2 rows
    //
    // These are the FIRST tests in the crate that exercise a row group
    // with TWO column chunks (one per struct field / map K and V leaf);
    // the chunk-list header uses (2<<4)|12 = 0x2c instead of the 1-chunk
    // 0x1c that every existing builder uses.

    /// SP144 T6a: build a REQ struct file. Schema (4 DFS preorder elements):
    ///   [0] root: Group num_children=1 REQUIRED
    ///   [1] my_struct: Group num_children=2 REQUIRED
    ///   [2] id: Leaf INT64 REQUIRED
    ///   [3] name: Leaf BYTE_ARRAY REQUIRED
    ///
    /// Rows: [{id:1, name:"alice"}, {id:2, name:"bob"}]
    ///   id chunk: PLAIN INT64 [1, 2] (16 bytes, max_def=0, no def stream)
    ///   name chunk: PLAIN BYTE_ARRAY ["alice","bob"]
    ///     (4-byte LE len 5 + "alice" + 4-byte LE len 3 + "bob" = 16 bytes)
    fn build_struct_required_file() -> Vec<u8> {
        // -- id chunk page --
        let mut id_payload = Vec::new();
        id_payload.extend_from_slice(&1i64.to_le_bytes());
        id_payload.extend_from_slice(&2i64.to_le_bytes());
        let id_bytes = id_payload.len() as i32; // 16
        let id_hdr = page_header_bytes(2, id_bytes);

        // -- name chunk page --
        let mut name_payload = Vec::new();
        name_payload.extend_from_slice(&5u32.to_le_bytes());
        name_payload.extend_from_slice(b"alice");
        name_payload.extend_from_slice(&3u32.to_le_bytes());
        name_payload.extend_from_slice(b"bob");
        let name_bytes = name_payload.len() as i32; // 16
        let name_hdr = page_header_bytes(2, name_bytes);

        // Page offsets relative to the start of the file:
        // [PAR1: 4 bytes][id_hdr][id_payload][name_hdr][name_payload][footer]
        let id_page_offset: i64 = 4;
        let name_page_offset: i64 =
            4 + id_hdr.len() as i64 + id_payload.len() as i64;

        // -- FileMetaData --
        let mut m = Vec::new();
        // f1 version=2
        m.push(0x15); uv(&mut m, zz(2));
        // f2 list<SchemaElement> 4 structs: 0x19, list-hdr (4<<4)|12=0x4c
        m.push(0x19); m.push(0x4c);

        // schema[0] root GROUP num_children=1 REQUIRED.
        m.push(0x48); uv(&mut m, 6); m.extend_from_slice(b"schema");
        m.push(0x15); uv(&mut m, zz(1));
        m.push(0x00);

        // schema[1] my_struct GROUP num_children=2 REQUIRED (no f3 needed
        // since REQUIRED=0 is the implicit default — matches build_nested_schema_file).
        m.push(0x48); uv(&mut m, 9); m.extend_from_slice(b"my_struct");
        m.push(0x15); uv(&mut m, zz(2));
        m.push(0x00);

        // schema[2] id LEAF INT64 REQUIRED.
        m.push(0x15); uv(&mut m, zz(2));                   // f1 type=INT64
        m.push(0x25); uv(&mut m, zz(0));                   // f3 repetition=REQUIRED
        m.push(0x18); uv(&mut m, 2); m.extend_from_slice(b"id");
        m.push(0x00);

        // schema[3] name LEAF BYTE_ARRAY REQUIRED.
        m.push(0x15); uv(&mut m, zz(6));                   // f1 type=BYTE_ARRAY(6)
        m.push(0x25); uv(&mut m, zz(0));                   // f3 repetition=REQUIRED
        m.push(0x18); uv(&mut m, 4); m.extend_from_slice(b"name");
        m.push(0x00);

        // f3 num_rows=2
        m.push(0x16); uv(&mut m, zz(2));
        // f4 list<RowGroup> 1
        m.push(0x19); m.push(0x1c);

        // RowGroup: f1 list<ColumnChunk> with 2 chunks (id + name).
        // list-hdr (2<<4)|12 = 0x2c — TWO ColumnChunk structs.
        m.push(0x19); m.push(0x2c);

        // ColumnChunk #1 (id):
        m.push(0x3c);                                      // f3 ColumnMetaData (delta 0->3)
        m.push(0x15); uv(&mut m, zz(2));                   // CMD f1 type=INT64
        m.push(0x19); m.push(0x15); uv(&mut m, zz(0));     // f2 encodings [PLAIN(0)]
        // f3 path_in_schema list<binary> ["my_struct","id"] — 2 elements: (2<<4)|8=0x28
        m.push(0x19); m.push(0x28);
        uv(&mut m, 9); m.extend_from_slice(b"my_struct");
        uv(&mut m, 2); m.extend_from_slice(b"id");
        m.push(0x15); uv(&mut m, zz(0));                   // f4 codec=UNCOMPRESSED
        m.push(0x16); uv(&mut m, zz(2));                   // f5 num_values=2
        m.push(0x46); uv(&mut m, zz(id_page_offset));      // f9 data_page_offset
        m.push(0x00); m.push(0x00);                        // stop CMD / ColumnChunk #1

        // ColumnChunk #2 (name):
        m.push(0x3c);
        m.push(0x15); uv(&mut m, zz(6));                   // CMD f1 type=BYTE_ARRAY
        m.push(0x19); m.push(0x15); uv(&mut m, zz(0));     // f2 encodings [PLAIN]
        m.push(0x19); m.push(0x28);                        // f3 path: 2 elements
        uv(&mut m, 9); m.extend_from_slice(b"my_struct");
        uv(&mut m, 4); m.extend_from_slice(b"name");
        m.push(0x15); uv(&mut m, zz(0));                   // f4 codec=UNCOMPRESSED
        m.push(0x16); uv(&mut m, zz(2));                   // f5 num_values=2
        m.push(0x46); uv(&mut m, zz(name_page_offset));    // f9 data_page_offset
        m.push(0x00); m.push(0x00);                        // stop CMD / ColumnChunk #2

        // RowGroup f3 num_rows=2 (delta 1->3=2 -> 0x26)
        m.push(0x26); uv(&mut m, zz(2));
        m.push(0x00); // stop RowGroup
        m.push(0x00); // stop FileMetaData

        // Assemble: [PAR1][id_hdr][id_payload][name_hdr][name_payload][footer][mlen u32 LE][PAR1]
        let mut f = Vec::new();
        f.extend_from_slice(b"PAR1");
        f.extend_from_slice(&id_hdr);
        f.extend_from_slice(&id_payload);
        f.extend_from_slice(&name_hdr);
        f.extend_from_slice(&name_payload);
        let mlen = m.len() as u32;
        f.extend_from_slice(&m);
        f.extend_from_slice(&mlen.to_le_bytes());
        f.extend_from_slice(b"PAR1");
        f
    }

    #[test]
    fn extract_decodes_struct_required_inline_roundtrip() {
        // SP144 T6a: REQ struct {id: i64 REQ, name: BYTE_ARRAY REQ}, 2 rows.
        //
        // Validates the T1-T5 struct pipeline:
        //   FileMetaData (4-elem schema, 1 RG, 2 chunks) →
        //   classify_column_plan → recognize_logical_type=None →
        //   classify_struct_plan → NestedStruct{outer_optional:false, 2 fields} →
        //   read_chunk_values_nested_struct → read_chunk_values per field
        //   (flat REQUIRED path, max_def=0) → assemble_struct zip.
        let file = build_struct_required_file();
        let result = extract(&file, &["my_struct"]).expect("extract REQ struct");
        assert_eq!(result.len(), 2, "two rows");
        assert_eq!(result[0], vec![PqValue::Struct(vec![
            ("id".into(), PqValue::I64(1)),
            ("name".into(), PqValue::Bytes(b"alice".to_vec())),
        ])]);
        assert_eq!(result[1], vec![PqValue::Struct(vec![
            ("id".into(), PqValue::I64(2)),
            ("name".into(), PqValue::Bytes(b"bob".to_vec())),
        ])]);
    }

    /// SP144 T6b: build an OPT struct file. Schema:
    ///   [0] root: Group num_children=1 REQUIRED
    ///   [1] my_struct: Group num_children=2 OPTIONAL  ← OPT
    ///   [2] id: Leaf INT64 REQUIRED  (within OPT parent → max_def=1)
    ///   [3] name: Leaf BYTE_ARRAY REQUIRED  (max_def=1)
    ///
    /// Rows: [{id:1, name:"alice"}, NULL_struct]
    ///   id chunk: max_def=1, n=2, def=[1,0], 1 present value:
    ///     def_section: [0x02,0,0,0][0x03, 0x01]  (bit_width=1: header
    ///       (1<<1)|1=0x03; LSB-first bits [1,0,0,0,0,0,0,0] = 0x01)
    ///     values: [1i64 LE] = 8 bytes
    ///   name chunk: same def shape; values: [4-byte LE len 5]["alice"]
    fn build_struct_optional_file() -> Vec<u8> {
        // -- id chunk page (max_def=1, 2 rows, 1 present) --
        let id_def_len: u32 = 2;
        let id_def_hybrid: &[u8] = &[0x03, 0x01]; // 1 bit-packed group, bits [1,0,0,0,0,0,0,0]
        let mut id_payload = Vec::new();
        id_payload.extend_from_slice(&id_def_len.to_le_bytes());
        id_payload.extend_from_slice(id_def_hybrid);
        id_payload.extend_from_slice(&1i64.to_le_bytes()); // present value
        let id_bytes = id_payload.len() as i32;
        let id_hdr = page_header_bytes(2, id_bytes); // n=2 (positions)

        // -- name chunk page (max_def=1, 2 rows, 1 present) --
        let name_def_len: u32 = 2;
        let name_def_hybrid: &[u8] = &[0x03, 0x01];
        let mut name_payload = Vec::new();
        name_payload.extend_from_slice(&name_def_len.to_le_bytes());
        name_payload.extend_from_slice(name_def_hybrid);
        name_payload.extend_from_slice(&5u32.to_le_bytes()); // BYTE_ARRAY len prefix
        name_payload.extend_from_slice(b"alice");
        let name_bytes = name_payload.len() as i32;
        let name_hdr = page_header_bytes(2, name_bytes);

        let id_page_offset: i64 = 4;
        let name_page_offset: i64 =
            4 + id_hdr.len() as i64 + id_payload.len() as i64;

        let mut m = Vec::new();
        m.push(0x15); uv(&mut m, zz(2));                       // f1 version=2
        m.push(0x19); m.push(0x4c);                            // f2 list<SchemaElement> 4

        // schema[0] root GROUP REQUIRED num_children=1
        m.push(0x48); uv(&mut m, 6); m.extend_from_slice(b"schema");
        m.push(0x15); uv(&mut m, zz(1));
        m.push(0x00);

        // schema[1] my_struct GROUP OPTIONAL(1) num_children=2
        // f3 repetition=OPTIONAL(1): delta 0->3=3 -> 0x35
        m.push(0x35); uv(&mut m, zz(1));
        m.push(0x18); uv(&mut m, 9); m.extend_from_slice(b"my_struct");
        m.push(0x15); uv(&mut m, zz(2));                       // num_children=2
        m.push(0x00);

        // schema[2] id LEAF INT64 REQUIRED
        m.push(0x15); uv(&mut m, zz(2));                       // f1 type=INT64
        m.push(0x25); uv(&mut m, zz(0));                       // f3 REQUIRED
        m.push(0x18); uv(&mut m, 2); m.extend_from_slice(b"id");
        m.push(0x00);

        // schema[3] name LEAF BYTE_ARRAY REQUIRED
        m.push(0x15); uv(&mut m, zz(6));                       // f1 type=BYTE_ARRAY
        m.push(0x25); uv(&mut m, zz(0));                       // f3 REQUIRED
        m.push(0x18); uv(&mut m, 4); m.extend_from_slice(b"name");
        m.push(0x00);

        // f3 num_rows=2
        m.push(0x16); uv(&mut m, zz(2));
        // f4 list<RowGroup> 1
        m.push(0x19); m.push(0x1c);

        // RowGroup: 2 ColumnChunks (id + name) — 0x2c list header.
        m.push(0x19); m.push(0x2c);

        // ColumnChunk #1 (id)
        m.push(0x3c);
        m.push(0x15); uv(&mut m, zz(2));                       // type=INT64
        m.push(0x19); m.push(0x15); uv(&mut m, zz(0));         // encodings [PLAIN]
        m.push(0x19); m.push(0x28);                            // path: 2 elements
        uv(&mut m, 9); m.extend_from_slice(b"my_struct");
        uv(&mut m, 2); m.extend_from_slice(b"id");
        m.push(0x15); uv(&mut m, zz(0));                       // codec=UNCOMPRESSED
        m.push(0x16); uv(&mut m, zz(2));                       // num_values=2
        m.push(0x46); uv(&mut m, zz(id_page_offset));
        m.push(0x00); m.push(0x00);

        // ColumnChunk #2 (name)
        m.push(0x3c);
        m.push(0x15); uv(&mut m, zz(6));                       // type=BYTE_ARRAY
        m.push(0x19); m.push(0x15); uv(&mut m, zz(0));         // encodings [PLAIN]
        m.push(0x19); m.push(0x28);
        uv(&mut m, 9); m.extend_from_slice(b"my_struct");
        uv(&mut m, 4); m.extend_from_slice(b"name");
        m.push(0x15); uv(&mut m, zz(0));
        m.push(0x16); uv(&mut m, zz(2));
        m.push(0x46); uv(&mut m, zz(name_page_offset));
        m.push(0x00); m.push(0x00);

        m.push(0x26); uv(&mut m, zz(2));                       // RG f3 num_rows=2
        m.push(0x00);                                          // stop RG
        m.push(0x00);                                          // stop FileMetaData

        let mut f = Vec::new();
        f.extend_from_slice(b"PAR1");
        f.extend_from_slice(&id_hdr);
        f.extend_from_slice(&id_payload);
        f.extend_from_slice(&name_hdr);
        f.extend_from_slice(&name_payload);
        let mlen = m.len() as u32;
        f.extend_from_slice(&m);
        f.extend_from_slice(&mlen.to_le_bytes());
        f.extend_from_slice(b"PAR1");
        f
    }

    #[test]
    fn extract_decodes_struct_optional_with_null_row_inline_roundtrip() {
        // SP144 T6b: OPT struct, with one all-Null row.
        //
        // Pipeline: classify_struct_plan with outer_optional=true →
        // 2 fields each with max_def_level=1 → per-field flat decode
        // via decode_page (max_def=1 arm: [def_len][def_hybrid][PLAIN present])
        // → scatter_nulls produces [I64(1), Null] for id, [Bytes("alice"), Null]
        // for name → assemble_struct with outer_optional=true:
        //   row 0: both fields present → Struct(id=1, name="alice")
        //   row 1: both fields Null → all-Null heuristic → PqValue::Null
        let file = build_struct_optional_file();
        let result = extract(&file, &["my_struct"]).expect("extract OPT struct");
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], vec![PqValue::Struct(vec![
            ("id".into(), PqValue::I64(1)),
            ("name".into(), PqValue::Bytes(b"alice".to_vec())),
        ])]);
        assert_eq!(result[1], vec![PqValue::Null]);
    }

    /// SP144 T6c: build a REQ-REP-REQ-REQ Map<String, i64> file with
    /// 2 records: [{"a":1, "b":2}, {"x":7}].
    /// Schema (5 DFS preorder elements):
    ///   [0] root: Group num_children=1 REQUIRED
    ///   [1] my_map: Group num_children=1 REQUIRED converted_type=MAP(1)
    ///   [2] key_value: Group num_children=2 REPEATED
    ///   [3] key: Leaf BYTE_ARRAY REQUIRED
    ///   [4] value: Leaf INT64 REQUIRED
    ///
    /// Schema-derived levels for both key and value:
    ///   max_def_level = 1 (one REPEATED ancestor, both leaves REQUIRED)
    ///   max_rep_level = 1
    ///
    /// Rep stream [0,1,0] bit_width=1: header (1<<1)|1=0x03; LSB-first
    ///   bits [0,1,0,0,0,0,0,0] = 0b00000010 = 0x02. Stream [0x03, 0x02],
    ///   length prefix u32 LE = 2.
    /// Def stream [1,1,1] bit_width=1: header 0x03; bits [1,1,1,0,0,0,0,0]
    ///   = 0b00000111 = 0x07. Stream [0x03, 0x07], length prefix 2.
    ///
    /// key chunk: rep+def + 3 PLAIN BYTE_ARRAY ("a","b","x") = 15 byte values.
    /// value chunk: rep+def + 3 INT64 (1,2,7) = 24 byte values.
    fn build_map_string_i64_required_file() -> Vec<u8> {
        // Shared rep+def section for both chunks (byte-identical since
        // both leaves share the REPEATED middle ancestor).
        let rep_len: u32 = 2;
        let rep_hybrid: &[u8] = &[0x03, 0x02];
        let def_len: u32 = 2;
        let def_hybrid: &[u8] = &[0x03, 0x07];

        // -- key chunk page --
        let mut key_payload = Vec::new();
        key_payload.extend_from_slice(&rep_len.to_le_bytes());
        key_payload.extend_from_slice(rep_hybrid);
        key_payload.extend_from_slice(&def_len.to_le_bytes());
        key_payload.extend_from_slice(def_hybrid);
        for s in ["a", "b", "x"] {
            key_payload.extend_from_slice(&(s.len() as u32).to_le_bytes());
            key_payload.extend_from_slice(s.as_bytes());
        }
        let key_bytes = key_payload.len() as i32;
        let key_hdr = page_header_bytes(3, key_bytes); // 3 (rep,def) pairs

        // -- value chunk page --
        let mut value_payload = Vec::new();
        value_payload.extend_from_slice(&rep_len.to_le_bytes());
        value_payload.extend_from_slice(rep_hybrid);
        value_payload.extend_from_slice(&def_len.to_le_bytes());
        value_payload.extend_from_slice(def_hybrid);
        for v in [1i64, 2, 7] {
            value_payload.extend_from_slice(&v.to_le_bytes());
        }
        let value_bytes = value_payload.len() as i32;
        let value_hdr = page_header_bytes(3, value_bytes);

        let key_page_offset: i64 = 4;
        let value_page_offset: i64 =
            4 + key_hdr.len() as i64 + key_payload.len() as i64;

        let mut m = Vec::new();
        m.push(0x15); uv(&mut m, zz(2));                       // f1 version=2
        // f2 list<SchemaElement> 5: list-hdr (5<<4)|12 = 0x5c
        m.push(0x19); m.push(0x5c);

        // schema[0] root GROUP num_children=1 REQUIRED.
        m.push(0x48); uv(&mut m, 6); m.extend_from_slice(b"schema");
        m.push(0x15); uv(&mut m, zz(1));
        m.push(0x00);

        // schema[1] my_map GROUP num_children=1 REQUIRED converted_type=MAP(1).
        // REQUIRED is default (no f3 needed). Use f4 name, f5 nc, f6 ct.
        m.push(0x48); uv(&mut m, 6); m.extend_from_slice(b"my_map");
        m.push(0x15); uv(&mut m, zz(1));                       // num_children=1
        m.push(0x15); uv(&mut m, zz(1));                       // converted_type=MAP(1)
        m.push(0x00);

        // schema[2] key_value GROUP REPEATED(2) num_children=2.
        // f3 REPEATED: delta 0->3=3 -> 0x35; zz(2)=4
        m.push(0x35); uv(&mut m, zz(2));
        m.push(0x18); uv(&mut m, 9); m.extend_from_slice(b"key_value");
        m.push(0x15); uv(&mut m, zz(2));
        m.push(0x00);

        // schema[3] key LEAF BYTE_ARRAY REQUIRED.
        m.push(0x15); uv(&mut m, zz(6));                       // f1 type=BYTE_ARRAY
        m.push(0x25); uv(&mut m, zz(0));                       // f3 REQUIRED
        m.push(0x18); uv(&mut m, 3); m.extend_from_slice(b"key");
        m.push(0x00);

        // schema[4] value LEAF INT64 REQUIRED.
        m.push(0x15); uv(&mut m, zz(2));                       // f1 type=INT64
        m.push(0x25); uv(&mut m, zz(0));                       // f3 REQUIRED
        m.push(0x18); uv(&mut m, 5); m.extend_from_slice(b"value");
        m.push(0x00);

        // f3 num_rows=2 (top-level records)
        m.push(0x16); uv(&mut m, zz(2));
        // f4 list<RowGroup> 1
        m.push(0x19); m.push(0x1c);

        // RowGroup: 2 ColumnChunks (key + value).
        m.push(0x19); m.push(0x2c);

        // ColumnChunk #1 (key)
        m.push(0x3c);
        m.push(0x15); uv(&mut m, zz(6));                       // type=BYTE_ARRAY
        m.push(0x19); m.push(0x15); uv(&mut m, zz(0));         // encodings [PLAIN]
        // f3 path: 3 elements ["my_map","key_value","key"] -> (3<<4)|8=0x38
        m.push(0x19); m.push(0x38);
        uv(&mut m, 6); m.extend_from_slice(b"my_map");
        uv(&mut m, 9); m.extend_from_slice(b"key_value");
        uv(&mut m, 3); m.extend_from_slice(b"key");
        m.push(0x15); uv(&mut m, zz(0));                       // codec=UNCOMPRESSED
        m.push(0x16); uv(&mut m, zz(3));                       // num_values=3 (rep/def pairs)
        m.push(0x46); uv(&mut m, zz(key_page_offset));
        m.push(0x00); m.push(0x00);

        // ColumnChunk #2 (value)
        m.push(0x3c);
        m.push(0x15); uv(&mut m, zz(2));                       // type=INT64
        m.push(0x19); m.push(0x15); uv(&mut m, zz(0));         // encodings [PLAIN]
        m.push(0x19); m.push(0x38);
        uv(&mut m, 6); m.extend_from_slice(b"my_map");
        uv(&mut m, 9); m.extend_from_slice(b"key_value");
        uv(&mut m, 5); m.extend_from_slice(b"value");
        m.push(0x15); uv(&mut m, zz(0));
        m.push(0x16); uv(&mut m, zz(3));
        m.push(0x46); uv(&mut m, zz(value_page_offset));
        m.push(0x00); m.push(0x00);

        m.push(0x26); uv(&mut m, zz(2));                       // RG num_rows=2
        m.push(0x00);                                          // stop RG
        m.push(0x00);                                          // stop FileMetaData

        let mut f = Vec::new();
        f.extend_from_slice(b"PAR1");
        f.extend_from_slice(&key_hdr);
        f.extend_from_slice(&key_payload);
        f.extend_from_slice(&value_hdr);
        f.extend_from_slice(&value_payload);
        let mlen = m.len() as u32;
        f.extend_from_slice(&m);
        f.extend_from_slice(&mlen.to_le_bytes());
        f.extend_from_slice(b"PAR1");
        f
    }

    #[test]
    fn extract_decodes_map_string_i64_required_inline_roundtrip() {
        // SP144 T6c: REQ-REP-REQ-REQ Map<String, i64>, 2 records.
        //
        // assemble_map_kv walk with max_def=1, outer_opt=false, value_opt=false:
        //   R0 (rep=0, def=1): no previous map, start new map → push ("a"->1)
        //   R1 (rep=1, def=1): continuing → push ("b"->2) into current map
        //   R2 (rep=0, def=1): flush previous → push Map[{"a"->1,"b"->2}];
        //                       start new map → push ("x"->7)
        //   trailing flush → push Map[{"x"->7}]
        let file = build_map_string_i64_required_file();
        let result = extract(&file, &["my_map"]).expect("extract Map<String,i64>");
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], vec![PqValue::Map(vec![
            (PqValue::Bytes(b"a".to_vec()), PqValue::I64(1)),
            (PqValue::Bytes(b"b".to_vec()), PqValue::I64(2)),
        ])]);
        assert_eq!(result[1], vec![PqValue::Map(vec![
            (PqValue::Bytes(b"x".to_vec()), PqValue::I64(7)),
        ])]);
    }

    /// Build an OPTIONAL+dict INT64 file for column "id".
    /// Logical rows: [7, null, 7]. defs=[1,0,1], dict=[7], present_count=2.
    /// Dict page: PLAIN [7i64]. Data page: PLAIN_DICTIONARY(2), n=3.
    /// Data page body after def-level stream = RLE dict indices for 2 present
    /// values (both index 0 → value 7).
    /// Dict index body: [0x01 (bit_width=1)][0x03 (1 bit-packed group hdr)]
    ///   [0x00 (bits: index0=0, index1=0 → both 0)].
    fn build_opt_dict_int64_file() -> Vec<u8> {
        // -- Dictionary page --
        let mut dict_data = Vec::new();
        dict_data.extend_from_slice(&7i64.to_le_bytes()); // dict[0] = 7
        let dbytes = dict_data.len() as i64; // 8

        let mut dict_hdr = Vec::new();
        dict_hdr.push(0x15); uv(&mut dict_hdr, zz(2));      // f1 type=DICTIONARY_PAGE(2)
        dict_hdr.push(0x15); uv(&mut dict_hdr, zz(dbytes)); // f2 uncompressed_page_size
        dict_hdr.push(0x15); uv(&mut dict_hdr, zz(dbytes)); // f3 compressed_page_size
        dict_hdr.push(0x4c);                                // f7 DictionaryPageHeader struct (delta 3->7=4)
        dict_hdr.push(0x15); uv(&mut dict_hdr, zz(1));      // g1 num_values=1
        dict_hdr.push(0x15); uv(&mut dict_hdr, zz(2));      // g2 encoding=PLAIN_DICTIONARY(2)
        dict_hdr.push(0x12);                                // g3 is_sorted=false
        dict_hdr.push(0x00); dict_hdr.push(0x00);           // stop DPH / PH

        // -- Data page --
        // n=3 rows (including nulls). defs=[1,0,1].
        // Def-level stream: 1 bit-packed group of 3 bits, bit_width=1.
        // groups=1 (ceil(3/8)=1), header = (1<<1)|1 = 3 (varint: 0x03).
        // bits byte: bit0=def[0]=1, bit1=def[1]=0, bit2=def[2]=1 → 0b00000101=0x05.
        // def_hybrid = [0x03, 0x05], def_len_prefix = 2 as u32 LE = [0x02,0,0,0].
        // present_count=2. Dict index body for 2 present values (both index 0):
        // bit_width=1, 1 bit-packed group: header=(1<<1)|1=3 (varint 0x03),
        // bits byte: bit0=0(idx0), bit1=0(idx1) → 0x00.
        // Full dict index body: [0x01 (bit_width byte)][0x03][0x00].
        let def_len: u32 = 2;
        let def_hybrid: &[u8] = &[0x03, 0x05]; // varint(3) + bits byte
        let dict_idx_body: &[u8] = &[0x01, 0x03, 0x00]; // bit_width=1, 1 grp hdr, bits

        let mut data_payload = Vec::new();
        data_payload.extend_from_slice(&def_len.to_le_bytes()); // [0x02,0,0,0]
        data_payload.extend_from_slice(def_hybrid);
        data_payload.extend_from_slice(dict_idx_body);
        let pbytes = data_payload.len() as i64;

        let mut data_hdr = Vec::new();
        data_hdr.push(0x15); uv(&mut data_hdr, zz(0));      // f1 type=DATA_PAGE(0)
        data_hdr.push(0x15); uv(&mut data_hdr, zz(pbytes)); // f2 uncompressed_page_size
        data_hdr.push(0x15); uv(&mut data_hdr, zz(pbytes)); // f3 compressed_page_size
        data_hdr.push(0x2c);                                // f5 DataPageHeader struct
        data_hdr.push(0x15); uv(&mut data_hdr, zz(3));      // g1 num_values=3 (rows incl nulls)
        data_hdr.push(0x15); uv(&mut data_hdr, zz(2));      // g2 encoding=PLAIN_DICTIONARY(2)
        data_hdr.push(0x00); data_hdr.push(0x00);           // stop DPH / PH

        let dict_page_offset: i64 = 4;
        let data_page_offset: i64 = 4 + dict_hdr.len() as i64 + dict_data.len() as i64;

        let mut m = Vec::new();
        m.push(0x15); uv(&mut m, zz(2));                    // f1 version=2
        m.push(0x19); m.push(0x2c);                         // f2 list<SchemaElement> 2
        m.push(0x48); uv(&mut m, 6); m.extend_from_slice(b"schema");
        m.push(0x15); uv(&mut m, zz(1));                    // root num_children=1
        m.push(0x00);
        m.push(0x15); uv(&mut m, zz(2));                    // leaf f1 type=INT64
        m.push(0x25); uv(&mut m, zz(1));                    // f3 repetition=OPTIONAL(1)
        m.push(0x18); uv(&mut m, 2); m.extend_from_slice(b"id");
        m.push(0x00);
        m.push(0x16); uv(&mut m, zz(3));                    // f3 num_rows=3
        m.push(0x19); m.push(0x1c);                         // f4 list<RowGroup> 1
        m.push(0x19); m.push(0x1c);                         // RG f1 list<ColumnChunk> 1
        m.push(0x3c);                                       // ColumnChunk f3 ColumnMetaData
        m.push(0x15); uv(&mut m, zz(2));                    // CMD f1 type=INT64
        m.push(0x19); m.push(0x15); uv(&mut m, zz(2));      // f2 encodings [PLAIN_DICTIONARY(2)]
        m.push(0x19); m.push(0x18); uv(&mut m, 2); m.extend_from_slice(b"id");
        m.push(0x15); uv(&mut m, zz(0));                    // f4 codec=UNCOMPRESSED
        m.push(0x16); uv(&mut m, zz(3));                    // f5 num_values=3
        m.push(0x46); uv(&mut m, zz(data_page_offset));     // f9 data_page_offset
        m.push(0x26); uv(&mut m, zz(dict_page_offset));     // f11 dictionary_page_offset
        m.push(0x00); m.push(0x00);                         // stop CMD / ColumnChunk
        m.push(0x26); uv(&mut m, zz(3));                    // RG f3 num_rows=3
        m.push(0x00); m.push(0x00);                         // stop RG / FileMetaData

        let mut f = Vec::new();
        f.extend_from_slice(b"PAR1");
        f.extend_from_slice(&dict_hdr);
        f.extend_from_slice(&dict_data);
        f.extend_from_slice(&data_hdr);
        f.extend_from_slice(&data_payload);
        let mlen = m.len() as u32;
        f.extend_from_slice(&m);
        f.extend_from_slice(&mlen.to_le_bytes());
        f.extend_from_slice(b"PAR1");
        f
    }

    #[test]
    fn extract_decodes_optional_dict_int64_with_nulls() {
        // [7, null, 7]: defs [1,0,1], dict [7], indices both 0.
        let f = build_opt_dict_int64_file();
        let rows = extract(&f, &["id"]).expect("opt-dict");
        assert_eq!(rows, vec![
            vec![PqValue::I64(7)], vec![PqValue::Null], vec![PqValue::I64(7)],
        ]);
    }

    // ── OBJ-2c-3 Task 3: DATA_PAGE_V2 hand-builders + tests ──────────
    //
    // The V2 PageHeader byte sequence below is derived independently
    // from parquet.thrift (it mirrors the meta.rs
    // `pageheader_decodes_data_page_header_v2_field8` fixture, itself
    // hand-derived) — NOT produced by the code under test. A failing
    // KAT means the decode path is wrong.

    /// V2 PLAIN INT64 file. `defs` (rows, 0/1) + `present_vals` (the
    /// non-null i64s). REQUIRED ⇒ defs all 1 & def_len 0; OPTIONAL ⇒
    /// def-level RLE-hybrid bytes (NOT 4-byte-prefixed) of length
    /// def_len. codec=UNCOMPRESSED, is_compressed=false. Layout:
    /// [PAR1][PageHeaderV2][rep(0)][def(def_len)][values PLAIN]
    /// [FileMetaData][mlen u32 LE][PAR1].
    fn build_v2_plain_i64(defs: &[u8], present_vals: &[i64], optional: bool) -> Vec<u8> {
        assert_eq!(present_vals.len(), defs.iter().filter(|&&d| d == 1).count());
        let n = defs.len();
        let nulls = defs.iter().filter(|&&d| d == 0).count();
        // def-level section: OPTIONAL ⇒ one bit-packed group of 8,
        // bit_width 1, NOT length-prefixed (V2). header=(1<<1)|1=0x03;
        // bits byte: bit i set iff defs[i]==1 (i<8). REQUIRED ⇒ empty.
        let def_section: Vec<u8> = if optional {
            let mut bits = 0u8;
            for (i, &d) in defs.iter().enumerate() {
                if d == 1 && i < 8 {
                    bits |= 1 << i;
                }
            }
            vec![0x03, bits]
        } else {
            Vec::new()
        };
        let def_len = def_section.len() as i64;
        let mut values = Vec::new();
        for v in present_vals {
            values.extend_from_slice(&v.to_le_bytes());
        }
        // page payload = [rep(0)][def_section][values]; rep section
        // empty (rep_levels_byte_length=0).
        let mut payload = Vec::new();
        payload.extend_from_slice(&def_section);
        payload.extend_from_slice(&values);
        let psz = payload.len() as i64; // uncompressed == compressed

        let mut hdr = Vec::new();
        hdr.push(0x15); uv(&mut hdr, zz(3));            // f1 type=DATA_PAGE_V2(3)
        hdr.push(0x15); uv(&mut hdr, zz(psz));          // f2 uncompressed
        hdr.push(0x15); uv(&mut hdr, zz(psz));          // f3 compressed
        hdr.push(0x5c);                                 // f8 struct (delta 3->8=5)
        hdr.push(0x15); uv(&mut hdr, zz(n as i64));     // g1 num_values
        hdr.push(0x15); uv(&mut hdr, zz(nulls as i64)); // g2 num_nulls
        hdr.push(0x15); uv(&mut hdr, zz(n as i64));     // g3 num_rows
        hdr.push(0x15); uv(&mut hdr, zz(0));            // g4 encoding=PLAIN
        hdr.push(0x15); uv(&mut hdr, zz(def_len));      // g5 def_levels_byte_length
        hdr.push(0x15); uv(&mut hdr, zz(0));            // g6 rep_levels_byte_length
        hdr.push(0x12);                                 // g7 is_compressed=false
        hdr.push(0x00); hdr.push(0x00);                 // stop DPHv2 / stop PH

        let data_page_offset: i64 = 4;
        // FileMetaData mirrors build_opt_plain_i64 exactly, swapping
        // repetition zz(if optional {1} else {0}).
        let mut m = Vec::new();
        m.push(0x15); uv(&mut m, zz(2));                // f1 version=2
        m.push(0x19); m.push(0x2c);                     // f2 list<SchemaElement> 2
        m.push(0x48); uv(&mut m, 6); m.extend_from_slice(b"schema");
        m.push(0x15); uv(&mut m, zz(1));                // root num_children=1
        m.push(0x00);
        m.push(0x15); uv(&mut m, zz(2));                // leaf f1 type=INT64
        m.push(0x25); uv(&mut m, zz(if optional { 1 } else { 0 })); // f3 repetition
        m.push(0x18); uv(&mut m, 2); m.extend_from_slice(b"id");
        m.push(0x00);
        m.push(0x16); uv(&mut m, zz(n as i64));         // f3 num_rows
        m.push(0x19); m.push(0x1c);                     // f4 list<RowGroup> 1
        m.push(0x19); m.push(0x1c);                     // RG f1 list<ColumnChunk> 1
        m.push(0x3c);                                   // ColumnChunk f3 CMD
        m.push(0x15); uv(&mut m, zz(2));                // CMD f1 type=INT64
        m.push(0x19); m.push(0x15); uv(&mut m, zz(0));  // f2 enc [PLAIN]
        m.push(0x19); m.push(0x18); uv(&mut m, 2); m.extend_from_slice(b"id");
        m.push(0x15); uv(&mut m, zz(0));                // f4 codec=UNCOMPRESSED
        m.push(0x16); uv(&mut m, zz(n as i64));         // f5 num_values
        m.push(0x46); uv(&mut m, zz(data_page_offset)); // f9 data_page_offset
        m.push(0x00); m.push(0x00);                     // stop CMD / ColumnChunk
        m.push(0x26); uv(&mut m, zz(n as i64));         // RG f3 num_rows
        m.push(0x00); m.push(0x00);                     // stop RG / FileMetaData

        let mut f = Vec::new();
        f.extend_from_slice(b"PAR1");
        f.extend_from_slice(&hdr);
        f.extend_from_slice(&payload);
        let mlen = m.len() as u32;
        f.extend_from_slice(&m);
        f.extend_from_slice(&mlen.to_le_bytes());
        f.extend_from_slice(b"PAR1");
        f
    }

    #[test]
    fn extract_decodes_v2_plain_required_int64() {
        let f = build_v2_plain_i64(&[1, 1], &[7, -2], false);
        assert_eq!(
            extract(&f, &["id"]).expect("v2 req"),
            vec![vec![PqValue::I64(7)], vec![PqValue::I64(-2)]]
        );
    }

    #[test]
    fn extract_decodes_v2_plain_optional_int64_with_nulls() {
        // [7, null, -2]: defs [1,0,1], present [7,-2]; def_section
        // = [0x03, 0b00000101=0x05] (NOT 4-byte-prefixed — V2).
        let f = build_v2_plain_i64(&[1, 0, 1], &[7, -2], true);
        assert_eq!(
            extract(&f, &["id"]).expect("v2 opt"),
            vec![vec![PqValue::I64(7)], vec![PqValue::Null], vec![PqValue::I64(-2)]]
        );
    }

    #[test]
    fn extract_v2_and_v1_plain_identical() {
        // Source-format independence: same logical [7,-2] V2 vs the
        // existing V1 build_parquet_file(0,0,0,false).
        let v2 = extract(&build_v2_plain_i64(&[1, 1], &[7, -2], false), &["id"]).unwrap();
        let v1 = extract(&build_parquet_file(0, 0, 0, false), &["id"]).unwrap();
        assert_eq!(v2, v1);
        assert_eq!(v2, vec![vec![PqValue::I64(7)], vec![PqValue::I64(-2)]]);
    }

    /// V2 + DICTIONARY: a V1-style DICTIONARY_PAGE (mirrors the SP105
    /// `build_opt_dict_int64_file` dict page) then a DATA_PAGE_V2 whose
    /// values section (after the rep(0)/def split) is
    /// `[bit_width][RLE-hybrid dict indices]`. Logical `[7,null,7]`,
    /// dict `[7]`, indices both 0 → `[[I64(7)],[Null],[I64(7)]]`.
    /// def_section `[0x03,0x05]` for [1,0,1]; values section
    /// `[0x01,0x03,0x00]` per the SP105 opt-dict KAT.
    fn build_v2_dict_int64_file() -> Vec<u8> {
        // -- Dictionary page (mirrors build_opt_dict_int64_file) --
        let mut dict_data = Vec::new();
        dict_data.extend_from_slice(&7i64.to_le_bytes()); // dict[0] = 7
        let dbytes = dict_data.len() as i64; // 8

        let mut dict_hdr = Vec::new();
        dict_hdr.push(0x15); uv(&mut dict_hdr, zz(2));      // f1 type=DICTIONARY_PAGE(2)
        dict_hdr.push(0x15); uv(&mut dict_hdr, zz(dbytes)); // f2 uncompressed_page_size
        dict_hdr.push(0x15); uv(&mut dict_hdr, zz(dbytes)); // f3 compressed_page_size
        dict_hdr.push(0x4c);                                // f7 DictionaryPageHeader struct
        dict_hdr.push(0x15); uv(&mut dict_hdr, zz(1));      // g1 num_values=1
        dict_hdr.push(0x15); uv(&mut dict_hdr, zz(2));      // g2 encoding=PLAIN_DICTIONARY(2)
        dict_hdr.push(0x12);                                // g3 is_sorted=false
        dict_hdr.push(0x00); dict_hdr.push(0x00);           // stop DPH / PH

        // -- DATA_PAGE_V2: n=3 rows, defs=[1,0,1] → 2 present.
        // def_section (V2, NOT length-prefixed): [0x03, 0x05].
        // values section = [bit_width=1][hybrid hdr 0x03][bits 0x00]
        // → 2 present dict indices both 0.
        let def_section: &[u8] = &[0x03, 0x05];
        let values_section: &[u8] = &[0x01, 0x03, 0x00];
        let def_len = def_section.len() as i64; // 2
        let mut payload = Vec::new();
        payload.extend_from_slice(def_section);
        payload.extend_from_slice(values_section);
        let psz = payload.len() as i64;

        let mut data_hdr = Vec::new();
        data_hdr.push(0x15); uv(&mut data_hdr, zz(3));      // f1 type=DATA_PAGE_V2(3)
        data_hdr.push(0x15); uv(&mut data_hdr, zz(psz));    // f2 uncompressed
        data_hdr.push(0x15); uv(&mut data_hdr, zz(psz));    // f3 compressed
        data_hdr.push(0x5c);                                // f8 struct (delta 3->8=5)
        data_hdr.push(0x15); uv(&mut data_hdr, zz(3));      // g1 num_values=3
        data_hdr.push(0x15); uv(&mut data_hdr, zz(1));      // g2 num_nulls=1
        data_hdr.push(0x15); uv(&mut data_hdr, zz(3));      // g3 num_rows=3
        data_hdr.push(0x15); uv(&mut data_hdr, zz(2));      // g4 encoding=PLAIN_DICTIONARY(2)
        data_hdr.push(0x15); uv(&mut data_hdr, zz(def_len)); // g5 def_levels_byte_length=2
        data_hdr.push(0x15); uv(&mut data_hdr, zz(0));      // g6 rep_levels_byte_length=0
        data_hdr.push(0x12);                                // g7 is_compressed=false
        data_hdr.push(0x00); data_hdr.push(0x00);           // stop DPHv2 / PH

        let dict_page_offset: i64 = 4;
        let data_page_offset: i64 = 4 + dict_hdr.len() as i64 + dict_data.len() as i64;

        let mut m = Vec::new();
        m.push(0x15); uv(&mut m, zz(2));                    // f1 version=2
        m.push(0x19); m.push(0x2c);                         // f2 list<SchemaElement> 2
        m.push(0x48); uv(&mut m, 6); m.extend_from_slice(b"schema");
        m.push(0x15); uv(&mut m, zz(1));                    // root num_children=1
        m.push(0x00);
        m.push(0x15); uv(&mut m, zz(2));                    // leaf f1 type=INT64
        m.push(0x25); uv(&mut m, zz(1));                    // f3 repetition=OPTIONAL(1)
        m.push(0x18); uv(&mut m, 2); m.extend_from_slice(b"id");
        m.push(0x00);
        m.push(0x16); uv(&mut m, zz(3));                    // f3 num_rows=3
        m.push(0x19); m.push(0x1c);                         // f4 list<RowGroup> 1
        m.push(0x19); m.push(0x1c);                         // RG f1 list<ColumnChunk> 1
        m.push(0x3c);                                       // ColumnChunk f3 CMD
        m.push(0x15); uv(&mut m, zz(2));                    // CMD f1 type=INT64
        m.push(0x19); m.push(0x15); uv(&mut m, zz(2));      // f2 encodings [PLAIN_DICTIONARY(2)]
        m.push(0x19); m.push(0x18); uv(&mut m, 2); m.extend_from_slice(b"id");
        m.push(0x15); uv(&mut m, zz(0));                    // f4 codec=UNCOMPRESSED
        m.push(0x16); uv(&mut m, zz(3));                    // f5 num_values=3
        m.push(0x46); uv(&mut m, zz(data_page_offset));     // f9 data_page_offset
        m.push(0x26); uv(&mut m, zz(dict_page_offset));     // f11 dictionary_page_offset
        m.push(0x00); m.push(0x00);                         // stop CMD / ColumnChunk
        m.push(0x26); uv(&mut m, zz(3));                    // RG f3 num_rows=3
        m.push(0x00); m.push(0x00);                         // stop RG / FileMetaData

        let mut f = Vec::new();
        f.extend_from_slice(b"PAR1");
        f.extend_from_slice(&dict_hdr);
        f.extend_from_slice(&dict_data);
        f.extend_from_slice(&data_hdr);
        f.extend_from_slice(&payload);
        let mlen = m.len() as u32;
        f.extend_from_slice(&m);
        f.extend_from_slice(&mlen.to_le_bytes());
        f.extend_from_slice(b"PAR1");
        f
    }

    #[test]
    fn extract_decodes_v2_dict_int64() {
        // [7, null, 7]: defs [1,0,1], dict [7], indices both 0.
        let f = build_v2_dict_int64_file();
        assert_eq!(
            extract(&f, &["id"]).expect("v2-dict"),
            vec![vec![PqValue::I64(7)], vec![PqValue::Null], vec![PqValue::I64(7)]]
        );
    }

    // ── OBJ-2c-3 Task 3 review: V1 fallible-check ORDERING lock ───────
    //
    // The `match ph.page_type` gate must NOT change any V1 observable,
    // including for hostile multi-malformed input. Pre-OBJ-2c-3 the V1
    // `page_type == 0` path ran its fallible checks in this exact order:
    //   1. n  = usize::try_from(ph.dp_num_values)  → "num_values range"
    //   2. dstart = off.checked_add(hlen)          → "page hdr len ovf"
    //   3. comp = usize::try_from(ph.compressed_size) → "page comp size range"
    //   4. uncomp = usize::try_from(ph.uncompressed_size) → "page size range"
    //   5. page_payload → dict-guard → decode_page
    // `dstart` (step 2) is `off (=4) + hlen (small)` and can never
    // overflow here, so NOTHING fallible sits between the `dp_num_values`
    // check (1) and the `compressed_size` check (3): a file with BOTH
    // `dp_num_values < 0` and `compressed_page_size < 0` MUST surface
    // the `dp_num_values` error ("num_values range") because that check
    // runs first. An accidental hoist of the comp/uncomp computation
    // ahead of the `0 =>` arm's `n` check would instead surface "page
    // comp size range" — exactly the e5fd553 defect this test locks.
    //
    // Bytes are hand-derived: a V1 DATA_PAGE PageHeader identical to
    // `page_header_bytes` except f3 `compressed_page_size` and g1
    // `dp_num_values` are set to i32 `-1` (zigzag of -1 is 1 → single
    // byte 0x01; `usize::try_from(-1i32)` fails for BOTH). FileMetaData
    // still declares chunk num_values=2 so the data-page loop is entered.
    fn build_v1_dp_num_values_and_comp_both_negative() -> Vec<u8> {
        // PLAIN page data: 7i64 + (-2)i64 LE (16 bytes) — never reached,
        // both range checks fire before any payload slicing.
        let mut page_data = Vec::new();
        page_data.extend_from_slice(&7i64.to_le_bytes());
        page_data.extend_from_slice(&(-2i64).to_le_bytes());

        // Hand-built PageHeader: f1 type=DATA_PAGE(0); f2 uncompressed=16
        // (valid); f3 compressed = -1 (malformed); f5 DataPageHeader{
        // g1 num_values = -1 (malformed); g2 encoding=PLAIN(0) }.
        let mut h = Vec::new();
        h.push(0x15); uv(&mut h, zz(0));  // f1 page_type=DATA_PAGE(0)
        h.push(0x15); uv(&mut h, zz(16)); // f2 uncompressed_page_size=16 (valid)
        h.push(0x15); uv(&mut h, zz(-1)); // f3 compressed_page_size=-1 (MALFORMED)
        h.push(0x2c);                     // f5 DataPageHeader struct (delta 3->5=2)
        h.push(0x15); uv(&mut h, zz(-1)); // g1 num_values=-1 (MALFORMED)
        h.push(0x15); uv(&mut h, zz(0));  // g2 encoding=PLAIN(0)
        h.push(0x00); h.push(0x00);       // stop DPH / stop PH

        let data_page_offset: i64 = 4;
        // Reuse the canonical one-column INT64 REQUIRED metadata; it
        // declares chunk num_values=2 so the loop reaches the page.
        let meta = filemetadata_bytes(0, 0, 0, 2, data_page_offset);
        let mlen = meta.len() as u32;

        let mut f = Vec::new();
        f.extend_from_slice(b"PAR1");
        f.extend_from_slice(&h);
        f.extend_from_slice(&page_data);
        f.extend_from_slice(&meta);
        f.extend_from_slice(&mlen.to_le_bytes());
        f.extend_from_slice(b"PAR1");
        f
    }

    #[test]
    fn v1_check_order_num_values_before_comp_size_unchanged() {
        // BOTH dp_num_values and compressed_size are malformed. Pre-T3
        // (and post-fix) the `dp_num_values` check runs FIRST → the
        // "num_values range" error wins. Against the e5fd553 hoist the
        // comp/uncomp checks ran before the `0 =>` arm, so this would
        // have produced "page comp size range" instead — proving the
        // defect and that the fix restores exact V1 ordering.
        let f = build_v1_dp_num_values_and_comp_both_negative();
        match extract(&f, &["id"]) {
            Err(PqError::Bad(msg)) => {
                assert!(
                    msg.contains("num_values"),
                    "expected the V1 dp_num_values check to win (pre-T3 \
                     ordering), got: {msg:?}"
                );
                assert!(
                    !msg.contains("comp size"),
                    "comp-size error must NOT win — that is the e5fd553 \
                     hoist defect; got: {msg:?}"
                );
            }
            other => panic!("expected PqError::Bad(num_values…), got {other:?}"),
        }
    }
}

// ── OBJ-2b-4 Task 4 PENTEST PASS — OPTIONAL def-level decode + null scatter ──
//
// Lock tests for the OPTIONAL (nullable) decode path: def-level stream
// truncation/lying-length/def>1/count-mismatch/dict-OOB/non-flat-schema,
// and positive correctness locks (all-null / all-present / mixed scatter).
// Each hostile case is wrapped in catch_unwind asserting no panic AND a
// typed Err. The positive locks assert exact Ok rows; a failure there
// means decode_page/scatter is wrong — never weaken the positive lock.
#[cfg(test)]
mod pentest_optional {
    use super::*;

    // ── inline Thrift compact helpers (mirrors mod tests) ──────────────
    fn uv(out: &mut Vec<u8>, mut v: u64) {
        loop {
            let b = (v & 0x7f) as u8;
            v >>= 7;
            if v == 0 { out.push(b); break; } else { out.push(b | 0x80); }
        }
    }
    fn zz(v: i64) -> u64 { ((v << 1) ^ (v >> 63)) as u64 }

    // ── re-declared builder: flat OPTIONAL PLAIN INT64 "id" file ───────
    //
    // Mirrors `mod tests::build_opt_plain_i64` exactly (not pub there).
    // `defs` (len = rows, values 0/1) + `present_vals` (non-null i64s).
    fn build_opt_plain_i64(defs: &[u8], present_vals: &[i64]) -> Vec<u8> {
        assert_eq!(present_vals.len(), defs.iter().filter(|&&d| d == 1).count());
        let n = defs.len();
        let groups = ((n + 7) / 8).max(1) as u64;
        let mut def_hybrid = Vec::new();
        let mut h = (groups << 1) | 1;
        loop {
            let b = (h & 0x7f) as u8; h >>= 7;
            if h == 0 { def_hybrid.push(b); break; } else { def_hybrid.push(b | 0x80); }
        }
        let nbytes = groups as usize;
        let mut bits = vec![0u8; nbytes];
        for (i, &d) in defs.iter().enumerate() {
            if d == 1 { bits[i / 8] |= 1 << (i % 8); }
        }
        def_hybrid.extend_from_slice(&bits);
        let mut payload = Vec::new();
        payload.extend_from_slice(&(def_hybrid.len() as u32).to_le_bytes());
        payload.extend_from_slice(&def_hybrid);
        for v in present_vals { payload.extend_from_slice(&v.to_le_bytes()); }
        let psz = payload.len() as i64;
        let n_i = n as i64;

        let mut hdr = Vec::new();
        hdr.push(0x15); uv(&mut hdr, zz(0));
        hdr.push(0x15); uv(&mut hdr, zz(psz));
        hdr.push(0x15); uv(&mut hdr, zz(psz));
        hdr.push(0x2c);
        hdr.push(0x15); uv(&mut hdr, zz(n_i));
        hdr.push(0x15); uv(&mut hdr, zz(0));
        hdr.push(0x00); hdr.push(0x00);

        let mut m = Vec::new();
        m.push(0x15); uv(&mut m, zz(2));
        m.push(0x19); m.push(0x2c);
        m.push(0x48); uv(&mut m, 6); m.extend_from_slice(b"schema");
        m.push(0x15); uv(&mut m, zz(1));
        m.push(0x00);
        m.push(0x15); uv(&mut m, zz(2));       // leaf type=INT64
        m.push(0x25); uv(&mut m, zz(1));       // f3 repetition=OPTIONAL(1)
        m.push(0x18); uv(&mut m, 2); m.extend_from_slice(b"id");
        m.push(0x00);
        m.push(0x16); uv(&mut m, zz(n_i));     // num_rows
        m.push(0x19); m.push(0x1c);
        m.push(0x19); m.push(0x1c);
        m.push(0x3c);
        m.push(0x15); uv(&mut m, zz(2));       // CMD type=INT64
        m.push(0x19); m.push(0x15); uv(&mut m, zz(0)); // enc [PLAIN]
        m.push(0x19); m.push(0x18); uv(&mut m, 2); m.extend_from_slice(b"id");
        m.push(0x15); uv(&mut m, zz(0));       // codec=UNCOMPRESSED
        m.push(0x16); uv(&mut m, zz(n_i));     // num_values
        m.push(0x46); uv(&mut m, zz(4));       // data_page_offset=4
        m.push(0x00); m.push(0x00);
        m.push(0x26); uv(&mut m, zz(n_i));     // RG num_rows
        m.push(0x00); m.push(0x00);

        let mut f = Vec::new();
        f.extend_from_slice(b"PAR1");
        f.extend_from_slice(&hdr);
        f.extend_from_slice(&payload);
        let mlen = m.len() as u32;
        f.extend_from_slice(&m);
        f.extend_from_slice(&mlen.to_le_bytes());
        f.extend_from_slice(b"PAR1");
        f
    }

    // ── re-declared builder: non-flat schema (root → group → leaf) ─────
    //
    // Mirrors `mod tests::build_nested_schema_file` (not pub there).
    fn build_nested_schema_file() -> Vec<u8> {
        let mut page_data = Vec::new();
        page_data.extend_from_slice(&7i64.to_le_bytes());
        let data_bytes = page_data.len() as i64;

        let mut hdr = Vec::new();
        hdr.push(0x15); uv(&mut hdr, zz(0));
        hdr.push(0x15); uv(&mut hdr, zz(data_bytes));
        hdr.push(0x15); uv(&mut hdr, zz(data_bytes));
        hdr.push(0x2c);
        hdr.push(0x15); uv(&mut hdr, zz(1));
        hdr.push(0x15); uv(&mut hdr, zz(0));
        hdr.push(0x00); hdr.push(0x00);

        let mut m = Vec::new();
        m.push(0x15); uv(&mut m, zz(2));
        m.push(0x19); m.push(0x3c); // 3 SchemaElements
        // schema[0] root group: f4 name="schema", f5 nc=1
        m.push(0x48); uv(&mut m, 6); m.extend_from_slice(b"schema");
        m.push(0x15); uv(&mut m, zz(1));
        m.push(0x00);
        // schema[1] intermediate group "g": no f1 type, f4 name="g", f5 nc=1
        m.push(0x48); uv(&mut m, 1); m.extend_from_slice(b"g");
        m.push(0x15); uv(&mut m, zz(1));
        m.push(0x00);
        // schema[2] leaf "id": f1 INT64, f3 REQUIRED, f4 name
        m.push(0x15); uv(&mut m, zz(2));
        m.push(0x25); uv(&mut m, zz(0));
        m.push(0x18); uv(&mut m, 2); m.extend_from_slice(b"id");
        m.push(0x00);
        m.push(0x16); uv(&mut m, zz(1));
        m.push(0x19); m.push(0x1c);
        m.push(0x19); m.push(0x1c);
        m.push(0x3c);
        m.push(0x15); uv(&mut m, zz(2));
        m.push(0x19); m.push(0x15); uv(&mut m, zz(0));
        // SP144 T5: path_in_schema list of 2 ["g", "id"]
        m.push(0x19); m.push(0x28);
        uv(&mut m, 1); m.extend_from_slice(b"g");
        uv(&mut m, 2); m.extend_from_slice(b"id");
        m.push(0x15); uv(&mut m, zz(0));
        m.push(0x16); uv(&mut m, zz(1));
        m.push(0x46); uv(&mut m, zz(4));
        m.push(0x00); m.push(0x00);
        m.push(0x26); uv(&mut m, zz(1));
        m.push(0x00); m.push(0x00);

        let mut f = Vec::new();
        f.extend_from_slice(b"PAR1");
        f.extend_from_slice(&hdr);
        f.extend_from_slice(&page_data);
        let mlen = m.len() as u32;
        f.extend_from_slice(&m);
        f.extend_from_slice(&mlen.to_le_bytes());
        f.extend_from_slice(b"PAR1");
        f
    }

    // ── re-declared builder: OPTIONAL+dict INT64 file ──────────────────
    //
    // Mirrors `mod tests::build_opt_dict_int64_file` (not pub there).
    // Logical rows [7, null, 7]: dict=[7], defs=[1,0,1], indices=[0,0].
    // `oob_bits`: None ⇒ 0x00 valid indices (both present values → index 0);
    // Some(0xFF) ⇒ indices 1,1 OOB for dict len 1 → Bad from resolve_dict_indices.
    fn build_opt_dict_int64_file_with_oob(oob_bits: Option<u8>) -> Vec<u8> {
        let mut dict_data = Vec::new();
        dict_data.extend_from_slice(&7i64.to_le_bytes());
        let dbytes = dict_data.len() as i64;

        let mut dict_hdr = Vec::new();
        dict_hdr.push(0x15); uv(&mut dict_hdr, zz(2));
        dict_hdr.push(0x15); uv(&mut dict_hdr, zz(dbytes));
        dict_hdr.push(0x15); uv(&mut dict_hdr, zz(dbytes));
        dict_hdr.push(0x4c);
        dict_hdr.push(0x15); uv(&mut dict_hdr, zz(1));
        dict_hdr.push(0x15); uv(&mut dict_hdr, zz(2));
        dict_hdr.push(0x12);
        dict_hdr.push(0x00); dict_hdr.push(0x00);

        // def-level: defs=[1,0,1], groups=1, bit_width=1.
        // header varint = (1<<1)|1 = 3 → [0x03]; bits byte: bit0=1,bit1=0,bit2=1 → 0x05.
        let def_hybrid: &[u8] = &[0x03, 0x05];
        let def_len: u32 = 2;
        // dict-index body for 2 present values, both index=0 (normal),
        // or override bits byte to force OOB index.
        // bit_width=1, 1 group: header=0x03, bits byte
        let idx_bits = oob_bits.unwrap_or(0x00); // 0x00=index 0,0; 0xFF=index 1,1 (OOB: dict len=1)
        let dict_idx_body: Vec<u8> = vec![0x01, 0x03, idx_bits];

        let mut data_payload = Vec::new();
        data_payload.extend_from_slice(&def_len.to_le_bytes());
        data_payload.extend_from_slice(def_hybrid);
        data_payload.extend_from_slice(&dict_idx_body);
        let pbytes = data_payload.len() as i64;

        let mut data_hdr = Vec::new();
        data_hdr.push(0x15); uv(&mut data_hdr, zz(0));
        data_hdr.push(0x15); uv(&mut data_hdr, zz(pbytes));
        data_hdr.push(0x15); uv(&mut data_hdr, zz(pbytes));
        data_hdr.push(0x2c);
        data_hdr.push(0x15); uv(&mut data_hdr, zz(3)); // n=3 rows incl nulls
        data_hdr.push(0x15); uv(&mut data_hdr, zz(2)); // PLAIN_DICTIONARY(2)
        data_hdr.push(0x00); data_hdr.push(0x00);

        let dict_page_offset: i64 = 4;
        let data_page_offset: i64 = 4 + dict_hdr.len() as i64 + dict_data.len() as i64;

        let mut m = Vec::new();
        m.push(0x15); uv(&mut m, zz(2));
        m.push(0x19); m.push(0x2c);
        m.push(0x48); uv(&mut m, 6); m.extend_from_slice(b"schema");
        m.push(0x15); uv(&mut m, zz(1));
        m.push(0x00);
        m.push(0x15); uv(&mut m, zz(2));       // leaf INT64
        m.push(0x25); uv(&mut m, zz(1));       // OPTIONAL(1)
        m.push(0x18); uv(&mut m, 2); m.extend_from_slice(b"id");
        m.push(0x00);
        m.push(0x16); uv(&mut m, zz(3));
        m.push(0x19); m.push(0x1c);
        m.push(0x19); m.push(0x1c);
        m.push(0x3c);
        m.push(0x15); uv(&mut m, zz(2));
        m.push(0x19); m.push(0x15); uv(&mut m, zz(2)); // enc [PLAIN_DICTIONARY]
        m.push(0x19); m.push(0x18); uv(&mut m, 2); m.extend_from_slice(b"id");
        m.push(0x15); uv(&mut m, zz(0));
        m.push(0x16); uv(&mut m, zz(3));
        m.push(0x46); uv(&mut m, zz(data_page_offset));
        m.push(0x26); uv(&mut m, zz(dict_page_offset));
        m.push(0x00); m.push(0x00);
        m.push(0x26); uv(&mut m, zz(3));
        m.push(0x00); m.push(0x00);

        let mut f = Vec::new();
        f.extend_from_slice(b"PAR1");
        f.extend_from_slice(&dict_hdr);
        f.extend_from_slice(&dict_data);
        f.extend_from_slice(&data_hdr);
        f.extend_from_slice(&data_payload);
        let mlen = m.len() as u32;
        f.extend_from_slice(&m);
        f.extend_from_slice(&mlen.to_le_bytes());
        f.extend_from_slice(b"PAR1");
        f
    }

    // Helper: wrap in catch_unwind, assert no panic + typed Bad.
    fn assert_no_panic_bad(file: &[u8]) {
        let owned = file.to_vec();
        let r = std::panic::catch_unwind(move || extract(&owned, &["id"]));
        assert!(r.is_ok(), "must NOT panic on hostile input");
        assert!(
            matches!(r.unwrap(), Err(PqError::Bad(_))),
            "hostile input must yield PqError::Bad"
        );
    }

    // Helper: no panic + Unsupported.
    // SP143 T6: currently unused (its sole caller now inlines a "g" path
    // request instead of "id" since the non-flat dispatch changed which
    // column-name actually reaches the rejection). Kept for future
    // pentest locks that need the same shape.
    #[allow(dead_code)]
    fn assert_no_panic_unsupported(file: &[u8]) {
        let owned = file.to_vec();
        let r = std::panic::catch_unwind(move || extract(&owned, &["id"]));
        assert!(r.is_ok(), "must NOT panic on hostile input");
        assert!(
            matches!(r.unwrap(), Err(PqError::Unsupported(_))),
            "hostile input must yield PqError::Unsupported"
        );
    }

    // ── Lock 1: def-level stream truncated ─────────────────────────────
    //
    // Take a valid build_opt_plain_i64 file and corrupt the payload by
    // chopping everything after the 4-byte length prefix. The body the
    // prefix refers to is empty, so decode_level_v1 → Bad("rle level
    // body truncated") or the hybrid decoder runs out of input.
    #[test]
    fn pentest_opt_def_stream_truncated_is_bad_no_panic() {
        // Build a valid [7, -2] optional file (defs=[1,1], present=[7,-2]).
        let good = build_opt_plain_i64(&[1, 1], &[7, -2]);
        // The page payload starts right after PAR1 (4) + page header.
        // We'll reconstruct with a corrupted payload: only the 4-byte
        // length prefix (saying e.g. 5 bytes follow), but with 0 bytes
        // actually following — the body is absent.
        //
        // Easier: build fresh with a hand-crafted payload.
        // n=2 rows, OPTIONAL. Payload = [0x05,0x00,0x00,0x00] (claims 5
        // body bytes, but nothing follows). Values section also absent.
        let corrupt_payload: Vec<u8> = vec![0x05, 0x00, 0x00, 0x00]; // 5 bytes claimed, 0 present
        let n: i64 = 2;
        let psz = corrupt_payload.len() as i64;

        let mut hdr = Vec::new();
        hdr.push(0x15); uv(&mut hdr, zz(0));
        hdr.push(0x15); uv(&mut hdr, zz(psz));
        hdr.push(0x15); uv(&mut hdr, zz(psz));
        hdr.push(0x2c);
        hdr.push(0x15); uv(&mut hdr, zz(n));
        hdr.push(0x15); uv(&mut hdr, zz(0));
        hdr.push(0x00); hdr.push(0x00);

        let mut m = Vec::new();
        m.push(0x15); uv(&mut m, zz(2));
        m.push(0x19); m.push(0x2c);
        m.push(0x48); uv(&mut m, 6); m.extend_from_slice(b"schema");
        m.push(0x15); uv(&mut m, zz(1)); m.push(0x00);
        m.push(0x15); uv(&mut m, zz(2));
        m.push(0x25); uv(&mut m, zz(1)); // OPTIONAL
        m.push(0x18); uv(&mut m, 2); m.extend_from_slice(b"id");
        m.push(0x00);
        m.push(0x16); uv(&mut m, zz(n));
        m.push(0x19); m.push(0x1c);
        m.push(0x19); m.push(0x1c);
        m.push(0x3c);
        m.push(0x15); uv(&mut m, zz(2));
        m.push(0x19); m.push(0x15); uv(&mut m, zz(0));
        m.push(0x19); m.push(0x18); uv(&mut m, 2); m.extend_from_slice(b"id");
        m.push(0x15); uv(&mut m, zz(0));
        m.push(0x16); uv(&mut m, zz(n));
        m.push(0x46); uv(&mut m, zz(4));
        m.push(0x00); m.push(0x00);
        m.push(0x26); uv(&mut m, zz(n));
        m.push(0x00); m.push(0x00);

        let mut f = Vec::new();
        f.extend_from_slice(b"PAR1");
        f.extend_from_slice(&hdr);
        f.extend_from_slice(&corrupt_payload);
        let mlen = m.len() as u32;
        f.extend_from_slice(&m);
        f.extend_from_slice(&mlen.to_le_bytes());
        f.extend_from_slice(b"PAR1");

        // Also verify the good baseline actually succeeds.
        assert!(extract(&good, &["id"]).is_ok(), "baseline must succeed");
        assert_no_panic_bad(&f);
    }

    // ── Lock 2: def-level 4-byte prefix undercounts the real hybrid body ──
    //
    // Distinct path from Lock 1 (prefix-past-EOF → "rle level body truncated"
    // inside decode_level_v1). Here the prefix is SMALLER than the actual
    // hybrid bytes present, so decode_level_v1 slices a body that is too
    // short for decode_hybrid to read a full bit-packed run → Bad("rle
    // bitpack run truncated") inside decode_hybrid, NOT the same guard as
    // Lock 1's decode_level_v1 slice check.
    //
    // Construction: use build_opt_plain_i64(&[1,0,1], &[7,-2]).
    // That produces def_hybrid = [0x03, 0x05] (2 bytes: varint header +
    // 1 bits-byte for n=3). The correct prefix would be 2, but we patch
    // it to 1 → decode_level_v1 reads body = [0x03] (1 byte only).
    // decode_hybrid sees header 0x03 (odd → bit-packed, 1 group of 8)
    // → needs 1*bit_width=1 byte at positions [1..2] inside the 1-byte
    // body → data.get(1..2) is None → Bad("rle bitpack run truncated").
    #[test]
    fn pentest_opt_def_prefix_undercount_is_bad() {
        // Build a valid file so we can borrow its structure; then reconstruct
        // with the patched prefix to guarantee correct metadata layout.
        let n: i64 = 3;
        // def_hybrid for defs=[1,0,1], groups=1: header=0x03, bits=0x05 → 2 bytes.
        // Real prefix = 2. We lie and say 1 → body is only [0x03].
        let undercount_prefix: u32 = 1; // real body is 2 bytes; we tell the decoder it is 1
        let real_def_hybrid: &[u8] = &[0x03, 0x05];
        let mut corrupt_payload: Vec<u8> = Vec::new();
        corrupt_payload.extend_from_slice(&undercount_prefix.to_le_bytes()); // prefix=1
        corrupt_payload.extend_from_slice(real_def_hybrid);                  // 2 bytes follow (decoder only sees 1)
        corrupt_payload.extend_from_slice(&7i64.to_le_bytes());
        corrupt_payload.extend_from_slice(&(-2i64).to_le_bytes());
        let psz = corrupt_payload.len() as i64;

        let mut hdr = Vec::new();
        hdr.push(0x15); uv(&mut hdr, zz(0));
        hdr.push(0x15); uv(&mut hdr, zz(psz));
        hdr.push(0x15); uv(&mut hdr, zz(psz));
        hdr.push(0x2c);
        hdr.push(0x15); uv(&mut hdr, zz(n));
        hdr.push(0x15); uv(&mut hdr, zz(0));
        hdr.push(0x00); hdr.push(0x00);

        let mut m = Vec::new();
        m.push(0x15); uv(&mut m, zz(2));
        m.push(0x19); m.push(0x2c);
        m.push(0x48); uv(&mut m, 6); m.extend_from_slice(b"schema");
        m.push(0x15); uv(&mut m, zz(1)); m.push(0x00);
        m.push(0x15); uv(&mut m, zz(2));
        m.push(0x25); uv(&mut m, zz(1)); // OPTIONAL
        m.push(0x18); uv(&mut m, 2); m.extend_from_slice(b"id");
        m.push(0x00);
        m.push(0x16); uv(&mut m, zz(n));
        m.push(0x19); m.push(0x1c);
        m.push(0x19); m.push(0x1c);
        m.push(0x3c);
        m.push(0x15); uv(&mut m, zz(2));
        m.push(0x19); m.push(0x15); uv(&mut m, zz(0));
        m.push(0x19); m.push(0x18); uv(&mut m, 2); m.extend_from_slice(b"id");
        m.push(0x15); uv(&mut m, zz(0));
        m.push(0x16); uv(&mut m, zz(n));
        m.push(0x46); uv(&mut m, zz(4));
        m.push(0x00); m.push(0x00);
        m.push(0x26); uv(&mut m, zz(n));
        m.push(0x00); m.push(0x00);

        let mut f = Vec::new();
        f.extend_from_slice(b"PAR1");
        f.extend_from_slice(&hdr);
        f.extend_from_slice(&corrupt_payload);
        let mlen = m.len() as u32;
        f.extend_from_slice(&m);
        f.extend_from_slice(&mlen.to_le_bytes());
        f.extend_from_slice(b"PAR1");

        assert_no_panic_bad(&f);
    }

    // ── Lock 3: def-level value > 1 (max_def_level exceeded) ───────────
    //
    // An RLE run with bit_width=1 whose repeated-value byte is 0x02.
    // The RLE grammar reads ceil(bit_width/8)=1 value byte, which CAN
    // be 0x02 — the high bits are NOT masked by bit_width in the RLE
    // repeated-value path (only bit-packed groups are bit-width-masked).
    // So decode_level_v1 returns levels containing 2, and decode_page's
    // `d > 1` check → Err(Bad("definition level exceeds max")).
    //
    // Construction (for n=2 rows):
    //   RLE header = varint(run_len << 1) = varint(2<<1) = varint(4) = 0x04 (even → RLE)
    //   value byte = 0x02
    //   body = [0x04, 0x02]  (len=2)
    //   4-byte prefix = [0x02, 0x00, 0x00, 0x00]
    //   full def stream = [0x02,0x00,0x00,0x00, 0x04,0x02]
    //
    // Which layer catches it: the RLE decoder returns level=2; decode_page's
    // `d > 1` check fires → Err(Bad("definition level exceeds max")).
    #[test]
    fn pentest_opt_def_level_gt_max_is_bad_no_panic() {
        let n: i64 = 2;
        // def stream: RLE run, value=2, run_len=2, bit_width=1.
        // body = [0x04 (varint run_len<<1 = 4), 0x02 (value byte)]
        // length prefix = 2 → [0x02,0x00,0x00,0x00]
        let def_body: &[u8] = &[0x04, 0x02]; // varint(4)=RLE run_len=2, value=2
        let mut payload = Vec::new();
        payload.extend_from_slice(&(def_body.len() as u32).to_le_bytes());
        payload.extend_from_slice(def_body);
        // No value bytes needed (the decode will reject before reaching them).
        let psz = payload.len() as i64;

        let mut hdr = Vec::new();
        hdr.push(0x15); uv(&mut hdr, zz(0));
        hdr.push(0x15); uv(&mut hdr, zz(psz));
        hdr.push(0x15); uv(&mut hdr, zz(psz));
        hdr.push(0x2c);
        hdr.push(0x15); uv(&mut hdr, zz(n));
        hdr.push(0x15); uv(&mut hdr, zz(0));
        hdr.push(0x00); hdr.push(0x00);

        let mut m = Vec::new();
        m.push(0x15); uv(&mut m, zz(2));
        m.push(0x19); m.push(0x2c);
        m.push(0x48); uv(&mut m, 6); m.extend_from_slice(b"schema");
        m.push(0x15); uv(&mut m, zz(1)); m.push(0x00);
        m.push(0x15); uv(&mut m, zz(2));
        m.push(0x25); uv(&mut m, zz(1)); // OPTIONAL
        m.push(0x18); uv(&mut m, 2); m.extend_from_slice(b"id");
        m.push(0x00);
        m.push(0x16); uv(&mut m, zz(n));
        m.push(0x19); m.push(0x1c);
        m.push(0x19); m.push(0x1c);
        m.push(0x3c);
        m.push(0x15); uv(&mut m, zz(2));
        m.push(0x19); m.push(0x15); uv(&mut m, zz(0));
        m.push(0x19); m.push(0x18); uv(&mut m, 2); m.extend_from_slice(b"id");
        m.push(0x15); uv(&mut m, zz(0));
        m.push(0x16); uv(&mut m, zz(n));
        m.push(0x46); uv(&mut m, zz(4));
        m.push(0x00); m.push(0x00);
        m.push(0x26); uv(&mut m, zz(n));
        m.push(0x00); m.push(0x00);

        let mut f = Vec::new();
        f.extend_from_slice(b"PAR1");
        f.extend_from_slice(&hdr);
        f.extend_from_slice(&payload);
        let mlen = m.len() as u32;
        f.extend_from_slice(&m);
        f.extend_from_slice(&mlen.to_le_bytes());
        f.extend_from_slice(b"PAR1");

        let owned = f.clone();
        // Inlined (not assert_no_panic_bad) to carry the {result:?} diagnostic — def>1 must hit decode_page's d>1 guard, not an earlier reject.
        let r = std::panic::catch_unwind(move || extract(&owned, &["id"]));
        assert!(r.is_ok(), "must NOT panic: def>1 case");
        let result = r.unwrap();
        assert!(
            matches!(result, Err(PqError::Bad(_))),
            "def level > max_def_level must be Err(Bad); got: {result:?}"
        );
    }

    // ── Lock 4: value section shorter than present count ────────────────
    //
    // defs say 3 rows present (defs=[1,1,1]) but only 2 i64s in the
    // value section. plain::decode_plain for 3 i64s from 16 bytes →
    // Bad("PLAIN INT64 truncated") (needs 24, gets 16).
    #[test]
    fn pentest_opt_value_section_shorter_than_present_is_bad_no_panic() {
        // n=3 rows, all defs=1 (3 present), but only 2 i64s in body.
        let n: i64 = 3;
        // def stream: 3 present. groups=1, bit_width=1.
        // header = (1<<1)|1 = 3 → [0x03]; bits byte: bits 0,1,2 all set → 0x07.
        let def_body: &[u8] = &[0x03, 0x07]; // varint(3), bits=0b00000111
        let mut payload = Vec::new();
        payload.extend_from_slice(&(def_body.len() as u32).to_le_bytes());
        payload.extend_from_slice(def_body);
        // Only 2 i64s (16 bytes) instead of the required 3 (24 bytes).
        payload.extend_from_slice(&7i64.to_le_bytes());
        payload.extend_from_slice(&(-2i64).to_le_bytes());
        let psz = payload.len() as i64;

        let mut hdr = Vec::new();
        hdr.push(0x15); uv(&mut hdr, zz(0));
        hdr.push(0x15); uv(&mut hdr, zz(psz));
        hdr.push(0x15); uv(&mut hdr, zz(psz));
        hdr.push(0x2c);
        hdr.push(0x15); uv(&mut hdr, zz(n));
        hdr.push(0x15); uv(&mut hdr, zz(0));
        hdr.push(0x00); hdr.push(0x00);

        let mut m = Vec::new();
        m.push(0x15); uv(&mut m, zz(2));
        m.push(0x19); m.push(0x2c);
        m.push(0x48); uv(&mut m, 6); m.extend_from_slice(b"schema");
        m.push(0x15); uv(&mut m, zz(1)); m.push(0x00);
        m.push(0x15); uv(&mut m, zz(2));
        m.push(0x25); uv(&mut m, zz(1)); // OPTIONAL
        m.push(0x18); uv(&mut m, 2); m.extend_from_slice(b"id");
        m.push(0x00);
        m.push(0x16); uv(&mut m, zz(n));
        m.push(0x19); m.push(0x1c);
        m.push(0x19); m.push(0x1c);
        m.push(0x3c);
        m.push(0x15); uv(&mut m, zz(2));
        m.push(0x19); m.push(0x15); uv(&mut m, zz(0));
        m.push(0x19); m.push(0x18); uv(&mut m, 2); m.extend_from_slice(b"id");
        m.push(0x15); uv(&mut m, zz(0));
        m.push(0x16); uv(&mut m, zz(n));
        m.push(0x46); uv(&mut m, zz(4));
        m.push(0x00); m.push(0x00);
        m.push(0x26); uv(&mut m, zz(n));
        m.push(0x00); m.push(0x00);

        let mut f = Vec::new();
        f.extend_from_slice(b"PAR1");
        f.extend_from_slice(&hdr);
        f.extend_from_slice(&payload);
        let mlen = m.len() as u32;
        f.extend_from_slice(&m);
        f.extend_from_slice(&mlen.to_le_bytes());
        f.extend_from_slice(b"PAR1");

        assert_no_panic_bad(&f);
    }

    // ── Lock 5: OPTIONAL + dict with out-of-range index ─────────────────
    //
    // Dict has 1 entry (index 0 valid). Index bits byte = 0xFF causes
    // both present-value indices to be 1, which is out-of-range for a
    // dict of length 1 → Bad from dict::resolve_dict_indices.
    #[test]
    fn pentest_opt_dict_oob_index_is_bad_no_panic() {
        // oob_bits=0xFF: bit-packed indices for 2 present values, both=1
        // (bit0=1, bit1=1 → index 1), but dict only has entry at index 0.
        let f = build_opt_dict_int64_file_with_oob(Some(0xFF));
        assert_no_panic_bad(&f);
    }

    // ── Lock 6: non-flat schema → no-panic, decodes via SP144 path ──────
    //
    // SP143 T6: dispatch routes non-flat files to `extract_nested`.
    // Pre-SP144-T5, the top-level group "g" (no LIST annotation) was
    // rejected as Unsupported (SP144 follow-up). SP144 T5 now decodes
    // the same shape as a single-field struct via `assemble_struct`.
    // The no-panic invariant — the original purpose of this lock —
    // continues to hold; the success-vs-Unsupported toggle is the
    // intended T5 behavior change. Decode shape is asserted by
    // `extract_decodes_nested_struct_schema_sp144` in `mod tests`.
    #[test]
    fn pentest_opt_non_flat_schema_no_panic_sp144_decodes() {
        let f = build_nested_schema_file();
        let owned = f.clone();
        let r = std::panic::catch_unwind(move || extract(&owned, &["g"]));
        assert!(r.is_ok(), "must NOT panic on nested-schema input");
        // Either Ok (T5 struct decode) or a deterministic typed error —
        // the lock is "no panic + deterministic outcome".
        match r.unwrap() {
            Ok(_) => {}
            Err(PqError::Unsupported(_)) | Err(PqError::Bad(_)) => {}
        }
    }

    // ── Positive correctness locks (MUST be Ok with exact values) ────────
    //
    // These assert the decode_page null-scatter is correct. If any fails,
    // the scatter logic is wrong — report BLOCKED, never weaken the lock.

    #[test]
    fn pentest_opt_positive_all_null_exact() {
        // defs=[0,0], present=[] → [[Null],[Null]]
        let f = build_opt_plain_i64(&[0, 0], &[]);
        let r = std::panic::catch_unwind(|| extract(&f, &["id"]));
        assert!(r.is_ok(), "no panic: all-null positive lock");
        assert_eq!(
            r.unwrap().expect("all-null must be Ok"),
            vec![vec![PqValue::Null], vec![PqValue::Null]],
            "all-null: exact placement required"
        );
    }

    #[test]
    fn pentest_opt_positive_all_present_exact() {
        // defs=[1,1], present=[7,-2] → [[I64(7)],[I64(-2)]]
        let f = build_opt_plain_i64(&[1, 1], &[7, -2]);
        let r = std::panic::catch_unwind(|| extract(&f, &["id"]));
        assert!(r.is_ok(), "no panic: all-present positive lock");
        assert_eq!(
            r.unwrap().expect("all-present must be Ok"),
            vec![vec![PqValue::I64(7)], vec![PqValue::I64(-2)]],
            "all-present: exact placement required"
        );
    }

    #[test]
    fn pentest_opt_positive_mixed_scatter_exact() {
        // defs=[1,0,1,1,0], present=[10,20,30]
        // Expected: [[I64(10)],[Null],[I64(20)],[I64(30)],[Null]]
        let f = build_opt_plain_i64(&[1, 0, 1, 1, 0], &[10, 20, 30]);
        let r = std::panic::catch_unwind(|| extract(&f, &["id"]));
        assert!(r.is_ok(), "no panic: mixed scatter positive lock");
        assert_eq!(
            r.unwrap().expect("mixed scatter must be Ok"),
            vec![
                vec![PqValue::I64(10)],
                vec![PqValue::Null],
                vec![PqValue::I64(20)],
                vec![PqValue::I64(30)],
                vec![PqValue::Null],
            ],
            "mixed scatter: exact placement required"
        );
    }
}

// ── Task 12 PENTEST PASS — adversarial lock tests ─────────────────────
//
// The Parquet object bytes are operator-declared-source-controlled =
// attacker-influenceable. Every test here asserts that `extract` on
// hostile input returns a typed `Err(PqError::_)` and NEVER panics,
// stack-overflows, or OOM-aborts the process. Each case is wrapped in
// `catch_unwind` (proving no panic) AND asserted to be a typed `Err`.
//
// The `value_count_overflow_*` / `oversized_byte_array_len_*` cases
// exercise the deferred `decode_plain` `Vec::with_capacity(count)`
// pre-reserve: WITHOUT the Task-12 fix `dp_num_values = i32::MAX`
// makes `Vec::<PqValue>::with_capacity(2_147_483_647)` request tens of
// GB and abort the process before the per-type bounds check; WITH the
// fix (`count.min(data.len())`) it returns `PqError::Bad` fast.
#[cfg(test)]
mod pentest {
    use super::*;

    // Spec-faithful compact-thrift primitives (same as the Task-4/6
    // hand-encoders in `tests` above; re-declared here so this module
    // is self-contained).
    fn uv(out: &mut Vec<u8>, mut v: u64) {
        loop {
            let b = (v & 0x7f) as u8;
            v >>= 7;
            if v == 0 { out.push(b); break; } else { out.push(b | 0x80); }
        }
    }
    fn zz(v: i64) -> u64 { ((v << 1) ^ (v >> 63)) as u64 }

    /// V1 PLAIN PageHeader with attacker-chosen `num_values` /
    /// `uncompressed_size` (`data_bytes`). Field layout per
    /// parquet.thrift PageHeader { 1:type, 2:uncompressed_page_size,
    /// 3:compressed_page_size, 4:crc (skipped),
    /// 5:DataPageHeader { 1:num_values, 2:encoding } }.
    fn page_header_bytes(num_values: i32, data_bytes: i32) -> Vec<u8> {
        let mut h = Vec::new();
        h.push(0x15); uv(&mut h, zz(0)); // f1 type = DATA_PAGE(0)
        h.push(0x15); uv(&mut h, zz(data_bytes as i64)); // f2 uncompressed
        h.push(0x15); uv(&mut h, zz(data_bytes as i64)); // f3 compressed
        h.push(0x2c); // f5 DataPageHeader struct (delta 3->5=2)
        h.push(0x15); uv(&mut h, zz(num_values as i64)); // g1 num_values
        h.push(0x15); uv(&mut h, zz(0)); // g2 encoding = PLAIN(0)
        h.push(0x00); // stop DataPageHeader
        h.push(0x00); // stop PageHeader
        h
    }

    /// FileMetaData for a one-column one-row-group file. `leaf_ptype`
    /// is the Type enum written to BOTH the schema leaf and the
    /// ColumnMetaData (so the Fix-1 schema/chunk guard is satisfied and
    /// we reach `decode_plain`). `col` is the leaf/path name.
    fn filemetadata_bytes(
        leaf_ptype: i64,
        col: &[u8],
        data_page_offset: i64,
    ) -> Vec<u8> {
        let mut b = Vec::new();
        b.push(0x15); uv(&mut b, zz(2)); // f1 version=2
        b.push(0x19); b.push(0x2c); // f2 list<SchemaElement> 2 structs
        // schema[0] root group
        b.push(0x48); uv(&mut b, 6); b.extend_from_slice(b"schema");
        b.push(0x15); uv(&mut b, zz(1)); // f5 num_children=1
        b.push(0x00);
        // schema[1] leaf
        b.push(0x15); uv(&mut b, zz(leaf_ptype)); // f1 type
        b.push(0x25); uv(&mut b, zz(0)); // f3 repetition=REQUIRED
        b.push(0x18); uv(&mut b, col.len() as u64);
        b.extend_from_slice(col); // f4 name
        b.push(0x00);
        b.push(0x16); uv(&mut b, zz(2)); // f3 num_rows=2
        b.push(0x19); b.push(0x1c); // f4 list<RowGroup> 1 struct
        b.push(0x19); b.push(0x1c); // RowGroup.f1 list<ColumnChunk> 1
        b.push(0x3c); // ColumnChunk.f3 ColumnMetaData struct
        b.push(0x15); uv(&mut b, zz(leaf_ptype)); // CMD f1 type
        b.push(0x19); b.push(0x15); uv(&mut b, zz(0)); // f2 encodings [PLAIN]
        b.push(0x19); b.push(0x18); uv(&mut b, col.len() as u64);
        b.extend_from_slice(col); // f3 path_in_schema [col]
        b.push(0x15); uv(&mut b, zz(0)); // f4 codec=UNCOMPRESSED
        b.push(0x16); uv(&mut b, zz(2)); // f5 num_values=2
        b.push(0x46); uv(&mut b, zz(data_page_offset)); // f9 data_page_offset
        b.push(0x00); // stop ColumnMetaData
        b.push(0x00); // stop ColumnChunk
        b.push(0x26); uv(&mut b, zz(2)); // RowGroup.f3 num_rows=2
        b.push(0x00); // stop RowGroup
        b.push(0x00); // stop FileMetaData
        b
    }

    /// Assemble `[PAR1][page_hdr][page_data][meta][mlen_le][PAR1]`.
    fn assemble(hdr: &[u8], page_data: &[u8], meta: &[u8]) -> Vec<u8> {
        let mlen = meta.len() as u32;
        let mut f = Vec::new();
        f.extend_from_slice(b"PAR1");
        f.extend_from_slice(hdr);
        f.extend_from_slice(page_data);
        f.extend_from_slice(meta);
        f.extend_from_slice(&mlen.to_le_bytes());
        f.extend_from_slice(b"PAR1");
        f
    }

    fn no_panic_typed_err(file: &[u8], col: &str) {
        let owned = file.to_vec();
        let c = col.to_string();
        let r = std::panic::catch_unwind(move || extract(&owned, &[c.as_str()]));
        assert!(r.is_ok(), "must NOT panic/OOM-unwind on hostile input");
        assert!(
            matches!(r.unwrap(), Err(PqError::Bad(_) | PqError::Unsupported(_))),
            "hostile input must yield a typed PqError"
        );
    }

    #[test]
    fn malformed_framing_is_typed_error_never_panic() {
        let cases: Vec<Vec<u8>> = vec![
            b"".to_vec(),
            b"PAR1".to_vec(),
            b"NOPExxxxxxxxPAR1".to_vec(),
            // lying metadata_len = 0xffffffff (huge, > file)
            {
                let mut v = b"PAR1".to_vec();
                v.extend_from_slice(&[0xAA, 0xBB, 0xCC]);
                v.extend_from_slice(&u32::MAX.to_le_bytes());
                v.extend_from_slice(b"PAR1");
                v
            },
            // metadata_len pointing back into the 4-byte header magic
            {
                let mut v = b"PAR1".to_vec();
                v.extend_from_slice(&[0x01]);
                v.extend_from_slice(&5u32.to_le_bytes());
                v.extend_from_slice(b"PAR1");
                v
            },
        ];
        for c in &cases {
            no_panic_typed_err(c, "id");
        }
    }

    #[test]
    fn value_count_overflow_rejected_no_oom() {
        // DataPageHeader.num_values = i32::MAX against a 16-byte INT64
        // PLAIN page. WITHOUT the fix, decode_plain does
        // Vec::<PqValue>::with_capacity(2_147_483_647) ~= tens of GB →
        // process OOM-abort BEFORE the `data.get(..need)?` check.
        // WITH the fix the reservation is bounded by page len and the
        // per-type bounds check returns PqError::Bad fast.
        let hdr = page_header_bytes(i32::MAX, 16);
        let mut page = Vec::new();
        page.extend_from_slice(&7i64.to_le_bytes());
        page.extend_from_slice(&(-2i64).to_le_bytes());
        let meta = filemetadata_bytes(2 /*INT64*/, b"id", 4);
        let file = assemble(&hdr, &page, &meta);
        no_panic_typed_err(&file, "id");
        assert!(
            matches!(extract(&file, &["id"]), Err(PqError::Bad(_))),
            "i32::MAX num_values vs tiny page must be PqError::Bad"
        );
    }

    #[test]
    fn oversized_byte_array_len_rejected_no_oom() {
        // One BYTE_ARRAY column; PLAIN page's first 4-byte length
        // prefix = 0x7fffffff but only a few payload bytes follow.
        // decode_plain's `data.get(p..p+len)?` must return Bad — not
        // attempt a ~2GB `to_vec()` / OOM / panic.
        let mut page = Vec::new();
        page.extend_from_slice(&0x7fff_ffffu32.to_le_bytes()); // lying len
        page.extend_from_slice(b"abcd"); // only 4 payload bytes
        let data_bytes = page.len() as i32;
        // num_values=1 (one BYTE_ARRAY element claimed).
        let hdr = page_header_bytes(1, data_bytes);
        let meta = filemetadata_bytes(6 /*BYTE_ARRAY*/, b"s", 4);
        let file = assemble(&hdr, &page, &meta);
        no_panic_typed_err(&file, "s");
        assert!(
            matches!(extract(&file, &["s"]), Err(PqError::Bad(_))),
            "oversized BYTE_ARRAY len prefix must be PqError::Bad"
        );

        // And a u32::MAX length prefix variant.
        let mut page2 = Vec::new();
        page2.extend_from_slice(&u32::MAX.to_le_bytes());
        page2.extend_from_slice(b"xy");
        let hdr2 = page_header_bytes(1, page2.len() as i32);
        let meta2 = filemetadata_bytes(6, b"s", 4);
        let file2 = assemble(&hdr2, &page2, &meta2);
        no_panic_typed_err(&file2, "s");
        assert!(matches!(
            extract(&file2, &["s"]),
            Err(PqError::Bad(_))
        ));
    }

    #[test]
    fn lying_page_size_and_offset_rejected() {
        // (a) uncompressed_size huge (much larger than the file AND
        // far beyond DEFAULT_MAX_PAGE_SIZE = 256 MiB). Pre-SP151 this
        // surfaced as Bad("page data truncated") at the page_payload
        // bounds check; SP151 catches it earlier as Unsupported via
        // the cap check. Both are typed safety-bounded errors — the
        // pentest contract is "no panic / no OOM / typed error", not
        // a specific variant. Accept either Bad or Unsupported.
        let hdr_a = page_header_bytes(2, i32::MAX);
        let mut page = Vec::new();
        page.extend_from_slice(&7i64.to_le_bytes());
        page.extend_from_slice(&(-2i64).to_le_bytes());
        let meta_a = filemetadata_bytes(2, b"id", 4);
        let file_a = assemble(&hdr_a, &page, &meta_a);
        no_panic_typed_err(&file_a, "id");
        assert!(matches!(
            extract(&file_a, &["id"]),
            Err(PqError::Bad(_)) | Err(PqError::Unsupported(_))
        ));

        // (b) data_page_offset past EOF: build a valid file, then
        // rebuild metadata claiming an absurd page offset.
        let hdr_b = page_header_bytes(2, 16);
        let meta_b = filemetadata_bytes(2, b"id", 1_000_000);
        let file_b = assemble(&hdr_b, &page, &meta_b);
        no_panic_typed_err(&file_b, "id");
        assert!(matches!(
            extract(&file_b, &["id"]),
            Err(PqError::Bad(_))
        ));
    }
}

// ── SP151 (OBJ-2c-4 follow-up) — synthetic >64 MiB page tests ────────
//
// Pre-SP151: a page declaring uncompressed_page_size > 64 MiB tripped
// the per-codec module ceiling (SNAPPY_MAX_DECOMP / GZIP_MAX_DECOMP /
// ZSTD_MAX_DECOMP, all 64 << 20) before any decode work. Pyarrow
// produces such pages for high-cardinality dictionary pages and
// many-row row groups; the default extract() then surfaced
// Unsupported("snappy page X exceeds 67108864 cap: OBJ-2c").
//
// Post-SP151: per-codec ceilings are 256 MiB and the user-facing
// DEFAULT_MAX_PAGE_SIZE is 256 MiB. Pages between 64 and 256 MiB
// decode (subject to the rest of the parsing succeeding). Pages
// above 256 MiB are rejected with a typed Unsupported naming the
// SP151 follow-up AND the extract_with_cap operator knob.
//
// These tests are SYNTHETIC (no real >64 MiB fixture on disk —
// pyarrow regen is a multi-GB job; the integration tests in
// fixture_roundtrip.rs cover the operator-facing API and the
// existing fixtures cover the regression surface). They construct a
// V1 page header that claims uncompressed_page_size N, then assert
// that:
//   - N=65 MiB at default cap (256 MiB): the SP151 cap check
//     PASSES (the decode then fails on missing page data because
//     the file doesn't actually carry 65 MiB of bytes, but the
//     failure is the truncated-page error from page_payload, NOT
//     the SP151 cap error). This is the regression lock for the
//     "64 MiB rejected" bug.
//   - N=300 MiB at default cap (256 MiB): the SP151 cap check
//     FIRES — typed Unsupported with the SP151 marker.
//   - N=65 MiB at custom cap 60 MiB: the SP151 cap check FIRES —
//     typed Unsupported (lower cap below the page size).
//
// Overflow safety: every page-header derivation uses checked_add /
// usize::try_from BEFORE the cap check. The cap check itself is
// `size > cap` — no arithmetic, no overflow risk. Vec::with_capacity
// at the page_payload allocation site is bounded by uncomp (already
// ≤ cap, ≤ per-codec ceiling).

#[cfg(test)]
mod sp151_tests {
    use super::*;

    fn uv(out: &mut Vec<u8>, mut v: u64) {
        loop {
            let b = (v & 0x7f) as u8;
            v >>= 7;
            if v == 0 { out.push(b); break; } else { out.push(b | 0x80); }
        }
    }
    fn zz(v: i64) -> u64 { ((v << 1) ^ (v >> 63)) as u64 }

    /// V1 PageHeader claiming `uncompressed_page_size = uncomp_bytes`
    /// and `compressed_page_size = uncomp_bytes`, num_values=2,
    /// PLAIN encoding. Same shape as the in-module page_header_bytes
    /// helper but accepts an i64 so we can exercise sizes > i32 range.
    fn page_header_claim_size(uncomp_bytes: i64) -> Vec<u8> {
        let mut h = Vec::new();
        h.push(0x15); uv(&mut h, zz(0));               // f1 type=DATA_PAGE(0)
        h.push(0x15); uv(&mut h, zz(uncomp_bytes));    // f2 uncompressed_page_size
        h.push(0x15); uv(&mut h, zz(uncomp_bytes));    // f3 compressed_page_size
        h.push(0x2c);                                  // f5 DataPageHeader struct
        h.push(0x15); uv(&mut h, zz(2));               // g1 num_values=2
        h.push(0x15); uv(&mut h, zz(0));               // g2 encoding=PLAIN(0)
        h.push(0x00); h.push(0x00);                    // stop DPH / PH
        h
    }

    /// FileMetaData for one INT64 REQUIRED column "id", UNCOMPRESSED,
    /// num_values=2, data_page_offset=4 (right after PAR1 magic).
    fn filemetadata_int64_one_page() -> Vec<u8> {
        let mut m = Vec::new();
        m.push(0x15); uv(&mut m, zz(2));               // f1 version=2
        m.push(0x19); m.push(0x2c);                    // f2 list<SchemaElement> 2
        m.push(0x48); uv(&mut m, 6); m.extend_from_slice(b"schema");
        m.push(0x15); uv(&mut m, zz(1));               // schema[0] num_children=1
        m.push(0x00);
        m.push(0x15); uv(&mut m, zz(2));               // schema[1] type=INT64
        m.push(0x25); uv(&mut m, zz(0));               // f3 repetition=REQUIRED
        m.push(0x18); uv(&mut m, 2); m.extend_from_slice(b"id");
        m.push(0x00);
        m.push(0x16); uv(&mut m, zz(2));               // f3 num_rows=2
        m.push(0x19); m.push(0x1c);                    // f4 list<RowGroup> 1
        m.push(0x19); m.push(0x1c);                    // RG.f1 list<CC> 1
        m.push(0x3c);                                  // CC.f3 CMD struct
        m.push(0x15); uv(&mut m, zz(2));               // CMD f1 type=INT64
        m.push(0x19); m.push(0x15); uv(&mut m, zz(0)); // f2 encodings [PLAIN]
        m.push(0x19); m.push(0x18); uv(&mut m, 2); m.extend_from_slice(b"id");
        m.push(0x15); uv(&mut m, zz(0));               // f4 codec=UNCOMPRESSED
        m.push(0x16); uv(&mut m, zz(2));               // f5 num_values=2
        m.push(0x46); uv(&mut m, zz(4));               // f9 data_page_offset=4
        m.push(0x00); m.push(0x00);                    // stop CMD / CC
        m.push(0x26); uv(&mut m, zz(2));               // RG f3 num_rows=2
        m.push(0x00); m.push(0x00);                    // stop RG / FMD
        m
    }

    fn assemble(hdr: &[u8], page_payload: &[u8], meta: &[u8]) -> Vec<u8> {
        let mut f = Vec::new();
        f.extend_from_slice(b"PAR1");
        f.extend_from_slice(hdr);
        f.extend_from_slice(page_payload);
        f.extend_from_slice(meta);
        f.extend_from_slice(&(meta.len() as u32).to_le_bytes());
        f.extend_from_slice(b"PAR1");
        f
    }

    #[test]
    fn sp151_65mib_page_passes_cap_check() {
        // 65 MiB declared uncompressed_size. Pre-SP151: rejected with
        // "snappy page X exceeds 67108864 cap" (well, UNCOMPRESSED so
        // codec-specific cap doesn't apply, but the conceptual
        // regression is "page > 64 MiB"). The cap check at the page
        // header site MUST PASS (256 MiB default ≥ 65 MiB).
        // The decode then fails downstream on missing data (the file
        // doesn't actually carry 65 MiB) — that's the page_payload
        // bounds check, NOT the SP151 cap.
        let hdr = page_header_claim_size(65 * 1024 * 1024);
        let payload = vec![0u8; 16]; // tiny — file doesn't really have 65 MiB
        let meta = filemetadata_int64_one_page();
        let file = assemble(&hdr, &payload, &meta);

        let err = extract(&file, &["id"])
            .expect_err("file is truncated relative to its 65 MiB claim");
        let msg = format!("{err:?}");
        // Crucially: the error must NOT mention SP151 cap — the cap
        // check PASSED. It should be the page_payload truncation
        // error, which uses the "page data truncated" string.
        assert!(
            !msg.contains("SP151"),
            "65 MiB page must NOT trip the SP151 cap at default 256 MiB: {msg}"
        );
        assert!(
            !msg.contains("max_page_size"),
            "65 MiB page must NOT trip the SP151 cap at default 256 MiB: {msg}"
        );
    }

    #[test]
    fn sp151_300mib_page_trips_cap_with_named_followup() {
        // 300 MiB declared uncompressed_size > 256 MiB default cap.
        // The SP151 cap check fires Unsupported NAMING SP151 + the
        // operator knob (extract_with_cap).
        let hdr = page_header_claim_size(300 * 1024 * 1024);
        let payload = vec![0u8; 16];
        let meta = filemetadata_int64_one_page();
        let file = assemble(&hdr, &payload, &meta);

        let err = extract(&file, &["id"])
            .expect_err("300 MiB > 256 MiB cap must reject");
        match err {
            PqError::Unsupported(msg) => {
                assert!(
                    msg.contains("SP151"),
                    "300 MiB page rejection must name SP151: {msg}"
                );
                assert!(
                    msg.contains("extract_with_cap"),
                    "300 MiB page rejection must name extract_with_cap: {msg}"
                );
            }
            other => panic!("expected Unsupported(SP151...), got {other:?}"),
        }
    }

    #[test]
    fn sp151_65mib_page_trips_custom_60mib_cap() {
        // 65 MiB page WITH operator-supplied 60 MiB cap (lower than
        // both the default and the page size). The cap check fires
        // Unsupported naming the (lower) cap value.
        let hdr = page_header_claim_size(65 * 1024 * 1024);
        let payload = vec![0u8; 16];
        let meta = filemetadata_int64_one_page();
        let file = assemble(&hdr, &payload, &meta);

        let cap = 60 * 1024 * 1024;
        let err = extract_with_cap(&file, &["id"], cap)
            .expect_err("65 MiB > 60 MiB user cap must reject");
        match err {
            PqError::Unsupported(msg) => {
                assert!(msg.contains("SP151"), "{msg}");
                let cap_str = format!("max_page_size cap {cap}");
                assert!(
                    msg.contains(&cap_str),
                    "rejection must echo the operator's cap value {cap}: {msg}"
                );
            }
            other => panic!("expected Unsupported(SP151...), got {other:?}"),
        }
    }

    #[test]
    fn sp151_huge_uncomp_no_oom_no_panic() {
        // Pentest: i32::MAX uncomp ≈ 2 GiB. Must FAST-reject as
        // Unsupported (SP151 cap fires before any allocation).
        // catch_unwind proves no panic / no OOM-abort.
        let hdr = page_header_claim_size(i32::MAX as i64);
        let payload = vec![0u8; 16];
        let meta = filemetadata_int64_one_page();
        let file = assemble(&hdr, &payload, &meta);

        let r = std::panic::catch_unwind(move || extract(&file, &["id"]));
        assert!(r.is_ok(), "huge uncomp must NOT panic/OOM");
        match r.unwrap() {
            Err(PqError::Unsupported(msg)) => {
                assert!(msg.contains("SP151"), "{msg}");
            }
            other => panic!("expected Unsupported(SP151...), got {other:?}"),
        }
    }
}

// ── OBJ-2c-3 Task 5 PENTEST PASS — DATA_PAGE_V2 decode adversarial ────
//
// The Parquet object bytes are operator-declared-source-controlled =
// attacker-influenceable. `decode_data_page_v2` (lib.rs:168) ingests
// these attacker bytes: rep_len, def_len, num_values, num_nulls,
// uncompressed_page_size, is_compressed, the def-level bytes, the
// values section, and the column codec. Every hostile case below is
// wrapped in `catch_unwind` (proving no panic / no OOM-unwind) AND
// asserted to be a TYPED `PqError::Bad`/`Unsupported` that returns
// FAST (no hang / no multi-GB allocation). The positive locks assert
// the exact decoded `Ok(rows)` — a positive-lock failure is a decoder
// bug (BLOCKED, never weaken); a hostile panic/OOM/hang is a real
// vuln (BLOCKED with exact detail).
//
// Every corrupt byte here is derived by reasoning about the V2 wire
// format INDEPENDENTLY (parquet.thrift PageHeader f1/f2/f3 + f8
// DataPageHeaderV2 g1 num_values / g2 num_nulls / g3 num_rows / g4
// encoding / g5 def_levels_byte_length / g6 rep_levels_byte_length /
// g7 is_compressed, then payload = [rep bytes][def bytes][values]),
// NOT by patching valid encoder output. The `v2_file` builder takes
// every attacker-controlled field as an explicit parameter so each
// lock injects its hostile value at the format level.
#[cfg(test)]
mod pentest_v2 {
    use super::*;

    // ── inline Thrift compact helpers (mirrors mod tests / pentest) ───
    fn uv(out: &mut Vec<u8>, mut v: u64) {
        loop {
            let b = (v & 0x7f) as u8;
            v >>= 7;
            if v == 0 { out.push(b); break; } else { out.push(b | 0x80); }
        }
    }
    fn zz(v: i64) -> u64 { ((v << 1) ^ (v >> 63)) as u64 }

    /// Every attacker-controlled byte of a one-column one-row-group V2
    /// file. The header field values (`hdr_*`) are written VERBATIM
    /// into the DataPageHeaderV2 / PageHeader (so a lock can declare a
    /// def_len that disagrees with the real payload, a giant
    /// num_values, etc.), while `payload` is the literal on-disk
    /// [rep][def][values] bytes and `phys_*` is what is physically
    /// written into the file's PageHeader f2/f3 size fields. `codec`
    /// is the chunk codec enum (0=UNCOMPRESSED, 1=SNAPPY, 2=GZIP).
    struct V2Spec<'a> {
        optional: bool,    // schema/chunk repetition (max_def_level 1 vs 0)
        rows: i64,         // REAL logical row count → chunk num_values / RG num_rows
                           // NOTE: rows ≠ hdr_num_values — conflating them was the initial builder defect; rows drives FileMetaData/ColumnChunk truth, hdr_* are attacker-injectable.
        codec: i64,        // chunk codec enum written to ColumnMetaData f4
        hdr_uncompressed: i64, // PageHeader f2 uncompressed_page_size
        hdr_compressed: i64,   // PageHeader f3 compressed_page_size
        hdr_num_values: i64,   // DPHv2 g1 num_values
        hdr_num_nulls: i64,    // DPHv2 g2 num_nulls
        hdr_encoding: i64,     // DPHv2 g4 (0=PLAIN, 2=PLAIN_DICTIONARY)
        hdr_def_len: i64,      // DPHv2 g5 def_levels_byte_length
        hdr_rep_len: i64,      // DPHv2 g6 rep_levels_byte_length
        hdr_is_compressed: bool, // DPHv2 g7
        payload: &'a [u8],     // literal on-disk [rep][def][values] bytes
        with_dict: bool,       // emit a leading PLAIN dict page [7]
    }

    /// Assemble `[PAR1]([dict_hdr][dict_data])?[data_hdr][payload][meta]
    /// [mlen_le][PAR1]`. Header sizes f2/f3 are taken from the spec so
    /// a lying compressed/uncompressed size is genuinely on-disk.
    fn v2_file(s: &V2Spec) -> Vec<u8> {
        // -- optional leading dictionary page (PLAIN INT64 [7]) --
        let (dict_hdr, dict_data): (Vec<u8>, Vec<u8>) = if s.with_dict {
            let mut dd = Vec::new();
            dd.extend_from_slice(&7i64.to_le_bytes());
            let db = dd.len() as i64;
            let mut dh = Vec::new();
            dh.push(0x15); uv(&mut dh, zz(2));   // f1 type=DICTIONARY_PAGE(2)
            dh.push(0x15); uv(&mut dh, zz(db));  // f2 uncompressed
            dh.push(0x15); uv(&mut dh, zz(db));  // f3 compressed
            dh.push(0x4c);                       // f7 DictionaryPageHeader
            dh.push(0x15); uv(&mut dh, zz(1));   // g1 num_values=1
            dh.push(0x15); uv(&mut dh, zz(2));   // g2 encoding=PLAIN_DICTIONARY
            dh.push(0x12);                       // g3 is_sorted=false
            dh.push(0x00); dh.push(0x00);        // stop DPH / PH
            (dh, dd)
        } else {
            (Vec::new(), Vec::new())
        };

        // -- DATA_PAGE_V2 PageHeader (all sizes/counts attacker-set) --
        let mut hdr = Vec::new();
        hdr.push(0x15); uv(&mut hdr, zz(3));                  // f1 type=DATA_PAGE_V2(3)
        hdr.push(0x15); uv(&mut hdr, zz(s.hdr_uncompressed)); // f2 uncompressed
        hdr.push(0x15); uv(&mut hdr, zz(s.hdr_compressed));   // f3 compressed
        hdr.push(0x5c);                                       // f8 struct (delta 3->8=5)
        hdr.push(0x15); uv(&mut hdr, zz(s.hdr_num_values));   // g1 num_values
        hdr.push(0x15); uv(&mut hdr, zz(s.hdr_num_nulls));    // g2 num_nulls
        hdr.push(0x15); uv(&mut hdr, zz(s.hdr_num_values));   // g3 num_rows (= num_values here)
        hdr.push(0x15); uv(&mut hdr, zz(s.hdr_encoding));     // g4 encoding
        hdr.push(0x15); uv(&mut hdr, zz(s.hdr_def_len));      // g5 def_levels_byte_length
        hdr.push(0x15); uv(&mut hdr, zz(s.hdr_rep_len));      // g6 rep_levels_byte_length
        hdr.push(if s.hdr_is_compressed { 0x11 } else { 0x12 }); // g7 is_compressed
        hdr.push(0x00); hdr.push(0x00);                       // stop DPHv2 / PH

        let dict_page_offset: i64 = 4;
        let data_page_offset: i64 =
            4 + dict_hdr.len() as i64 + dict_data.len() as i64;
        let leaf_rep: i64 = if s.optional { 1 } else { 0 };
        let enc_for_meta: i64 = if s.with_dict { 2 } else { 0 };

        let mut m = Vec::new();
        m.push(0x15); uv(&mut m, zz(2));                 // f1 version=2
        m.push(0x19); m.push(0x2c);                      // f2 list<SchemaElement> 2
        m.push(0x48); uv(&mut m, 6); m.extend_from_slice(b"schema");
        m.push(0x15); uv(&mut m, zz(1));                 // root num_children=1
        m.push(0x00);
        m.push(0x15); uv(&mut m, zz(2));                 // leaf f1 type=INT64
        m.push(0x25); uv(&mut m, zz(leaf_rep));          // f3 repetition
        m.push(0x18); uv(&mut m, 2); m.extend_from_slice(b"id");
        m.push(0x00);
        m.push(0x16); uv(&mut m, zz(s.rows));            // f3 num_rows
        m.push(0x19); m.push(0x1c);                      // f4 list<RowGroup> 1
        m.push(0x19); m.push(0x1c);                      // RG f1 list<ColumnChunk> 1
        m.push(0x3c);                                    // ColumnChunk f3 CMD
        m.push(0x15); uv(&mut m, zz(2));                 // CMD f1 type=INT64
        m.push(0x19); m.push(0x15); uv(&mut m, zz(enc_for_meta)); // f2 encodings
        m.push(0x19); m.push(0x18); uv(&mut m, 2); m.extend_from_slice(b"id");
        m.push(0x15); uv(&mut m, zz(s.codec));           // f4 codec
        m.push(0x16); uv(&mut m, zz(s.rows));            // f5 chunk num_values
        m.push(0x46); uv(&mut m, zz(data_page_offset));  // f9 data_page_offset
        if s.with_dict {
            m.push(0x26); uv(&mut m, zz(dict_page_offset)); // f11 dict_page_offset
        }
        m.push(0x00); m.push(0x00);                      // stop CMD / ColumnChunk
        m.push(0x26); uv(&mut m, zz(s.rows));            // RG f3 num_rows
        m.push(0x00); m.push(0x00);                      // stop RG / FileMetaData

        let mut f = Vec::new();
        f.extend_from_slice(b"PAR1");
        if s.with_dict {
            f.extend_from_slice(&dict_hdr);
            f.extend_from_slice(&dict_data);
        }
        f.extend_from_slice(&hdr);
        f.extend_from_slice(s.payload);
        let mlen = m.len() as u32;
        f.extend_from_slice(&m);
        f.extend_from_slice(&mlen.to_le_bytes());
        f.extend_from_slice(b"PAR1");
        f
    }

    // ── well-formed reference payloads (independently reasoned) ───────
    //
    // V2 OPTIONAL def-section for defs[1,0,1]: ONE bit-packed group of
    // 8, bit_width 1, NOT 4-byte length-prefixed (V2): hybrid header
    // (1<<1)|1 = 0x03, then one bits byte with bit i set iff defs[i]==1
    // → 0b00000101 = 0x05. PLAIN INT64 values are little-endian i64.
    fn opt_def_101() -> [u8; 2] { [0x03, 0x05] }
    fn req_no_def() -> [u8; 0] { [] }
    fn plain_i64(vals: &[i64]) -> Vec<u8> {
        let mut v = Vec::new();
        for x in vals { v.extend_from_slice(&x.to_le_bytes()); }
        v
    }

    // Helper: catch_unwind, assert NO panic + a typed Err (Bad OR
    // Unsupported). Mirrors `pentest::no_panic_typed_err` exactly.
    fn no_panic_typed_err(file: &[u8]) {
        let owned = file.to_vec();
        let r = std::panic::catch_unwind(move || extract(&owned, &["id"]));
        assert!(r.is_ok(), "must NOT panic/OOM-unwind on hostile V2 input");
        assert!(
            matches!(
                r.unwrap(),
                Err(PqError::Bad(_) | PqError::Unsupported(_))
            ),
            "hostile V2 input must yield a typed PqError"
        );
    }

    // Helper: catch_unwind, assert NO panic + a typed Err whose
    // message CONTAINS `needle` (locks the SPECIFIC guard, not just
    // "some error"). Still tolerant of Bad vs Unsupported.
    fn no_panic_err_contains(file: &[u8], needle: &str) {
        let owned = file.to_vec();
        let r = std::panic::catch_unwind(move || extract(&owned, &["id"]));
        assert!(r.is_ok(), "must NOT panic/OOM-unwind on hostile V2 input");
        let e = r.unwrap();
        let msg = match &e {
            Err(PqError::Bad(m)) | Err(PqError::Unsupported(m)) => m.clone(),
            other => panic!("expected typed PqError, got {other:?}"),
        };
        assert!(
            msg.contains(needle),
            "expected error containing {needle:?}, got {msg:?}"
        );
    }

    // ════════════════ HOSTILE LOCKS ═════════════════════════════════

    // L1: lying def_len so rep_len(0)+def_len > compressed_size →
    // lvl_end > region.len() → Bad, no OOB read. payload is the real
    // 2-byte def section but g5 claims def_len=64 (>> region).
    #[test]
    fn v2_lying_def_len_exceeds_region_bad_no_oob() {
        let payload = {
            let mut p = opt_def_101().to_vec();
            p.extend_from_slice(&plain_i64(&[7, -2]));
            p
        };
        let f = v2_file(&V2Spec {
            optional: true, codec: 0,
            rows: 3,
            hdr_uncompressed: payload.len() as i64,
            hdr_compressed: payload.len() as i64,
            hdr_num_values: 3, hdr_num_nulls: 1, hdr_encoding: 0,
            hdr_def_len: 64, // LIE: 64 > payload.len()
            hdr_rep_len: 0, hdr_is_compressed: false,
            payload: &payload, with_dict: false,
        });
        no_panic_err_contains(&f, "v2 levels exceed page");
    }

    // L1b: lying rep_len so rep_len+def_len > compressed_size. (rep>0
    // is itself Unsupported — see L2 — so to exercise the level-len /
    // region bound we keep rep small but make rep+def overflow region
    // via a huge def_len AND a valid-looking small rep. Covered by L1;
    // here we additionally lock rep_len+def_len addition overflow.)
    #[test]
    fn v2_level_len_add_overflow_typed_bad() {
        // def_len = usize::MAX-ish via i32::MAX, rep_len exercised by L2.
        let payload = plain_i64(&[7, -2]);
        let f = v2_file(&V2Spec {
            optional: true, codec: 0,
            rows: 2,
            hdr_uncompressed: payload.len() as i64,
            hdr_compressed: payload.len() as i64,
            hdr_num_values: 2, hdr_num_nulls: 0, hdr_encoding: 0,
            hdr_def_len: i32::MAX as i64, // enormous declared def_len
            hdr_rep_len: 0, hdr_is_compressed: false,
            payload: &payload, with_dict: false,
        });
        // i32::MAX def_len >> region → "v2 levels exceed page" Bad,
        // fast, no OOB and no allocation of i32::MAX bytes.
        no_panic_err_contains(&f, "v2 levels exceed page");
    }

    // L2: rep_len > 0 → Unsupported("REPEATED/nested V2 ... OBJ-2c-5"),
    // no panic. Payload carries 1 rep byte + def + values so the
    // declared rep_len is internally consistent (isolating the guard).
    #[test]
    fn v2_rep_len_nonzero_unsupported_obj2c5() {
        let mut payload = vec![0x00]; // 1 rep byte
        payload.extend_from_slice(&opt_def_101());
        payload.extend_from_slice(&plain_i64(&[7, -2]));
        let f = v2_file(&V2Spec {
            optional: true, codec: 0,
            rows: 3,
            hdr_uncompressed: payload.len() as i64,
            hdr_compressed: payload.len() as i64,
            hdr_num_values: 3, hdr_num_nulls: 1, hdr_encoding: 0,
            hdr_def_len: 2, hdr_rep_len: 1, // rep_len > 0
            hdr_is_compressed: false,
            payload: &payload, with_dict: false,
        });
        no_panic_err_contains(&f, "REPEATED/nested V2");
        no_panic_err_contains(&f, "OBJ-2c-5");
    }

    // L3: uncompressed_page_size < rep_len+def_len → vt = uncomp -
    // lvl_end underflows → Bad("v2 values target underflow").
    // Region is sized by compressed_size (real), but g2 f2
    // uncompressed is a LIE smaller than lvl_end.
    #[test]
    fn v2_uncompressed_lt_levels_vt_underflow_bad() {
        let payload = {
            let mut p = opt_def_101().to_vec();
            p.extend_from_slice(&plain_i64(&[7, -2]));
            p
        };
        let f = v2_file(&V2Spec {
            optional: true, codec: 0,
            rows: 3,
            hdr_uncompressed: 1, // LIE: 1 < def_len(2)
            hdr_compressed: payload.len() as i64,
            hdr_num_values: 3, hdr_num_nulls: 1, hdr_encoding: 0,
            hdr_def_len: 2, hdr_rep_len: 0, hdr_is_compressed: false,
            payload: &payload, with_dict: false,
        });
        no_panic_err_contains(&f, "v2 values target underflow");
    }

    // L4: a V2 OPTIONAL def-level value > 1. max_def_level==1 but the
    // bit-packed byte sets a 2-bit-ish pattern. To force a decoded
    // level > 1 we widen the def section to bit_width 2: hybrid header
    // (1<<1)|1=0x03, then ceil(3 levels * 2bits /8)=1 byte with the
    // first level = 0b11 = 3. decode_hybrid(_, bit_width=1, n) is
    // called by the decoder with bit_width fixed to 1, so to actually
    // surface ">1" we instead pack level value 1,1,1 but claim n so a
    // packed bit decodes as a def of value... the decoder hard-codes
    // bit_width=1, max=1. The only way a decoded def exceeds 1 is RLE:
    // an RLE run of value 3. Build def = RLE run: header (count<<1)|0,
    // value byte. count=3 → header (3<<1)|0 = 0x06, then 1 value byte
    // = 0x03 (def level 3 > max 1). That is a spec-valid hybrid stream
    // decoding to [3,3,3] → triggers "v2 def-level exceeds max".
    #[test]
    fn v2_def_level_exceeds_max_bad() {
        // RLE run, len width = ceil(bit_width=1 /8)=1 byte → value 0x03.
        let def_section: &[u8] = &[0x06, 0x03]; // RLE run x3 of value 3
        let mut payload = def_section.to_vec();
        payload.extend_from_slice(&plain_i64(&[7, -2]));
        let f = v2_file(&V2Spec {
            optional: true, codec: 0,
            rows: 3,
            hdr_uncompressed: payload.len() as i64,
            hdr_compressed: payload.len() as i64,
            hdr_num_values: 3, hdr_num_nulls: 1, hdr_encoding: 0,
            hdr_def_len: def_section.len() as i64,
            hdr_rep_len: 0, hdr_is_compressed: false,
            payload: &payload, with_dict: false,
        });
        no_panic_err_contains(&f, "v2 def-level exceeds max");
    }

    // L5: num_nulls inconsistent with decoded def-levels. defs[1,0,1]
    // → 1 null, 2 present; but g2 num_nulls is LIED to 0 → n - nn (3)
    // != present (2) → Bad("v2 num_nulls vs def-levels mismatch").
    #[test]
    fn v2_num_nulls_vs_def_levels_mismatch_bad() {
        let mut payload = opt_def_101().to_vec();
        payload.extend_from_slice(&plain_i64(&[7, -2]));
        let f = v2_file(&V2Spec {
            optional: true, codec: 0,
            rows: 3,
            hdr_uncompressed: payload.len() as i64,
            hdr_compressed: payload.len() as i64,
            hdr_num_values: 3,
            hdr_num_nulls: 0, // LIE: real null count is 1
            hdr_encoding: 0,
            hdr_def_len: 2, hdr_rep_len: 0, hdr_is_compressed: false,
            payload: &payload, with_dict: false,
        });
        no_panic_err_contains(&f, "v2 num_nulls vs def-levels mismatch");
    }

    // L6: uncompressed / !is_compressed values section length != vt.
    // codec=UNCOMPRESSED, def section [0x03,0x05] (2 bytes), values =
    // ONE i64 (8 bytes) but uncompressed_page_size claims 2+16=18 so
    // vt=16 while values_section.len()=8 → Bad("v2 raw values length
    // mismatch").
    #[test]
    fn v2_raw_values_length_mismatch_bad() {
        let mut payload = opt_def_101().to_vec();
        payload.extend_from_slice(&plain_i64(&[7])); // only 8 bytes of values
        let f = v2_file(&V2Spec {
            optional: true, codec: 0,
            rows: 3,
            hdr_uncompressed: 18, // claims vt = 18-2 = 16
            hdr_compressed: payload.len() as i64, // real region = 10
            hdr_num_values: 3, hdr_num_nulls: 1, hdr_encoding: 0,
            hdr_def_len: 2, hdr_rep_len: 0, hdr_is_compressed: false,
            payload: &payload, with_dict: false,
        });
        no_panic_err_contains(&f, "v2 raw values length mismatch");
    }

    // L7: region shorter than rep_len+def_len (truncated V2 page):
    // declare def_len=2 but the on-disk payload is EMPTY (0 bytes), so
    // the V2 region is shorter than lvl_end → Bad, typed, no slice
    // panic. compressed_size=0 → v2_region is a 0-length slice.
    #[test]
    fn v2_truncated_page_shorter_than_levels_bad() {
        let payload: &[u8] = &[];
        let f = v2_file(&V2Spec {
            optional: true, codec: 0,
            rows: 3,
            hdr_uncompressed: 2, hdr_compressed: 0, // region = 0 bytes
            hdr_num_values: 3, hdr_num_nulls: 1, hdr_encoding: 0,
            hdr_def_len: 2, // > region.len()==0
            hdr_rep_len: 0, hdr_is_compressed: false,
            payload, with_dict: false,
        });
        no_panic_err_contains(&f, "v2 levels exceed page");
    }

    // L8a: V2 + corrupt Snappy values section, is_compressed=true,
    // codec=SNAPPY(1). Garbage values bytes → snappy::decompress
    // returns a typed Bad/Unsupported (preamble/cap) FAST, no
    // panic/OOM. def section is valid; values = 8 garbage bytes;
    // uncompressed_page_size makes vt small (16) so the snappy cap is
    // not hit but the garbage preamble fails fast.
    #[test]
    fn v2_corrupt_snappy_values_typed_no_oom() {
        let mut payload = opt_def_101().to_vec();
        payload.extend_from_slice(&[0xFF, 0xFE, 0xAB, 0xCD, 0x01, 0x02, 0x03, 0x04]);
        let f = v2_file(&V2Spec {
            optional: true, codec: 1, // SNAPPY
            rows: 3,
            hdr_uncompressed: 18, // vt = 16
            hdr_compressed: payload.len() as i64,
            hdr_num_values: 3, hdr_num_nulls: 1, hdr_encoding: 0,
            hdr_def_len: 2, hdr_rep_len: 0,
            hdr_is_compressed: true, // force the Snappy arm
            payload: &payload, with_dict: false,
        });
        no_panic_typed_err(&f);
    }

    // L8b: V2 + corrupt GZIP values section, is_compressed=true,
    // codec=GZIP(2). Garbage values bytes (no 0x1f 0x8b magic) →
    // gzip::decompress returns Bad("gzip magic")/"member too short"
    // FAST, no panic/OOM.
    #[test]
    fn v2_corrupt_gzip_values_typed_no_oom() {
        let mut payload = opt_def_101().to_vec();
        payload.extend_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x11, 0x22, 0x33]);
        let f = v2_file(&V2Spec {
            optional: true, codec: 2, // GZIP
            rows: 3,
            hdr_uncompressed: 18, // vt = 16
            hdr_compressed: payload.len() as i64,
            hdr_num_values: 3, hdr_num_nulls: 1, hdr_encoding: 0,
            hdr_def_len: 2, hdr_rep_len: 0,
            hdr_is_compressed: true, // force the Gzip arm
            payload: &payload, with_dict: false,
        });
        no_panic_typed_err(&f);
    }

    // L8c: V2 + GZIP, is_compressed=true, attacker declares a HUGE
    // uncompressed_page_size so vt is enormous → gzip::decompress
    // hits the GZIP_MAX_DECOMP cap → Unsupported, FAST, NO multi-GB
    // allocation (cap check precedes any Vec reservation).
    #[test]
    fn v2_gzip_huge_uncompressed_hits_cap_no_oom() {
        let mut payload = opt_def_101().to_vec();
        payload.extend_from_slice(&[0x1f, 0x8b, 0x08, 0x00, 0, 0, 0, 0]);
        let f = v2_file(&V2Spec {
            optional: true, codec: 2,
            rows: 3,
            hdr_uncompressed: i32::MAX as i64, // vt ~= 2GiB → cap
            hdr_compressed: payload.len() as i64,
            hdr_num_values: 3, hdr_num_nulls: 1, hdr_encoding: 0,
            hdr_def_len: 2, hdr_rep_len: 0,
            hdr_is_compressed: true,
            payload: &payload, with_dict: false,
        });
        no_panic_typed_err(&f);
    }

    // L9: def_len declared non-zero for a REQUIRED (max_def_level==0)
    // column → Bad("v2 def_len non-zero for REQUIRED"). optional=false
    // so the schema/chunk leaf is REQUIRED, but g5 def_len=2.
    #[test]
    fn v2_def_len_nonzero_for_required_bad() {
        let mut payload = opt_def_101().to_vec(); // 2 stray "def" bytes
        payload.extend_from_slice(&plain_i64(&[7, -2]));
        let f = v2_file(&V2Spec {
            optional: false, // REQUIRED → max_def_level 0
            rows: 2,
            codec: 0,
            hdr_uncompressed: payload.len() as i64,
            hdr_compressed: payload.len() as i64,
            hdr_num_values: 2, hdr_num_nulls: 0, hdr_encoding: 0,
            hdr_def_len: 2, // non-zero for REQUIRED
            hdr_rep_len: 0, hdr_is_compressed: false,
            payload: &payload, with_dict: false,
        });
        no_panic_err_contains(&f, "v2 def_len non-zero for REQUIRED");
    }

    // L10: huge declared num_values + huge uncompressed_page_size with
    // a tiny actual page → must error FAST with a typed safety-bounded
    // error, NO multi-GB allocation / OOM. num_values=i32::MAX,
    // REQUIRED so the decoder reaches decode_plain(present=i32::MAX)
    // whose reservation is bounded by data.len() (Task-12 fix);
    // uncompressed=i32::MAX would make vt huge but the SP151 cap-check
    // (256 MiB) now fires first as Unsupported. Either Bad or
    // Unsupported satisfies the pentest contract ("no panic / no OOM /
    // typed error") — the SP151 cap fast-rejects this hostile input
    // earlier than the V1 raw-length-mismatch guard did.
    #[test]
    fn v2_huge_num_values_and_size_tiny_page_no_oom() {
        let payload = plain_i64(&[7, -2]); // 16 bytes only
        let f = v2_file(&V2Spec {
            optional: false, codec: 0,
            rows: 2,
            hdr_uncompressed: i32::MAX as i64,
            hdr_compressed: payload.len() as i64,
            hdr_num_values: i32::MAX as i64,
            hdr_num_nulls: 0, hdr_encoding: 0,
            hdr_def_len: 0, hdr_rep_len: 0, hdr_is_compressed: false,
            payload: &payload, with_dict: false,
        });
        // Must return typed Err fast (no hang / no OOM-abort).
        no_panic_typed_err(&f);
        assert!(
            matches!(
                extract(&f, &["id"]),
                Err(PqError::Bad(_)) | Err(PqError::Unsupported(_))
            ),
            "i32::MAX num_values/size vs tiny V2 page must be typed err"
        );
    }

    // SP151: V2-specific cap pentest. Pins that the V2 cap-check fires
    // BEFORE allocating decompression buffers — the V2 decode path is
    // independent of V1's page_payload codec dispatch and was the
    // hardest to plumb (two distinct cap sites: comp at the page-loop,
    // uncomp at decode_data_page_v2). Hostile uncompressed=2 GiB +
    // compressed=16 bytes (tiny actual payload) triggers the
    // Unsupported(SP151) at the V2 comp-cap check first.
    #[test]
    fn v2_sp151_uncomp_cap_check_fires_with_named_followup() {
        let payload = plain_i64(&[7, -2]);
        let f = v2_file(&V2Spec {
            optional: false, codec: 0,
            rows: 2,
            hdr_uncompressed: 300 * 1024 * 1024, // 300 MiB > 256 MiB cap
            hdr_compressed: payload.len() as i64,
            hdr_num_values: 2,
            hdr_num_nulls: 0, hdr_encoding: 0,
            hdr_def_len: 0, hdr_rep_len: 0, hdr_is_compressed: false,
            payload: &payload, with_dict: false,
        });
        match extract(&f, &["id"]) {
            Err(PqError::Unsupported(msg)) => {
                assert!(msg.contains("SP151"), "V2 cap rejection must name SP151: {msg}");
                assert!(
                    msg.contains("v2 page"),
                    "V2 cap rejection must identify the page kind: {msg}"
                );
            }
            other => panic!("expected Unsupported(SP151 v2...), got {other:?}"),
        }
    }

    // ════════════════ POSITIVE CORRECTNESS LOCKS ════════════════════
    //
    // These assert the EXACT decoded rows. A failure here is a decoder
    // bug → BLOCKED, never weaken the expectation.
    //
    // Note on positive-lock scope: the V2+INT96 positive lock and the FLBA-dict
    // positive lock from the original SP108 T5 plan are NOT present here.
    // V2 page structure is covered by `mod pentest_v2` (generic across physical
    // types); H5 above proves V2+INT96 compose safely on the hostile path.
    // FLBA-dict positive coverage is superseded by P7 (precision=38 boundary)
    // and P8 (i128::MIN sign-extend), which are higher-yield for this surface.

    // P1: V2 PLAIN REQUIRED [7,-2] → [[I64(7)],[I64(-2)]].
    #[test]
    fn v2_plain_required_positive_lock() {
        let payload = {
            let mut p = req_no_def().to_vec();
            p.extend_from_slice(&plain_i64(&[7, -2]));
            p
        };
        let f = v2_file(&V2Spec {
            optional: false, codec: 0,
            rows: 2,
            hdr_uncompressed: payload.len() as i64,
            hdr_compressed: payload.len() as i64,
            hdr_num_values: 2, hdr_num_nulls: 0, hdr_encoding: 0,
            hdr_def_len: 0, hdr_rep_len: 0, hdr_is_compressed: false,
            payload: &payload, with_dict: false,
        });
        assert_eq!(
            extract(&f, &["id"]).expect("v2 plain required"),
            vec![vec![PqValue::I64(7)], vec![PqValue::I64(-2)]]
        );
    }

    // P2: V2 PLAIN OPTIONAL [7,null,-2] → scatter
    // [[I64(7)],[Null],[I64(-2)]].
    #[test]
    fn v2_plain_optional_scatter_positive_lock() {
        let mut payload = opt_def_101().to_vec(); // defs [1,0,1]
        payload.extend_from_slice(&plain_i64(&[7, -2]));
        let f = v2_file(&V2Spec {
            optional: true, codec: 0,
            rows: 3,
            hdr_uncompressed: payload.len() as i64,
            hdr_compressed: payload.len() as i64,
            hdr_num_values: 3, hdr_num_nulls: 1, hdr_encoding: 0,
            hdr_def_len: 2, hdr_rep_len: 0, hdr_is_compressed: false,
            payload: &payload, with_dict: false,
        });
        assert_eq!(
            extract(&f, &["id"]).expect("v2 plain optional"),
            vec![
                vec![PqValue::I64(7)],
                vec![PqValue::Null],
                vec![PqValue::I64(-2)]
            ]
        );
    }

    // P3: V2 + dict [7,null,7] → [[I64(7)],[Null],[I64(7)]]. dict
    // page = PLAIN [7]; values section = [bit_width=1][hybrid hdr
    // 0x03][bits 0x00] → 2 present dict indices both 0 (SP105 KAT).
    #[test]
    fn v2_dict_positive_lock() {
        let mut payload = opt_def_101().to_vec(); // defs [1,0,1]
        payload.extend_from_slice(&[0x01, 0x03, 0x00]); // dict idx 0,0
        let f = v2_file(&V2Spec {
            optional: true, codec: 0,
            rows: 3,
            hdr_uncompressed: payload.len() as i64,
            hdr_compressed: payload.len() as i64,
            hdr_num_values: 3, hdr_num_nulls: 1,
            hdr_encoding: 2, // PLAIN_DICTIONARY
            hdr_def_len: 2, hdr_rep_len: 0, hdr_is_compressed: false,
            payload: &payload, with_dict: true,
        });
        assert_eq!(
            extract(&f, &["id"]).expect("v2 dict"),
            vec![
                vec![PqValue::I64(7)],
                vec![PqValue::Null],
                vec![PqValue::I64(7)]
            ]
        );
    }

    // P4: V2 file with codec=GZIP(2) but is_compressed=FALSE — raw
    // (uncompressed) values under a gzip CHUNK codec must decode
    // correctly: the `_ if !ph.v2_is_compressed` arm overrides the
    // column codec and treats the values section as raw. Logical
    // [7,-2] REQUIRED → [[I64(7)],[I64(-2)]].
    //
    // The V2 + gzip *COMPRESSED* compose is already proven
    // NON-self-referentially by Task 4's real-pyarrow
    // `crates/kessel-parquet/tests/fixtures/v2_gzip.parquet`
    // roundtrip (tests/fixture_roundtrip.rs) — NOT rebuilt here.
    #[test]
    fn v2_gzip_codec_but_not_compressed_raw_positive_lock() {
        let payload = {
            let mut p = req_no_def().to_vec();
            p.extend_from_slice(&plain_i64(&[7, -2]));
            p
        };
        let f = v2_file(&V2Spec {
            optional: false,
            rows: 2,
            codec: 2, // GZIP chunk codec
            hdr_uncompressed: payload.len() as i64,
            hdr_compressed: payload.len() as i64,
            hdr_num_values: 2, hdr_num_nulls: 0, hdr_encoding: 0,
            hdr_def_len: 0, hdr_rep_len: 0,
            hdr_is_compressed: false, // raw values despite GZIP codec
            payload: &payload, with_dict: false,
        });
        assert_eq!(
            extract(&f, &["id"]).expect("v2 gzip-codec raw values"),
            vec![vec![PqValue::I64(7)], vec![PqValue::I64(-2)]]
        );
    }
}

// ── SP108 T5 PENTEST PASS — INT96 + DECIMAL + FLBA decode adversarial ─
//
// SP108 added three attacker-influenceable decode paths to plain.rs:
//   1. INT96 (12-byte LE u64 nanos_of_day + u32 julian_day → Timestamp(ns))
//   2. DECIMAL (logical type carried on INT32/INT64/FLBA/BYTE_ARRAY,
//      decoded as i128 unscaled + scale)
//   3. FLBA non-DECIMAL (raw type_length bytes → Bytes)
//
// Every byte in every code path above is attacker-controlled. The locks
// below assert: NO panic, NO OOM, NO hang on hostile inputs (catch_unwind
// proves no panic; fast typed-Err proves no OOM/hang); and TYPED
// PqError::Bad/Unsupported with the right SPECIFIC message — locking the
// exact guard, not merely "some error". Positive locks assert byte-exact
// Ok(rows) — a positive-lock failure is a decoder bug (BLOCKED, never
// weaken), a hostile-input panic/OOM/hang is a real vuln (BLOCKED with
// exact detail).
//
// Every corrupt byte is hand-derived from parquet.thrift / the SP108
// decoder recipes (JULIAN_UNIX_EPOCH=2_440_588, NS_PER_DAY=86_400e9, i128
// sign-extend), NOT produced by the code under test. The builders take
// every attacker-controlled field as an explicit parameter; each test
// injects its hostile value at the format level.
#[cfg(test)]
mod pentest_int96_decimal {
    use super::*;

    // ── inline Thrift compact helpers (mirrors mod tests / pentest_v2) ─
    fn uv(out: &mut Vec<u8>, mut v: u64) {
        loop {
            let b = (v & 0x7f) as u8;
            v >>= 7;
            if v == 0 { out.push(b); break; } else { out.push(b | 0x80); }
        }
    }
    fn zz(v: i64) -> u64 { ((v << 1) ^ (v >> 63)) as u64 }

    /// Schema-level leaf metadata for one column "d". Every field is
    /// attacker-controlled so hostile locks can inject DECIMAL on INT96,
    /// type_length out of range, etc. None disables emission of that
    /// optional field.
    struct LeafMeta<'a> {
        /// Parquet Type enum: 0=BOOL, 1=INT32, 2=INT64, 3=INT96,
        /// 6=BYTE_ARRAY, 7=FIXED_LEN_BYTE_ARRAY.
        ptype: i64,
        /// FixedLenByteArray byte width (None for non-FLBA).
        type_length: Option<i64>,
        /// Repetition: 0=REQUIRED, 1=OPTIONAL.
        rep: i64,
        /// ConvertedType (5=DECIMAL). None disables.
        converted_type: Option<i64>,
        /// SchemaElement.f7 scale.
        scale: Option<i64>,
        /// SchemaElement.f8 precision.
        precision: Option<i64>,
        /// LogicalType DecimalType{scale,precision}. None disables.
        /// Used by the converted-vs-logical disagreement lock.
        logical_decimal: Option<(i64, i64)>,
        col: &'a [u8],
    }

    impl<'a> LeafMeta<'a> {
        fn plain(ptype: i64, col: &'a [u8]) -> Self {
            Self {
                ptype,
                type_length: None,
                rep: 0,
                converted_type: None,
                scale: None,
                precision: None,
                logical_decimal: None,
                col,
            }
        }
    }

    /// Emit the schema leaf SchemaElement struct bytes. Mirrors the
    /// hand-built leaves in `crate::meta::tests` exactly: each optional
    /// field tracks the LAST emitted id so the field-delta (compact
    /// thrift) is computed correctly.
    fn schema_leaf_bytes(m: &LeafMeta) -> Vec<u8> {
        let mut leaf = Vec::new();
        let mut last_id: i64 = 0;
        // f1 type: always emitted.
        leaf.push(0x15); uv(&mut leaf, zz(m.ptype));
        last_id = 1;
        // f2 type_length (i32).
        if let Some(n) = m.type_length {
            let delta = (2 - last_id) as u8;
            leaf.push((delta << 4) | 5);
            uv(&mut leaf, zz(n));
            last_id = 2;
        }
        // f3 repetition (i32).
        let delta = (3 - last_id) as u8;
        leaf.push((delta << 4) | 5);
        uv(&mut leaf, zz(m.rep));
        last_id = 3;
        // f4 name (binary).
        let delta = (4 - last_id) as u8;
        leaf.push((delta << 4) | 8);
        uv(&mut leaf, m.col.len() as u64);
        leaf.extend_from_slice(m.col);
        last_id = 4;
        // f6 converted_type (i32).
        if let Some(ct) = m.converted_type {
            let delta = (6 - last_id) as u8;
            leaf.push((delta << 4) | 5);
            uv(&mut leaf, zz(ct));
            last_id = 6;
        }
        // f7 scale (i32).
        if let Some(s) = m.scale {
            let delta = (7 - last_id) as u8;
            leaf.push((delta << 4) | 5);
            uv(&mut leaf, zz(s));
            last_id = 7;
        }
        // f8 precision (i32).
        if let Some(p) = m.precision {
            let delta = (8 - last_id) as u8;
            leaf.push((delta << 4) | 5);
            uv(&mut leaf, zz(p));
            last_id = 8;
        }
        // f10 LogicalType union → arm 5 DecimalType{1:scale, 2:precision}.
        if let Some((ls, lp)) = m.logical_decimal {
            let delta = (10 - last_id) as u8;
            leaf.push((delta << 4) | 12); // STRUCT
            // LogicalType union inner (field IDs reset). f5 DecimalType
            // struct: delta 0->5=5 -> (5<<4)|12=0x5c
            leaf.push(0x5c);
            // DecimalType inner. f1 scale, f2 precision.
            leaf.push(0x15); uv(&mut leaf, zz(ls));
            leaf.push(0x15); uv(&mut leaf, zz(lp));
            leaf.push(0x00); // stop DecimalType
            leaf.push(0x00); // stop LogicalType union
        }
        // last_id is intentionally write-only (no further fields).
        let _ = last_id;
        leaf.push(0x00); // stop SchemaElement
        leaf
    }

    /// Full FileMetaData for a single-column, single-row-group V1
    /// PLAIN file. `num_rows` is the chunk + RG + leaf row count.
    /// `data_page_offset` is the absolute byte offset of the data
    /// page header (4 if right after PAR1, else after the dict page).
    /// `encoding` is the chunk encoding written into ColumnMetaData
    /// (0=PLAIN, 2=PLAIN_DICTIONARY); leaves are otherwise identical.
    fn filemetadata_bytes(
        leaf: &LeafMeta,
        num_rows: i64,
        encoding: i64,
        data_page_offset: i64,
        dict_page_offset: Option<i64>,
    ) -> Vec<u8> {
        let leaf_bytes = schema_leaf_bytes(leaf);
        let mut b = Vec::new();
        b.push(0x15); uv(&mut b, zz(2));                     // f1 version=2
        b.push(0x19); b.push(0x2c);                          // f2 list<SchemaElement> 2
        // schema[0] root group
        b.push(0x48); uv(&mut b, 6); b.extend_from_slice(b"schema");
        b.push(0x15); uv(&mut b, zz(1));                     // root num_children=1
        b.push(0x00);
        // schema[1] leaf
        b.extend_from_slice(&leaf_bytes);
        b.push(0x16); uv(&mut b, zz(num_rows));              // f3 num_rows
        b.push(0x19); b.push(0x1c);                          // f4 list<RowGroup> 1
        b.push(0x19); b.push(0x1c);                          // RG f1 list<ColumnChunk> 1
        b.push(0x3c);                                        // ColumnChunk f3 ColumnMetaData
        b.push(0x15); uv(&mut b, zz(leaf.ptype));            // CMD f1 type
        b.push(0x19); b.push(0x15); uv(&mut b, zz(encoding)); // f2 encodings [encoding]
        b.push(0x19); b.push(0x18); uv(&mut b, leaf.col.len() as u64);
        b.extend_from_slice(leaf.col);                        // f3 path_in_schema
        b.push(0x15); uv(&mut b, zz(0));                     // f4 codec=UNCOMPRESSED
        b.push(0x16); uv(&mut b, zz(num_rows));              // f5 num_values
        b.push(0x46); uv(&mut b, zz(data_page_offset));      // f9 data_page_offset
        if let Some(dp) = dict_page_offset {
            b.push(0x26); uv(&mut b, zz(dp));                // f11 dictionary_page_offset
        }
        b.push(0x00); b.push(0x00);                          // stop CMD / ColumnChunk
        b.push(0x26); uv(&mut b, zz(num_rows));              // RG f3 num_rows
        b.push(0x00); b.push(0x00);                          // stop RG / FileMetaData
        b
    }

    /// V1 PLAIN page header (DATA_PAGE(0), PLAIN(0) encoding).
    fn plain_v1_page_hdr(num_values: i32, data_bytes: i32) -> Vec<u8> {
        let mut h = Vec::new();
        h.push(0x15); uv(&mut h, zz(0));                           // f1 type=DATA_PAGE(0)
        h.push(0x15); uv(&mut h, zz(data_bytes as i64));           // f2 uncompressed
        h.push(0x15); uv(&mut h, zz(data_bytes as i64));           // f3 compressed
        h.push(0x2c);                                              // f5 DPH (delta 3->5=2)
        h.push(0x15); uv(&mut h, zz(num_values as i64));           // g1 num_values
        h.push(0x15); uv(&mut h, zz(0));                           // g2 encoding=PLAIN
        h.push(0x00); h.push(0x00);                                // stop DPH / PH
        h
    }

    /// V1 dict-encoded data-page header (DATA_PAGE(0),
    /// PLAIN_DICTIONARY(2) encoding).
    fn dict_v1_data_page_hdr(num_values: i32, data_bytes: i32) -> Vec<u8> {
        let mut h = Vec::new();
        h.push(0x15); uv(&mut h, zz(0));
        h.push(0x15); uv(&mut h, zz(data_bytes as i64));
        h.push(0x15); uv(&mut h, zz(data_bytes as i64));
        h.push(0x2c);
        h.push(0x15); uv(&mut h, zz(num_values as i64));
        h.push(0x15); uv(&mut h, zz(2));                           // g2 encoding=PLAIN_DICTIONARY
        h.push(0x00); h.push(0x00);
        h
    }

    /// Dictionary page header (DICTIONARY_PAGE(2), PLAIN_DICTIONARY(2)).
    fn dict_page_hdr(num_dict_values: i32, data_bytes: i32) -> Vec<u8> {
        let mut h = Vec::new();
        h.push(0x15); uv(&mut h, zz(2));                           // f1 type=DICTIONARY_PAGE(2)
        h.push(0x15); uv(&mut h, zz(data_bytes as i64));
        h.push(0x15); uv(&mut h, zz(data_bytes as i64));
        h.push(0x4c);                                              // f7 DPH (delta 3->7=4)
        h.push(0x15); uv(&mut h, zz(num_dict_values as i64));      // g1 num_values
        h.push(0x15); uv(&mut h, zz(2));                           // g2 encoding=PLAIN_DICTIONARY
        h.push(0x12);                                              // g3 is_sorted=false
        h.push(0x00); h.push(0x00);
        h
    }

    /// Assemble `[PAR1][page_hdr][page_data][meta][mlen_le][PAR1]`.
    fn assemble_one_page(hdr: &[u8], data: &[u8], meta: &[u8]) -> Vec<u8> {
        let mut f = Vec::new();
        f.extend_from_slice(b"PAR1");
        f.extend_from_slice(hdr);
        f.extend_from_slice(data);
        let mlen = meta.len() as u32;
        f.extend_from_slice(meta);
        f.extend_from_slice(&mlen.to_le_bytes());
        f.extend_from_slice(b"PAR1");
        f
    }

    /// Assemble a dict-page file
    /// `[PAR1][dict_hdr][dict_data][data_hdr][data_payload][meta][mlen_le][PAR1]`.
    fn assemble_dict_page(
        dict_hdr: &[u8], dict_data: &[u8],
        data_hdr: &[u8], data_payload: &[u8],
        meta: &[u8],
    ) -> Vec<u8> {
        let mut f = Vec::new();
        f.extend_from_slice(b"PAR1");
        f.extend_from_slice(dict_hdr);
        f.extend_from_slice(dict_data);
        f.extend_from_slice(data_hdr);
        f.extend_from_slice(data_payload);
        let mlen = meta.len() as u32;
        f.extend_from_slice(meta);
        f.extend_from_slice(&mlen.to_le_bytes());
        f.extend_from_slice(b"PAR1");
        f
    }

    /// 12-byte little-endian INT96 payload from (nanos_of_day,
    /// julian_day). Independent of the code under test: parquet.thrift
    /// INT96 = `<u64 nanos_of_day LE><u32 julian_day LE>` (12 bytes).
    fn int96_le(nod: u64, jd: u32) -> [u8; 12] {
        let mut out = [0u8; 12];
        out[..8].copy_from_slice(&nod.to_le_bytes());
        out[8..].copy_from_slice(&jd.to_le_bytes());
        out
    }

    // ── catch_unwind helpers — mirrors `pentest_v2::no_panic_typed_err`
    // and `pentest_v2::no_panic_err_contains` EXACTLY (column name is
    // always "d" for this module). Kept for parity with the V2 pentest
    // module's helper surface; some specific call sites use the
    // `_contains` variant below directly, so this fn may currently look
    // unused in this module — leave it for future hostile-vector tests.

    #[allow(dead_code)]
    fn no_panic_typed_err(file: &[u8]) {
        let owned = file.to_vec();
        let r = std::panic::catch_unwind(move || extract(&owned, &["d"]));
        assert!(r.is_ok(), "must NOT panic/OOM-unwind on hostile input");
        assert!(
            matches!(
                r.unwrap(),
                Err(PqError::Bad(_) | PqError::Unsupported(_))
            ),
            "hostile input must yield a typed PqError"
        );
    }

    fn no_panic_err_contains(file: &[u8], needle: &str) {
        let owned = file.to_vec();
        let needle = needle.to_string();
        let r = std::panic::catch_unwind(move || extract(&owned, &["d"]));
        assert!(r.is_ok(), "must NOT panic/OOM-unwind on hostile input");
        let e = r.unwrap();
        let msg = match &e {
            Err(PqError::Bad(m)) | Err(PqError::Unsupported(m)) => m.clone(),
            other => panic!("expected typed PqError, got {other:?}"),
        };
        assert!(
            msg.contains(&needle),
            "expected error containing {needle:?}, got {msg:?}"
        );
    }

    // ════════════════ HOSTILE LOCKS — INT96 ═══════════════════════════

    // H1: truncated INT96 page — declare num_values=1 (needs 12 bytes),
    // provide only 11. `decode_plain` `data.get(..12).ok_or("int96
    // truncated")` fires; NO from_le_bytes panic, NO OOB.
    #[test]
    fn int96_truncated_payload_bad() {
        let payload = vec![0u8; 11]; // one byte short of a single INT96
        let mut leaf = LeafMeta::plain(3 /*INT96*/, b"d");
        leaf.rep = 0; // REQUIRED
        let hdr = plain_v1_page_hdr(1, payload.len() as i32);
        let meta = filemetadata_bytes(&leaf, 1, 0, 4, None);
        let f = assemble_one_page(&hdr, &payload, &meta);
        no_panic_err_contains(&f, "int96 truncated");
    }

    // H2: INT96 julian_day overflow — jd=u32::MAX. Per plain.rs:
    //   day_offset = i64::from(u32::MAX) - 2_440_588 = 4_292_526_707
    //   day_offset * NS_PER_DAY (86_400e9) ≈ 3.7e23, overflows i64::MAX
    //   (9.2e18) → checked_mul None → Bad("int96 ns overflow").
    // Hand-derived from JULIAN_UNIX_EPOCH=2_440_588 + NS_PER_DAY recipe,
    // NOT from the decoder.
    #[test]
    fn int96_julian_day_overflow_bad() {
        let payload = int96_le(0, u32::MAX); // nod=0 (valid), jd huge
        let leaf = LeafMeta::plain(3 /*INT96*/, b"d");
        let hdr = plain_v1_page_hdr(1, payload.len() as i32);
        let meta = filemetadata_bytes(&leaf, 1, 0, 4, None);
        let f = assemble_one_page(&hdr, &payload, &meta);
        no_panic_err_contains(&f, "int96 ns overflow");
    }

    // H3: INT96 nanos_of_day at the exclusive upper bound
    // (86_400_000_000_000 == 1 day in ns; valid range is `[0,
    // 86_400e9)`). Hand-derived from parquet.thrift INT96 semantics.
    #[test]
    fn int96_nanos_of_day_out_of_range_bad() {
        let payload = int96_le(86_400_000_000_000, 2_440_588);
        let leaf = LeafMeta::plain(3 /*INT96*/, b"d");
        let hdr = plain_v1_page_hdr(1, payload.len() as i32);
        let meta = filemetadata_bytes(&leaf, 1, 0, 4, None);
        let f = assemble_one_page(&hdr, &payload, &meta);
        no_panic_err_contains(&f, "nanos-of-day out of range");
    }

    // H4: INT96 dict-encoded with index OUT OF RANGE. dict has ONE
    // INT96 entry (12 bytes PLAIN); data page index stream has one
    // index of value 5 → dict.get(5) returns None → Bad("dict index
    // out of range"). Independent: standard parquet dict-page layout.
    //
    // Data page payload for OPTIONAL/REQUIRED dict indices when
    // REQUIRED: `[bit_width][hybrid index stream]`. We use bit_width=3
    // (fits up to 7), encoded as a single RLE run of length 1 of
    // value 5: hybrid header varint((1<<1)|0)=0x02, run-value (3-bit
    // packed into ceil(3/8)=1 byte) = 0x05.
    #[test]
    fn int96_dict_index_out_of_range_bad() {
        // dict page: ONE INT96 = (0, 2_440_588) = epoch midnight.
        let dict_data = int96_le(0, 2_440_588).to_vec();
        let dict_hdr = dict_page_hdr(1, dict_data.len() as i32);
        // data page: bit_width=3, RLE run of 1 of index=5.
        let data_payload: Vec<u8> = vec![0x03, 0x02, 0x05];
        let data_hdr = dict_v1_data_page_hdr(1, data_payload.len() as i32);
        // Layout offsets.
        let dict_off: i64 = 4;
        let data_off: i64 = 4 + dict_hdr.len() as i64 + dict_data.len() as i64;
        let leaf = LeafMeta::plain(3 /*INT96*/, b"d");
        let meta = filemetadata_bytes(&leaf, 1, 2 /*PLAIN_DICTIONARY*/, data_off, Some(dict_off));
        let f = assemble_dict_page(&dict_hdr, &dict_data, &data_hdr, &data_payload, &meta);
        no_panic_err_contains(&f, "dict index out of range");
    }

    // H5: V2 DATA_PAGE with INT96 + truncated values section. Even
    // though SP107's V2 path is unchanged, exercise it with an INT96
    // spec to prove the new physical-type arm composes safely with
    // V2 — values section is only 11 bytes for num_values=1, so
    // `decode_plain` INT96 arm hits the `data.get(..12)` bound and
    // returns Bad. No panic, no OOM.
    //
    // V2 PageHeader fields g1 num_values=1, g5 def_len=0 (REQUIRED),
    // g6 rep_len=0; payload = ONLY the 11-byte truncated values.
    #[test]
    fn int96_v2_truncated_values_bad() {
        let payload: Vec<u8> = vec![0u8; 11];
        let mut hdr = Vec::new();
        hdr.push(0x15); uv(&mut hdr, zz(3));                       // f1 type=DATA_PAGE_V2(3)
        hdr.push(0x15); uv(&mut hdr, zz(payload.len() as i64));    // f2 uncompressed
        hdr.push(0x15); uv(&mut hdr, zz(payload.len() as i64));    // f3 compressed
        hdr.push(0x5c);                                            // f8 struct (delta 3->8=5)
        hdr.push(0x15); uv(&mut hdr, zz(1));                       // g1 num_values=1
        hdr.push(0x15); uv(&mut hdr, zz(0));                       // g2 num_nulls=0
        hdr.push(0x15); uv(&mut hdr, zz(1));                       // g3 num_rows=1
        hdr.push(0x15); uv(&mut hdr, zz(0));                       // g4 encoding=PLAIN
        hdr.push(0x15); uv(&mut hdr, zz(0));                       // g5 def_len=0
        hdr.push(0x15); uv(&mut hdr, zz(0));                       // g6 rep_len=0
        hdr.push(0x12);                                            // g7 is_compressed=false
        hdr.push(0x00); hdr.push(0x00);
        let leaf = LeafMeta::plain(3 /*INT96*/, b"d");
        let meta = filemetadata_bytes(&leaf, 1, 0, 4, None);
        let f = assemble_one_page(&hdr, &payload, &meta);
        no_panic_err_contains(&f, "int96 truncated");
    }

    // ════════════════ HOSTILE LOCKS — DECIMAL metadata ════════════════

    /// Tiny helper: a one-row INT32 + DECIMAL leaf with custom precision
    /// and scale, just enough to reach build_plain_spec. We use INT32
    /// here for every metadata-only test because INT32 (4 bytes/value)
    /// is the smallest physical type that carries DECIMAL.
    fn int32_decimal_file(precision: Option<i64>, scale: Option<i64>) -> Vec<u8> {
        let mut leaf = LeafMeta::plain(1 /*INT32*/, b"d");
        leaf.converted_type = Some(5); // DECIMAL
        leaf.precision = precision;
        leaf.scale = scale;
        // One INT32 value (12345 LE).
        let payload = 12345i32.to_le_bytes().to_vec();
        let hdr = plain_v1_page_hdr(1, payload.len() as i32);
        let meta = filemetadata_bytes(&leaf, 1, 0, 4, None);
        assemble_one_page(&hdr, &payload, &meta)
    }

    // H6: DECIMAL precision > 38 (i128 cap) → Unsupported. Hand-derived
    // bound: i128 max ≈ 1.7e38; precision 39 demands 10^39 > i128.
    #[test]
    fn decimal_precision_gt_38_unsupported() {
        let f = int32_decimal_file(Some(39), Some(0));
        no_panic_err_contains(&f, "DECIMAL precision");
        assert!(matches!(
            extract(&f, &["d"]),
            Err(PqError::Unsupported(_))
        ));
    }

    // H7: DECIMAL precision < 1 (precision=0, the only value < 1 a
    // valid i32 can represent without going negative). Per
    // build_plain_spec: precision<1 → Unsupported.
    #[test]
    fn decimal_precision_zero_unsupported() {
        let f = int32_decimal_file(Some(0), Some(0));
        no_panic_err_contains(&f, "DECIMAL precision");
        assert!(matches!(
            extract(&f, &["d"]),
            Err(PqError::Unsupported(_))
        ));
    }

    // H8: DECIMAL scale < 0 → Bad. Wire is i32 zz-encoded; scale=-1
    // round-trips faithfully through Thrift compact.
    #[test]
    fn decimal_scale_negative_bad() {
        let f = int32_decimal_file(Some(5), Some(-1));
        no_panic_err_contains(&f, "DECIMAL scale -1");
    }

    // H9: DECIMAL scale > precision → Bad ("scale {s} out of range for
    // precision {p}").
    #[test]
    fn decimal_scale_gt_precision_bad() {
        let f = int32_decimal_file(Some(5), Some(10));
        no_panic_err_contains(&f, "out of range for precision");
    }

    // H10: FLBA DECIMAL type_length=17 (> 16 i128 cap) → Bad. Per
    // build_plain_spec: `n == 0 || n > 16` → "DECIMAL FLBA width out
    // of range".
    #[test]
    fn decimal_flba_width_17_bad() {
        let mut leaf = LeafMeta::plain(7 /*FLBA*/, b"d");
        leaf.type_length = Some(17);
        leaf.converted_type = Some(5);
        leaf.precision = Some(20);
        leaf.scale = Some(2);
        // page provides 17 bytes (one value) - enough that build_plain_spec
        // is reached before any per-value decode.
        let payload = vec![0u8; 17];
        let hdr = plain_v1_page_hdr(1, payload.len() as i32);
        let meta = filemetadata_bytes(&leaf, 1, 0, 4, None);
        let f = assemble_one_page(&hdr, &payload, &meta);
        no_panic_err_contains(&f, "DECIMAL FLBA width out of range");
    }

    // H11: FLBA DECIMAL type_length=0 → Bad (zero-width FLBA cannot
    // sign-extend; build_plain_spec rejects).
    #[test]
    fn decimal_flba_width_zero_bad() {
        let mut leaf = LeafMeta::plain(7 /*FLBA*/, b"d");
        leaf.type_length = Some(0);
        leaf.converted_type = Some(5);
        leaf.precision = Some(5);
        leaf.scale = Some(2);
        let payload: Vec<u8> = Vec::new();
        let hdr = plain_v1_page_hdr(1, 0);
        let meta = filemetadata_bytes(&leaf, 1, 0, 4, None);
        let f = assemble_one_page(&hdr, &payload, &meta);
        no_panic_err_contains(&f, "DECIMAL FLBA width out of range");
    }

    // H12: DECIMAL precision=15 on INT32 (> 9 i32 max precision) →
    // Bad. Per parquet spec INT32 can only carry DECIMAL with
    // precision ≤ 9 (10^9 ≈ 2^30 fits in i32 range).
    #[test]
    fn decimal_int32_precision_15_bad() {
        let f = int32_decimal_file(Some(15), Some(2));
        no_panic_err_contains(&f, "DECIMAL precision > 9 on INT32");
    }

    // H13: DECIMAL precision=25 on INT64 (> 18 i64 max precision) →
    // Bad. Parquet spec: INT64 DECIMAL precision ≤ 18 (10^18 ≈ 2^60
    // fits in i64 range).
    #[test]
    fn decimal_int64_precision_25_bad() {
        let mut leaf = LeafMeta::plain(2 /*INT64*/, b"d");
        leaf.converted_type = Some(5);
        leaf.precision = Some(25);
        leaf.scale = Some(2);
        let payload = 1i64.to_le_bytes().to_vec();
        let hdr = plain_v1_page_hdr(1, payload.len() as i32);
        let meta = filemetadata_bytes(&leaf, 1, 0, 4, None);
        let f = assemble_one_page(&hdr, &payload, &meta);
        no_panic_err_contains(&f, "DECIMAL precision > 18 on INT64");
    }

    // H14: BYTE_ARRAY DECIMAL per-value u32 LE length-prefix = 17 (>
    // 16 i128 cap). build_plain_spec is fine — but the per-value
    // decode in plain.rs fires "BYTE_ARRAY DECIMAL width out of range
    // (1..=16)" BEFORE any flba_be_to_i128 with > 16 bytes.
    #[test]
    fn decimal_byte_array_per_value_length_17_bad() {
        let mut leaf = LeafMeta::plain(6 /*BYTE_ARRAY*/, b"d");
        leaf.converted_type = Some(5);
        leaf.precision = Some(20);
        leaf.scale = Some(2);
        // one BYTE_ARRAY value: u32 LE len=17, then 17 dummy bytes.
        let mut payload = Vec::new();
        payload.extend_from_slice(&17u32.to_le_bytes());
        payload.extend_from_slice(&[0u8; 17]);
        let hdr = plain_v1_page_hdr(1, payload.len() as i32);
        let meta = filemetadata_bytes(&leaf, 1, 0, 4, None);
        let f = assemble_one_page(&hdr, &payload, &meta);
        no_panic_err_contains(&f, "BYTE_ARRAY DECIMAL width out of range");
    }

    // H15: BYTE_ARRAY DECIMAL per-value u32 LE length-prefix = 0.
    // Zero-length DECIMAL has no sign byte → malformed. Per-value
    // check rejects as Bad. Independent: parquet spec says DECIMAL
    // byte-array values must be ≥ 1 byte (at minimum a single sign
    // byte for the unscaled i128).
    #[test]
    fn decimal_byte_array_per_value_length_zero_bad() {
        let mut leaf = LeafMeta::plain(6 /*BYTE_ARRAY*/, b"d");
        leaf.converted_type = Some(5);
        leaf.precision = Some(5);
        leaf.scale = Some(2);
        let payload = 0u32.to_le_bytes().to_vec(); // len=0 for one value
        let hdr = plain_v1_page_hdr(1, payload.len() as i32);
        let meta = filemetadata_bytes(&leaf, 1, 0, 4, None);
        let f = assemble_one_page(&hdr, &payload, &meta);
        no_panic_err_contains(&f, "BYTE_ARRAY DECIMAL width out of range");
    }

    // H16: converted_type=DECIMAL(scale=2,precision=9) AND LogicalType
    // DecimalType{scale=3,precision=9} — the two sides DISAGREE.
    // decode_schema_element's defense-in-depth agreement check fires
    // at metadata-decode time → Bad with the EXACT substring locked.
    #[test]
    fn decimal_converted_vs_logical_disagree_bad() {
        let mut leaf = LeafMeta::plain(1 /*INT32*/, b"d");
        leaf.converted_type = Some(5);
        leaf.scale = Some(2);
        leaf.precision = Some(9);
        leaf.logical_decimal = Some((3, 9)); // scale DISAGREES (3 vs 2)
        let payload = 1i32.to_le_bytes().to_vec();
        let hdr = plain_v1_page_hdr(1, payload.len() as i32);
        let meta = filemetadata_bytes(&leaf, 1, 0, 4, None);
        let f = assemble_one_page(&hdr, &payload, &meta);
        no_panic_err_contains(
            &f,
            "converted_type vs LogicalType",
        );
    }

    // H17: DECIMAL on INT96 physical type → Bad. SP108 explicitly
    // rejects this combination (INT96 has no meaningful unscaled-i128
    // interpretation). Precision is valid (9) so it reaches the
    // physical-type branch.
    #[test]
    fn decimal_on_int96_bad() {
        let mut leaf = LeafMeta::plain(3 /*INT96*/, b"d");
        leaf.converted_type = Some(5);
        leaf.precision = Some(9);
        leaf.scale = Some(0);
        let payload = int96_le(0, 2_440_588).to_vec();
        let hdr = plain_v1_page_hdr(1, payload.len() as i32);
        let meta = filemetadata_bytes(&leaf, 1, 0, 4, None);
        let f = assemble_one_page(&hdr, &payload, &meta);
        no_panic_err_contains(&f, "DECIMAL on INT96");
    }

    // ════════════════ HOSTILE LOCKS — FLBA non-DECIMAL ════════════════

    // H18: FLBA non-DECIMAL with type_length=65_537 — above the
    // 65_536 cap. Per build_plain_spec: `n == 0 || n > 65_536` → Bad.
    #[test]
    fn flba_non_decimal_type_length_huge_bad() {
        let mut leaf = LeafMeta::plain(7 /*FLBA*/, b"d");
        leaf.type_length = Some(65_537);
        // We DON'T provide that many bytes — the metadata-level cap
        // check fires before any per-value read, so this is fine.
        let payload: Vec<u8> = vec![0u8; 16];
        let hdr = plain_v1_page_hdr(1, payload.len() as i32);
        let meta = filemetadata_bytes(&leaf, 1, 0, 4, None);
        let f = assemble_one_page(&hdr, &payload, &meta);
        no_panic_err_contains(&f, "FLBA type_length out of range");
    }

    // H19: FLBA non-DECIMAL truncated — leaf declares type_length=16,
    // num_values=1 (need=16 bytes), payload only 8. The decode_plain
    // FLBA arm's `data.get(..need).ok_or("flba truncated")` fires
    // BEFORE any chunks_exact step, NO from_le_bytes panic.
    #[test]
    fn flba_non_decimal_truncated_bad() {
        let mut leaf = LeafMeta::plain(7 /*FLBA*/, b"d");
        leaf.type_length = Some(16);
        let payload = vec![0u8; 8]; // 8 < 16
        let hdr = plain_v1_page_hdr(1, payload.len() as i32);
        let meta = filemetadata_bytes(&leaf, 1, 0, 4, None);
        let f = assemble_one_page(&hdr, &payload, &meta);
        no_panic_err_contains(&f, "flba truncated");
    }

    // ════════════════ POSITIVE CORRECTNESS LOCKS ════════════════════
    //
    // These assert EXACT Ok(rows). Failure = decoder bug → BLOCKED.

    // P1: INT96 PLAIN REQUIRED — two 12-byte values:
    //   (nod=0, jd=2_440_588) → ns=0
    //   (nod=1_500_000_000, jd=2_440_588) → ns=1_500_000_000
    // Hand-derived from JULIAN_UNIX_EPOCH=2_440_588 + ns conversion
    // (day_offset=0, day_ns=0, +nod_i64).
    #[test]
    fn int96_plain_required_decode_ok() {
        let mut payload = Vec::new();
        payload.extend_from_slice(&int96_le(0, 2_440_588));
        payload.extend_from_slice(&int96_le(1_500_000_000, 2_440_588));
        let leaf = LeafMeta::plain(3 /*INT96*/, b"d");
        let hdr = plain_v1_page_hdr(2, payload.len() as i32);
        let meta = filemetadata_bytes(&leaf, 2, 0, 4, None);
        let f = assemble_one_page(&hdr, &payload, &meta);
        assert_eq!(
            extract(&f, &["d"]).expect("int96 plain required"),
            vec![
                vec![PqValue::Timestamp(0)],
                vec![PqValue::Timestamp(1_500_000_000)],
            ]
        );
    }

    // P2: INT96 PLAIN OPTIONAL [ts0, null, ts0] — defs=[1,0,1].
    // Hand-derived V1 OPTIONAL payload: [u32 LE def_len=2][def_hybrid:
    // header 0x03 (1 group), bits 0x05 (bit0=1, bit1=0, bit2=1)]
    // [12-byte INT96 (0,2_440_588)][12-byte INT96 (0,2_440_588)].
    #[test]
    fn int96_plain_optional_with_null_ok() {
        let def_hybrid: [u8; 2] = [0x03, 0x05];
        let mut payload = Vec::new();
        payload.extend_from_slice(&(def_hybrid.len() as u32).to_le_bytes());
        payload.extend_from_slice(&def_hybrid);
        payload.extend_from_slice(&int96_le(0, 2_440_588));
        payload.extend_from_slice(&int96_le(0, 2_440_588));
        let mut leaf = LeafMeta::plain(3 /*INT96*/, b"d");
        leaf.rep = 1; // OPTIONAL
        let hdr = plain_v1_page_hdr(3, payload.len() as i32);
        let meta = filemetadata_bytes(&leaf, 3, 0, 4, None);
        let f = assemble_one_page(&hdr, &payload, &meta);
        assert_eq!(
            extract(&f, &["d"]).expect("int96 plain optional"),
            vec![
                vec![PqValue::Timestamp(0)],
                vec![PqValue::Null],
                vec![PqValue::Timestamp(0)],
            ]
        );
    }

    // P3: INT96 dict-encoded — dict=[ts0], indices=[0,0,0] for 3
    // REQUIRED rows. Hand-derived dict payload: [bit_width=1]
    // [hybrid header 0x03 (1 bit-packed group of 8)][bits 0x00 (all
    // indices=0)]. Decoded indices [0,0,0] → all dict[0] → ts0.
    #[test]
    fn int96_dict_decode_ok() {
        let dict_data = int96_le(0, 2_440_588).to_vec();
        let dict_hdr = dict_page_hdr(1, dict_data.len() as i32);
        // 3 indices, all 0. bit_width=1, 1 bit-packed group.
        let data_payload: Vec<u8> = vec![0x01, 0x03, 0x00];
        let data_hdr = dict_v1_data_page_hdr(3, data_payload.len() as i32);
        let dict_off: i64 = 4;
        let data_off: i64 = 4 + dict_hdr.len() as i64 + dict_data.len() as i64;
        let leaf = LeafMeta::plain(3 /*INT96*/, b"d");
        let meta = filemetadata_bytes(&leaf, 3, 2 /*PLAIN_DICTIONARY*/, data_off, Some(dict_off));
        let f = assemble_dict_page(&dict_hdr, &dict_data, &data_hdr, &data_payload, &meta);
        assert_eq!(
            extract(&f, &["d"]).expect("int96 dict"),
            vec![
                vec![PqValue::Timestamp(0)],
                vec![PqValue::Timestamp(0)],
                vec![PqValue::Timestamp(0)],
            ]
        );
    }

    // P4: cross-physical DECIMAL 3-way determinism — INT32, INT64, and
    // FLBA(16) hand-built files at matched precision=9 (the INT32 cap,
    // shared with INT64 and FLBA so the SAME precision/scale is legal
    // on all three), scale=2, unscaled value=12345 → ALL three yield
    // the IDENTICAL `PqValue::Decimal { unscaled: 12345, scale: 2 }`.
    // Hand-derived: INT32 LE=12345; INT64 LE=12345; FLBA(16) BE
    // sign-extended = 14 zero bytes + 0x30, 0x39 (12345 = 0x3039 in 2
    // BE bytes; full 16 BE = [00,…,00,30,39] from i128::to_be_bytes).
    //
    // Non-self-referential: T4's pyarrow fixture pin proves the same
    // claim via REAL writers (INT32/INT64/FLBA). This T5 lock proves
    // it via HAND-BUILT bytes (zero pyarrow dependence) — together
    // the two locks are independent attestations that decode is
    // source-format-independent.
    #[test]
    fn decimal_3way_int32_int64_flba_identical_ok() {
        let expected = vec![vec![PqValue::Decimal { unscaled: 12345, scale: 2 }]];

        // INT32 file.
        {
            let mut leaf = LeafMeta::plain(1 /*INT32*/, b"d");
            leaf.converted_type = Some(5);
            leaf.precision = Some(9);
            leaf.scale = Some(2);
            let payload = 12345i32.to_le_bytes().to_vec();
            let hdr = plain_v1_page_hdr(1, payload.len() as i32);
            let meta = filemetadata_bytes(&leaf, 1, 0, 4, None);
            let f = assemble_one_page(&hdr, &payload, &meta);
            assert_eq!(extract(&f, &["d"]).expect("int32 decimal"), expected);
        }

        // INT64 file.
        {
            let mut leaf = LeafMeta::plain(2 /*INT64*/, b"d");
            leaf.converted_type = Some(5);
            leaf.precision = Some(9);
            leaf.scale = Some(2);
            let payload = 12345i64.to_le_bytes().to_vec();
            let hdr = plain_v1_page_hdr(1, payload.len() as i32);
            let meta = filemetadata_bytes(&leaf, 1, 0, 4, None);
            let f = assemble_one_page(&hdr, &payload, &meta);
            assert_eq!(extract(&f, &["d"]).expect("int64 decimal"), expected);
        }

        // FLBA(16) file. 16 BE bytes: positive 12345 sign-extended.
        {
            let mut leaf = LeafMeta::plain(7 /*FLBA*/, b"d");
            leaf.type_length = Some(16);
            leaf.converted_type = Some(5);
            leaf.precision = Some(9);
            leaf.scale = Some(2);
            let payload = 12345i128.to_be_bytes().to_vec();
            assert_eq!(payload.len(), 16);
            let hdr = plain_v1_page_hdr(1, payload.len() as i32);
            let meta = filemetadata_bytes(&leaf, 1, 0, 4, None);
            let f = assemble_one_page(&hdr, &payload, &meta);
            assert_eq!(extract(&f, &["d"]).expect("flba decimal"), expected);
        }
    }

    // P5: DECIMAL OPTIONAL with a null in the middle → scatter_nulls
    // composes correctly with the DECIMAL decode path. INT32 leaf,
    // OPTIONAL, defs=[1,0,1], two present values [12345, -456].
    #[test]
    fn decimal_int32_optional_null_scatter_ok() {
        let def_hybrid: [u8; 2] = [0x03, 0x05]; // bits 1,0,1
        let mut payload = Vec::new();
        payload.extend_from_slice(&(def_hybrid.len() as u32).to_le_bytes());
        payload.extend_from_slice(&def_hybrid);
        payload.extend_from_slice(&12345i32.to_le_bytes());
        payload.extend_from_slice(&(-456i32).to_le_bytes());
        let mut leaf = LeafMeta::plain(1 /*INT32*/, b"d");
        leaf.rep = 1; // OPTIONAL
        leaf.converted_type = Some(5);
        leaf.precision = Some(9);
        leaf.scale = Some(2);
        let hdr = plain_v1_page_hdr(3, payload.len() as i32);
        let meta = filemetadata_bytes(&leaf, 3, 0, 4, None);
        let f = assemble_one_page(&hdr, &payload, &meta);
        assert_eq!(
            extract(&f, &["d"]).expect("int32 decimal optional"),
            vec![
                vec![PqValue::Decimal { unscaled: 12345, scale: 2 }],
                vec![PqValue::Null],
                vec![PqValue::Decimal { unscaled: -456, scale: 2 }],
            ]
        );
    }

    // P6: FLBA non-DECIMAL, 16-byte UUID-shaped values → PqValue::Bytes.
    // Two values: all-0xAA and all-0xBB. Independent: parquet FLBA
    // non-DECIMAL is N raw bytes per value, no length prefix.
    #[test]
    fn flba_uuid_required_decode_ok() {
        let mut payload = Vec::new();
        payload.extend_from_slice(&[0xAA; 16]);
        payload.extend_from_slice(&[0xBB; 16]);
        let mut leaf = LeafMeta::plain(7 /*FLBA*/, b"d");
        leaf.type_length = Some(16);
        let hdr = plain_v1_page_hdr(2, payload.len() as i32);
        let meta = filemetadata_bytes(&leaf, 2, 0, 4, None);
        let f = assemble_one_page(&hdr, &payload, &meta);
        assert_eq!(
            extract(&f, &["d"]).expect("flba uuid"),
            vec![
                vec![PqValue::Bytes(vec![0xAA; 16])],
                vec![PqValue::Bytes(vec![0xBB; 16])],
            ]
        );
    }

    // P7: DECIMAL precision=38 boundary — maximum supported precision
    // on FLBA(16). Unscaled value 12345 fits trivially. Locks that the
    // build_plain_spec precision == 38 case is INCLUSIVE, not <.
    #[test]
    fn decimal_precision_38_boundary_ok() {
        let mut leaf = LeafMeta::plain(7 /*FLBA*/, b"d");
        leaf.type_length = Some(16);
        leaf.converted_type = Some(5);
        leaf.precision = Some(38);
        leaf.scale = Some(0);
        let payload = 12345i128.to_be_bytes().to_vec();
        let hdr = plain_v1_page_hdr(1, payload.len() as i32);
        let meta = filemetadata_bytes(&leaf, 1, 0, 4, None);
        let f = assemble_one_page(&hdr, &payload, &meta);
        assert_eq!(
            extract(&f, &["d"]).expect("decimal precision=38 boundary"),
            vec![vec![PqValue::Decimal { unscaled: 12345, scale: 0 }]]
        );
    }

    // P8: DECIMAL FLBA(16) i128::MIN — sign-extend at the MOST negative
    // boundary. Bytes: 0x80 then 15 × 0x00 → BE i128 = i128::MIN.
    // Independent of decoder: i128::MIN.to_be_bytes() == [0x80, 0, …, 0].
    #[test]
    fn decimal_flba_i128_min_sign_extend_ok() {
        let mut leaf = LeafMeta::plain(7 /*FLBA*/, b"d");
        leaf.type_length = Some(16);
        leaf.converted_type = Some(5);
        leaf.precision = Some(38);
        leaf.scale = Some(0);
        let payload = i128::MIN.to_be_bytes().to_vec();
        assert_eq!(payload[0], 0x80);
        assert!(payload[1..].iter().all(|&b| b == 0x00));
        let hdr = plain_v1_page_hdr(1, payload.len() as i32);
        let meta = filemetadata_bytes(&leaf, 1, 0, 4, None);
        let f = assemble_one_page(&hdr, &payload, &meta);
        assert_eq!(
            extract(&f, &["d"]).expect("flba i128::MIN"),
            vec![vec![PqValue::Decimal { unscaled: i128::MIN, scale: 0 }]]
        );
    }
}

#[cfg(test)]
mod nested_decode_tests {
    //! SP143 T4: KATs for the multi-bit rep/def-level decode sibling
    //! helpers. Hand-built payloads (no reliance on a Parquet writer)
    //! prove the V1 (length-prefixed) and V2 (raw) layout difference
    //! is correctly routed to `decode_level_v1` vs `decode_hybrid`.
    //!
    //! Scenario: a `List<Optional<i64>>` column with `max_rep_level=1`
    //! and `max_def_level=3`. One logical record with list = [10, null, 20]:
    //!   - rep = [0, 1, 1] (start of record, continue list, continue list)
    //!   - def = [3, 2, 3] (item present, item null, item present)
    //!   - values = [10, 20] (only the two present slots)
    //!
    //! Hand-derived bit-packed bytes:
    //!   rep, bit_width=1, 3 values [0,1,1] padded to 8 with zeros:
    //!     header = (1 << 1) | 1 = 0x03   (1 group of 8, bit-packed)
    //!     payload byte = bit0=0, bit1=1, bit2=1, bits3-7=0
    //!                  = 0b00000110 = 0x06
    //!     => rep_data = [0x03, 0x06]      (rep_len = 2)
    //!
    //!   def, bit_width=2, 3 values [3,2,3] padded to 8 with zeros:
    //!     header = (1 << 1) | 1 = 0x03
    //!     byte0 bits (0-1)=3=11, (2-3)=2=10, (4-5)=3=11, (6-7)=0=00
    //!         = 0b00_11_10_11 = 0x3B
    //!     byte1 = remaining 5 zero values @ 2 bits each = 0x00
    //!     => def_data = [0x03, 0x3B, 0x00] (def_len = 3)
    //!
    //!   values, PLAIN INT64 [10, 20] = 16 bytes LE.
    use super::*;

    #[test]
    fn nested_v1_decode_one_rep_three_def_levels() {
        let mut payload = Vec::new();
        // V1: 4-byte LE u32 length prefix on each level section.
        payload.extend_from_slice(&2u32.to_le_bytes());           // rep_len = 2
        payload.extend_from_slice(&[0x03, 0x06]);                  // rep_data
        payload.extend_from_slice(&3u32.to_le_bytes());           // def_len = 3
        payload.extend_from_slice(&[0x03, 0x3B, 0x00]);            // def_data
        payload.extend_from_slice(&10i64.to_le_bytes());           // value 0
        payload.extend_from_slice(&20i64.to_le_bytes());           // value 1

        let (rep, def, values) = decode_page_v1_nested(
            &payload,
            0, // dp_encoding: PLAIN
            plain::PlainSpec::plain(meta::Type::Int64),
            3, // n = num_values
            1, // max_rep_level
            3, // max_def_level
            &[],
        )
        .expect("nested v1 decode");

        assert_eq!(rep, vec![0u32, 1, 1]);
        assert_eq!(def, vec![3u32, 2, 3]);
        assert_eq!(values, vec![PqValue::I64(10), PqValue::I64(20)]);
    }

    #[test]
    fn nested_v2_decode_one_rep_three_def_levels_uncompressed() {
        // V2: NO 4-byte length prefix on the level sections — the byte
        // lengths come from rep_levels_byte_length / def_levels_byte_length
        // arguments (i.e. the page-header fields).
        let mut payload = Vec::new();
        payload.extend_from_slice(&[0x03, 0x06]);                  // rep_data, 2 bytes
        payload.extend_from_slice(&[0x03, 0x3B, 0x00]);            // def_data, 3 bytes
        payload.extend_from_slice(&10i64.to_le_bytes());           // value 0
        payload.extend_from_slice(&20i64.to_le_bytes());           // value 1
        let uncomp = payload.len() as u32;

        let (rep, def, values) = decode_data_page_v2_nested(
            &payload,
            0, // dp_encoding: PLAIN
            plain::PlainSpec::plain(meta::Type::Int64),
            3, // n = num_values
            1, // max_rep_level
            3, // max_def_level
            2, // rep_levels_byte_length
            3, // def_levels_byte_length
            false, // is_compressed
            meta::Codec::Uncompressed,
            uncomp,
            &[],
        )
        .expect("nested v2 decode");

        assert_eq!(rep, vec![0u32, 1, 1]);
        assert_eq!(def, vec![3u32, 2, 3]);
        assert_eq!(values, vec![PqValue::I64(10), PqValue::I64(20)]);
    }
}

// ────────────────────────────────────────────────────────────────────────
// SP143 T10: adversarial pentest matrix for List<primitive> nested decode.
//
// Every test below is wrapped in `std::panic::catch_unwind` and asserts:
//   (1) NO panic / NO OOM-abort on hostile input
//   (2) returns a TYPED `PqError` (Bad or Unsupported), not Ok
//
// Spec reference: docs/superpowers/specs/2026-05-25-kesseldb-parquet-
// nested-list-design.md §4 (13-row pentest matrix). Each `pt<N>_…` test
// below maps 1:1 to a spec row; the few rows that are already covered
// by the T5/T6/T7 unit suites (and would require crate-private schema
// builders to re-prove here) are tagged with `_covered_by` documentation.
//
// Direct-stream approach (Steps 2/3 of T10): we call
// `assemble_list_primitive`, `decode_page_v1_nested`,
// `decode_data_page_v2_nested`, and `rle::decode_hybrid` directly with
// hand-built byte arrays / level vecs. This isolates the decode layer
// from the full-file pipeline (which is independently pentested in
// `mod pentest` / `mod pentest_v2`).
// ────────────────────────────────────────────────────────────────────────
#[cfg(test)]
mod sp143_pentest {
    use super::*;
    use crate::assembly::assemble_list_primitive;
    use std::panic::catch_unwind;

    /// Asserts a closure does not panic AND returns Err of any typed
    /// PqError variant. Mirrors `pentest::no_panic_typed_err` but for
    /// the direct-call (no full-file) pentest layer.
    fn assert_well_behaved_err<F, T>(name: &str, f: F)
    where
        F: FnOnce() -> Result<T, PqError> + std::panic::UnwindSafe,
    {
        let r = catch_unwind(f);
        match r {
            Ok(Ok(_)) => panic!("{name}: expected Err, got Ok"),
            Ok(Err(_)) => { /* OK — typed error */ }
            Err(_) => panic!("{name}: PANICKED on adversarial input"),
        }
    }

    // ── Row 1: def value > max_def_level ─────────────────────────────
    // OPT-REP-OPT shape: max_def=2. A def stream containing 3 (> max)
    // must be rejected by `classify`.
    #[test]
    fn pt1_def_level_overflow() {
        assert_well_behaved_err("def overflow", || {
            assemble_list_primitive(
                &[0u32, 0],
                &[3u32, 1],
                &[PqValue::I64(1)],
                2,
                false,
                true,
            )
        });
    }

    // ── Row 2: rep value > max_rep_level ─────────────────────────────
    // Single-level LIST has max_rep=1 by construction. A rep stream
    // containing 2 must be rejected at the per-position rep>1 guard.
    #[test]
    fn pt2_rep_level_overflow() {
        assert_well_behaved_err("rep overflow", || {
            assemble_list_primitive(
                &[0u32, 2],
                &[1u32, 1],
                &[PqValue::I64(1), PqValue::I64(2)],
                1,
                false,
                false,
            )
        });
    }

    // ── Row 3: bit_width > 64 in the level-decoder ───────────────────
    // The rle::decode_hybrid validates bit_width ≤ 64 up front and
    // returns Bad on overflow. (Spec mentions bit_width=33; we test
    // 65 which is the actual rejection boundary — 33 is a legal value
    // for u32-wide outputs.) Call decode_hybrid directly.
    #[test]
    fn pt3_bit_width_too_large() {
        use crate::rle::decode_hybrid;
        assert_well_behaved_err("bit_width too large", || {
            decode_hybrid(&[0x01, 0x02, 0x03], 65, 4)
        });
    }

    // ── Row 4: rep_levels_byte_length > V2 page payload ──────────────
    // decode_data_page_v2_nested guards `lvl_end > payload.len()` with
    // a typed Bad before any slice. rep_len=100 against a 5-byte
    // payload triggers the guard.
    #[test]
    fn pt4_v2_rep_section_overrun() {
        assert_well_behaved_err("v2 rep section overrun", || {
            decode_data_page_v2_nested(
                &[0u8; 5],
                0, // PLAIN
                plain::PlainSpec::plain(meta::Type::Int64),
                3,   // n
                1,   // max_rep_level
                1,   // max_def_level
                100, // rep_levels_byte_length — LIES (> payload.len())
                0,   // def_levels_byte_length
                false,
                meta::Codec::Uncompressed,
                10,
                &[],
            )
        });
    }

    // ── Row 5: value stream truncated (def says present, no value) ───
    // REQ-REP-REQ: max_def=1, 3 positions all "present", but values
    // vec has only 1 element. Assembler returns Bad("value stream
    // exhausted").
    #[test]
    fn pt5_value_stream_truncated() {
        assert_well_behaved_err("value truncated", || {
            assemble_list_primitive(
                &[0u32, 1, 1],
                &[1u32, 1, 1],
                &[PqValue::I64(1)], // only 1 value, def implies 3
                1,
                false,
                false,
            )
        });
    }

    // ── Row 6: value stream overrun (values vec longer than implied) ─
    // OPT-REP-REQ: max_def=2. One ItemPresent, but values vec has 2.
    // Assembler returns Bad("values not fully consumed").
    #[test]
    fn pt6_value_stream_overrun() {
        assert_well_behaved_err("value overrun", || {
            assemble_list_primitive(
                &[0u32],
                &[2u32],
                &[PqValue::I64(1), PqValue::I64(2)],
                2,
                true,
                false,
            )
        });
    }

    // ── Row 7: rep_levels and def_levels lengths differ ──────────────
    // First-line invariant in assemble_list_primitive.
    #[test]
    fn pt7_rep_def_length_mismatch() {
        assert_well_behaved_err("len mismatch", || {
            assemble_list_primitive(
                &[0u32, 1, 0], // 3 reps
                &[1u32, 1],    // 2 defs
                &[PqValue::I64(1), PqValue::I64(2)],
                1,
                false,
                false,
            )
        });
    }

    // ── Row 8: rep level far above max (e.g. 5 for max_rep=1) ────────
    // Same guard as Row 2 but with a value way above the bit_width
    // implied by max_rep_level — exercises that the assembler's rep>1
    // check fires before any over-decoded level slips through.
    #[test]
    fn pt8_rep_level_far_overflow() {
        assert_well_behaved_err("rep far overflow", || {
            assemble_list_primitive(
                &[0u32, 5],
                &[1u32, 1],
                &[PqValue::I64(1)],
                1,
                false,
                false,
            )
        });
    }

    // ── Row 9: rep=1 at first position (no active list) ──────────────
    // A continuation marker can only appear after an open list; the
    // very first position with rep=1 must be rejected.
    #[test]
    fn pt9_rep1_without_active_list() {
        assert_well_behaved_err("rep1 first", || {
            assemble_list_primitive(
                &[1u32, 0],
                &[1u32, 1],
                &[PqValue::I64(1), PqValue::I64(2)],
                1,
                false,
                false,
            )
        });
    }

    // ── Row 10: def implies item-null but element is REQUIRED ────────
    // OPT outer + REP + REQ element: max_def=3 hypothetically (the
    // canonical such shape has max_def=2 = outer_opt + rep, so def=2
    // is ItemPresent). But hostile inputs can lie about max_def_level
    // and produce a def value strictly between the empty-list
    // threshold and max → the classifier sees ItemNull but the schema
    // says REQ element → returns Bad ("def N implies item null but
    // element is REQUIRED").
    #[test]
    fn pt10_item_null_with_required_element() {
        assert_well_behaved_err("item null with REQ element", || {
            assemble_list_primitive(
                &[0u32],
                &[2u32],
                &[],
                3,
                true,
                false,
            )
        });
    }

    // ── Row 11: empty level streams but non-empty values ─────────────
    // 0 levels + N values: should return Bad ("values not fully
    // consumed") — the empty-input short-circuit must still validate
    // the value stream is also empty.
    #[test]
    fn pt11_empty_streams_with_values() {
        assert_well_behaved_err("empty streams with values", || {
            assemble_list_primitive(
                &[],
                &[],
                &[PqValue::I64(1)],
                1,
                false,
                false,
            )
        });
    }

    // ── Row 12: huge num_values claim into the level decoder ─────────
    // decode_hybrid is the lowest-level RLE decoder; it must NOT
    // attempt `Vec::with_capacity(num_values)` with a hostile huge
    // num_values (8 GB for 1e9 u64 → process OOM-abort). The T10
    // fix caps initial capacity to RLE_INITIAL_CAP and lets the vec
    // grow naturally; the bit-packed/RLE run loop will bail on the
    // first truncated payload byte. Confirm: typed Err, no OOM.
    #[test]
    fn pt12_huge_num_values_no_oom() {
        use crate::rle::decode_hybrid;
        assert_well_behaved_err("huge num_values direct", || {
            // 1-byte payload, ask for 1e9 values: decoder must bail
            // on the first run/header read (input exhaustion), NOT
            // pre-allocate Vec<u64> of 1e9 capacity.
            decode_hybrid(&[0x00], 1, 1_000_000_000)
        });
    }

    // ── Row 12b: same OOM-defense via the nested page-level path ─────
    // End-to-end variant: the V1 nested page decoder receives a tiny
    // payload but the upstream caller claims n=1e9. Must bail typed
    // Err, no OOM, no panic.
    #[test]
    fn pt12b_huge_n_via_page_v1_nested_no_oom() {
        assert_well_behaved_err("huge n via v1 nested page", || {
            // 5 bytes payload: a 4-byte u32 LE level-length prefix
            // of 1 + 1 byte of (truncated) level data. The
            // decode_level_v1 length prefix says "1 byte of level
            // data", which is way too few for 1e9 levels →
            // decode_hybrid bails on input exhaustion.
            let mut payload = Vec::new();
            payload.extend_from_slice(&1u32.to_le_bytes());
            payload.push(0x00);
            decode_page_v1_nested(
                &payload,
                0, // PLAIN
                plain::PlainSpec::plain(meta::Type::Int64),
                1_000_000_000, // n — HOSTILE
                1,             // max_rep_level
                1,             // max_def_level
                &[],
            )
        });
    }

    // ── Row 13: deep / non-canonical nesting — _covered_by_T6 ────────
    // T6 (commit b15bc7d) added classify_column_plan rejections for:
    //   • List<group<…>>  → Unsupported(SP145)
    //   • non-canonical 3-node LIST pattern → Bad
    //   • Map / struct columns → Unsupported(SP144)
    // Those rejections fire BEFORE any nested page decoder is invoked;
    // proving them again here would require constructing a full
    // FileMetaData with a hostile schema thrift, which is materially
    // covered by T6 unit tests and (end-to-end) by `mod pentest`'s
    // `pentest_opt_non_flat_schema_unsupported_no_panic` test. T10's
    // mandate is the decode-layer adversarial surface; the schema
    // gate is upstream of that surface. Inline-skip with rationale.
    #[test]
    fn pt13_deep_nesting_rejected_covered_by_t6() {
        // Documentation-only: see T6 commit b15bc7d for the actual
        // classify_column_plan rejection paths. This test exists so
        // the spec row's coverage is explicit in the test suite
        // namespace (sp143_pentest::pt13_…) for traceability.
    }
}

// ────────────────────────────────────────────────────────────────────────
// SP144 T8: adversarial pentest matrix for Map+struct nested decode.
//
// Mirrors SP143 T10 discipline for the Map+struct code paths added in
// T3 (assemble_map_kv), T4 (assemble_struct), and T5 (classify_map_plan
// + classify_struct_plan dispatch). Every test below is wrapped in
// `std::panic::catch_unwind` and asserts:
//   (1) NO panic / NO OOM-abort on hostile input
//   (2) returns a TYPED `PqError` (Bad or Unsupported), not Ok
//
// Spec reference: docs/superpowers/specs/2026-05-25-kesseldb-parquet-
// map-struct-design.md §4 (pentest matrix).
//
// Two layers, mirroring T10:
//   * Unit layer (pt1..pt11): call `assemble_map_kv` / `assemble_struct`
//     directly with hand-built level + value vectors. Isolates the
//     assembler from the full-file pipeline.
//   * Integration layer (pt12..pt15): build a hand-crafted
//     `meta::SchemaNode` and call `classify_column_plan` directly.
//     Proves the malformed-schema gate fires upstream of any decode.
// ────────────────────────────────────────────────────────────────────────
#[cfg(test)]
mod sp144_pentest {
    use super::*;
    use crate::assembly::{assemble_map_kv, assemble_struct};
    use std::panic::catch_unwind;

    /// Asserts a closure does not panic AND returns Err of any typed
    /// PqError variant. Mirrors `sp143_pentest::assert_well_behaved_err`.
    fn assert_well_behaved_err<F, T>(name: &str, f: F)
    where
        F: FnOnce() -> Result<T, PqError> + std::panic::UnwindSafe,
    {
        let r = catch_unwind(f);
        match r {
            Ok(Ok(_)) => panic!("{name}: expected Err, got Ok"),
            Ok(Err(_)) => { /* OK — typed error */ }
            Err(_) => panic!("{name}: PANICKED on adversarial input"),
        }
    }

    // ── Row 1: assemble_map_kv with rep/def length mismatch ──────────
    // First-line invariant in assemble_map_kv (line 405).
    #[test]
    fn sp144_pt1_map_rep_def_length_mismatch() {
        assert_well_behaved_err("map rep/def len mismatch", || {
            assemble_map_kv(
                &[0u32, 1, 0],
                &[1u32, 1],
                &[PqValue::Bytes(b"a".to_vec())],
                &[PqValue::I64(1)],
                1,
                false,
                false,
            )
        });
    }

    // ── Row 2: key stream truncated (k_cursor exhausted mid-loop) ────
    // REQ-REP-REQ-REQ: max_def=1, rep=[0,1], def=[1,1] → both
    // ItemPresent. Empty keys vec → "key stream exhausted at
    // position 0" Bad.
    #[test]
    fn sp144_pt2_map_key_stream_truncated() {
        assert_well_behaved_err("map key truncated", || {
            assemble_map_kv(
                &[0u32, 1],
                &[1u32, 1],
                &[],
                &[PqValue::I64(1), PqValue::I64(2)],
                1,
                false,
                false,
            )
        });
    }

    // ── Row 3: value stream truncated ────────────────────────────────
    // Same shape as pt2 but values vec empty. assemble_map_kv consumes
    // key first, then value → "value stream exhausted at position 0".
    #[test]
    fn sp144_pt3_map_value_stream_truncated() {
        assert_well_behaved_err("map value truncated", || {
            assemble_map_kv(
                &[0u32, 1],
                &[1u32, 1],
                &[PqValue::Bytes(b"a".to_vec()), PqValue::Bytes(b"b".to_vec())],
                &[],
                1,
                false,
                false,
            )
        });
    }

    // ── Row 4: keys over-provisioned (cursor < keys.len() at end) ────
    // REQ-REP-REQ-REQ: rep=[0], def=[1] implies exactly 1 key + 1
    // value, but we pass 2 keys. End-of-loop check fires
    // "keys not fully consumed".
    #[test]
    fn sp144_pt4_map_keys_unconsumed_overflow() {
        assert_well_behaved_err("map keys overflow", || {
            assemble_map_kv(
                &[0u32],
                &[1u32],
                &[PqValue::Bytes(b"a".to_vec()), PqValue::Bytes(b"b".to_vec())],
                &[PqValue::I64(1)],
                1,
                false,
                false,
            )
        });
    }

    // ── Row 5: rep level overflow (rep > 1) ──────────────────────────
    // Per-position guard at line 467: rep > 1 for a Map is always Bad.
    #[test]
    fn sp144_pt5_map_rep_level_overflow() {
        assert_well_behaved_err("map rep overflow", || {
            assemble_map_kv(
                &[0u32, 2],
                &[1u32, 1],
                &[PqValue::Bytes(b"a".to_vec()), PqValue::Bytes(b"b".to_vec())],
                &[PqValue::I64(1), PqValue::I64(2)],
                1,
                false,
                false,
            )
        });
    }

    // ── Row 6: def level overflow (def > max_def_level) ──────────────
    // classify() first guard at line 434: def > max → Bad.
    #[test]
    fn sp144_pt6_map_def_level_overflow() {
        assert_well_behaved_err("map def overflow", || {
            assemble_map_kv(
                &[0u32],
                &[5u32],
                &[PqValue::Bytes(b"a".to_vec())],
                &[PqValue::I64(1)],
                2,
                true,
                false,
            )
        });
    }

    // ── Row 7: def implies value-null but value is REQUIRED ──────────
    // Hostile combination: outer_optional=true, value_optional=false,
    // max_def=3. def=2 is strictly between empty_list_threshold (1)
    // and max_def (3) → classify routes to ValueNull → guard at line
    // 449 fires Bad("def 2 implies value null but value is REQUIRED").
    #[test]
    fn sp144_pt7_map_value_null_with_required_value() {
        assert_well_behaved_err("map value null with REQ value", || {
            assemble_map_kv(
                &[0u32],
                &[2u32],
                &[],
                &[],
                3,
                true,
                false,
            )
        });
    }

    // ── Row 8: struct field_names vs field_columns length mismatch ───
    // First-line invariant in assemble_struct (line 700).
    #[test]
    fn sp144_pt8_struct_names_columns_mismatch() {
        assert_well_behaved_err("struct names/cols mismatch", || {
            assemble_struct(
                &["a".to_string(), "b".to_string(), "c".to_string()],
                &[
                    vec![PqValue::I64(1)],
                    vec![PqValue::Bool(true)],
                ],
                false,
            )
        });
    }

    // ── Row 9: struct field column lengths differ across columns ─────
    // Row-count invariant in assemble_struct (line 711).
    #[test]
    fn sp144_pt9_struct_field_length_mismatch() {
        assert_well_behaved_err("struct field len mismatch", || {
            assemble_struct(
                &["a".to_string(), "b".to_string()],
                &[
                    vec![PqValue::I64(1), PqValue::I64(2)],
                    vec![PqValue::Bool(true)],
                ],
                false,
            )
        });
    }

    // ── Row 10: struct with empty fields ─────────────────────────────
    // Schema-shape invariant in assemble_struct (line 706).
    #[test]
    fn sp144_pt10_struct_empty_fields() {
        assert_well_behaved_err("struct empty fields", || {
            assemble_struct(&[], &[], false)
        });
    }

    // ── Row 11: empty rep stream + non-empty keys (Map n==0 lock) ────
    // n==0 fast path at line 413 must validate keys + values are also
    // empty — otherwise hostile callers could slip un-decoded values
    // past the assembler.
    #[test]
    fn sp144_pt11_map_empty_streams_with_values() {
        assert_well_behaved_err("map empty streams with values", || {
            assemble_map_kv(
                &[],
                &[],
                &[PqValue::Bytes(b"a".to_vec())],
                &[],
                1,
                false,
                false,
            )
        });
    }

    // ====================================================================
    // Integration-level: malformed schemas rejected by classify_column_plan
    // BEFORE any decode runs. Build hand-crafted SchemaNode trees and
    // call classify_column_plan directly (it's crate-private but the
    // sp144_pentest module sits inside the crate via `use super::*`).
    //
    // The leaves list can be empty in every pt12..pt15 case because each
    // rejection fires upstream of the `leaves.iter().find(...)` step.
    // ====================================================================

    // ── Row 12: MAP with REPEATED middle that has 1 child (not 2) ────
    // classify_map_plan line 1488: "non-canonical MAP (key_value
    // children != 2): SP145 follow-up". Reject before decode.
    #[test]
    fn sp144_pt12_malformed_map_one_child() {
        let root = meta::SchemaNode::Group {
            name: "root".into(),
            repetition: meta::Repetition::Required,
            children: vec![meta::SchemaNode::Group {
                name: "my_map".into(),
                repetition: meta::Repetition::Required,
                children: vec![meta::SchemaNode::Group {
                    name: "key_value".into(),
                    repetition: meta::Repetition::Repeated,
                    children: vec![meta::SchemaNode::Leaf {
                        name: "element".into(),
                        repetition: meta::Repetition::Required,
                        ptype: meta::Type::Int64,
                        max_def_level: 1,
                        max_rep_level: 1,
                        path: vec![
                            "root".into(),
                            "my_map".into(),
                            "key_value".into(),
                            "element".into(),
                        ],
                    }],
                    logical_type: None,
                }],
                logical_type: Some(meta::LogicalType::Map),
            }],
            logical_type: None,
        };
        let result = classify_column_plan(&root, "my_map", &[]);
        match result {
            Err(PqError::Unsupported(msg))
                if msg.contains("non-canonical MAP") =>
            {
                /* OK */
            }
            Err(other) => panic!(
                "pt12: expected Unsupported(non-canonical MAP), got Err({other:?})"
            ),
            Ok(_) => panic!(
                "pt12: expected Unsupported(non-canonical MAP), got Ok(_)"
            ),
        }
    }

    // ── Row 13: MAP with OPTIONAL key (spec violation) ───────────────
    // classify_map_plan line 1498-1500: "MAP key `…` must be
    // REQUIRED per Parquet spec".
    #[test]
    fn sp144_pt13_map_optional_key_rejected() {
        let root = meta::SchemaNode::Group {
            name: "root".into(),
            repetition: meta::Repetition::Required,
            children: vec![meta::SchemaNode::Group {
                name: "my_map".into(),
                repetition: meta::Repetition::Required,
                children: vec![meta::SchemaNode::Group {
                    name: "key_value".into(),
                    repetition: meta::Repetition::Repeated,
                    children: vec![
                        meta::SchemaNode::Leaf {
                            name: "key".into(),
                            repetition: meta::Repetition::Optional, // VIOLATION
                            ptype: meta::Type::ByteArray,
                            max_def_level: 2,
                            max_rep_level: 1,
                            path: vec![
                                "root".into(),
                                "my_map".into(),
                                "key_value".into(),
                                "key".into(),
                            ],
                        },
                        meta::SchemaNode::Leaf {
                            name: "value".into(),
                            repetition: meta::Repetition::Required,
                            ptype: meta::Type::Int64,
                            max_def_level: 1,
                            max_rep_level: 1,
                            path: vec![
                                "root".into(),
                                "my_map".into(),
                                "key_value".into(),
                                "value".into(),
                            ],
                        },
                    ],
                    logical_type: None,
                }],
                logical_type: Some(meta::LogicalType::Map),
            }],
            logical_type: None,
        };
        let result = classify_column_plan(&root, "my_map", &[]);
        match result {
            Err(PqError::Bad(msg))
                if msg.contains("MAP key") && msg.contains("REQUIRED") =>
            {
                /* OK */
            }
            Err(other) => panic!(
                "pt13: expected Bad(MAP key must be REQUIRED), got Err({other:?})"
            ),
            Ok(_) => panic!(
                "pt13: expected Bad(MAP key must be REQUIRED), got Ok(_)"
            ),
        }
    }

    // ── Row 14: MAP key is a group (struct-as-key, deep nesting) ─────
    // classify_map_plan line 1505: "MAP<group, _> (key is a group):
    // SP145 follow-up".
    #[test]
    fn sp144_pt14_map_group_key_rejected() {
        let root = meta::SchemaNode::Group {
            name: "root".into(),
            repetition: meta::Repetition::Required,
            children: vec![meta::SchemaNode::Group {
                name: "my_map".into(),
                repetition: meta::Repetition::Required,
                children: vec![meta::SchemaNode::Group {
                    name: "key_value".into(),
                    repetition: meta::Repetition::Repeated,
                    children: vec![
                        // Key is a Group, not a Leaf → SP145
                        meta::SchemaNode::Group {
                            name: "key".into(),
                            repetition: meta::Repetition::Required,
                            children: vec![meta::SchemaNode::Leaf {
                                name: "inner".into(),
                                repetition: meta::Repetition::Required,
                                ptype: meta::Type::Int64,
                                max_def_level: 1,
                                max_rep_level: 1,
                                path: vec![
                                    "root".into(),
                                    "my_map".into(),
                                    "key_value".into(),
                                    "key".into(),
                                    "inner".into(),
                                ],
                            }],
                            logical_type: None,
                        },
                        meta::SchemaNode::Leaf {
                            name: "value".into(),
                            repetition: meta::Repetition::Required,
                            ptype: meta::Type::Int64,
                            max_def_level: 1,
                            max_rep_level: 1,
                            path: vec![
                                "root".into(),
                                "my_map".into(),
                                "key_value".into(),
                                "value".into(),
                            ],
                        },
                    ],
                    logical_type: None,
                }],
                logical_type: Some(meta::LogicalType::Map),
            }],
            logical_type: None,
        };
        let result = classify_column_plan(&root, "my_map", &[]);
        match result {
            Err(PqError::Unsupported(msg)) if msg.contains("SP145") => {
                /* OK */
            }
            Err(other) => panic!(
                "pt14: expected Unsupported(SP145), got Err({other:?})"
            ),
            Ok(_) => panic!(
                "pt14: expected Unsupported(SP145), got Ok(_)"
            ),
        }
    }

    // ── Row 15: struct with nested-LIST child rejected ───────────────
    // classify_struct_plan line 1700-1704: "struct `…` contains nested
    // group `…`: SP145 follow-up". The struct outer has logical_type
    // None, so the dispatch in classify_column_plan routes to
    // classify_struct_plan; the nested LIST group child WAS rejected
    // with SP145 in SP144 T8 — SP145 T5 LIFTS this rejection and the
    // shape now classifies. Repurposed to verify SP145 acceptance:
    // classify_column_plan should reach the leaf-finding stage (where
    // an empty `leaves` slice fails with Bad("missing")), NOT return
    // an SP145 Unsupported. The Bad("missing") is now the expected
    // surface for an empty-leaves test fixture.
    #[test]
    fn sp145_pt_struct_with_nested_list_accepted_classifies_until_leaf_lookup() {
        let root = meta::SchemaNode::Group {
            name: "root".into(),
            repetition: meta::Repetition::Required,
            children: vec![meta::SchemaNode::Group {
                name: "my_struct".into(),
                repetition: meta::Repetition::Required,
                children: vec![
                    meta::SchemaNode::Group {
                        name: "tags".into(),
                        repetition: meta::Repetition::Required,
                        children: vec![meta::SchemaNode::Group {
                            name: "list".into(),
                            repetition: meta::Repetition::Repeated,
                            children: vec![meta::SchemaNode::Leaf {
                                name: "element".into(),
                                repetition: meta::Repetition::Required,
                                ptype: meta::Type::ByteArray,
                                max_def_level: 1,
                                max_rep_level: 1,
                                path: vec![
                                    "root".into(),
                                    "my_struct".into(),
                                    "tags".into(),
                                    "list".into(),
                                    "element".into(),
                                ],
                            }],
                            logical_type: None,
                        }],
                        logical_type: Some(meta::LogicalType::List),
                    },
                    meta::SchemaNode::Leaf {
                        name: "id".into(),
                        repetition: meta::Repetition::Required,
                        ptype: meta::Type::Int64,
                        max_def_level: 0,
                        max_rep_level: 0,
                        path: vec![
                            "root".into(),
                            "my_struct".into(),
                            "id".into(),
                        ],
                    },
                ],
                logical_type: None,
            }],
            logical_type: None,
        };
        let result = classify_column_plan(&root, "my_struct", &[]);
        match result {
            Err(PqError::Unsupported(msg)) if msg.contains("SP145") => panic!(
                "SP145 regressed: struct<List> should classify, got Unsupported(SP145): {msg}"
            ),
            // SP145 acceptance — classification routed into the nested
            // LIST path. Empty leaves list trips the leaf-lookup Bad —
            // that's the expected post-lift surface.
            Err(PqError::Bad(msg)) if msg.contains("missing") => { /* OK */ }
            Err(other) => panic!(
                "sp145_pt: expected Bad(missing) post-lift, got Err({other:?})"
            ),
            Ok(_) => panic!(
                "sp145_pt: expected Bad(missing) post-lift (empty leaves), got Ok(_)"
            ),
        }
    }
}

// ────────────────────────────────────────────────────────────────────────
// SP145 T8: pentest matrix for the 4 new deep-nesting code paths.
//
// Every row: adversarial input → typed PqError, NO panic, NO OOM.
//
// Rows 1-7: per-assembler stream pathologies (rep/def overflow, value
//   underflow, cursor exhaustion, level-mismatch overflow in each of
//   the 4 new assemblers).
// Rows 8-15: classify-side malformed inputs at the recursion boundary
//   (List<List<List<T>>> 3-deep rejected, List<Map<...>> rejected,
//   non-canonical inner shapes, struct field length mismatch in
//   recursive assembly, K rep stream diverging from V rep stream).
// ────────────────────────────────────────────────────────────────────────
#[cfg(test)]
mod sp145_pentest {
    use super::*;
    use crate::assembly::{
        assemble_list_of_list_primitive, assemble_list_of_struct,
        assemble_map_of_struct, assemble_map_of_list,
    };
    use std::panic::catch_unwind;

    fn assert_well_behaved_err<F, T>(name: &str, f: F)
    where
        F: FnOnce() -> Result<T, PqError> + std::panic::UnwindSafe,
    {
        let r = catch_unwind(f);
        match r {
            Ok(Ok(_)) => panic!("{name}: expected Err, got Ok"),
            Ok(Err(_)) => { /* OK */ }
            Err(_) => panic!("{name}: PANICKED on adversarial input"),
        }
    }

    // ── Row 1: list_of_list rep overflow ────────────────────────────
    #[test]
    fn sp145_pt1_lol_rep_overflow() {
        assert_well_behaved_err("lol rep overflow", || {
            assemble_list_of_list_primitive(
                &[0u32, 5],
                &[2u32, 2],
                &[PqValue::I64(1), PqValue::I64(2)],
                2, false, false, false,
            )
        });
    }

    // ── Row 2: list_of_list value underflow ─────────────────────────
    #[test]
    fn sp145_pt2_lol_value_underflow() {
        assert_well_behaved_err("lol value underflow", || {
            assemble_list_of_list_primitive(
                &[0u32, 2],
                &[2u32, 2],
                &[PqValue::I64(1)],
                2, false, false, false,
            )
        });
    }

    // ── Row 3: list_of_list value unconsumed overflow ───────────────
    #[test]
    fn sp145_pt3_lol_value_unconsumed_overflow() {
        assert_well_behaved_err("lol value unconsumed", || {
            assemble_list_of_list_primitive(
                &[0u32],
                &[2u32],
                &[PqValue::I64(1), PqValue::I64(2)],
                2, false, false, false,
            )
        });
    }

    // ── Row 4: list_of_list def overflow ────────────────────────────
    #[test]
    fn sp145_pt4_lol_def_overflow() {
        assert_well_behaved_err("lol def overflow", || {
            assemble_list_of_list_primitive(
                &[0u32],
                &[99u32], // > max_def
                &[PqValue::I64(1)],
                2, false, false, false,
            )
        });
    }

    // ── Row 5: list_of_struct field length mismatch ─────────────────
    #[test]
    fn sp145_pt5_los_field_length_mismatch() {
        assert_well_behaved_err("los field length mismatch", || {
            assemble_list_of_struct(
                &[0u32, 1],
                &[1u32, 1],
                &["a".to_string(), "b".to_string()],
                &[
                    vec![PqValue::I64(1), PqValue::I64(2)],
                    vec![PqValue::I64(3)], // length 1 vs 2
                ],
                1, false,
            )
        });
    }

    // ── Row 6: list_of_struct rep overflow ──────────────────────────
    #[test]
    fn sp145_pt6_los_rep_overflow() {
        assert_well_behaved_err("los rep overflow", || {
            assemble_list_of_struct(
                &[0u32, 7],
                &[1u32, 1],
                &["a".to_string()],
                &[vec![PqValue::I64(1), PqValue::I64(2)]],
                1, false,
            )
        });
    }

    // ── Row 7: map_of_struct keys count mismatch ────────────────────
    #[test]
    fn sp145_pt7_mos_keys_mismatch() {
        assert_well_behaved_err("mos keys mismatch", || {
            assemble_map_of_struct(
                &[0u32, 1],
                &[1u32, 1],
                &[PqValue::Bytes(b"a".to_vec())], // 1 key
                &["v".to_string()],
                &[vec![PqValue::I64(1), PqValue::I64(2)]], // 2 values
                1, false,
            )
        });
    }

    // ── Row 8: map_of_struct rep overflow ───────────────────────────
    #[test]
    fn sp145_pt8_mos_rep_overflow() {
        assert_well_behaved_err("mos rep overflow", || {
            assemble_map_of_struct(
                &[0u32, 4],
                &[1u32, 1],
                &[PqValue::Bytes(b"a".to_vec()), PqValue::Bytes(b"b".to_vec())],
                &["v".to_string()],
                &[vec![PqValue::I64(1), PqValue::I64(2)]],
                1, false,
            )
        });
    }

    // ── Row 9: map_of_list rep overflow ─────────────────────────────
    #[test]
    fn sp145_pt9_mol_rep_overflow() {
        assert_well_behaved_err("mol rep overflow", || {
            assemble_map_of_list(
                &[0u32, 9],
                &[2u32, 2],
                &[PqValue::Bytes(b"a".to_vec())],
                &[PqValue::Bytes(b"x".to_vec()), PqValue::Bytes(b"y".to_vec())],
                2, false, false,
            )
        });
    }

    // ── Row 10: map_of_list value-stream underflow ──────────────────
    #[test]
    fn sp145_pt10_mol_value_underflow() {
        assert_well_behaved_err("mol value underflow", || {
            assemble_map_of_list(
                &[0u32, 2],
                &[2u32, 2],
                &[PqValue::Bytes(b"a".to_vec())],
                &[PqValue::Bytes(b"x".to_vec())], // need 2
                2, false, false,
            )
        });
    }

    // ── Row 11: SP146 LIFTS the 3-deep List<List<List<T>>> reject ───
    // Originally pinned the SP145-era "3+ deep LIST nesting: SP146
    // follow-up" reject. SP146 T2 implements 3-deep nesting, so this
    // shape now CLASSIFIES into NestedListOfListOfListPrimitive. Empty
    // `leaves` slice + tree-only classify causes a Bad("missing") at
    // the leaf-lookup stage — the test pins acceptance up to that
    // expected secondary failure.
    #[test]
    fn sp146_pt11_classify_accepts_list_list_list() {
        // Outer LIST → middle REP → element (LIST) → middle REP →
        //   element (LIST) → middle REP → element (i64)
        let root = meta::SchemaNode::Group {
            name: "root".into(),
            repetition: meta::Repetition::Required,
            children: vec![meta::SchemaNode::Group {
                name: "lol_outer".into(),
                repetition: meta::Repetition::Required,
                children: vec![meta::SchemaNode::Group {
                    name: "list".into(),
                    repetition: meta::Repetition::Repeated,
                    children: vec![meta::SchemaNode::Group {
                        name: "element".into(),
                        repetition: meta::Repetition::Required,
                        children: vec![meta::SchemaNode::Group {
                            name: "list".into(),
                            repetition: meta::Repetition::Repeated,
                            children: vec![meta::SchemaNode::Group {
                                name: "element".into(),
                                repetition: meta::Repetition::Required,
                                children: vec![meta::SchemaNode::Group {
                                    name: "list".into(),
                                    repetition: meta::Repetition::Repeated,
                                    children: vec![meta::SchemaNode::Leaf {
                                        name: "element".into(),
                                        repetition: meta::Repetition::Required,
                                        ptype: meta::Type::Int64,
                                        max_def_level: 3,
                                        max_rep_level: 3,
                                        path: vec!["root".into(), "lol_outer".into(),
                                                   "list".into(), "element".into(),
                                                   "list".into(), "element".into(),
                                                   "list".into(), "element".into()],
                                    }],
                                    logical_type: None,
                                }],
                                logical_type: Some(meta::LogicalType::List),
                            }],
                            logical_type: None,
                        }],
                        logical_type: Some(meta::LogicalType::List),
                    }],
                    logical_type: None,
                }],
                logical_type: Some(meta::LogicalType::List),
            }],
            logical_type: None,
        };
        let r = classify_column_plan(&root, "lol_outer", &[]);
        match r {
            // SP146 T2 lifts the reject; classify reaches leaf-lookup with
            // empty leaves slice and surfaces Bad("missing from flat leaves list").
            Err(PqError::Bad(msg)) if msg.contains("missing from flat leaves list")
                || msg.contains("element") => { /* OK */ }
            Err(other) => panic!("pt11: expected Bad(leaf-missing), got Err({other:?})"),
            Ok(_) => panic!("pt11: classified to Ok but leaves slice was empty"),
        }
    }

    // ── Row 12: SP146 LIFTS the List<Map<K,V>> reject ──────────────
    // Originally pinned "List<Map<...>>: SP146 follow-up" reject.
    // SP146 T3 lifts. Schema now classifies into NestedListOfMap.
    // With well-formed leaves provided in the flat list, classify_column_plan
    // returns Ok(plan); we just verify it's no longer the SP146 reject.
    #[test]
    fn sp146_pt12_classify_accepts_list_of_map() {
        let root = meta::SchemaNode::Group {
            name: "root".into(),
            repetition: meta::Repetition::Required,
            children: vec![meta::SchemaNode::Group {
                name: "lom".into(),
                repetition: meta::Repetition::Required,
                children: vec![meta::SchemaNode::Group {
                    name: "list".into(),
                    repetition: meta::Repetition::Repeated,
                    children: vec![meta::SchemaNode::Group {
                        name: "element".into(),
                        repetition: meta::Repetition::Required,
                        children: vec![meta::SchemaNode::Group {
                            name: "key_value".into(),
                            repetition: meta::Repetition::Repeated,
                            children: vec![
                                meta::SchemaNode::Leaf {
                                    name: "key".into(),
                                    repetition: meta::Repetition::Required,
                                    ptype: meta::Type::ByteArray,
                                    max_def_level: 2,
                                    max_rep_level: 2,
                                    path: vec!["root".into(), "lom".into(), "list".into(),
                                               "element".into(), "key_value".into(), "key".into()],
                                },
                                meta::SchemaNode::Leaf {
                                    name: "value".into(),
                                    repetition: meta::Repetition::Required,
                                    ptype: meta::Type::Int64,
                                    max_def_level: 2,
                                    max_rep_level: 2,
                                    path: vec!["root".into(), "lom".into(), "list".into(),
                                               "element".into(), "key_value".into(), "value".into()],
                                },
                            ],
                            logical_type: None,
                        }],
                        logical_type: Some(meta::LogicalType::Map),
                    }],
                    logical_type: None,
                }],
                logical_type: Some(meta::LogicalType::List),
            }],
            logical_type: None,
        };
        // Empty leaves slice → classify reaches leaf-lookup and surfaces
        // Bad("missing from flat leaves"). The SP146 reject no longer fires.
        let r = classify_column_plan(&root, "lom", &[]);
        match r {
            Err(PqError::Bad(msg)) if msg.contains("missing from flat leaves") => { /* OK */ }
            Err(other) => panic!("pt12: expected Bad(leaf-missing), got Err({other:?})"),
            Ok(_) => panic!("pt12: classified to Ok but leaves slice was empty"),
        }
    }

    // ── Row 13: SP146 LIFTS the Map<_, Map<...>> reject ────────────
    // Originally pinned "MAP<_, Map>: SP146 follow-up". SP146 T4
    // lifts; classify_map_of_group's LogicalType::Map arm now classifies
    // into NestedMapOfMap. Test the secondary leaf-lookup Bad surface.
    #[test]
    fn sp146_pt13_classify_accepts_map_of_map() {
        // The K leaf the classify lookup will need:
        let outer_key_leaf = meta::SchemaLeaf {
            name: "key".into(),
            repetition: meta::Repetition::Required,
            ptype: meta::Type::ByteArray,
            type_length: None,
            converted_type: None,
            scale: None,
            precision: None,
            logical_type_decimal: None,
        };
        // The INNER (Map V's Map) K leaf path also called "key" —
        // collisions are OK for this test because the lookup is by
        // unique name, and `classify_map_of_group` only looks up the
        // OUTER K. We add ONLY the outer K leaf to verify the path
        // reaches the SP146 Map<_, Map> reject without other
        // leaves interfering.
        let leaves_for_test = vec![outer_key_leaf];
        let root = meta::SchemaNode::Group {
            name: "root".into(),
            repetition: meta::Repetition::Required,
            children: vec![meta::SchemaNode::Group {
                name: "mom".into(),
                repetition: meta::Repetition::Required,
                children: vec![meta::SchemaNode::Group {
                    name: "key_value".into(),
                    repetition: meta::Repetition::Repeated,
                    children: vec![
                        meta::SchemaNode::Leaf {
                            name: "key".into(),
                            repetition: meta::Repetition::Required,
                            ptype: meta::Type::ByteArray,
                            max_def_level: 1,
                            max_rep_level: 1,
                            path: vec!["root".into(), "mom".into(), "key_value".into(), "key".into()],
                        },
                        meta::SchemaNode::Group {
                            name: "value".into(),
                            repetition: meta::Repetition::Required,
                            children: vec![meta::SchemaNode::Group {
                                name: "key_value".into(),
                                repetition: meta::Repetition::Repeated,
                                children: vec![
                                    meta::SchemaNode::Leaf {
                                        name: "key".into(),
                                        repetition: meta::Repetition::Required,
                                        ptype: meta::Type::ByteArray,
                                        max_def_level: 2,
                                        max_rep_level: 2,
                                        path: vec!["root".into(), "mom".into(), "key_value".into(),
                                                   "value".into(), "key_value".into(), "key".into()],
                                    },
                                    meta::SchemaNode::Leaf {
                                        name: "value".into(),
                                        repetition: meta::Repetition::Required,
                                        ptype: meta::Type::Int64,
                                        max_def_level: 2,
                                        max_rep_level: 2,
                                        path: vec!["root".into(), "mom".into(), "key_value".into(),
                                                   "value".into(), "key_value".into(), "value".into()],
                                    },
                                ],
                                logical_type: None,
                            }],
                            logical_type: Some(meta::LogicalType::Map),
                        },
                    ],
                    logical_type: None,
                }],
                logical_type: Some(meta::LogicalType::Map),
            }],
            logical_type: None,
        };
        // Outer K is provided; inner K/V leaves are NOT in flat list →
        // classify reaches inner-leaf lookup and surfaces Bad("missing").
        let r = classify_column_plan(&root, "mom", &leaves_for_test);
        match r {
            Err(PqError::Bad(msg)) if msg.contains("missing from flat leaves") => { /* OK */ }
            Err(other) => panic!("pt13: expected Bad(inner-leaf-missing), got Err({other:?})"),
            Ok(_) => panic!("pt13: classified to Ok but inner leaves were not provided"),
        }
    }

    // ── Row 14: classify rejects non-canonical List inner — middle
    //          not REPEATED in a nested-list shape ───────────────────
    #[test]
    fn sp145_pt14_classify_rejects_noncanonical_inner_list() {
        let root = meta::SchemaNode::Group {
            name: "root".into(),
            repetition: meta::Repetition::Required,
            children: vec![meta::SchemaNode::Group {
                name: "lol_bad".into(),
                repetition: meta::Repetition::Required,
                children: vec![meta::SchemaNode::Group {
                    name: "list".into(),
                    repetition: meta::Repetition::Repeated,
                    children: vec![meta::SchemaNode::Group {
                        name: "element".into(),
                        repetition: meta::Repetition::Required,
                        // Inner LIST's middle should be REPEATED but
                        // we set it REQUIRED → non-canonical.
                        children: vec![meta::SchemaNode::Group {
                            name: "list".into(),
                            repetition: meta::Repetition::Required, // ← BAD
                            children: vec![meta::SchemaNode::Leaf {
                                name: "element".into(),
                                repetition: meta::Repetition::Required,
                                ptype: meta::Type::Int64,
                                max_def_level: 2,
                                max_rep_level: 1,
                                path: vec!["root".into(), "lol_bad".into(),
                                           "list".into(), "element".into(),
                                           "list".into(), "element".into()],
                            }],
                            logical_type: None,
                        }],
                        logical_type: Some(meta::LogicalType::List),
                    }],
                    logical_type: None,
                }],
                logical_type: Some(meta::LogicalType::List),
            }],
            logical_type: None,
        };
        let r = classify_column_plan(&root, "lol_bad", &[]);
        match r {
            Err(PqError::Unsupported(msg)) if msg.contains("middle not REPEATED") => { /* OK */ }
            Err(other) => panic!("pt14: expected Unsupported(middle not REPEATED), got Err({other:?})"),
            Ok(_) => panic!("pt14: expected Unsupported, got Ok(_)"),
        }
    }

    // ── Row 15: map_of_list value unconsumed overflow ───────────────
    #[test]
    fn sp145_pt15_mol_value_unconsumed() {
        assert_well_behaved_err("mol value unconsumed", || {
            assemble_map_of_list(
                &[0u32],
                &[2u32],
                &[PqValue::Bytes(b"a".to_vec())],
                &[PqValue::Bytes(b"x".to_vec()), PqValue::Bytes(b"y".to_vec())], // extra
                2, false, false,
            )
        });
    }

    // ── Row 16 (bonus): list_of_struct empty rep/def with items ─────
    #[test]
    fn sp145_pt16_los_empty_levels_with_items() {
        assert_well_behaved_err("los empty levels with items", || {
            assemble_list_of_struct(
                &[],
                &[],
                &["a".to_string()],
                &[vec![PqValue::I64(1)]], // items but no levels
                1, false,
            )
        });
    }

    // ── SP146 pentest rows (rows 17-23) ────────────────────────────
    // Adversarial inputs for the 3 new SP146 assemblers + classifier
    // updates. Every row: well-behaved Err, no panic, no infinite loop.

    use crate::assembly::{
        assemble_list_of_list_of_list_primitive,
        assemble_list_of_map_kv,
        assemble_map_of_map_kv,
    };

    // ── Row 17: 3-deep List<List<List> rep overflow ────────────────
    #[test]
    fn sp146_pt17_lll_rep_overflow() {
        assert_well_behaved_err("lll rep overflow", || {
            assemble_list_of_list_of_list_primitive(
                &[0u32, 5],
                &[3u32, 3],
                &[PqValue::I64(1), PqValue::I64(2)],
                3, false, false, false, false,
            )
        });
    }

    // ── Row 18: 3-deep value-stream underflow ──────────────────────
    #[test]
    fn sp146_pt18_lll_value_underflow() {
        assert_well_behaved_err("lll value underflow", || {
            assemble_list_of_list_of_list_primitive(
                &[0u32, 3],
                &[3u32, 3],
                &[PqValue::I64(1)], // need 2
                3, false, false, false, false,
            )
        });
    }

    // ── Row 19: 3-deep def overflow ────────────────────────────────
    #[test]
    fn sp146_pt19_lll_def_overflow() {
        assert_well_behaved_err("lll def overflow", || {
            assemble_list_of_list_of_list_primitive(
                &[0u32],
                &[99u32], // > max_def
                &[PqValue::I64(1)],
                3, false, false, false, false,
            )
        });
    }

    // ── Row 20: List<Map> rep overflow ─────────────────────────────
    #[test]
    fn sp146_pt20_lom_rep_overflow() {
        assert_well_behaved_err("lom rep overflow", || {
            assemble_list_of_map_kv(
                &[0u32, 5],
                &[2u32, 2],
                &[PqValue::Bytes(b"a".to_vec()), PqValue::Bytes(b"b".to_vec())],
                &[PqValue::I64(1), PqValue::I64(2)],
                2, false, false,
            )
        });
    }

    // ── Row 21: List<Map> value-stream underflow ───────────────────
    #[test]
    fn sp146_pt21_lom_value_underflow() {
        assert_well_behaved_err("lom value underflow", || {
            assemble_list_of_map_kv(
                &[0u32, 2],
                &[2u32, 2],
                &[PqValue::Bytes(b"a".to_vec()), PqValue::Bytes(b"b".to_vec())],
                &[PqValue::I64(1)], // need 2
                2, false, false,
            )
        });
    }

    // ── Row 22: Map<_, Map> rep overflow ───────────────────────────
    #[test]
    fn sp146_pt22_mom_rep_overflow() {
        assert_well_behaved_err("mom rep overflow", || {
            assemble_map_of_map_kv(
                &[0u32, 4],
                &[2u32, 2],
                &[PqValue::Bytes(b"a".to_vec())],
                &[PqValue::Bytes(b"x".to_vec()), PqValue::Bytes(b"y".to_vec())],
                &[PqValue::I64(1), PqValue::I64(2)],
                2, false, false,
            )
        });
    }

    // ── Row 23: Map<_, Map> outer-key underflow ────────────────────
    #[test]
    fn sp146_pt23_mom_outer_key_underflow() {
        assert_well_behaved_err("mom outer-key underflow", || {
            assemble_map_of_map_kv(
                &[0u32, 1],
                &[2u32, 2],
                &[PqValue::Bytes(b"a".to_vec())], // need 2 (one per rep=0 + rep=1)
                &[PqValue::Bytes(b"x".to_vec()), PqValue::Bytes(b"y".to_vec())],
                &[PqValue::I64(1), PqValue::I64(2)],
                2, false, false,
            )
        });
    }

    // ── Row 24: Map<_, Map> inner-value unconsumed overflow ────────
    #[test]
    fn sp146_pt24_mom_inner_value_unconsumed() {
        assert_well_behaved_err("mom inner-value unconsumed", || {
            assemble_map_of_map_kv(
                &[0u32],
                &[2u32],
                &[PqValue::Bytes(b"a".to_vec())],
                &[PqValue::Bytes(b"x".to_vec())],
                &[PqValue::I64(1), PqValue::I64(2)], // extra
                2, false, false,
            )
        });
    }
}

/// SP149 T4: pentest the LZ4_RAW block decoder against adversarial inputs.
/// The hand-rolled `lz4::decompress` MUST reject every malformed input
/// listed below with `PqError::Bad` — no panics, no infinite loops, no
/// silent truncation. These pin the well-behaved-rejection invariants
/// the SP149 KATs only check positively.
#[cfg(test)]
mod sp149_pentest {
    use crate::lz4::decompress;

    /// Row 1: zero-offset is invalid per the LZ4 spec (offset ranges
    /// 1..=65_535). A zero would mean "copy from the current byte" —
    /// would loop forever in a naïve decoder. We reject explicitly.
    #[test]
    fn sp149_pt_lz4_zero_offset_rejected() {
        let src = vec![0x40, 0x61, 0x62, 0x63, 0x64, 0x00, 0x00];
        assert!(decompress(&src, 8).is_err());
    }

    /// Row 2: an offset that points before the start of output. A naïve
    /// decoder would underflow and panic; we surface a Bad error.
    /// Token 0x00: lit_len=0, match_len=4; offset=1 with empty output
    /// → match wants `out[0-1]` which doesn't exist.
    #[test]
    fn sp149_pt_lz4_offset_exceeds_output_rejected() {
        let src = vec![0x00, 0x01, 0x00];
        assert!(decompress(&src, 4).is_err());
    }

    /// Row 3: literal section that overruns the source buffer. Token
    /// 0xa0 declares 10 literal bytes but src has only 5 trailing bytes.
    /// Without the bounds check this would panic on the slice or extend
    /// past EOF. With the check it's a Bad error.
    #[test]
    fn sp149_pt_lz4_truncated_literal_rejected() {
        let src = vec![0xa0, 1, 2, 3, 4, 5];
        assert!(decompress(&src, 10).is_err());
    }

    /// Row 4: decoded output shorter than declared `expected_uncompressed_size`.
    /// Pyarrow's writer should always pad, but a tampered footer or a
    /// truncated read could create this — we reject rather than silently
    /// return short.
    #[test]
    fn sp149_pt_lz4_size_mismatch_rejected() {
        let src = vec![0x50, 0x68, 0x65, 0x6c, 0x6c, 0x6f];
        assert!(decompress(&src, 100).is_err());
    }

    /// Row 5: degenerate empty input with declared zero size MUST succeed
    /// (empty in → empty out). Some codecs reject empty pages; LZ4_RAW
    /// must not, because pyarrow can legally emit zero-byte pages for
    /// all-null columns once null-only encoding lands.
    #[test]
    fn sp149_pt_lz4_empty_src_with_expected_zero() {
        let out = decompress(&[], 0).expect("empty in empty out");
        assert_eq!(out, vec![]);
    }

    /// Row 6: a truncated offset (only 1 of 2 expected bytes after
    /// literals) must be rejected with Bad("truncated offset").
    /// Token 0x40 declares lit_len=4, match_len=4, then literals "abcd",
    /// then we leave only 1 offset byte (0x04) — the second is missing.
    #[test]
    fn sp149_pt_lz4_truncated_offset_rejected() {
        let src = vec![0x40, 0x61, 0x62, 0x63, 0x64, 0x04];
        let err = decompress(&src, 8).unwrap_err();
        let msg = format!("{err:?}");
        assert!(msg.contains("truncated offset") || msg.contains("offset"),
                "expected truncated-offset error, got: {msg}");
    }

    /// Row 7: a missing lit-len extra byte (lit-nibble == 15 but src
    /// runs out before the extra byte). Token 0xf0 says "lit_len = 15 +
    /// extras" but we provide no extras at all.
    #[test]
    fn sp149_pt_lz4_truncated_lit_len_extra_rejected() {
        let src = vec![0xf0]; // expects extra-byte, never delivered
        let err = decompress(&src, 16).unwrap_err();
        let msg = format!("{err:?}");
        assert!(msg.contains("truncated lit-len") || msg.contains("lit-len"),
                "expected lit-len-extra truncation error, got: {msg}");
    }
}

