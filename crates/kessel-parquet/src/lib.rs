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
mod zstd_fse;
mod zstd_literals;
mod zstd_huffman;
mod zstd_huffstream;
mod zstd_sequences;

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
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PqError {
    /// Malformed / truncated / out-of-bounds Parquet bytes.
    Bad(String),
    /// Well-formed but uses a feature outside OBJ-2a (names the
    /// OBJ-2b/2c follow-on).
    Unsupported(String),
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
        meta::Codec::Other(_) => Err(PqError::Unsupported(
            "compression codec (zstd/lz4/brotli): OBJ-2c".into(),
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
        meta::Codec::Other(_) => {
            return Err(PqError::Unsupported(
                "compression codec (zstd/lz4/brotli): OBJ-2c".into(),
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
        meta::Codec::Uncompressed | meta::Codec::Snappy | meta::Codec::Gzip => {}
        meta::Codec::Other(_) => {
            return Err(PqError::Unsupported(
                "compression codec (zstd/lz4/brotli): OBJ-2c".into(),
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

/// Decode the `wanted` leaf columns (in that output order) from a
/// whole Parquet object. OBJ-2a: flat REQUIRED columns, PLAIN,
/// UNCOMPRESSED, V1 data pages, all row groups concatenated.
pub fn extract(
    bytes: &[u8],
    wanted: &[&str],
) -> Result<Vec<Vec<PqValue>>, PqError> {
    let md_bytes = footer::metadata_slice(bytes)?;
    let md = meta::FileMetaData::decode(md_bytes)?;

    // Reject nested schemas once, file-level, before any per-leaf work.
    // A non-flat file is rejected with the same Unsupported error regardless
    // of which (or how many) columns are requested.
    if !md.flat_schema {
        return Err(PqError::Unsupported("nested schema: OBJ-2c".into()));
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
    fn extract_rejects_zstd_codec_obj2c() {
        // Repurposed from extract_rejects_gzip_codec_obj2c: GZIP(2) is now
        // SUPPORTED; ZSTD(6) is still Unsupported (OBJ-2c follow-on).
        let f = build_parquet_file(0, 6, 0, false); // codec=ZSTD(6)=Other(6)
        assert!(
            matches!(extract(&f, &["id"]), Err(PqError::Unsupported(_))),
            "ZSTD codec must be Unsupported (OBJ-2c)"
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
        m.push(0x19); m.push(0x18); uv(&mut m, 2); m.extend_from_slice(b"id"); // f3 path
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
    fn extract_rejects_nested_schema_obj2c() {
        // A non-flat schema (root → intermediate group → leaf) must be
        // Unsupported("nested schema: OBJ-2c") regardless of repetition.
        let f = build_nested_schema_file();
        assert!(
            matches!(extract(&f, &["id"]), Err(PqError::Unsupported(_))),
            "nested schema must be Unsupported (OBJ-2c)"
        );
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
        m.push(0x19); m.push(0x18); uv(&mut m, 2); m.extend_from_slice(b"id");
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

    // ── Lock 6: non-flat schema → Unsupported, no panic ─────────────────
    //
    // root → intermediate group → leaf: flat_schema=false → extract →
    // Err(Unsupported("nested schema: OBJ-2c")), no panic.
    #[test]
    fn pentest_opt_non_flat_schema_unsupported_no_panic() {
        let f = build_nested_schema_file();
        assert_no_panic_unsupported(&f);
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
        // (a) uncompressed_size huge (much larger than the file).
        let hdr_a = page_header_bytes(2, i32::MAX);
        let mut page = Vec::new();
        page.extend_from_slice(&7i64.to_le_bytes());
        page.extend_from_slice(&(-2i64).to_le_bytes());
        let meta_a = filemetadata_bytes(2, b"id", 4);
        let file_a = assemble(&hdr_a, &page, &meta_a);
        no_panic_typed_err(&file_a, "id");
        assert!(matches!(
            extract(&file_a, &["id"]),
            Err(PqError::Bad(_))
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
    // a tiny actual page → must error FAST with a typed Bad, NO
    // multi-GB allocation / OOM. num_values=i32::MAX, REQUIRED so the
    // decoder reaches decode_plain(present=i32::MAX) whose reservation
    // is bounded by data.len() (Task-12 fix); uncompressed=i32::MAX
    // makes vt huge but values_section.len() (tiny) != vt → the raw
    // values length-mismatch guard fires before any big alloc.
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
            matches!(extract(&f, &["id"]), Err(PqError::Bad(_))),
            "i32::MAX num_values/size vs tiny V2 page must be Bad"
        );
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
    // always "d" for this module).

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
