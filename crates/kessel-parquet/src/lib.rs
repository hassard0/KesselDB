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
    wp: meta::Type,
    n: usize,
    max_def_level: u32,
    dict: &[PqValue],
) -> Result<Vec<PqValue>, PqError> {
    if max_def_level == 0 {
        return match dp_encoding {
            0 => plain::decode_plain(payload, wp, n),
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
        0 => plain::decode_plain(body, wp, present)?,
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
    let mut out = Vec::with_capacity(n);
    let mut it = vals.into_iter();
    for &d in &defs {
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

/// Read one column chunk's values across all its pages.
/// Flat REQUIRED or OPTIONAL, UNCOMPRESSED or SNAPPY, V1. Supports: an
/// optional leading DICTIONARY_PAGE then zero-or-more DATA_PAGEs; each data
/// page is PLAIN (dictionary-fallback) or PLAIN_DICTIONARY/RLE_DICTIONARY.
fn read_chunk_values(
    file: &[u8],
    cc: &meta::ColumnChunk,
    want_ptype: meta::Type,
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
        plain::decode_plain(&payload, want_ptype, dn)?
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
        if ph.page_type != 0 {
            return Err(PqError::Unsupported(
                "non-V1 data page (V2/index): OBJ-2c".into(),
            ));
        }
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
        if matches!(ph.dp_encoding, 2 | 8) && cc.dictionary_page_offset.is_none() {
            return Err(PqError::Bad(
                "dictionary-encoded data page without dictionary_page_offset"
                    .into(),
            ));
        }
        let vals = decode_page(&payload, ph.dp_encoding, want_ptype, n, max_def_level, &dict)?;
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
    // supported physical type.
    // Also collect the schema-declared physical type for each wanted column
    // so the per-row-group loop can verify ColumnMetaData type consistency.
    let mut wanted_ptypes: Vec<meta::Type> = Vec::with_capacity(wanted.len());
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
            | meta::Type::ByteArray => {}
            t => {
                return Err(PqError::Unsupported(format!(
                    "physical type {t:?}: OBJ-2c"
                )))
            }
        }
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
            let vals = read_chunk_values(bytes, cc, wanted_ptypes[ci], wanted_max_def_levels[ci])?;
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
