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

/// Read one column chunk's values across all its pages.
/// Flat REQUIRED, UNCOMPRESSED, V1. Supports: an optional leading
/// DICTIONARY_PAGE then one-or-more DATA_PAGEs; each data page is
/// PLAIN (dictionary-fallback) or PLAIN_DICTIONARY/RLE_DICTIONARY.
fn read_chunk_values(
    file: &[u8],
    cc: &meta::ColumnChunk,
    want_ptype: meta::Type,
) -> Result<Vec<PqValue>, PqError> {
    if cc.codec != meta::Codec::Uncompressed {
        return Err(PqError::Unsupported("compression: OBJ-2b-3".into()));
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
        let dend = dstart
            .checked_add(
                usize::try_from(ph.uncompressed_size)
                    .map_err(|_| PqError::Bad("dict page size range".into()))?,
            )
            .ok_or_else(|| PqError::Bad("dict page size ovf".into()))?;
        let dpage = file
            .get(dstart..dend)
            .ok_or_else(|| PqError::Bad("dict page data truncated".into()))?;
        plain::decode_plain(dpage, want_ptype, dn)?
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
        let dend = dstart
            .checked_add(
                usize::try_from(ph.uncompressed_size)
                    .map_err(|_| PqError::Bad("page size range".into()))?,
            )
            .ok_or_else(|| PqError::Bad("page size ovf".into()))?;
        let page = file
            .get(dstart..dend)
            .ok_or_else(|| PqError::Bad("page data truncated".into()))?;
        let vals = match ph.dp_encoding {
            0 => plain::decode_plain(page, want_ptype, n)?,
            2 | 8 => {
                if cc.dictionary_page_offset.is_none() {
                    return Err(PqError::Bad(
                        "dictionary-encoded data page without dictionary_page_offset"
                            .into(),
                    ));
                }
                dict::resolve_dict_indices(page, &dict, n)?
            }
            _ => {
                return Err(PqError::Unsupported(
                    "data page encoding (DELTA/BYTE_STREAM_SPLIT): OBJ-2c"
                        .into(),
                ))
            }
        };
        if out.len().checked_add(vals.len()).map(|t| t > want_rows).unwrap_or(true) {
            return Err(PqError::Bad(
                "data page values exceed chunk num_values".into(),
            ));
        }
        out.extend(vals);
        off = dend;
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

    // Resolve each wanted name to its leaf; enforce REQUIRED + flat.
    // Also collect the schema-declared physical type for each wanted column
    // so the per-row-group loop can verify ColumnMetaData type consistency.
    let mut wanted_ptypes: Vec<meta::Type> = Vec::with_capacity(wanted.len());
    for w in wanted {
        let leaf = md
            .leaves
            .iter()
            .find(|l| &l.name == w)
            .ok_or_else(|| {
                PqError::Bad(format!("column `{w}` not found in Parquet schema"))
            })?;
        if leaf.repetition != meta::Repetition::Required {
            return Err(PqError::Unsupported(
                "OPTIONAL/REPEATED columns: OBJ-2b".into(),
            ));
        }
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
            let vals = read_chunk_values(bytes, cc, wanted_ptypes[ci])?;
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
    ///   3: i32 uncompressed_page_size
    ///   4: i32 compressed_page_size
    ///   5: DataPageHeader data_page_header (struct)
    ///
    /// DataPageHeader:
    ///   1: i32 num_values
    ///   2: Encoding encoding (i32 enum)
    fn page_header_bytes(num_values: i32, data_bytes: i32) -> Vec<u8> {
        let mut h = Vec::new();
        // f1 page_type = DATA_PAGE(0): (1<<4)|5=0x15, zz(0)=0
        h.push(0x15); uv(&mut h, zz(0));
        // f3 uncompressed_page_size: delta 1→3=2 → (2<<4)|5=0x25
        h.push(0x25); uv(&mut h, zz(data_bytes as i64));
        // f4 compressed_page_size: delta 3→4=1 → (1<<4)|5=0x15
        h.push(0x15); uv(&mut h, zz(data_bytes as i64));
        // f5 DataPageHeader struct: delta 4→5=1 → (1<<4)|12=0x1c
        h.push(0x1c);
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
        h.push(0x25); uv(&mut h, zz(data_bytes as i64));
        h.push(0x15); uv(&mut h, zz(data_bytes as i64));
        h.push(0x1c);
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

    #[test]
    fn extract_rejects_snappy_codec() {
        // codec = SNAPPY(2) in ColumnMetaData
        // → triggers "compression" gate
        let snappy_file = build_parquet_file(0, 2, 0, false);
        assert!(
            matches!(extract(&snappy_file, &["id"]), Err(PqError::Unsupported(_))),
            "snappy codec must be Unsupported"
        );
    }

    #[test]
    fn extract_rejects_optional_repetition() {
        // repetition = OPTIONAL(1) in SchemaElement
        // → triggers "OPTIONAL/REPEATED columns" gate (checked at schema level)
        let optional_file = build_parquet_file(0, 0, 1, false);
        assert!(
            matches!(extract(&optional_file, &["id"]), Err(PqError::Unsupported(_))),
            "optional repetition must be Unsupported"
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
    fn build_dict_int64_file() -> Vec<u8> {
        let mut dict_data = Vec::new();
        dict_data.extend_from_slice(&7i64.to_le_bytes());
        dict_data.extend_from_slice(&(-2i64).to_le_bytes());
        let dbytes = dict_data.len() as i64; // 16
        let mut dict_hdr = Vec::new();
        dict_hdr.push(0x15); uv(&mut dict_hdr, zz(2));        // f1 type=DICTIONARY_PAGE(2)
        dict_hdr.push(0x25); uv(&mut dict_hdr, zz(dbytes));   // f3 uncompressed_page_size
        dict_hdr.push(0x15); uv(&mut dict_hdr, zz(dbytes));   // f4 compressed_page_size
        dict_hdr.push(0x3c);                                  // f7 struct (delta 4->7=3)
        dict_hdr.push(0x15); uv(&mut dict_hdr, zz(2));        // g1 num_values=2
        dict_hdr.push(0x15); uv(&mut dict_hdr, zz(2));        // g2 encoding=PLAIN_DICTIONARY(2)
        dict_hdr.push(0x12);                                  // g3 is_sorted=false
        dict_hdr.push(0x00);                                  // stop DictionaryPageHeader
        dict_hdr.push(0x00);                                  // stop PageHeader

        let data_payload: Vec<u8> = vec![0x01, 0x03, 0x04];
        let pbytes = data_payload.len() as i64; // 3
        let mut data_hdr = Vec::new();
        data_hdr.push(0x15); uv(&mut data_hdr, zz(0));        // f1 type=DATA_PAGE(0)
        data_hdr.push(0x25); uv(&mut data_hdr, zz(pbytes));   // f3 uncompressed_page_size
        data_hdr.push(0x15); uv(&mut data_hdr, zz(pbytes));   // f4 compressed_page_size
        data_hdr.push(0x1c);                                  // f5 struct (delta 4->5=1)
        data_hdr.push(0x15); uv(&mut data_hdr, zz(3));        // g1 num_values=3
        data_hdr.push(0x15); uv(&mut data_hdr, zz(2));        // g2 encoding=PLAIN_DICTIONARY(2)
        data_hdr.push(0x00);                                  // stop DataPageHeader
        data_hdr.push(0x00);                                  // stop PageHeader

        let dict_page_offset: i64 = 4;
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
        m.push(0x26); uv(&mut m, zz(dict_page_offset));       // f11 dictionary_page_offset (delta 9->11=2,i64)
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
    /// parquet.thrift PageHeader { 1:type, 3:uncompressed_page_size,
    /// 4:compressed_page_size, 5:DataPageHeader { 1:num_values,
    /// 2:encoding } }.
    fn page_header_bytes(num_values: i32, data_bytes: i32) -> Vec<u8> {
        let mut h = Vec::new();
        h.push(0x15); uv(&mut h, zz(0)); // f1 type = DATA_PAGE(0)
        h.push(0x25); uv(&mut h, zz(data_bytes as i64)); // f3 uncompressed
        h.push(0x15); uv(&mut h, zz(data_bytes as i64)); // f4 compressed
        h.push(0x1c); // f5 DataPageHeader struct
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
