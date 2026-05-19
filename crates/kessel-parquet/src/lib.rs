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
            if cc.codec != meta::Codec::Uncompressed {
                return Err(PqError::Unsupported(
                    "compression: OBJ-2b/2c".into(),
                ));
            }
            if cc.encodings.iter().any(|e| *e != meta::Encoding::Plain) {
                return Err(PqError::Unsupported(
                    "non-PLAIN encoding (dictionary/RLE/DELTA): OBJ-2b"
                        .into(),
                ));
            }
            let off = usize::try_from(cc.data_page_offset)
                .map_err(|_| PqError::Bad("page offset range".into()))?;
            let page_region = bytes
                .get(off..)
                .ok_or_else(|| PqError::Bad("page offset past EOF".into()))?;
            let (ph, hdr_len) = meta::decode_page_header(page_region)?;
            // V1 data page only (PageType DATA_PAGE == 0).
            if ph.page_type != 0 {
                return Err(PqError::Unsupported(
                    "non-V1 / dictionary / index page: OBJ-2b".into(),
                ));
            }
            if ph.dp_encoding != 0 {
                return Err(PqError::Unsupported(
                    "data page encoding != PLAIN: OBJ-2b".into(),
                ));
            }
            let n = usize::try_from(ph.dp_num_values)
                .map_err(|_| PqError::Bad("num_values range".into()))?;
            let dstart = off
                .checked_add(hdr_len)
                .ok_or_else(|| PqError::Bad("page hdr len ovf".into()))?;
            let dend = dstart
                .checked_add(
                    usize::try_from(ph.uncompressed_size)
                        .map_err(|_| PqError::Bad("page size range".into()))?,
                )
                .ok_or_else(|| PqError::Bad("page size ovf".into()))?;
            let page = bytes
                .get(dstart..dend)
                .ok_or_else(|| PqError::Bad("page data truncated".into()))?;
            let vals = plain::decode_plain(page, cc.ptype, n)?;
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
    ///   - `encoding`:   Encoding enum value (0=PLAIN, 2=PLAIN_DICTIONARY)
    ///   - `codec`:      CompressionCodec enum value (0=UNCOMPRESSED, 2=SNAPPY)
    ///   - `repetition`: RepetitionType enum value (0=REQUIRED, 1=OPTIONAL)
    ///   - `data_page_offset`: actual byte offset of the page header in the file
    fn filemetadata_bytes(
        encoding: i64,
        codec: i64,
        repetition: i64,
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
        //   f1 type=INT64(2): (1<<4)|5=0x15, zz(2)=4
        b.push(0x15); uv(&mut b, zz(2));
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
    /// support-matrix rejection tests.
    fn build_parquet_file(
        encoding: i64,
        codec: i64,
        repetition: i64,
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
        let meta = filemetadata_bytes(encoding, codec, repetition, data_page_offset);
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

    #[test]
    fn extract_kat_spec_faithful_parquet_file() {
        // Good file: PLAIN(0), UNCOMPRESSED(0), REQUIRED(0), plain page hdr
        let file = build_parquet_file(0, 0, 0, false);

        // KAT: extract "id" → rows [[I64(7)], [I64(-2)]]
        let rows = extract(&file, &["id"]).expect("extract");
        assert_eq!(rows, vec![
            vec![PqValue::I64(7)],
            vec![PqValue::I64(-2)],
        ]);

        // ── support-matrix rejection tests ────────────────────────────

        // dict_file: encoding = PLAIN_DICTIONARY(2) in ColumnMetaData encodings
        // → triggers "non-PLAIN encoding" gate
        let dict_file = build_parquet_file(2, 0, 0, false);
        assert!(
            matches!(extract(&dict_file, &["id"]), Err(PqError::Unsupported(_))),
            "dict encoding must be Unsupported"
        );

        // snappy_file: codec = SNAPPY(2) in ColumnMetaData
        // → triggers "compression" gate
        let snappy_file = build_parquet_file(0, 2, 0, false);
        assert!(
            matches!(extract(&snappy_file, &["id"]), Err(PqError::Unsupported(_))),
            "snappy codec must be Unsupported"
        );

        // optional_file: repetition = OPTIONAL(1) in SchemaElement
        // → triggers "OPTIONAL/REPEATED columns" gate (checked at schema level)
        let optional_file = build_parquet_file(0, 0, 1, false);
        assert!(
            matches!(extract(&optional_file, &["id"]), Err(PqError::Unsupported(_))),
            "optional repetition must be Unsupported"
        );

        // good_file + missing column name → Bad
        let good_file = build_parquet_file(0, 0, 0, false);
        assert!(
            matches!(extract(&good_file, &["missing"]), Err(PqError::Bad(_))),
            "missing column must be Bad"
        );

        // dict_page_hdr: page header encoding field = PLAIN_DICTIONARY(2)
        // → triggers "data page encoding != PLAIN" gate
        let dict_page_file = build_parquet_file(0, 0, 0, true);
        assert!(
            matches!(extract(&dict_page_file, &["id"]), Err(PqError::Unsupported(_))),
            "dict page encoding must be Unsupported"
        );
    }
}
