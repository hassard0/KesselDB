//! Parquet `FileMetaData` (the subset OBJ-2a needs). Field IDs from
//! the published `parquet.thrift`. Unknown fields are skipped.
#![allow(dead_code)]

use crate::thrift::{ctype, StructReader};
use crate::PqError;

fn bad(s: &str) -> PqError {
    PqError::Bad(s.to_string())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Type {
    Boolean,
    Int32,
    Int64,
    Int96,
    Float,
    Double,
    ByteArray,
    FixedLenByteArray,
    Other(i32),
}
impl Type {
    fn from_i32(v: i32) -> Type {
        match v {
            0 => Type::Boolean,
            1 => Type::Int32,
            2 => Type::Int64,
            3 => Type::Int96,
            4 => Type::Float,
            5 => Type::Double,
            6 => Type::ByteArray,
            7 => Type::FixedLenByteArray,
            o => Type::Other(o),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Repetition {
    Required,
    Optional,
    Repeated,
    Other(i32),
}
impl Repetition {
    fn from_i32(v: i32) -> Repetition {
        match v {
            0 => Repetition::Required,
            1 => Repetition::Optional,
            2 => Repetition::Repeated,
            o => Repetition::Other(o),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Codec {
    Uncompressed,
    Other(i32),
}
impl Codec {
    fn from_i32(v: i32) -> Codec {
        if v == 0 { Codec::Uncompressed } else { Codec::Other(v) }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Encoding {
    Plain,
    /// RLE (id=3). For flat REQUIRED columns appears in the
    /// ColumnMetaData encoding list describing the (zero-length)
    /// level encoding; not the data page encoding.
    Rle,
    /// PLAIN_DICTIONARY (id=2): dictionary indices, legacy tag.
    PlainDictionary,
    /// RLE_DICTIONARY (id=8): dictionary indices, current tag.
    RleDictionary,
    Other(i32),
}
impl Encoding {
    fn from_i32(v: i32) -> Encoding {
        match v {
            0 => Encoding::Plain,
            2 => Encoding::PlainDictionary,
            3 => Encoding::Rle,
            8 => Encoding::RleDictionary,
            o => Encoding::Other(o),
        }
    }
}

#[derive(Clone, Debug)]
pub struct SchemaLeaf {
    pub name: String,
    pub ptype: Type,
    pub repetition: Repetition,
}

#[derive(Clone, Debug)]
pub struct ColumnChunk {
    pub path: Vec<String>,
    pub ptype: Type,
    pub codec: Codec,
    pub encodings: Vec<Encoding>,
    pub num_values: i64,
    pub data_page_offset: i64,
    pub dictionary_page_offset: Option<i64>,
}

#[derive(Clone, Debug)]
pub struct RowGroup {
    pub columns: Vec<ColumnChunk>,
    pub num_rows: i64,
}

#[derive(Clone, Debug)]
pub struct FileMetaData {
    pub version: i32,
    pub num_rows: i64,
    /// Flat leaf schema elements (root group element with
    /// num_children>0 is excluded; only true leaves kept).
    pub leaves: Vec<SchemaLeaf>,
    pub row_groups: Vec<RowGroup>,
}

impl FileMetaData {
    pub fn decode(bytes: &[u8]) -> Result<FileMetaData, PqError> {
        let mut s = StructReader::new(bytes);
        let mut version = 0i32;
        let mut num_rows = 0i64;
        let mut leaves: Vec<SchemaLeaf> = Vec::new();
        let mut row_groups: Vec<RowGroup> = Vec::new();
        while let Some(f) = s.next_field()? {
            match f.id {
                1 => version = s.read_i32(&f)?,
                2 => {
                    let (et, count) = s.list_header()?;
                    if et != ctype::STRUCT {
                        return Err(bad("schema list type"));
                    }
                    let saved = s.save_last_id();
                    for _ in 0..count {
                        if let Some(le) = decode_schema_element(&mut s)? {
                            leaves.push(le);
                        }
                        s.restore_last_id(saved);
                    }
                    s.restore_last_id(f.id);
                }
                3 => num_rows = s.read_i64(&f)?,
                4 => {
                    let (et, count) = s.list_header()?;
                    if et != ctype::STRUCT {
                        return Err(bad("row_groups list type"));
                    }
                    let saved = s.save_last_id();
                    for _ in 0..count {
                        row_groups.push(decode_row_group(&mut s)?);
                        s.restore_last_id(saved);
                    }
                    s.restore_last_id(f.id);
                }
                _ => s.skip(f.ctype)?,
            }
        }
        Ok(FileMetaData { version, num_rows, leaves, row_groups })
    }
}

/// Returns Some(leaf) for a true leaf (num_children == 0), None for
/// a group element (root / nested) — OBJ-2a only consumes leaves;
/// nested groups are detected later via repetition checks.
fn decode_schema_element(
    s: &mut StructReader,
) -> Result<Option<SchemaLeaf>, PqError> {
    // Each nested struct in Thrift compact resets field-ID deltas to 0.
    s.reset_last_id();
    let mut ptype = Type::Other(-1);
    let mut repetition = Repetition::Required;
    let mut name = String::new();
    let mut num_children = 0i32;
    let mut saw_type = false;
    while let Some(f) = s.next_field()? {
        match f.id {
            1 => {
                ptype = Type::from_i32(s.read_i32(&f)?);
                saw_type = true;
            }
            3 => repetition = Repetition::from_i32(s.read_i32(&f)?),
            4 => {
                name = String::from_utf8_lossy(s.read_binary(&f)?)
                    .into_owned()
            }
            5 => num_children = s.read_i32(&f)?,
            _ => s.skip(f.ctype)?,
        }
    }
    if num_children > 0 || !saw_type {
        return Ok(None); // group element, not a leaf
    }
    Ok(Some(SchemaLeaf { name, ptype, repetition }))
}

fn decode_row_group(
    s: &mut StructReader,
) -> Result<RowGroup, PqError> {
    s.reset_last_id();
    let mut columns: Vec<ColumnChunk> = Vec::new();
    let mut num_rows = 0i64;
    while let Some(f) = s.next_field()? {
        match f.id {
            1 => {
                let (et, count) = s.list_header()?;
                if et != ctype::STRUCT {
                    return Err(bad("columns list type"));
                }
                let saved = s.save_last_id();
                for _ in 0..count {
                    columns.push(decode_column_chunk(s)?);
                    s.restore_last_id(saved);
                }
                s.restore_last_id(f.id);
            }
            3 => num_rows = s.read_i64(&f)?,
            _ => s.skip(f.ctype)?,
        }
    }
    Ok(RowGroup { columns, num_rows })
}

fn decode_column_chunk(
    s: &mut StructReader,
) -> Result<ColumnChunk, PqError> {
    s.reset_last_id();
    let mut out: Option<ColumnChunk> = None;
    while let Some(f) = s.next_field()? {
        match f.id {
            3 => {
                if f.ctype != ctype::STRUCT { return Err(bad("ColumnChunk.meta_data: expected struct")); }
                out = Some(decode_column_meta(s)?);
                s.restore_last_id(f.id);
            }
            _ => s.skip(f.ctype)?,
        }
    }
    out.ok_or_else(|| bad("ColumnChunk missing meta_data"))
}

fn decode_column_meta(
    s: &mut StructReader,
) -> Result<ColumnChunk, PqError> {
    s.reset_last_id();
    let mut ptype = Type::Other(-1);
    let mut codec = Codec::Uncompressed;
    let mut encodings: Vec<Encoding> = Vec::new();
    let mut path: Vec<String> = Vec::new();
    let mut num_values = 0i64;
    let mut data_page_offset = 0i64;
    let mut dictionary_page_offset: Option<i64> = None;
    while let Some(f) = s.next_field()? {
        match f.id {
            1 => ptype = Type::from_i32(s.read_i32(&f)?),
            2 => {
                let (et, count) = s.list_header()?;
                for _ in 0..count {
                    if et != ctype::I32 && et != ctype::I8
                        && et != ctype::I16
                    {
                        return Err(bad("encodings list type"));
                    }
                    let v = i32::try_from(s.reader().ivarint()?)
                        .map_err(|_| bad("encoding range"))?;
                    encodings.push(Encoding::from_i32(v));
                }
            }
            3 => {
                let (et, count) = s.list_header()?;
                if et != ctype::BINARY {
                    return Err(bad("path list type"));
                }
                for _ in 0..count {
                    let n = usize::try_from(s.reader().uvarint()?)
                        .map_err(|_| bad("path len"))?;
                    let seg = s.reader().take(n)?;
                    path.push(
                        String::from_utf8_lossy(seg).into_owned(),
                    );
                }
            }
            4 => codec = Codec::from_i32(s.read_i32(&f)?),
            5 => num_values = s.read_i64(&f)?,
            9 => data_page_offset = s.read_i64(&f)?,
            11 => dictionary_page_offset = Some(s.read_i64(&f)?),
            _ => s.skip(f.ctype)?,
        }
    }
    Ok(ColumnChunk {
        path,
        ptype,
        codec,
        encodings,
        num_values,
        data_page_offset,
        dictionary_page_offset,
    })
}

/// V1 DataPageHeader (PageHeader: 1:PageType type, 3:i32
/// uncompressed_page_size, 5:DataPageHeader data_page_header;
/// DataPageHeader: 1:i32 num_values, 2:Encoding encoding).
/// Field 7: DictionaryPageHeader { 1:i32 num_values, 2:Encoding encoding,
/// 3:bool is_sorted }.
#[derive(Clone, Debug)]
pub struct PageHeader {
    pub page_type: i32,
    pub uncompressed_size: i32,
    pub compressed_size: i32,
    pub dp_num_values: i32,
    pub dp_encoding: i32,
    pub dict_num_values: i32,
    pub dict_encoding: i32,
}

pub fn decode_page_header(
    bytes: &[u8],
) -> Result<(PageHeader, usize), PqError> {
    let mut s = StructReader::new(bytes);
    let mut ph = PageHeader {
        page_type: -1,
        uncompressed_size: 0,
        compressed_size: 0,
        dp_num_values: 0,
        dp_encoding: -1,
        dict_num_values: 0,
        dict_encoding: -1,
    };
    while let Some(f) = s.next_field()? {
        match f.id {
            1 => ph.page_type = s.read_i32(&f)?,
            3 => ph.uncompressed_size = s.read_i32(&f)?,
            4 => ph.compressed_size = s.read_i32(&f)?,
            5 => {
                // nested DataPageHeader struct
                if f.ctype != ctype::STRUCT { return Err(bad("PageHeader.data_page_header: expected struct")); }
                s.reset_last_id();
                while let Some(g) = s.next_field()? {
                    match g.id {
                        1 => ph.dp_num_values = s.read_i32(&g)?,
                        2 => ph.dp_encoding = s.read_i32(&g)?,
                        _ => s.skip(g.ctype)?,
                    }
                }
                s.restore_last_id(f.id);
            }
            7 => {
                // DictionaryPageHeader { 1:i32 num_values, 2:Encoding encoding,
                // 3:bool is_sorted }. Same per-struct last_id bracketing as f5.
                if f.ctype != ctype::STRUCT { return Err(bad("PageHeader.dictionary_page_header: expected struct")); }
                s.reset_last_id();
                while let Some(g) = s.next_field()? {
                    match g.id {
                        1 => ph.dict_num_values = s.read_i32(&g)?,
                        2 => ph.dict_encoding = s.read_i32(&g)?,
                        _ => s.skip(g.ctype)?,
                    }
                }
                s.restore_last_id(f.id);
            }
            _ => s.skip(f.ctype)?,
        }
    }
    Ok((ph, s.reader_pos()))
}

#[cfg(test)]
mod tests {
    use super::*;

    // Helpers to hand-build a compact-thrift struct (spec-faithful,
    // independent of the decoder under test).
    fn uv(out: &mut Vec<u8>, mut v: u64) {
        loop {
            let b = (v & 0x7f) as u8;
            v >>= 7;
            if v == 0 { out.push(b); break; } else { out.push(b | 0x80); }
        }
    }
    fn zz(v: i64) -> u64 { ((v << 1) ^ (v >> 63)) as u64 }

    #[test]
    fn decode_minimal_filemetadata() {
        // parquet.thrift FileMetaData: 1:i32 version, 2:list<SchemaElement>
        // schema, 3:i64 num_rows, 4:list<RowGroup> row_groups.
        // SchemaElement: 1:Type type, 4:RepetitionType repetition_type,
        // 5:i32 num_children, 8(name moved) — actual ids: 1 type,
        // 3 repetition_type, 4 name, 5 num_children (per parquet.thrift).
        // RowGroup: 1:list<ColumnChunk> columns, 2:i64 total_byte_size,
        // 3:i64 num_rows. ColumnChunk: 3:ColumnMetaData meta_data.
        // ColumnMetaData: 1:Type type, 2:list<Encoding> encodings,
        // 3:list<string> path_in_schema, 4:CompressionCodec codec,
        // 5:i64 num_values, 9:i64 data_page_offset.
        //
        // We assert: version, num_rows, one schema leaf "id" Type=INT64
        // REQUIRED, one row group with one ColumnChunk for "id".
        // (Bytes hand-assembled below; field-header = (delta<<4)|ctype.)
        let mut b = Vec::new();
        // FileMetaData struct:
        // f1 i32 version=1 : header (1<<4)|5=0x15, zz(1)=2
        b.push(0x15); uv(&mut b, zz(1));
        // f2 list<struct> schema, 2 elements (root group + leaf):
        //   header (1<<4)|9=0x19 ; list size/type byte (2<<4)|12=0x2c
        b.push(0x19); b.push(0x2c);
        //   schema[0] root group: name f4="root", num_children f5=1
        //     name: header (4<<4)|8=0x48, len4 "root"
        b.push(0x48); uv(&mut b, 4); b.extend_from_slice(b"root");
        //     num_children: delta to f5 from f4 =1 -> (1<<4)|5=0x15, zz(1)=2
        b.push(0x15); uv(&mut b, zz(1));
        b.push(0x00); // stop schema[0]
        //   schema[1] leaf "id": type f1=INT64(2), repetition f3=REQUIRED(0),
        //     name f4="id"
        //     type: (1<<4)|5=0x15, zz(2)=4   (Type enum INT64=2)
        b.push(0x15); uv(&mut b, zz(2));
        //     repetition_type: delta f1->f3 =2 -> (2<<4)|5=0x25, zz(0)=0  (REQUIRED=0)
        b.push(0x25); uv(&mut b, zz(0));
        //     name: delta f3->f4 =1 -> (1<<4)|8=0x18, len2 "id"
        b.push(0x18); uv(&mut b, 2); b.extend_from_slice(b"id");
        b.push(0x00); // stop schema[1]
        // f3 i64 num_rows=2 : delta f2->f3 =1 -> (1<<4)|6=0x16, zz(2)=4
        b.push(0x16); uv(&mut b, zz(2));
        // f4 list<RowGroup> row_groups, 1 element :
        //   delta f3->f4=1 -> (1<<4)|9=0x19 ; list (1<<4)|12=0x1c
        b.push(0x19); b.push(0x1c);
        //   RowGroup: f1 list<ColumnChunk> columns (1 elem),
        //     f3 i64 num_rows=2
        //     f1 list: (1<<4)|9=0x19 ; list (1<<4)|12=0x1c
        b.push(0x19); b.push(0x1c);
        //       ColumnChunk: f3 ColumnMetaData meta_data
        //         delta 0->3 =3 -> (3<<4)|12=0x3c
        b.push(0x3c);
        //         ColumnMetaData: f1 Type=INT64(2), f2 list<Encoding>[PLAIN(0)],
        //           f3 list<string> path=["id"], f4 codec=UNCOMPRESSED(0),
        //           f5 i64 num_values=2, f9 i64 data_page_offset=4
        //         f1 type: (1<<4)|5=0x15 zz(2)=4
        b.push(0x15); uv(&mut b, zz(2));
        //         f2 encodings list<i32> 1 elem PLAIN(0):
        //           delta1 -> (1<<4)|9=0x19 ; list (1<<4)|5=0x15 ; zz(0)=0
        b.push(0x19); b.push(0x15); uv(&mut b, zz(0));
        //         f3 path_in_schema list<string> ["id"]:
        //           delta1 -> (1<<4)|9=0x19 ; list (1<<4)|8=0x18 ; len2 "id"
        b.push(0x19); b.push(0x18); uv(&mut b, 2); b.extend_from_slice(b"id");
        //         f4 codec UNCOMPRESSED(0): delta1 -> (1<<4)|5=0x15 zz(0)=0
        b.push(0x15); uv(&mut b, zz(0));
        //         f5 num_values=2: delta1 -> (1<<4)|6=0x16 zz(2)=4
        b.push(0x16); uv(&mut b, zz(2));
        //         f9 data_page_offset=4: delta f5->f9 =4 -> (4<<4)|6=0x46 zz(4)=8
        b.push(0x46); uv(&mut b, zz(4));
        b.push(0x00); // stop ColumnMetaData
        b.push(0x00); // stop ColumnChunk
        //   RowGroup f3 num_rows=2 : last id in RG was 1 (columns),
        //     delta 1->3 =2 -> (2<<4)|6=0x26 zz(2)=4
        b.push(0x26); uv(&mut b, zz(2));
        b.push(0x00); // stop RowGroup
        b.push(0x00); // stop FileMetaData

        let md = FileMetaData::decode(&b).expect("decode");
        assert_eq!(md.version, 1);
        assert_eq!(md.num_rows, 2);
        assert_eq!(md.leaves.len(), 1);
        let leaf = &md.leaves[0];
        assert_eq!(leaf.name, "id");
        assert_eq!(leaf.ptype, Type::Int64);
        assert_eq!(leaf.repetition, Repetition::Required);
        assert_eq!(md.row_groups.len(), 1);
        let cc = &md.row_groups[0].columns[0];
        assert_eq!(cc.path, vec!["id".to_string()]);
        assert_eq!(cc.ptype, Type::Int64);
        assert_eq!(cc.codec, Codec::Uncompressed);
        assert_eq!(cc.encodings, vec![Encoding::Plain]);
        assert_eq!(cc.num_values, 2);
        assert_eq!(cc.data_page_offset, 4);
        assert_eq!(cc.dictionary_page_offset, None);
    }

    #[test]
    fn columnmeta_decodes_dictionary_page_offset_field11() {
        let mut b = Vec::new();
        b.push(0x15); uv(&mut b, zz(1));                 // f1 version=1
        b.push(0x19); b.push(0x2c);                      // f2 list<struct> 2
        b.push(0x48); uv(&mut b, 4); b.extend_from_slice(b"root"); // schema[0] name
        b.push(0x15); uv(&mut b, zz(1));                 // schema[0] f5 num_children=1
        b.push(0x00);                                    // stop schema[0]
        b.push(0x15); uv(&mut b, zz(2));                 // schema[1] f1 type=INT64(2)
        b.push(0x25); uv(&mut b, zz(0));                 // f3 repetition=REQUIRED
        b.push(0x18); uv(&mut b, 2); b.extend_from_slice(b"id"); // f4 name
        b.push(0x00);                                    // stop schema[1]
        b.push(0x16); uv(&mut b, zz(3));                 // f3 num_rows=3
        b.push(0x19); b.push(0x1c);                      // f4 list<RowGroup> 1
        b.push(0x19); b.push(0x1c);                      // RG f1 list<ColumnChunk> 1
        b.push(0x3c);                                    // ColumnChunk f3 ColumnMetaData
        b.push(0x15); uv(&mut b, zz(2));                 // CMD f1 type=INT64(2)
        b.push(0x19); b.push(0x15); uv(&mut b, zz(2));   // f2 encodings [PLAIN_DICTIONARY(2)]
        b.push(0x19); b.push(0x18); uv(&mut b, 2); b.extend_from_slice(b"id"); // f3 path ["id"]
        b.push(0x15); uv(&mut b, zz(0));                 // f4 codec=UNCOMPRESSED
        b.push(0x16); uv(&mut b, zz(3));                 // f5 num_values=3
        b.push(0x46); uv(&mut b, zz(40));                // f9 data_page_offset=40 (delta 5->9=4, i64=6 -> 0x46)
        b.push(0x26); uv(&mut b, zz(4));                 // f11 dictionary_page_offset=4 (delta 9->11=2, i64=6 -> 0x26)
        b.push(0x00);                                    // stop ColumnMetaData
        b.push(0x00);                                    // stop ColumnChunk
        b.push(0x26); uv(&mut b, zz(3));                 // RG f3 num_rows=3
        b.push(0x00);                                    // stop RowGroup
        b.push(0x00);                                    // stop FileMetaData

        let md = FileMetaData::decode(&b).expect("decode");
        let cc = &md.row_groups[0].columns[0];
        assert_eq!(cc.dictionary_page_offset, Some(4));
        assert_eq!(cc.data_page_offset, 40);
        assert_eq!(cc.encodings, vec![Encoding::PlainDictionary]);
    }

    #[test]
    fn pageheader_decodes_dictionary_page_header_field7() {
        let mut h = Vec::new();
        h.push(0x15); uv(&mut h, zz(2));   // f1 type=DICTIONARY_PAGE(2)
        h.push(0x25); uv(&mut h, zz(16));  // f3 uncompressed_page_size=16 (delta 1->3=2,i32=5 ->0x25)
        h.push(0x15); uv(&mut h, zz(16));  // f4 compressed_page_size=16 (delta 3->4=1 ->0x15)
        h.push(0x3c);                      // f7 DictionaryPageHeader struct (delta 4->7=3, struct=12 ->0x3c)
        h.push(0x15); uv(&mut h, zz(2));   // g1 num_values=2
        h.push(0x15); uv(&mut h, zz(2));   // g2 encoding=PLAIN_DICTIONARY(2) (delta 1->2=1 ->0x15)
        h.push(0x12);                      // g3 is_sorted=false (delta 2->3=1, BOOL_FALSE=2 ->0x12)
        h.push(0x00);                      // stop DictionaryPageHeader
        h.push(0x00);                      // stop PageHeader

        let (ph, _len) = decode_page_header(&h).expect("decode");
        assert_eq!(ph.page_type, 2);
        assert_eq!(ph.dict_num_values, 2);
        assert_eq!(ph.dict_encoding, 2);
    }
}
