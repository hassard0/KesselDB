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
    /// SNAPPY (parquet CompressionCodec id = 1), raw block format.
    Snappy,
    /// GZIP (parquet CompressionCodec id = 2), RFC 1952 member.
    Gzip,
    Other(i32),
}
impl Codec {
    fn from_i32(v: i32) -> Codec {
        match v {
            0 => Codec::Uncompressed,
            1 => Codec::Snappy,
            2 => Codec::Gzip,
            o => Codec::Other(o),
        }
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
    /// On-disk index layout is identical to RleDictionary;
    /// readers MUST treat the two tags the same (do not branch).
    PlainDictionary,
    /// RLE_DICTIONARY (id=8): dictionary indices, current tag.
    /// Same on-disk index layout as PlainDictionary (see above).
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

/// A decoded schema element: either a leaf (physical column) or a group
/// (nested struct node, including the root). Used internally to compute
/// `FileMetaData::flat_schema`.
#[derive(Clone, Debug)]
pub(crate) enum SchemaNode {
    Leaf(SchemaLeaf),
    Group { num_children: i32 },
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
    /// Only `SchemaNode::Leaf` elements (true leaves: a physical type,
    /// `num_children == 0`) are collected here; `Group` elements
    /// (root/intermediate/typeless) are excluded.
    pub leaves: Vec<SchemaLeaf>,
    pub row_groups: Vec<RowGroup>,
    /// True iff the schema is flat: exactly one root group whose
    /// `num_children` equals the number of leaf elements, with no
    /// intermediate group nodes. Required for OBJ-2b OPTIONAL decode.
    pub flat_schema: bool,
}

impl FileMetaData {
    pub fn decode(bytes: &[u8]) -> Result<FileMetaData, PqError> {
        let mut s = StructReader::new(bytes);
        let mut version = 0i32;
        let mut num_rows = 0i64;
        let mut leaves: Vec<SchemaLeaf> = Vec::new();
        let mut row_groups: Vec<RowGroup> = Vec::new();
        let mut nodes: Vec<SchemaNode> = Vec::new();
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
                        let node = decode_schema_element(&mut s)?;
                        if let SchemaNode::Leaf(ref le) = node {
                            leaves.push(le.clone());
                        }
                        nodes.push(node);
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
        // Flat schema = exactly one root Group followed only by Leaf
        // elements, and the root's declared num_children matches the
        // actual leaf count (catches a lying child count). `.first()`
        // also handles the empty-schema case (=> false). A nc==0 root
        // with zero leaves is vacuously "flat" but yields no leaves and
        // fails downstream OBJ-2b column resolution — harmless.
        // Negative nc: a negative i32 cast to usize becomes a huge
        // number, != nodes.len()-1, so flat_schema=false — safe.
        let flat_schema = if let Some(SchemaNode::Group { num_children: nc }) =
            nodes.first()
        {
            nodes[1..].iter().all(|n| matches!(n, SchemaNode::Leaf(_)))
                && *nc as usize == nodes.len() - 1
        } else {
            false
        };
        Ok(FileMetaData { version, num_rows, leaves, row_groups, flat_schema })
    }
}

/// Decodes one SchemaElement struct and returns a `SchemaNode`.
/// An element with `num_children > 0` or no physical type field is a
/// `Group`; an element with `num_children == 0` and a physical type is
/// a `Leaf`. This faithfully reflects parquet.thrift SchemaElement:
///   {1:Type type (absent for groups), 3:RepetitionType,
///    4:name (binary), 5:num_children (i32)}.
fn decode_schema_element(
    s: &mut StructReader,
) -> Result<SchemaNode, PqError> {
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
        // Group element (root or intermediate): has no physical type
        // or explicitly has num_children > 0.
        return Ok(SchemaNode::Group { num_children });
    }
    Ok(SchemaNode::Leaf(SchemaLeaf { name, ptype, repetition }))
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

/// V1 DataPageHeader (PageHeader: 1:PageType type, 2:i32
/// uncompressed_page_size, 3:i32 compressed_page_size,
/// 5:DataPageHeader data_page_header;
/// DataPageHeader: 1:i32 num_values, 2:Encoding encoding).
/// Field 7: DictionaryPageHeader { 1:i32 num_values, 2:Encoding encoding,
/// 3:bool is_sorted }.
/// Field 8: DataPageHeaderV2 { 1:i32 num_values, 2:i32 num_nulls,
/// 3:i32 num_rows, 4:Encoding encoding,
/// 5:i32 definition_levels_byte_length,
/// 6:i32 repetition_levels_byte_length,
/// 7:optional bool is_compressed (default true) }.
#[derive(Clone, Debug)]
pub struct PageHeader {
    pub page_type: i32,
    pub uncompressed_size: i32,
    pub compressed_size: i32,
    pub dp_num_values: i32,
    pub dp_encoding: i32,
    /// DictionaryPageHeader fields. `dict_encoding == -1` and
    /// `dict_num_values == 0` mean "no dictionary page header was
    /// present"; only trust these when `page_type == 2` (DICTIONARY_PAGE).
    pub dict_num_values: i32,
    pub dict_encoding: i32,
    // V2 (DataPageHeaderV2, PageHeader field 8). Only meaningful when
    // page_type == 3 (DATA_PAGE_V2). v2_is_compressed defaults true
    // (the thrift field is optional, default true).
    pub v2_num_values: i32,
    pub v2_num_nulls: i32,
    pub v2_num_rows: i32,
    pub v2_encoding: i32,       // default -1
    pub v2_def_len: i32,
    pub v2_rep_len: i32,
    pub v2_is_compressed: bool, // default true
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
        v2_num_values: 0,
        v2_num_nulls: 0,
        v2_num_rows: 0,
        v2_encoding: -1,
        v2_def_len: 0,
        v2_rep_len: 0,
        v2_is_compressed: true,
    };
    while let Some(f) = s.next_field()? {
        match f.id {
            1 => ph.page_type = s.read_i32(&f)?,
            2 => ph.uncompressed_size = s.read_i32(&f)?,
            3 => ph.compressed_size = s.read_i32(&f)?,
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
            8 => {
                // DataPageHeaderV2 { 1:i32 num_values, 2:i32 num_nulls,
                // 3:i32 num_rows, 4:Encoding encoding,
                // 5:i32 definition_levels_byte_length,
                // 6:i32 repetition_levels_byte_length,
                // 7:optional bool is_compressed (default true) }.
                // Same per-struct last_id bracketing as field 5 / field 7.
                if f.ctype != ctype::STRUCT { return Err(bad("PageHeader.data_page_header_v2: expected struct")); }
                s.reset_last_id();
                while let Some(g) = s.next_field()? {
                    match g.id {
                        1 => ph.v2_num_values = s.read_i32(&g)?,
                        2 => ph.v2_num_nulls = s.read_i32(&g)?,
                        3 => ph.v2_num_rows = s.read_i32(&g)?,
                        4 => ph.v2_encoding = s.read_i32(&g)?,
                        5 => ph.v2_def_len = s.read_i32(&g)?,
                        6 => ph.v2_rep_len = s.read_i32(&g)?,
                        7 => ph.v2_is_compressed = s.read_bool(&g)?,
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
    fn columnmeta_decodes_gzip_codec() {
        fn build(codec: i64) -> Vec<u8> {
            let mut b = Vec::new();
            b.push(0x15); uv(&mut b, zz(1));                 // f1 version=1
            b.push(0x19); b.push(0x2c);                      // f2 list<struct> 2
            b.push(0x48); uv(&mut b, 4); b.extend_from_slice(b"root");
            b.push(0x15); uv(&mut b, zz(1));                 // num_children=1
            b.push(0x00);
            b.push(0x15); uv(&mut b, zz(2));                 // leaf type=INT64
            b.push(0x25); uv(&mut b, zz(0));                 // repetition=REQUIRED
            b.push(0x18); uv(&mut b, 2); b.extend_from_slice(b"id");
            b.push(0x00);
            b.push(0x16); uv(&mut b, zz(1));                 // num_rows=1
            b.push(0x19); b.push(0x1c);                      // list<RowGroup> 1
            b.push(0x19); b.push(0x1c);                      // RG list<ColumnChunk> 1
            b.push(0x3c);                                    // ColumnChunk f3 CMD
            b.push(0x15); uv(&mut b, zz(2));                 // CMD type=INT64
            b.push(0x19); b.push(0x15); uv(&mut b, zz(0));   // encodings [PLAIN]
            b.push(0x19); b.push(0x18); uv(&mut b, 2); b.extend_from_slice(b"id");
            b.push(0x15); uv(&mut b, zz(codec));             // f4 codec
            b.push(0x16); uv(&mut b, zz(1));                 // num_values=1
            b.push(0x46); uv(&mut b, zz(4));                 // data_page_offset=4
            b.push(0x00); b.push(0x00);
            b.push(0x26); uv(&mut b, zz(1));                 // RG num_rows=1
            b.push(0x00); b.push(0x00);
            b
        }
        assert_eq!(
            FileMetaData::decode(&build(2)).unwrap()
                .row_groups[0].columns[0].codec, Codec::Gzip);
        assert_eq!(
            FileMetaData::decode(&build(6)).unwrap()
                .row_groups[0].columns[0].codec, Codec::Other(6));
    }

    #[test]
    fn columnmeta_decodes_snappy_codec() {
        fn build(codec: i64) -> Vec<u8> {
            let mut b = Vec::new();
            b.push(0x15); uv(&mut b, zz(1));                 // f1 version=1
            b.push(0x19); b.push(0x2c);                      // f2 list<struct> 2
            b.push(0x48); uv(&mut b, 4); b.extend_from_slice(b"root");
            b.push(0x15); uv(&mut b, zz(1));                 // num_children=1
            b.push(0x00);
            b.push(0x15); uv(&mut b, zz(2));                 // leaf f1 type=INT64
            b.push(0x25); uv(&mut b, zz(0));                 // f3 repetition=REQUIRED
            b.push(0x18); uv(&mut b, 2); b.extend_from_slice(b"id");
            b.push(0x00);
            b.push(0x16); uv(&mut b, zz(1));                 // f3 num_rows=1
            b.push(0x19); b.push(0x1c);                      // f4 list<RowGroup> 1
            b.push(0x19); b.push(0x1c);                      // RG f1 list<ColumnChunk> 1
            b.push(0x3c);                                    // ColumnChunk f3 ColumnMetaData
            b.push(0x15); uv(&mut b, zz(2));                 // CMD f1 type=INT64
            b.push(0x19); b.push(0x15); uv(&mut b, zz(0));   // f2 encodings [PLAIN]
            b.push(0x19); b.push(0x18); uv(&mut b, 2); b.extend_from_slice(b"id");
            b.push(0x15); uv(&mut b, zz(codec));             // f4 codec
            b.push(0x16); uv(&mut b, zz(1));                 // f5 num_values=1
            b.push(0x46); uv(&mut b, zz(4));                 // f9 data_page_offset=4
            b.push(0x00);                                    // stop ColumnMetaData
            b.push(0x00);                                    // stop ColumnChunk
            b.push(0x26); uv(&mut b, zz(1));                 // RG f3 num_rows=1
            b.push(0x00);                                    // stop RowGroup
            b.push(0x00);                                    // stop FileMetaData
            b
        }
        let md1 = FileMetaData::decode(&build(1)).expect("snappy");
        assert_eq!(md1.row_groups[0].columns[0].codec, Codec::Snappy);
        let md7 = FileMetaData::decode(&build(7)).expect("other");
        assert_eq!(md7.row_groups[0].columns[0].codec, Codec::Other(7));
    }

    #[test]
    fn flat_schema_true_for_root_plus_leaves_false_for_nested_group() {
        // Builds a minimal FileMetaData whose schema is either flat
        // (root group + 1 leaf) or nested (root group + intermediate
        // group + 1 leaf). We verify flat_schema is true/false accordingly.
        //
        // Compact-thrift encoding used throughout:
        //   field header = (field_delta << 4) | ctype
        //   ctype: i32=5, i64=6, binary=8, struct=12, list=9
        //   i32/i64 values are zigzag uvariants.
        //
        // parquet.thrift SchemaElement field IDs:
        //   1:Type (i32), 3:RepetitionType (i32), 4:name (binary),
        //   5:num_children (i32).
        //   A GROUP element has NO field-1 type and f5 num_children > 0.
        //
        // Flat schema bytes:  [Group{nc=1}, Leaf("id")]  → flat_schema=true
        // Nested schema bytes: [Group{nc=1}, Group{nc=1}, Leaf("id")] → flat_schema=false
        fn build(nested: bool) -> Vec<u8> {
            let mut b = Vec::new();
            // FileMetaData f1 version=1
            b.push(0x15); uv(&mut b, zz(1));
            // FileMetaData f2 list<SchemaElement>:
            //   2 elements (flat) or 3 elements (nested).
            //   list header byte = (count << 4) | STRUCT_ctype(12)
            let count: u8 = if nested { 3 } else { 2 };
            b.push(0x19); b.push((count << 4) | 12);

            // schema[0] root GROUP: f4 name="schema", f5 num_children=1.
            //   Root always has exactly 1 immediate child
            //   (the intermediate group in nested, the leaf in flat).
            //   NO f1 type field (groups have no physical type).
            //   Field IDs reset to 0 at struct start.
            //   f4 name: delta=4, binary=8 → (4<<4)|8=0x48
            b.push(0x48); uv(&mut b, 6); b.extend_from_slice(b"schema");
            //   f5 num_children: delta f4→f5=1, i32=5 → (1<<4)|5=0x15; zz(1)=2
            b.push(0x15); uv(&mut b, zz(1));
            b.push(0x00); // stop schema[0]

            if nested {
                // schema[1] intermediate GROUP "g": no f1 type, f4 name,
                //   f5 num_children=1. Minimal (no f3 repetition needed).
                //   Field IDs reset to 0.
                //   f4 name: delta=4, binary=8 → 0x48; len=1; "g"
                b.push(0x48); uv(&mut b, 1); b.extend_from_slice(b"g");
                //   f5 num_children=1: delta f4→f5=1, i32=5 → 0x15; zz(1)=2
                b.push(0x15); uv(&mut b, zz(1));
                b.push(0x00); // stop schema[1] (intermediate group)
            }

            // schema[last] leaf "id": f1 type=INT64(2), f3 rep=REQUIRED(0),
            //   f4 name="id". Field IDs reset to 0.
            //   f1 type=INT64(2): delta=1, i32=5 → (1<<4)|5=0x15; zz(2)=4
            b.push(0x15); uv(&mut b, zz(2));
            //   f3 repetition=REQUIRED(0): delta f1→f3=2, i32=5 → (2<<4)|5=0x25; zz(0)=0
            b.push(0x25); uv(&mut b, zz(0));
            //   f4 name="id": delta f3→f4=1, binary=8 → (1<<4)|8=0x18; len=2
            b.push(0x18); uv(&mut b, 2); b.extend_from_slice(b"id");
            b.push(0x00); // stop leaf

            // f3 num_rows=1 (FileMetaData): delta f2→f3=1, i64=6 → 0x16; zz(1)=2
            b.push(0x16); uv(&mut b, zz(1));
            // f4 list<RowGroup> 1: delta f3→f4=1 → 0x19; list (1<<4)|12=0x1c
            b.push(0x19); b.push(0x1c);
            //   RowGroup f1 list<ColumnChunk> 1: delta 0→1=1 → 0x19; 0x1c
            b.push(0x19); b.push(0x1c);
            //     ColumnChunk f3 ColumnMetaData: delta 0→3=3 → (3<<4)|12=0x3c
            b.push(0x3c);
            //     ColumnMetaData f1 type=INT64(2): delta=1, i32=5 → 0x15; zz(2)=4
            b.push(0x15); uv(&mut b, zz(2));
            //     f2 encodings [PLAIN(0)]: delta=1, list=9 → 0x19; (1<<4)|i32(5)=0x15; zz(0)
            b.push(0x19); b.push(0x15); uv(&mut b, zz(0));
            //     f3 path ["id"]: delta=1, list=9 → 0x19; (1<<4)|binary(8)=0x18; len=2
            b.push(0x19); b.push(0x18); uv(&mut b, 2); b.extend_from_slice(b"id");
            //     f4 codec=UNCOMPRESSED(0): delta=1, i32=5 → 0x15; zz(0)=0
            b.push(0x15); uv(&mut b, zz(0));
            //     f5 num_values=1: delta=1, i64=6 → 0x16; zz(1)=2
            b.push(0x16); uv(&mut b, zz(1));
            //     f9 data_page_offset=4: delta f5→f9=4, i64=6 → (4<<4)|6=0x46; zz(4)=8
            b.push(0x46); uv(&mut b, zz(4));
            b.push(0x00); // stop ColumnMetaData
            b.push(0x00); // stop ColumnChunk
            //   RowGroup f3 num_rows=1: delta f1→f3=2, i64=6 → (2<<4)|6=0x26; zz(1)=2
            b.push(0x26); uv(&mut b, zz(1));
            b.push(0x00); // stop RowGroup
            b.push(0x00); // stop FileMetaData
            b
        }

        // Flat: nodes = [Group{nc=1}, Leaf("id")]
        //   nodes.len()=2, nodes[0]=Group{nc=1}, nodes[1..] all Leaf,
        //   nc=1 == len-1=1  ⇒ flat_schema=true.
        let md_flat = FileMetaData::decode(&build(false)).expect("flat");
        assert!(md_flat.flat_schema, "root+leaves only ⇒ flat");

        // Nested: nodes = [Group{nc=1}, Group{nc=1}, Leaf("id")]
        //   nodes[1] is a Group ⇒ nodes[1..].all(Leaf) is false
        //   ⇒ flat_schema=false.
        let md_nested = FileMetaData::decode(&build(true)).expect("nested");
        assert!(!md_nested.flat_schema, "intermediate group ⇒ not flat");
    }

    #[test]
    fn pageheader_decodes_dictionary_page_header_field7() {
        let mut h = Vec::new();
        h.push(0x15); uv(&mut h, zz(2));   // f1 type=DICTIONARY_PAGE(2)
        h.push(0x15); uv(&mut h, zz(16));  // f2 uncompressed_page_size=16 (delta 1->2=1,i32=5 ->0x15)
        h.push(0x15); uv(&mut h, zz(16));  // f3 compressed_page_size=16 (delta 2->3=1 ->0x15)
        h.push(0x4c);                      // f7 DictionaryPageHeader struct (delta 3->7=4, struct=12 ->0x4c)
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

    #[test]
    fn pageheader_decodes_data_page_header_v2_field8() {
        // PageHeader { 1:type=DATA_PAGE_V2(3), 2:uncompressed=18,
        //   3:compressed=18, 8:DataPageHeaderV2{1:num_values=3,
        //   2:num_nulls=1, 3:num_rows=3, 4:encoding=PLAIN(0),
        //   5:def_levels_byte_length=2, 6:rep_levels_byte_length=0,
        //   7:is_compressed=false } }
        // compact field header = (delta<<4)|ctype; i32-zigzag=5,
        // struct=12, BOOL_FALSE=2. zz(n)=(n<<1)^(n>>63).
        let mut h = Vec::new();
        h.push(0x15); uv(&mut h, zz(3));   // f1 type=DATA_PAGE_V2(3) (delta 0->1=1,i32)
        h.push(0x15); uv(&mut h, zz(18));  // f2 uncompressed=18 (delta 1->2=1,i32)
        h.push(0x15); uv(&mut h, zz(18));  // f3 compressed=18 (delta 2->3=1,i32)
        h.push(0x5c);                      // f8 DataPageHeaderV2 struct (delta 3->8=5 -> (5<<4)|12=0x5c)
        h.push(0x15); uv(&mut h, zz(3));   // g1 num_values=3 (reset; delta 0->1=1,i32)
        h.push(0x15); uv(&mut h, zz(1));   // g2 num_nulls=1 (delta 1->2=1,i32)
        h.push(0x15); uv(&mut h, zz(3));   // g3 num_rows=3 (delta 2->3=1,i32)
        h.push(0x15); uv(&mut h, zz(0));   // g4 encoding=PLAIN(0) (delta 3->4=1,i32)
        h.push(0x15); uv(&mut h, zz(2));   // g5 def_levels_byte_length=2 (delta 4->5=1,i32)
        h.push(0x15); uv(&mut h, zz(0));   // g6 rep_levels_byte_length=0 (delta 5->6=1,i32)
        h.push(0x12);                      // g7 is_compressed=false (delta 6->7=1, BOOL_FALSE=2 -> 0x12)
        h.push(0x00);                      // stop DataPageHeaderV2
        h.push(0x00);                      // stop PageHeader

        let (ph, _len) = decode_page_header(&h).expect("decode");
        assert_eq!(ph.page_type, 3);
        assert_eq!(ph.v2_num_values, 3);
        assert_eq!(ph.v2_num_nulls, 1);
        assert_eq!(ph.v2_num_rows, 3);
        assert_eq!(ph.v2_encoding, 0);
        assert_eq!(ph.v2_def_len, 2);
        assert_eq!(ph.v2_rep_len, 0);
        assert_eq!(ph.v2_is_compressed, false);
    }
}
