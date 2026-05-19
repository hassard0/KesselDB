//! Parquet file footer framing. Spec: `PAR1` <data...>
//! <FileMetaData> [u32 LE metadata_len] `PAR1`.
#![allow(dead_code)]

use crate::PqError;

const MAGIC: &[u8; 4] = b"PAR1";
/// Hard cap on the Thrift FileMetaData size (defensive — a real
/// metadata blob for a tiny mapped subset is KBs, not MBs).
const MAX_META: usize = 16 * 1024 * 1024;

fn bad(s: &str) -> PqError {
    PqError::Bad(s.to_string())
}

/// Return the `FileMetaData` thrift byte slice (validated framing).
pub fn metadata_slice(file: &[u8]) -> Result<&[u8], PqError> {
    if file.len() < 12 {
        return Err(bad("file too short for a Parquet footer"));
    }
    if &file[..4] != MAGIC {
        return Err(bad("missing PAR1 header magic"));
    }
    let n = file.len();
    if &file[n - 4..] != MAGIC {
        return Err(bad("missing PAR1 trailer magic"));
    }
    let len_pos = n - 8;
    let mlen = u32::from_le_bytes(
        file[len_pos..len_pos + 4].try_into().unwrap(),
    ) as usize;
    if mlen > MAX_META {
        return Err(bad("metadata_len exceeds cap"));
    }
    let meta_start = len_pos
        .checked_sub(mlen)
        .ok_or_else(|| bad("metadata_len larger than file"))?;
    if meta_start < 4 {
        return Err(bad("metadata overlaps header magic"));
    }
    Ok(&file[meta_start..len_pos])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn footer_framing_spec_kat() {
        let mut f = Vec::new();
        f.extend_from_slice(b"PAR1");
        f.extend_from_slice(&[0xAA, 0xBB, 0xCC]);
        f.extend_from_slice(&3u32.to_le_bytes());
        f.extend_from_slice(b"PAR1");
        let meta = metadata_slice(&f).unwrap();
        assert_eq!(meta, &[0xAA, 0xBB, 0xCC]);
    }

    #[test]
    fn footer_rejects_bad_magic_and_lying_len() {
        assert!(metadata_slice(b"NOPE....PAR1").is_err());
        let mut f = Vec::new();
        f.extend_from_slice(b"PAR1");
        f.extend_from_slice(&[0x01]);
        f.extend_from_slice(&9_000_000u32.to_le_bytes());
        f.extend_from_slice(b"PAR1");
        assert!(matches!(metadata_slice(&f), Err(crate::PqError::Bad(_))));
        assert!(metadata_slice(b"PAR1").is_err());
        let mut g = Vec::new();
        g.extend_from_slice(b"PAR1");
        g.extend_from_slice(&[0x01]);
        g.extend_from_slice(&1u32.to_le_bytes());
        g.extend_from_slice(b"XXXX");
        assert!(metadata_slice(&g).is_err());
    }
}
