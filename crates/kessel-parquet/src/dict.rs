//! Parquet dictionary-index resolution. The data-page payload for a
//! flat REQUIRED dictionary column is `<1 byte bit_width>` then a
//! non-length-prefixed RLE/bit-packing hybrid stream of indices
//! (SP102 `rle::decode_hybrid`). Indices are resolved against the
//! PLAIN-decoded dictionary, every lookup bounds-checked. Zero deps,
//! pure, never panics/OOMs.
#![allow(dead_code)]

use crate::rle;
use crate::{PqError, PqValue};

fn bad(s: &str) -> PqError {
    PqError::Bad(s.to_string())
}

/// Resolve a dictionary-encoded data-page payload to values.
/// `payload` is the WHOLE data-page payload: `payload[0]` is the
/// bit width, `payload[1..]` is the non-length-prefixed hybrid
/// index stream. `dict` is the PLAIN-decoded dictionary; `n` is the
/// data page's num_values. Every index is bounds-checked against
/// `dict.len()` (OOB → Bad). Never panics / OOM-aborts.
pub fn resolve_dict_indices(
    payload: &[u8],
    dict: &[PqValue],
    n: usize,
) -> Result<Vec<PqValue>, PqError> {
    let bit_width = *payload
        .get(0)
        .ok_or_else(|| bad("dict data page empty (no bit-width byte)"))?;
    let stream = payload.get(1..).unwrap_or(&[]);
    let idxs = rle::decode_hybrid(stream, bit_width as u32, n)?;
    // OOM bound (plain.rs:35 stance): `n` is the page num_values,
    // upstream-capped; never reserve from a stream-derived count.
    let mut out: Vec<PqValue> = Vec::with_capacity(n);
    for raw in idxs {
        let i = usize::try_from(raw)
            .map_err(|_| bad("dict index range"))?;
        let v = dict
            .get(i)
            .ok_or_else(|| bad("dict index out of range"))?;
        out.push(v.clone());
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dict_abc() -> Vec<PqValue> {
        vec![
            PqValue::Bytes(b"a".to_vec()),
            PqValue::Bytes(b"b".to_vec()),
            PqValue::Bytes(b"c".to_vec()),
        ]
    }

    // KAT — bit_width=2, one bit-packed group of 8 values
    // [0,2,1,1,0,0,0,0] (padded; decoder truncates to n=4).
    // header = (1 group << 1)|1 = 0x03. LSB-of-stream-first 2-bit
    // packing: v0=0,v1=2,v2=1,v3=1 → byte0 = 0b0101_1000 = 0x58,
    // byte1 = 0x00. Payload = [bit_width=0x02, 0x03, 0x58, 0x00].
    #[test]
    fn kat_resolve_bitpacked_width2() {
        let payload = [0x02u8, 0x03, 0x58, 0x00];
        let got = resolve_dict_indices(&payload, &dict_abc(), 4)
            .expect("resolve");
        assert_eq!(
            got,
            vec![
                PqValue::Bytes(b"a".to_vec()),
                PqValue::Bytes(b"c".to_vec()),
                PqValue::Bytes(b"b".to_vec()),
                PqValue::Bytes(b"b".to_vec()),
            ]
        );
    }

    // KAT — bit_width=0: every index 0 → every value dict[0].
    // hybrid = RLE run_len=4 (header varint(4<<1)=0x08), bit_width=0
    // → NO value byte. Payload = [bit_width=0x00, 0x08].
    #[test]
    fn kat_resolve_bitwidth0_all_dict0() {
        let payload = [0x00u8, 0x08];
        let got = resolve_dict_indices(&payload, &dict_abc(), 4)
            .expect("resolve");
        assert_eq!(got, vec![PqValue::Bytes(b"a".to_vec()); 4]);
    }

    // OOB — bit_width=3 RLE run value=5 (header varint(4<<1)=0x08,
    // value 0x05) but dict has only 3 entries → Bad.
    #[test]
    fn resolve_oob_index_is_bad() {
        let payload = [0x03u8, 0x08, 0x05];
        assert!(matches!(
            resolve_dict_indices(&payload, &dict_abc(), 4),
            Err(PqError::Bad(_))
        ));
    }

    #[test]
    fn resolve_empty_payload_is_bad() {
        assert!(matches!(
            resolve_dict_indices(&[], &dict_abc(), 1),
            Err(PqError::Bad(_))
        ));
    }
}

// ── PENTEST PASS — adversarial lock tests ─────────────────────────
// Dictionary payloads/headers are operator-source-controlled. Each
// case: no panic / no OOM / no stack-overflow, and a well-formed
// Result (typed Bad/Unsupported, OR correct Ok for the valid
// bit_width==0 case which we must NOT over-reject).
#[cfg(test)]
mod pentest {
    use super::*;

    fn d3() -> Vec<PqValue> {
        vec![
            PqValue::Bytes(b"x".to_vec()),
            PqValue::Bytes(b"y".to_vec()),
            PqValue::Bytes(b"z".to_vec()),
        ]
    }

    fn no_panic_bad(payload: &[u8], dict: Vec<PqValue>, n: usize) {
        let p = payload.to_vec();
        let r = std::panic::catch_unwind(move || {
            resolve_dict_indices(&p, &dict, n)
        });
        assert!(r.is_ok(), "must NOT panic/OOM-unwind");
        assert!(
            matches!(
                r.unwrap(),
                Err(PqError::Bad(_)) | Err(PqError::Unsupported(_))
            ),
            "hostile input must be a typed error"
        );
    }

    #[test]
    fn empty_payload_bad() {
        no_panic_bad(&[], d3(), 4);
    }

    #[test]
    fn oob_index_bad() {
        no_panic_bad(&[0x03, 0x08, 0x09], d3(), 4);
    }

    #[test]
    fn huge_bit_width_byte_bad() {
        no_panic_bad(&[200, 0x08, 0x01], d3(), 4);
    }

    #[test]
    fn truncated_index_stream_bad() {
        no_panic_bad(&[0x08, 0x03], d3(), 8);
    }

    #[test]
    fn lying_n_vs_short_stream_bad() {
        no_panic_bad(&[0x01, 0x03, 0x00], d3(), usize::from(u16::MAX));
    }

    #[test]
    fn bitwidth0_multi_entry_dict_decodes_to_dict0() {
        // VALID Parquet: bit_width=0 → all indices 0 → every row is
        // dict[0], even with a 3-entry dict. MUST decode (Ok), NOT
        // reject — proves no over-rejection of valid input.
        let payload = [0x00u8, 0x08];
        let got = resolve_dict_indices(&payload, &d3(), 4)
            .expect("bit_width=0 is valid");
        assert_eq!(got, vec![PqValue::Bytes(b"x".to_vec()); 4]);
    }
}
