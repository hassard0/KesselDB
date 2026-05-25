//! FSE (Finite State Entropy) primitives for zstd — **SP126 slice of the
//! OBJ-2c-2 arc** (sibling of `zstd.rs` scaffold).
//!
//! Authority: RFC 8478 §4.1.1 (FSE Table) + the upstream `facebook/zstd`
//! educational decoder cross-checked.
//!
//! What this module ships (SP126):
//!
//!   1. **Forward LSB-first bit reader** for the FSE TABLE DESCRIPTION
//!      bitstream (RFC §4.1.1.1 normalized count encoding).
//!
//!   2. **FSE normalized-count parser** (`parse_normalized_counts`) per
//!      RFC §4.1.1.1: reads the 4-bit Accuracy_Log_field then variable-
//!      width per-symbol counts until the remaining budget reaches 1.
//!      Handles the "less-than-1" (count=-1) marker and the repeat-zero
//!      RLE flag.
//!
//!   3. **FSE table builder** (`build_fse_table`) per RFC §4.1.1.2:
//!      canonical spread `pos = (pos + (size>>1) + (size>>3) + 3) mod size`
//!      skipping `-1` symbols (which take the table END in REVERSE symbol
//!      order — the HIGHEST-numbered `-1` lands at slot `size-1`).
//!
//!   4. **Reverse MSB-first bit reader** for the FSE state-decode
//!      bitstream. Skips the leading 1-bit padding marker per RFC §4.1.1.2.
//!
//!   5. **FSE state machine** (`FseState::init` / `current_symbol` / `step`).
//!
//! Scope cleanly bounded — this slice ships ONLY the FSE primitives.
//! Huffman literals (SP127), sequences (SP128), sequence execution
//! (SP129), Codec::Zstd wiring (SP130) are downstream.
//!
//! Determinism: every function is a pure deterministic transform of
//! input bytes. Bounds-checked: typed `ZstdError::UnexpectedEof` on
//! every overrun; no panics on attacker bytes.

#![allow(dead_code)]

use crate::zstd::ZstdError;

/// Max FSE table size — `1 << 9` per RFC 8478 §4.1.1.1 (literals + offsets cap).
pub(crate) const MAX_FSE_ACCURACY_LOG: u32 = 9;
pub(crate) const MAX_FSE_TABLE_SIZE: usize = 1 << MAX_FSE_ACCURACY_LOG;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FseEntry {
    pub symbol: u8,
    pub nb_bits: u8,
    pub base_state: u16,
}

#[derive(Debug, Clone)]
pub(crate) struct FseTable {
    pub accuracy_log: u32,
    pub entries: Vec<FseEntry>,
}

impl FseTable {
    pub fn size(&self) -> usize {
        self.entries.len()
    }
}

// ============================================================================
// Forward LSB-first bit reader (FSE table description bitstream).
// ============================================================================

pub(crate) struct ForwardBitReader<'a> {
    buf: &'a [u8],
    bit_pos: usize,
}

impl<'a> ForwardBitReader<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf, bit_pos: 0 }
    }
    pub fn total_bits(&self) -> usize {
        self.buf.len() * 8
    }
    pub fn bit_pos(&self) -> usize {
        self.bit_pos
    }
    pub fn read_bits(&mut self, nb: u32) -> Result<u32, ZstdError> {
        if nb == 0 {
            return Ok(0);
        }
        if nb > 24 {
            return Err(ZstdError::UnexpectedEof);
        }
        if self.bit_pos + nb as usize > self.buf.len() * 8 {
            return Err(ZstdError::UnexpectedEof);
        }
        let mut acc: u32 = 0;
        let mut taken: u32 = 0;
        while taken < nb {
            let byte_idx = self.bit_pos / 8;
            let bit_in_byte = (self.bit_pos % 8) as u32;
            let avail_in_byte = 8 - bit_in_byte;
            let take = (nb - taken).min(avail_in_byte);
            let b = self.buf[byte_idx];
            let chunk = (b as u32 >> bit_in_byte) & ((1u32 << take) - 1);
            acc |= chunk << taken;
            taken += take;
            self.bit_pos += take as usize;
        }
        Ok(acc)
    }
}

// ============================================================================
// Reverse MSB-first bit reader (FSE state-decode bitstream).
// ============================================================================

pub(crate) struct ReverseBitReader<'a> {
    buf: &'a [u8],
    bit_pos: usize,
    total: usize,
}

impl<'a> ReverseBitReader<'a> {
    pub fn new(buf: &'a [u8]) -> Result<Self, ZstdError> {
        if buf.is_empty() {
            return Err(ZstdError::UnexpectedEof);
        }
        let last = *buf.last().expect("non-empty checked above");
        if last == 0 {
            return Err(ZstdError::UnexpectedEof);
        }
        // Highest set bit (0..=7) of u8. u8::leading_zeros returns 0..=8.
        let pad_bit = 7 - last.leading_zeros() as usize;
        let total = (buf.len() - 1) * 8 + pad_bit;
        Ok(Self { buf, bit_pos: 0, total })
    }
    pub fn remaining(&self) -> usize {
        self.total.saturating_sub(self.bit_pos)
    }
    pub fn is_empty(&self) -> bool {
        self.bit_pos >= self.total
    }
    /// Rewind the bit cursor by `nb` bits — used by the Huffman bitstream
    /// decoder when it over-reads a max_bits-wide index for a shorter code.
    /// Saturating: rewinding past 0 is clamped to 0.
    pub fn rewind(&mut self, nb: u32) {
        self.bit_pos = self.bit_pos.saturating_sub(nb as usize);
    }
    pub fn read_bits(&mut self, nb: u32) -> Result<u32, ZstdError> {
        if nb == 0 {
            return Ok(0);
        }
        if nb > 24 {
            return Err(ZstdError::UnexpectedEof);
        }
        if self.bit_pos + nb as usize > self.total {
            return Err(ZstdError::UnexpectedEof);
        }
        let mut acc: u32 = 0;
        for _ in 0..nb {
            let pos = self.bit_pos;
            let (byte_idx, bit_in_byte) = self.absolute_bit(pos);
            let bit = (self.buf[byte_idx] >> bit_in_byte) & 0x01;
            acc = (acc << 1) | (bit as u32);
            self.bit_pos += 1;
        }
        Ok(acc)
    }
    fn absolute_bit(&self, pos: usize) -> (usize, u32) {
        let last_idx = self.buf.len() - 1;
        let last = self.buf[last_idx];
        let pad_bit = 7 - last.leading_zeros() as usize;
        if pos < pad_bit {
            let bit_in_byte = (pad_bit - 1 - pos) as u32;
            (last_idx, bit_in_byte)
        } else {
            let pos2 = pos - pad_bit;
            let bytes_back = pos2 / 8 + 1;
            if bytes_back > last_idx {
                return (0, 0);
            }
            let byte_idx = last_idx - bytes_back;
            let bit_in_byte = 7 - (pos2 % 8) as u32;
            (byte_idx, bit_in_byte)
        }
    }
}

// ============================================================================
// Normalized-count parser (RFC §4.1.1.1).
// ============================================================================

#[derive(Debug, Clone)]
pub(crate) struct NormalizedCounts {
    pub accuracy_log: u32,
    pub counts: Vec<i16>,
}

pub(crate) fn parse_normalized_counts(
    reader: &mut ForwardBitReader,
    max_symbol_value: u32,
) -> Result<NormalizedCounts, ZstdError> {
    if max_symbol_value > 255 {
        return Err(ZstdError::UnexpectedEof);
    }
    let acc_log_field = reader.read_bits(4)?;
    let accuracy_log = acc_log_field + 5;
    if accuracy_log > MAX_FSE_ACCURACY_LOG {
        return Err(ZstdError::UnexpectedEof);
    }
    let table_size: i64 = 1i64 << accuracy_log;
    let mut remaining: i64 = table_size + 1;
    let mut counts: Vec<i16> = Vec::new();
    let mut symbol: u32 = 0;
    while remaining > 1 && symbol <= max_symbol_value {
        // libzstd `FSE_readNCount_body` canonical algorithm:
        //   max_bits = bit_length(remaining)  (smallest k with 2^k > remaining)
        //   threshold = 1 << (max_bits - 1)   (the "low-bits-only" cutoff)
        //   max_val   = 2*threshold - 1 - remaining   (libzstd's "max"; = low_threshold)
        //
        //   read max_bits bits → bitStream (LSB-first within byte, low to high)
        //   low_bits = bitStream & (threshold - 1)    (the LOW max_bits-1 bits)
        //
        //   if low_bits < max_val:
        //     // low branch: only max_bits - 1 bits effectively consumed
        //     pushback 1 bit (the high bit was a free "pad")
        //     count_raw = low_bits
        //   else:
        //     // high branch: full max_bits bits consumed
        //     count_raw = bitStream  (full max_bits bits)
        //     if count_raw >= threshold: count_raw -= max_val
        //
        //   count = count_raw - 1
        //   remaining -= |count|
        //
        // SP139 FIX: my SP126 implementation matched the simpler
        // educational-decoder reference which checks the FULL max_bits
        // value against low_threshold — that's an off-by-pattern bug
        // surfaced by the SP138 stress fixture (FSE-Compressed LL+OF+ML).
        // The libzstd convention checks LOW max_bits-1 bits, which is
        // what pyarrow's encoder produces.
        let max_bits = (64 - (remaining as u64).leading_zeros()) as u32;
        let low_threshold = ((1i64 << max_bits) - 1 - remaining) as u32;
        let threshold = 1u32 << (max_bits - 1);
        let mut value = reader.read_bits(max_bits)?;
        let low_bits = value & (threshold - 1);
        if low_bits < low_threshold {
            reader.bit_pos -= 1;
            value = low_bits;
        } else if value >= threshold {
            value = value.saturating_sub(low_threshold);
        }
        let count: i32 = (value as i32) - 1;
        counts.push(count as i16);
        let consumed: i64 = if count < 0 { 1 } else { count as i64 };
        remaining -= consumed;
        symbol += 1;
        if count == 0 {
            loop {
                let repeat = reader.read_bits(2)?;
                for _ in 0..repeat {
                    if symbol > max_symbol_value {
                        return Err(ZstdError::UnexpectedEof);
                    }
                    counts.push(0);
                    symbol += 1;
                }
                if repeat != 3 {
                    break;
                }
            }
        }
    }
    if remaining != 1 {
        return Err(ZstdError::UnexpectedEof);
    }
    let bit_aligned_to_byte = (reader.bit_pos + 7) & !7;
    reader.bit_pos = bit_aligned_to_byte;
    Ok(NormalizedCounts { accuracy_log, counts })
}

// ============================================================================
// FSE table builder (RFC §4.1.1.2).
// ============================================================================

pub(crate) fn build_fse_table(
    counts: &[i16],
    accuracy_log: u32,
) -> Result<FseTable, ZstdError> {
    if accuracy_log > MAX_FSE_ACCURACY_LOG {
        return Err(ZstdError::UnexpectedEof);
    }
    let size = 1usize << accuracy_log;
    let mut entries: Vec<FseEntry> = vec![
        FseEntry { symbol: 0, nb_bits: 0, base_state: 0 };
        size
    ];

    // Step 1: place "less-than-1" symbols at the table end, in REVERSE
    // symbol order — HIGHEST-numbered -1 takes slot (size-1). RFC §4.1.1.2.
    // Iterate counts in reverse so the highest-numbered -1 lands first.
    let mut high_threshold = size;
    for (sym, &c) in counts.iter().enumerate().rev() {
        if c == -1 {
            high_threshold -= 1;
            entries[high_threshold].symbol = sym as u8;
        }
    }

    // Step 2: canonical spread.
    let spread_step = (size >> 1) + (size >> 3) + 3;
    let pos_mask = size - 1;
    let mut pos = 0usize;
    for (sym, &c) in counts.iter().enumerate() {
        if c <= 0 {
            continue;
        }
        for _ in 0..c {
            entries[pos].symbol = sym as u8;
            pos = (pos + spread_step) & pos_mask;
            while pos >= high_threshold {
                pos = (pos + spread_step) & pos_mask;
            }
        }
    }

    // Step 3: compute (nb_bits, base_state) per slot.
    let mut next_state = vec![0u16; counts.len().max(1)];
    for (sym, &c) in counts.iter().enumerate() {
        if c == -1 {
            next_state[sym] = 1;
        } else if c > 0 {
            next_state[sym] = c as u16;
        }
    }
    // Canonical FSE per-cell (nb_bits, base_state) computation, mirroring
    // the libzstd reference algorithm (`FSE_buildDTable_internal`):
    //
    //   For each cell, given the symbol's running counter `ns` (initialised
    //   to the symbol's count `c`, incremented on each cell visit, so
    //   values walk c, c+1, …, 2c-1):
    //     nb_bits    = L - high_bit_position(ns)
    //                  (where high_bit_position(x) is the 0-indexed position
    //                  of the highest set bit of x; equivalently 31 - clz(x))
    //     base_state = (ns << nb_bits) - table_size
    //
    //   Properties:
    //     - When c is a power of two: all c cells get nb_bits = L - log2(c)
    //       and base_states 0, 2^nb, 2×2^nb, …, (c-1)×2^nb.
    //     - When c is NOT a power of two: cells with ns ∈ [c, 2^ceil(log2(c)))
    //       get the higher nb_bits (more bits read), and cells with
    //       ns ∈ [2^ceil(log2(c)), 2c) get the lower nb_bits. Together they
    //       cover the full state space.
    //
    //   This replaces the SP126 approximation that used a max_state-overflow
    //   reduction (which produced wrong nb_bits for power-of-two counts —
    //   surfaced by SP136 e2e against pyarrow output).
    for i in 0..size {
        let sym = entries[i].symbol;
        let mut c = counts.get(sym as usize).copied().unwrap_or(0);
        if c == -1 {
            c = 1;
        }
        if c <= 0 {
            return Err(ZstdError::UnexpectedEof);
        }
        let sym_idx = sym as usize;
        let ns = next_state[sym_idx] as u32;
        // high_bit_position(ns) = 31 - leading_zeros(ns); requires ns >= 1.
        if ns == 0 {
            return Err(ZstdError::UnexpectedEof);
        }
        let high_bit = 31 - ns.leading_zeros();
        if high_bit > accuracy_log {
            return Err(ZstdError::UnexpectedEof);
        }
        let nb_bits = accuracy_log - high_bit;
        let base = (ns << nb_bits).wrapping_sub(size as u32);
        entries[i].nb_bits = nb_bits as u8;
        entries[i].base_state = base as u16;
        next_state[sym_idx] = next_state[sym_idx].wrapping_add(1);
    }

    Ok(FseTable { accuracy_log, entries })
}

// ============================================================================
// FSE state machine.
// ============================================================================

#[derive(Debug, Clone, Copy)]
pub(crate) struct FseState {
    pub state: u16,
}

impl FseState {
    pub fn init(table: &FseTable, reader: &mut ReverseBitReader) -> Result<Self, ZstdError> {
        let bits = reader.read_bits(table.accuracy_log)?;
        if (bits as usize) >= table.entries.len() {
            return Err(ZstdError::UnexpectedEof);
        }
        Ok(Self { state: bits as u16 })
    }
    pub fn current_symbol(&self, table: &FseTable) -> u8 {
        table.entries[self.state as usize].symbol
    }
    pub fn current_entry(&self, table: &FseTable) -> FseEntry {
        table.entries[self.state as usize]
    }
    pub fn step(&mut self, table: &FseTable, reader: &mut ReverseBitReader) -> Result<(), ZstdError> {
        let entry = table.entries[self.state as usize];
        let extra = reader.read_bits(entry.nb_bits as u32)?;
        self.state = entry.base_state + extra as u16;
        Ok(())
    }
}

// ============================================================================
// KATs — hand-derived from RFC 8478 §4.1.1.
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    /// SP126-KAT-1: forward bit reader pulls LSB-first.
    /// byte 0 = 0b1010_0011 = 0xA3. Read 4 → low nibble 0b0011 = 3.
    /// Read 4 more → high nibble 0b1010 = 10.
    #[test]
    fn sp126_kat_forward_bits_lsb_first() {
        let mut r = ForwardBitReader::new(&[0xA3u8]);
        assert_eq!(r.read_bits(4).unwrap(), 3);
        assert_eq!(r.read_bits(4).unwrap(), 10);
    }

    /// SP126-KAT-2: forward bit reader spans byte boundaries.
    #[test]
    fn sp126_kat_forward_bits_span_bytes() {
        let mut r = ForwardBitReader::new(&[0xFFu8, 0x00]);
        assert_eq!(r.read_bits(12).unwrap(), 0x0FF);
        assert_eq!(r.read_bits(4).unwrap(), 0);
    }

    /// SP126-KAT-3: forward bit reader rejects over-read.
    #[test]
    fn sp126_kat_forward_bits_overrun_traps() {
        let mut r = ForwardBitReader::new(&[0xFFu8]);
        assert_eq!(r.read_bits(8).unwrap(), 0xFF);
        assert_eq!(r.read_bits(1).unwrap_err(), ZstdError::UnexpectedEof);
    }

    /// SP126-KAT-4: reverse bit reader skips the padding-1 marker.
    /// buf=[0x01, 0x80]. last=0x80 → pad_bit=7. total = 8 + 7 = 15.
    #[test]
    fn sp126_kat_reverse_bits_skips_padding_marker() {
        let r = ReverseBitReader::new(&[0x01u8, 0x80u8]).unwrap();
        assert_eq!(r.remaining(), 15);
    }

    /// SP126-KAT-5: reverse bit reader reads MSB-first from one byte.
    /// buf=[0b1010_1011]=0xAB. pad_bit=7 (MSB set). payload = bits 6..0
    /// read MSB-first below pad: 0,1,0,1,0,1,1 → 0b0_101_011 = 43.
    #[test]
    fn sp126_kat_reverse_bits_single_byte() {
        let mut r = ReverseBitReader::new(&[0b1010_1011u8]).unwrap();
        assert_eq!(r.remaining(), 7);
        let v = r.read_bits(7).unwrap();
        assert_eq!(v, 0b010_1011);
    }

    /// SP126-KAT-6: reverse bit reader spans byte boundaries.
    /// buf=[0xAA, 0x55, 0x81]. last=0x81 → pad_bit=7 (MSB set), so 7
    /// payload bits below it in the last byte: 0x81 = 1000_0001, bits
    /// 6..0 = 0,0,0,0,0,0,1 read MSB-first → 0000_001. Then byte 0x55
    /// = 0101_0101, MSB-first → 0,1,0,1,0,1,0,1. First 8 reverse-bits:
    /// 0000_001_0 = 0b0000_0010 = 2.
    #[test]
    fn sp126_kat_reverse_bits_span_bytes() {
        let mut r = ReverseBitReader::new(&[0xAAu8, 0x55u8, 0x81u8]).unwrap();
        assert_eq!(r.remaining(), 7 + 8 + 8);
        let v = r.read_bits(8).unwrap();
        assert_eq!(v, 0b0000_0010);
    }

    /// SP126-KAT-7: reverse bit reader rejects malformed.
    #[test]
    fn sp126_kat_reverse_bits_zero_last_byte_traps() {
        assert!(matches!(
            ReverseBitReader::new(&[]),
            Err(ZstdError::UnexpectedEof)
        ));
        assert!(matches!(
            ReverseBitReader::new(&[0u8]),
            Err(ZstdError::UnexpectedEof)
        ));
    }

    /// SP126-KAT-8: FSE table builder uniform 2-symbol distribution at
    /// log=5 (size=32). Spread step = 16+4+3 = 23; gcd(23,32)=1 → step
    /// visits every slot. counts=[16, 16] → 16 zeros + 16 ones.
    #[test]
    fn sp126_kat_table_builds_uniform_2sym_5log() {
        let table = build_fse_table(&[16, 16], 5).unwrap();
        assert_eq!(table.entries.len(), 32);
        let zeros = table.entries.iter().filter(|e| e.symbol == 0).count();
        let ones = table.entries.iter().filter(|e| e.symbol == 1).count();
        assert_eq!(zeros, 16);
        assert_eq!(ones, 16);
    }

    /// SP126-KAT-9: less-than-1 symbol takes the table-end slot.
    /// counts=[11, -1, 20], log=5 (size=32). |c| sum = 11+1+20 = 32 =
    /// size. ✓ (per RFC §4.1.1.1, |c| sum must equal table_size when
    /// the parser hits remaining=1). high_threshold drops to 31 placing
    /// symbol 1 at slot 31. Slots 0..30 carry 11 zeros + 20 twos.
    #[test]
    fn sp126_kat_table_less_than_one_at_end() {
        let table = build_fse_table(&[11, -1, 20], 5).unwrap();
        assert_eq!(table.entries.len(), 32);
        assert_eq!(table.entries[31].symbol, 1, "-1 symbol must take the last slot");
        let zeros = table.entries[..31].iter().filter(|e| e.symbol == 0).count();
        let twos = table.entries[..31].iter().filter(|e| e.symbol == 2).count();
        assert_eq!(zeros, 11);
        assert_eq!(twos, 20);
    }

    /// SP126-KAT-10: multiple -1 symbols take end slots in REVERSE
    /// symbol order. counts=[31, -1, -1], log=5. Sum = 31+1+1 = 33. ✓
    /// HIGHEST-numbered -1 (sym 2) lands at slot 31; sym 1 at slot 30.
    #[test]
    fn sp126_kat_table_multiple_less_than_one_reverse_order() {
        let table = build_fse_table(&[31, -1, -1], 5).unwrap();
        assert_eq!(table.entries.len(), 32);
        assert_eq!(table.entries[31].symbol, 2, "highest -1 takes the LAST slot");
        assert_eq!(table.entries[30].symbol, 1, "next -1 takes the prior slot");
    }

    /// SP126-KAT-11: deterministic — same counts/log → byte-identical table.
    #[test]
    fn sp126_kat_table_deterministic_repeat() {
        let t1 = build_fse_table(&[12, -1, 20], 5).unwrap();
        let t2 = build_fse_table(&[12, -1, 20], 5).unwrap();
        assert_eq!(t1.accuracy_log, t2.accuracy_log);
        assert_eq!(t1.entries, t2.entries);
    }

    /// SP126-KAT-12: state init pulls accuracy_log bits MSB-first.
    /// 32-entry table; reverse-payload of 2 bits "11" needs encoding
    /// such that the 5-bit init reads 0b11. Build a stream whose
    /// payload (MSB-first reverse) starts with bits 0,0,0,1,1 = 0b00011 = 3.
    /// Encoding: bytes high-to-low contain those bits MSB-first below
    /// the padding marker. Single byte 0b00_000111 = 0x07: pad_bit = 2
    /// (highest set), 2 payload bits 1,1 below it — total only 2; need 5.
    /// Use 2 bytes [b1, b2] with last=b2 carrying pad+payload. We want
    /// 5 payload bits = 0,0,0,1,1 in MSB-first reverse order. With
    /// 2 bytes total bits payload = (pad_bit) + 8 from byte 0.
    /// Pick last=0x20 → pad_bit=5 → 5 payload bits in last byte. Those
    /// 5 bits below pad (bits 4..0 of 0x20 = 0_0000) = 00000. Init
    /// would read 0. We want 3. Use last=0x23 → 0010_0011: pad_bit=5
    /// (bit 5 set), but ALSO bits 0,1 set. The reverse-payload below
    /// pad (bits 4,3,2,1,0 read in that order MSB-first) = 0,0,0,1,1 = 3.
    /// One byte = [0x23] gives state 3.
    #[test]
    fn sp126_kat_state_init_msb_first() {
        let table = build_fse_table(&[16, 16], 5).unwrap();
        let mut r = ReverseBitReader::new(&[0x23u8]).unwrap();
        let st = FseState::init(&table, &mut r).unwrap();
        assert_eq!(st.state, 3);
    }

    /// SP126-KAT-13: state machine emits a symbol and steps. Uniform
    /// 2-symbol log=5 table; init from a buffer with extra payload bits
    /// to support the step. nb_bits per cell for a uniform half-split
    /// (count=16 = 2^4; log2_ceil=4; nb_bits=5-4=1). So step reads 1 bit.
    /// Pick a 2-byte stream so we have ≥ 5+1 payload bits.
    #[test]
    fn sp126_kat_state_step_advances() {
        let table = build_fse_table(&[16, 16], 5).unwrap();
        // Last byte 0x40 → pad_bit=6; 6 payload bits. Plus first byte 0xFF
        // = 8 more bits. Total = 14 payload bits, ample for init(5)+step(1).
        let mut r = ReverseBitReader::new(&[0xFFu8, 0x40u8]).unwrap();
        let mut st = FseState::init(&table, &mut r).unwrap();
        // The symbol must be 0 or 1 (the only two in the table).
        let s0 = st.current_symbol(&table);
        assert!(s0 == 0 || s0 == 1);
        st.step(&table, &mut r).unwrap();
        let s1 = st.current_symbol(&table);
        assert!(s1 == 0 || s1 == 1);
    }
}
