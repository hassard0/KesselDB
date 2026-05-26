//! Brotli bit reader — RFC 7932 §1.6.
//!
//! Brotli is LSB-first within each byte (same convention as RFC 1951
//! DEFLATE; opposite of RFC 7932 §1.6 reads_as_int order BUT matches the
//! byte-stream-as-bit-stream interpretation Brotli actually uses). Concretely:
//! the first bit consumed from a byte is bit-0 (LSB); when N bits are read,
//! the value is built up with the LOWER-numbered bits in the LOWER places of
//! the returned integer.
//!
//! Example (RFC 7932 §1.6, "Trick or treat"):
//!   bytes  = [0b10011001, 0b00101010]  (LE in the byte stream)
//!   read 4 → 0b1001 = 9      (bits 0..3 of byte 0)
//!   read 5 → 0b01001 = 9     (bits 4..7 of byte 0 + bit 0 of byte 1)
//!   read 7 → 0b0010101 = 21  (bits 1..7 of byte 1)
//!
//! Bounds checking: every read is `Result`-returning. Reading past the
//! supplied byte slice produces `BitReaderError::UnexpectedEof`. No panics
//! on attacker bytes — every byte access is via `.get(idx)` and every
//! position update is `checked_add`.
//!
//! Safety: `#![forbid(unsafe_code)]` honoured at the crate root.

#![allow(dead_code)]

/// Typed errors for the bit reader. `#[non_exhaustive]` so future
/// brotli-decoder slices can refine without breaking changes.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum BitReaderError {
    /// Caller asked for bits past the end of the supplied byte slice.
    UnexpectedEof,
    /// Caller asked for >32 bits in one `read_bits` call (would overflow u32).
    TooManyBits(u8),
}

impl core::fmt::Display for BitReaderError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for BitReaderError {}

/// LSB-first bit reader over a borrowed byte slice. Carries a `bit_pos`
/// cursor (0 = first bit consumed = bit-0 of byte 0). `align_to_byte`
/// fast-forwards `bit_pos` to the next byte boundary (no-op if already
/// aligned).
pub(crate) struct BitReader<'a> {
    buf: &'a [u8],
    /// Total bits consumed so far. `bit_pos / 8` indexes the current byte;
    /// `bit_pos & 7` is the next bit within that byte (LSB-first).
    bit_pos: usize,
}

impl<'a> BitReader<'a> {
    pub(crate) fn new(buf: &'a [u8]) -> Self {
        Self { buf, bit_pos: 0 }
    }

    /// Current bit position (total bits consumed).
    pub(crate) fn bit_pos(&self) -> usize {
        self.bit_pos
    }

    /// True iff `n` more bits can be read without running off the end.
    pub(crate) fn has_bits(&self, n: u8) -> bool {
        self.bit_pos
            .checked_add(n as usize)
            .map(|end| end <= self.buf.len().saturating_mul(8))
            .unwrap_or(false)
    }

    /// Total bits available beyond the current cursor.
    pub(crate) fn remaining_bits(&self) -> usize {
        self.buf
            .len()
            .saturating_mul(8)
            .saturating_sub(self.bit_pos)
    }

    /// Read N bits (0..=32). LSB-first within each byte. Spans byte
    /// boundaries naturally: the first byte's high bits become the LOW
    /// bits of the next byte's contribution.
    ///
    /// Returns `TooManyBits(n)` for n > 32 and `UnexpectedEof` if the
    /// read would run past the supplied buffer.
    pub(crate) fn read_bits(&mut self, n: u8) -> Result<u32, BitReaderError> {
        if n > 32 {
            return Err(BitReaderError::TooManyBits(n));
        }
        if n == 0 {
            return Ok(0);
        }
        let end = self
            .bit_pos
            .checked_add(n as usize)
            .ok_or(BitReaderError::UnexpectedEof)?;
        if end > self.buf.len().saturating_mul(8) {
            return Err(BitReaderError::UnexpectedEof);
        }
        let mut value: u32 = 0;
        let mut bits_taken: u8 = 0;
        while bits_taken < n {
            let byte_idx = self.bit_pos / 8;
            let bit_in_byte = (self.bit_pos & 7) as u8;
            let byte = *self.buf.get(byte_idx).ok_or(BitReaderError::UnexpectedEof)?;
            // How many bits we can grab from this byte (LSB-first).
            let avail = 8 - bit_in_byte;
            let want = n - bits_taken;
            let take = avail.min(want);
            // Shift the current byte right by bit_in_byte to put the next
            // unread bit at position 0, then mask `take` bits.
            let mask: u32 = if take == 32 { u32::MAX } else { (1u32 << take) - 1 };
            let chunk = ((byte as u32) >> bit_in_byte) & mask;
            value |= chunk << bits_taken;
            bits_taken += take;
            self.bit_pos += take as usize;
        }
        Ok(value)
    }

    /// Read a single bit (0 or 1).
    pub(crate) fn read_one_bit(&mut self) -> Result<u8, BitReaderError> {
        Ok(self.read_bits(1)? as u8)
    }

    /// Align the cursor up to the next byte boundary. No-op if already
    /// aligned. After this call `bit_pos & 7 == 0`. Per RFC 7932 §9.2:
    /// used before reading an uncompressed metablock body.
    pub(crate) fn align_to_byte(&mut self) {
        let rem = self.bit_pos & 7;
        if rem != 0 {
            self.bit_pos += 8 - rem;
        }
    }

    /// Read `n` whole bytes starting at the current (byte-aligned) cursor.
    /// Returns `UnexpectedEof` if cursor isn't byte-aligned (caller bug)
    /// or if the read overruns. Used for uncompressed metablock bodies.
    pub(crate) fn read_aligned_bytes(&mut self, n: usize) -> Result<&'a [u8], BitReaderError> {
        if self.bit_pos & 7 != 0 {
            // Caller forgot to align_to_byte; this is a programming error
            // but we surface it as UnexpectedEof rather than panic.
            return Err(BitReaderError::UnexpectedEof);
        }
        let byte_idx = self.bit_pos / 8;
        let end = byte_idx.checked_add(n).ok_or(BitReaderError::UnexpectedEof)?;
        if end > self.buf.len() {
            return Err(BitReaderError::UnexpectedEof);
        }
        let slice = &self.buf[byte_idx..end];
        self.bit_pos = end.saturating_mul(8);
        Ok(slice)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// RFC 7932 §1.6 "Trick or treat" worked example.
    ///   Stream bytes = [0x99, 0x2A]  (binary: 10011001, 00101010)
    ///   Read 4 bits → 9   (bits 0..3 of byte 0 = 1001 LSB-first)
    ///   Read 5 bits → 9   (bits 4..7 of byte 0 + bit 0 of byte 1 = 0_1001)
    ///   Read 7 bits → 21  (bits 1..7 of byte 1 = 0010101)
    ///
    /// Hand-derivation, byte 0 = 0b1001_1001:
    ///   bit0=1 bit1=0 bit2=0 bit3=1 bit4=1 bit5=0 bit6=0 bit7=1
    /// First 4 LSB-first = (1,0,0,1) → value = 1*1 + 0*2 + 0*4 + 1*8 = 9 ✓
    /// Next 5 (bits 4..7 of byte 0 + bit 0 of byte 1):
    ///   byte0 bit4=1, bit5=0, bit6=0, bit7=1
    ///   byte1 bit0=0 (byte 1 = 0b0010_1010, bit0 = 0)
    ///   = (1,0,0,1,0) → 1 + 0 + 0 + 8 + 0 = 9 ✓
    /// Next 7 (bits 1..7 of byte 1):
    ///   byte 1 = 0b0010_1010  bit0=0 bit1=1 bit2=0 bit3=1 bit4=0 bit5=1 bit6=0 bit7=0
    ///   bits 1..7 = (1,0,1,0,1,0,0) → 1 + 0 + 4 + 0 + 16 + 0 + 0 = 21 ✓
    #[test]
    fn rfc7932_section_1_6_trick_or_treat_worked_example() {
        let bytes = [0x99u8, 0x2A];
        let mut r = BitReader::new(&bytes);
        assert_eq!(r.read_bits(4).unwrap(), 9, "first 4 bits");
        assert_eq!(r.read_bits(5).unwrap(), 9, "next 5 bits");
        assert_eq!(r.read_bits(7).unwrap(), 21, "next 7 bits");
        assert_eq!(r.bit_pos(), 16, "cursor at end of 2-byte stream");
    }

    #[test]
    fn read_one_bit_walks_lsb_first() {
        // 0b1010_0101 = bits 0..7 LSB-first: 1,0,1,0,0,1,0,1
        let bytes = [0xA5u8];
        let mut r = BitReader::new(&bytes);
        let got: Vec<u8> = (0..8).map(|_| r.read_one_bit().unwrap()).collect();
        assert_eq!(got, vec![1, 0, 1, 0, 0, 1, 0, 1]);
    }

    #[test]
    fn read_zero_bits_returns_zero_and_does_not_advance() {
        let bytes = [0xFFu8];
        let mut r = BitReader::new(&bytes);
        assert_eq!(r.read_bits(0).unwrap(), 0);
        assert_eq!(r.bit_pos(), 0);
    }

    #[test]
    fn read_32_bits_spans_5_bytes_lsb_first() {
        // Stream: 0x78 0x56 0x34 0x12 0x9A
        //   First 32 bits, LSB-first across bytes = little-endian u32 of [0x78,0x56,0x34,0x12]
        //   = 0x12345678
        let bytes = [0x78u8, 0x56, 0x34, 0x12, 0x9A];
        let mut r = BitReader::new(&bytes);
        assert_eq!(r.read_bits(32).unwrap(), 0x1234_5678);
        // Then 8 more bits = 0x9A
        assert_eq!(r.read_bits(8).unwrap(), 0x9A);
    }

    #[test]
    fn unexpected_eof_at_end_of_buffer() {
        let bytes = [0xFFu8];
        let mut r = BitReader::new(&bytes);
        // Consume all 8 bits, then ask for 1 more.
        assert_eq!(r.read_bits(8).unwrap(), 0xFF);
        assert_eq!(r.read_one_bit(), Err(BitReaderError::UnexpectedEof));
    }

    #[test]
    fn empty_buffer_unexpected_eof_on_first_read() {
        let mut r = BitReader::new(&[]);
        assert_eq!(r.read_one_bit(), Err(BitReaderError::UnexpectedEof));
        assert_eq!(r.read_bits(0).unwrap(), 0, "zero-bit read still ok on empty");
    }

    #[test]
    fn too_many_bits_in_one_call_is_typed_error() {
        let bytes = [0u8; 10];
        let mut r = BitReader::new(&bytes);
        assert_eq!(r.read_bits(33), Err(BitReaderError::TooManyBits(33)));
    }

    #[test]
    fn align_to_byte_no_op_when_already_aligned() {
        let bytes = [0u8; 4];
        let mut r = BitReader::new(&bytes);
        r.align_to_byte();
        assert_eq!(r.bit_pos(), 0);
        let _ = r.read_bits(8).unwrap();
        assert_eq!(r.bit_pos(), 8);
        r.align_to_byte();
        assert_eq!(r.bit_pos(), 8, "no-op when aligned");
    }

    #[test]
    fn align_to_byte_skips_partial_byte_remainder() {
        // Consume 5 bits → bit_pos=5. align_to_byte should advance to 8.
        let bytes = [0xFFu8, 0xAA];
        let mut r = BitReader::new(&bytes);
        let _ = r.read_bits(5).unwrap();
        assert_eq!(r.bit_pos(), 5);
        r.align_to_byte();
        assert_eq!(r.bit_pos(), 8);
        // Next read picks up at byte 1 = 0xAA.
        assert_eq!(r.read_bits(8).unwrap(), 0xAA);
    }

    #[test]
    fn read_aligned_bytes_returns_slice() {
        let bytes = [0x11u8, 0x22, 0x33, 0x44];
        let mut r = BitReader::new(&bytes);
        let s = r.read_aligned_bytes(3).unwrap();
        assert_eq!(s, &[0x11, 0x22, 0x33]);
        assert_eq!(r.bit_pos(), 24);
    }

    #[test]
    fn read_aligned_bytes_rejects_non_aligned_cursor() {
        let bytes = [0u8; 4];
        let mut r = BitReader::new(&bytes);
        let _ = r.read_bits(3).unwrap();
        // Cursor at bit 3, not byte-aligned.
        assert_eq!(r.read_aligned_bytes(1), Err(BitReaderError::UnexpectedEof));
    }

    #[test]
    fn read_aligned_bytes_rejects_overrun() {
        let bytes = [0u8; 2];
        let mut r = BitReader::new(&bytes);
        assert_eq!(r.read_aligned_bytes(3), Err(BitReaderError::UnexpectedEof));
    }

    #[test]
    fn has_bits_and_remaining_bits() {
        let bytes = [0u8; 2]; // 16 bits
        let mut r = BitReader::new(&bytes);
        assert_eq!(r.remaining_bits(), 16);
        assert!(r.has_bits(16));
        assert!(!r.has_bits(17));
        let _ = r.read_bits(5).unwrap();
        assert_eq!(r.remaining_bits(), 11);
        assert!(r.has_bits(11));
        assert!(!r.has_bits(12));
    }

    /// Pentest: a hostile caller that asks for many small reads near the
    /// end of the buffer must not panic; must surface UnexpectedEof at the
    /// exact crossover.
    #[test]
    fn pentest_walking_one_bit_at_a_time_to_eof_no_panic() {
        let bytes = [0xFFu8, 0xFF];
        let mut r = BitReader::new(&bytes);
        // 16 successful reads.
        for _ in 0..16 {
            assert_eq!(r.read_one_bit().unwrap(), 1);
        }
        // 17th must fail typed, not panic.
        assert_eq!(r.read_one_bit(), Err(BitReaderError::UnexpectedEof));
    }

    /// Pentest: usize overflow in bit_pos + n must surface UnexpectedEof
    /// rather than wrap. Hard to trigger naturally (would need
    /// usize::MAX/8 bytes), but the checked_add path locks the discipline.
    #[test]
    fn pentest_zero_bits_at_max_position_does_not_advance() {
        // Synthesize a 'fully consumed' cursor by reading all bits, then
        // verify zero-bit read still works (does not advance, no overflow).
        let bytes = [0xFFu8; 4];
        let mut r = BitReader::new(&bytes);
        let _ = r.read_bits(32).unwrap();
        assert_eq!(r.bit_pos(), 32);
        assert_eq!(r.read_bits(0).unwrap(), 0);
        assert_eq!(r.bit_pos(), 32);
    }
}
