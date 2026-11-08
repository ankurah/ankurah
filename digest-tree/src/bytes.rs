//! Length-prefixed fields, and a cursor over bytes being decoded: what store
//! rows and the bytes peers exchange share.

/// Append `field` preceded by its length as an unsigned 32-bit big-endian
/// integer.
pub(crate) fn write_field(out: &mut Vec<u8>, field: &[u8]) {
    out.extend_from_slice(&u32::try_from(field.len()).expect("a field shorter than 4 GiB").to_be_bytes());
    out.extend_from_slice(field);
}

/// The bytes of an encoding still to read, front to back. Each read is
/// `None` when too few bytes remain, for the caller to report as its own
/// kind of error.
pub(crate) struct ByteReader<'a>(&'a [u8]);

impl<'a> ByteReader<'a> {
    pub(crate) fn new(bytes: &'a [u8]) -> Self { Self(bytes) }

    /// The next `len` bytes.
    pub(crate) fn take(&mut self, len: usize) -> Option<&'a [u8]> {
        let (taken, rest) = self.0.split_at_checked(len)?;
        self.0 = rest;
        Some(taken)
    }

    /// The next byte.
    pub(crate) fn byte(&mut self) -> Option<u8> {
        let (&byte, rest) = self.0.split_first()?;
        self.0 = rest;
        Some(byte)
    }

    /// The next four bytes, as an unsigned big-endian integer.
    pub(crate) fn u32_be(&mut self) -> Option<u32> {
        let (bytes, rest) = self.0.split_first_chunk::<4>()?;
        self.0 = rest;
        Some(u32::from_be_bytes(*bytes))
    }

    /// The next field [`write_field`] wrote.
    pub(crate) fn field(&mut self) -> Option<&'a [u8]> {
        let len = self.u32_be()?;
        self.take(len as usize)
    }

    /// The bytes not yet read.
    pub(crate) fn rest(&self) -> &'a [u8] { self.0 }
}
