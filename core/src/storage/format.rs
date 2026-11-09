/// Why a storage engine refuses to open a store: the store is in a format the engine does not read.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum StoreFormatError {
    /// The store records another format version.
    #[error("the store is in format version {found} and this engine reads format version {expected}; reset the development database")]
    Mismatch { found: u32, expected: u32 },
    /// The store's format record holds no version number.
    #[error(
        "the store's format record {record} names no version; this engine reads format version {expected}; reset the development database"
    )]
    UnreadableRecord { record: String, expected: u32 },
    /// The store holds Ankurah data and records no format version, as a store from before format
    /// versions were recorded does.
    #[error("the store holds Ankurah data but records no format version; this engine reads format version {expected}; reset the development database")]
    Unversioned { expected: u32 },
}

impl StoreFormatError {
    /// Refuse a store whose format record, the version as decimal text, is not `expected`.
    pub fn check_decimal_record(record: &str, expected: u32) -> Result<(), Self> {
        match record.parse::<u32>() {
            Ok(found) if found == expected => Ok(()),
            Ok(found) => Err(Self::Mismatch { found, expected }),
            Err(_) => Err(Self::UnreadableRecord { record: record.to_owned(), expected }),
        }
    }
}
