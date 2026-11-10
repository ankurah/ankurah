//! A store records this engine's `STORE_FORMAT_VERSION` when it is created and is checked on every
//! open; a store recording another format refuses to open with a `StoreFormatError`.

use ankurah_core::storage::StoreFormatError;
use ankurah_storage_sled::{SledStorageEngine, STORE_FORMAT_VERSION};

#[test]
fn a_store_of_another_format_is_refused_naming_both_versions() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    drop(SledStorageEngine::with_path(dir.path().to_path_buf())?);
    {
        // Rewrite the recorded version out of band.
        let db = sled::open(dir.path().join("sled"))?;
        db.open_tree("_ankurah_meta")?.insert("store_format_version", &(STORE_FORMAT_VERSION + 1).to_be_bytes())?;
        db.flush()?;
    }

    let error = SledStorageEngine::with_path(dir.path().to_path_buf()).err().expect("the store is refused");
    let refusal = error.downcast_ref::<StoreFormatError>().unwrap_or_else(|| panic!("not a store format refusal: {error}"));
    assert_eq!(*refusal, StoreFormatError::Mismatch { found: STORE_FORMAT_VERSION + 1, expected: STORE_FORMAT_VERSION });
    Ok(())
}
