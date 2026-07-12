//! Durable node-key persistence helpers.
//!
//! Core owns signing keys, but embedders own where durable secrets live.
//! This module provides the small filesystem primitive needed to persist the
//! 32-byte Ed25519 seed and pass the returned key to
//! [`crate::Node::new_durable_with_signing_key`].

#[cfg(not(target_family = "wasm"))]
use std::{
    fs::{self, OpenOptions},
    io::{self, Read, Write},
    path::Path,
};

/// Load a 32-byte Ed25519 signing seed, or create it once when absent.
///
/// Creation writes and fsyncs a same-directory temporary file, then publishes
/// it with an atomic no-replace hard link. Concurrent starters can therefore
/// read only a complete seed, never a partially written winner. On Unix the
/// file is mode `0600` and the parent directory is fsynced before return, so a
/// durable system root is never started from a key whose directory entry can
/// still disappear after a crash. The raw seed is intentionally simple so an
/// embedder can move it into a platform key store without changing Ankurah's
/// key type.
#[cfg(not(target_family = "wasm"))]
pub fn load_or_create_signing_key(path: impl AsRef<Path>) -> io::Result<ed25519_dalek::SigningKey> {
    let path = path.as_ref();
    match load_signing_key(path) {
        Ok(key) => return Ok(key),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }

    create_parent_dirs(path)?;

    let key = {
        use rand::RngCore;
        let mut seed = [0u8; 32];
        rand::rngs::OsRng.fill_bytes(&mut seed);
        ed25519_dalek::SigningKey::from_bytes(&seed)
    };

    let (temp_path, mut file) = create_temp_key_file(path)?;
    let result = (|| {
        file.write_all(&key.to_bytes())?;
        file.sync_all()?;
        drop(file);

        match fs::hard_link(&temp_path, path) {
            Ok(()) => {
                fs::remove_file(&temp_path)?;
                sync_parent_dir(path)?;
                Ok(key)
            }
            // The winner's final path is published only after its complete
            // contents were fsynced. Sync the shared directory ourselves so
            // it is durable before this loser returns and persists a root.
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                let _ = fs::remove_file(&temp_path);
                sync_parent_dir(path)?;
                load_signing_key(path)
            }
            Err(error) => Err(error),
        }
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temp_path);
    }
    result
}

#[cfg(not(target_family = "wasm"))]
fn create_temp_key_file(path: &Path) -> io::Result<(std::path::PathBuf, fs::File)> {
    use rand::RngCore;

    let parent = path.parent().filter(|parent| !parent.as_os_str().is_empty()).unwrap_or_else(|| Path::new("."));
    let name = path.file_name().ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "node key path has no filename"))?;
    for _ in 0..16 {
        let mut random = [0u8; 8];
        rand::rngs::OsRng.fill_bytes(&mut random);
        let temp_path = parent.join(format!(".{}.{}.tmp", name.to_string_lossy(), u64::from_le_bytes(random)));
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        match options.open(&temp_path) {
            Ok(file) => return Ok((temp_path, file)),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error),
        }
    }
    Err(io::Error::new(io::ErrorKind::AlreadyExists, "could not allocate a unique node-key temporary file"))
}

#[cfg(all(not(target_family = "wasm"), unix))]
fn sync_parent_dir(path: &Path) -> io::Result<()> {
    let parent = path.parent().filter(|parent| !parent.as_os_str().is_empty()).unwrap_or_else(|| Path::new("."));
    fs::File::open(parent)?.sync_all()
}

#[cfg(all(not(target_family = "wasm"), not(unix)))]
fn sync_parent_dir(_path: &Path) -> io::Result<()> { Ok(()) }

#[cfg(not(target_family = "wasm"))]
fn create_parent_dirs(path: &Path) -> io::Result<()> {
    if let Some(parent) = path.parent().filter(|parent| !parent.as_os_str().is_empty()) {
        fs::create_dir_all(parent)?;
    }
    Ok(())
}

/// Load an existing raw 32-byte Ed25519 signing seed.
#[cfg(not(target_family = "wasm"))]
pub fn load_signing_key(path: impl AsRef<Path>) -> io::Result<ed25519_dalek::SigningKey> {
    let mut file = OpenOptions::new().read(true).open(path)?;
    let mut seed = [0u8; 32];
    file.read_exact(&mut seed)?;
    let mut trailing = [0u8; 1];
    if file.read(&mut trailing)? != 0 {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "node key must contain exactly 32 bytes"));
    }
    Ok(ed25519_dalek::SigningKey::from_bytes(&seed))
}

#[cfg(all(test, not(target_family = "wasm")))]
mod tests {
    use super::*;

    #[test]
    fn load_or_create_preserves_identity() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("node.key");
        let first = load_or_create_signing_key(&path).unwrap();
        let second = load_or_create_signing_key(&path).unwrap();
        assert_eq!(first.verifying_key(), second.verifying_key());
        assert_eq!(std::fs::read(path).unwrap().len(), 32);
    }

    #[test]
    fn bare_filename_needs_no_parent_directory() { create_parent_dirs(Path::new("node.key")).unwrap(); }

    #[test]
    fn concurrent_creators_publish_only_one_complete_key() {
        let dir = tempfile::tempdir().unwrap();
        let path = std::sync::Arc::new(dir.path().join("node.key"));
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(9));
        let mut threads = Vec::new();
        for _ in 0..8 {
            let path = path.clone();
            let barrier = barrier.clone();
            threads.push(std::thread::spawn(move || {
                barrier.wait();
                load_or_create_signing_key(&*path).unwrap().verifying_key()
            }));
        }
        barrier.wait();
        let keys: Vec<_> = threads.into_iter().map(|thread| thread.join().unwrap()).collect();
        assert!(keys.windows(2).all(|pair| pair[0] == pair[1]));
        assert_eq!(fs::read(&*path).unwrap().len(), 32);
        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 1, "temporary key files must be cleaned up");
    }
}
