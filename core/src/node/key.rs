use ankurah_proto as proto;
use std::fmt;

/// A node's ed25519 signing key. Its verifying key is the node's [`proto::Identity`].
///
/// A node holds its key privately and does all of its own signing; policy
/// agents and transports never see it, and nothing serializes or prints it.
pub struct NodeKey(ed25519_dalek::SigningKey);

impl NodeKey {
    /// Draw a fresh key.
    pub fn generate() -> Self {
        let mut seed = [0u8; 32];
        rand::RngCore::fill_bytes(&mut rand::rngs::OsRng, &mut seed);
        Self::from_seed(seed)
    }

    /// Reconstruct a key from its 32-byte seed.
    pub fn from_seed(seed: [u8; 32]) -> Self { Self(ed25519_dalek::SigningKey::from_bytes(&seed)) }

    /// The 32-byte seed, for persisting this key.
    pub fn to_seed(&self) -> [u8; 32] { self.0.to_bytes() }

    /// The identity this key signs as.
    pub fn identity(&self) -> proto::Identity { proto::Identity::Ed25519(self.0.verifying_key().to_bytes()) }

    /// This key's identity as the peer layer names a node.
    pub fn node_id(&self) -> proto::NodeId { self.identity().into() }

    /// Sign `message` under this key.
    pub fn sign(&self, message: &[u8]) -> proto::Signature {
        use ed25519_dalek::Signer;
        proto::Signature::Ed25519(self.0.sign(message).to_bytes())
    }

    /// Load the key whose seed is stored at `path`, first writing a fresh seed there if the file does not exist.
    /// The file holds exactly the 32 seed bytes; anything else is refused. A fresh seed is written to a
    /// dot-prefixed staged file beside `path` and linked into place. Removing the staged file is then
    /// attempted; if that fails, the staged file stays behind, and later starts ignore it.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn load_or_create(path: impl AsRef<std::path::Path>) -> std::io::Result<Self> {
        use std::io::{ErrorKind, Write};
        let path = path.as_ref();
        let load = || -> std::io::Result<Self> {
            let seed: [u8; 32] = std::fs::read(path)?.try_into().map_err(|bytes: Vec<u8>| {
                std::io::Error::new(ErrorKind::InvalidData, format!("node key file {} holds {} bytes, not 32", path.display(), bytes.len()))
            })?;
            Ok(Self::from_seed(seed))
        };
        match load() {
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            result => return result,
        }

        // Stage the complete seed beside `path`, then publish it there, so `path` never holds a partial seed.
        let key = Self::generate();
        let name = path.file_name().ok_or_else(|| std::io::Error::new(ErrorKind::InvalidInput, "node key path names no file"))?;
        let mut staged_name = std::ffi::OsString::from(".");
        staged_name.push(name);
        staged_name.push(format!(".{:016x}.tmp", rand::random::<u64>()));
        let staged = path.with_file_name(staged_name);
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
        let mut file = options.open(&staged)?;
        let written = file.write_all(&key.to_seed()).and_then(|()| file.sync_all());
        drop(file);
        // A link, unlike a rename, never replaces a seed another process published first.
        let published = written.and_then(|()| std::fs::hard_link(&staged, path));
        let _ = std::fs::remove_file(&staged);
        match published {
            Ok(()) => {
                #[cfg(unix)]
                sync_directory_of(path)?;
                Ok(key)
            }
            Err(error) if error.kind() == ErrorKind::AlreadyExists => load(),
            Err(error) => Err(error),
        }
    }

    /// Sign `body` as this key's attestation.
    pub fn attest(&self, body: proto::AttestationBody) -> proto::Attestation {
        let signature = self.sign(&body.signable_bytes());
        proto::Attestation { attester: self.identity(), body, signature }
    }

    /// Sign a presence answering `challenge`: this key's identity and its durable claim.
    pub fn sign_presence(&self, durable: bool, challenge: proto::HandshakeChallenge) -> proto::Presence {
        let mut presence = proto::Presence {
            node_id: self.node_id(),
            durable,
            challenge,
            timestamp: proto::time::unix_ms_now(),
            signature: proto::Signature::Ed25519([0; 64]),
            protocol_version: proto::PROTOCOL_VERSION,
        };
        presence.signature = self.sign(&presence.claims().signable_bytes());
        presence
    }
}

/// Persist the directory entry that names `path`, so a newly linked key survives power loss.
#[cfg(unix)]
fn sync_directory_of(path: &std::path::Path) -> std::io::Result<()> {
    let directory = path.parent().filter(|parent| !parent.as_os_str().is_empty()).unwrap_or(std::path::Path::new("."));
    match std::fs::File::open(directory).and_then(|directory| directory.sync_all()) {
        Err(error) if error.kind() == std::io::ErrorKind::Unsupported => Ok(()),
        result => result,
    }
}

impl fmt::Debug for NodeKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { write!(f, "NodeKey({})", self.identity()) }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_seed_reconstructs_the_same_identity() {
        let key = NodeKey::generate();
        assert_eq!(NodeKey::from_seed(key.to_seed()).identity(), key.identity());
        assert_ne!(NodeKey::generate().identity(), key.identity());
    }

    #[test]
    fn a_signature_verifies_under_the_keys_identity() {
        let key = NodeKey::from_seed([9; 32]);
        assert!(key.identity().verify(b"message", &key.sign(b"message")));
        assert!(!NodeKey::from_seed([8; 32]).identity().verify(b"message", &key.sign(b"message")));
    }

    #[test]
    fn debug_prints_the_identity_and_not_the_seed() {
        let key = NodeKey::from_seed([9; 32]);
        assert_eq!(format!("{key:?}"), format!("NodeKey({})", key.identity()));
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn load_or_create_persists_one_key_and_refuses_a_malformed_file() {
        let dir = std::env::temp_dir().join(format!("ankurah-node-key-{}", NodeKey::generate().node_id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("node.key");

        let created = NodeKey::load_or_create(&path).unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), created.to_seed());
        assert_eq!(NodeKey::load_or_create(&path).unwrap().node_id(), created.node_id());

        let entries: Vec<_> = std::fs::read_dir(&dir).unwrap().map(|entry| entry.unwrap().file_name()).collect();
        assert_eq!(entries, ["node.key"], "no staged seed is left behind");

        for malformed in [&[1u8; 31][..], &[]] {
            std::fs::write(&path, malformed).unwrap();
            assert_eq!(NodeKey::load_or_create(&path).unwrap_err().kind(), std::io::ErrorKind::InvalidData);
        }
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
