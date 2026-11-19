//! The keypair a node's storage engine keeps for it: one entry in one ordered
//! map, read under one lock, so that nodes that start at once and share one
//! database end with one keypair.

use crate::error::{MutationError, NodeHaltReason};
use crate::storage::names::NODE_KEYPAIR;
use crate::storage::{LockName, MapId, MapRead, MapRows, MapWrite, StorageCommitOutcome, StorageEngine, StorageTransaction};

use super::NodeKey;

/// The keypair's entry in its map.
pub(super) const KEYPAIR: &[u8] = b"keypair";

/// Why a node could not look up its keypair.
#[derive(Debug, thiserror::Error)]
pub(super) enum KeyLookupError {
    #[error(transparent)]
    Storage(#[from] MutationError),
    #[error("the storage engine's answer does not fit the keypair read")]
    AnswerMisfit,
    #[error("the stored keypair is not a node key")]
    NotANodeKey,
}

impl From<KeyLookupError> for NodeHaltReason {
    fn from(error: KeyLookupError) -> Self { Self::KeypairLookup(error.to_string()) }
}

/// The keypair `storage` keeps for its node: the stored one, or a new one,
/// stored now.
///
/// A lookup that loses the race to store the first keypair, as a browser tab
/// does when another tab's commit lands first, fails with a write conflict,
/// and reads again to take the keypair that landed.
pub(super) async fn look_up<SE: StorageEngine>(storage: &SE) -> Result<NodeKey, KeyLookupError> {
    loop {
        let mut trx = storage.transaction();
        let read = MapRead::Get { map: keypair_map(), keys: vec![KEYPAIR.to_vec()] };
        let stored = match trx.map_lock_and_read(&[keypair_lock()], vec![read]).await {
            Ok(mut answers) => match answers.pop() {
                Some(MapRows::Got(mut values)) if answers.is_empty() && values.len() == 1 => values.pop().flatten(),
                _ => return Err(KeyLookupError::AnswerMisfit),
            },
            Err(MutationError::WriteConflict) => continue,
            Err(error) => return Err(error.into()),
        };
        if let Some(stored) = stored {
            let text = String::from_utf8(stored).map_err(|_| KeyLookupError::NotANodeKey)?;
            return text.parse().map_err(|_| KeyLookupError::NotANodeKey);
        }

        let key = NodeKey::generate();
        trx.map_write(vec![MapWrite::Put { map: keypair_map(), key: KEYPAIR.to_vec(), value: key.to_secret_text().into_bytes() }]).await?;
        match trx.commit().await.and_then(StorageCommitOutcome::committed) {
            Ok(_) => return Ok(key),
            Err(MutationError::WriteConflict) => continue,
            Err(error) => return Err(error.into()),
        }
    }
}

/// The map that holds the node's keypair.
pub(super) fn keypair_map() -> MapId { MapId::new([NODE_KEYPAIR]) }

/// The lock a lookup takes before it reads the keypair.
pub(super) fn keypair_lock() -> LockName { LockName::new([NODE_KEYPAIR]) }
