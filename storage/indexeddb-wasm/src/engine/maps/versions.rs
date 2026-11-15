//! The reset epoch and lock versions observed by a storage transaction or
//! snapshot, and the requests that read them. Opening another native read
//! transaction or committing checks them for intervening changes.

use std::collections::BTreeMap;

use ankurah_core::storage::LockName;
use web_sys::IdbObjectStore;

use super::layout;
use super::requests::Request;
use super::MapError;

/// The reset epoch and the lock names' versions as a transaction or a
/// snapshot noted them: the epoch at its first map call, and each name's
/// version as it took the name. Read again, they are equal to what was noted
/// unless one has moved since.
#[derive(PartialEq)]
pub(super) struct Noted {
    epoch: Vec<u8>,
    pub(super) versions: BTreeMap<LockName, u64>,
}

/// Pending requests for the reset epoch and selected lock versions. The
/// initial read records the epoch. Each new native read transaction and the
/// commit also check previously observed lock versions for changes.
pub(super) struct VersionRequests {
    epoch: Request,
    versions: Vec<(LockName, Request)>,
}

impl VersionRequests {
    /// Request the reset epoch and the versions of `names` from `store`.
    pub(super) fn request<'n>(store: &IdbObjectStore, names: impl IntoIterator<Item = &'n LockName>) -> Result<Self, MapError> {
        let epoch = Request::made(store.get(&layout::epoch_key()))?;
        let versions = names.into_iter().map(|name| version_request(store, name)).collect::<Result<_, _>>()?;

        Ok(Self { epoch, versions })
    }

    /// What the requests read, once they have succeeded.
    pub(super) async fn answer(self) -> Result<Noted, MapError> {
        Ok(Noted { epoch: layout::epoch_of(&self.epoch.result().await?)?, versions: versions_read(self.versions).await? })
    }
}

/// A request, in `store`, a transaction's object store of the versions, of
/// the version of the lock `name`.
pub(super) fn version_request(store: &IdbObjectStore, name: &LockName) -> Result<(LockName, Request), MapError> {
    Request::made(store.get(&layout::version_key(name))).map(|request| (name.clone(), request))
}

/// The version each of `requests` read, by name, once they have succeeded.
pub(super) async fn versions_read(requests: Vec<(LockName, Request)>) -> Result<BTreeMap<LockName, u64>, MapError> {
    let mut versions = BTreeMap::new();
    for (name, request) in requests {
        versions.insert(name, layout::version_of(&request.result().await?)?);
    }

    Ok(versions)
}
