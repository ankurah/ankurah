//! Checks the measured query's result, as installed at its endpoint, against what the server holds.

use std::collections::{BTreeMap, BTreeSet};

use ankurah::{core::storage::StorageEngine, proto, ResultSet};

use crate::{
    fixture::{Expected, StartupRowView},
    BenchNode,
};

/// A query result captured at a landmark: each member's full state, head clock included, and the
/// members in result order when the selection is ordered.
pub struct InstalledResult {
    order: Option<Vec<proto::EntityId>>,
    states: BTreeMap<proto::EntityId, proto::State>,
}

impl InstalledResult {
    /// Captures `resultset` under one read guard, so its membership and order come from one moment.
    /// Each entity's state is read atomically; the states together are not one snapshot.
    pub fn capture(resultset: &ResultSet<StartupRowView>, ordered: bool) -> anyhow::Result<Self> {
        let result = resultset.read();
        let order = ordered.then(|| result.iter_entities().map(|(id, _)| id).collect());
        let states = result.iter_entities().map(|(id, entity)| Ok((id, entity.to_state()?))).collect::<anyhow::Result<_>>()?;
        Ok(Self { order, states })
    }

    /// Checks the captured order, membership and states against `expected` and the server's
    /// storage, then checks that the client persisted the same states. The benchmark writes nothing
    /// once the measured operation begins, so the server's storage still holds what the client had
    /// to install.
    pub async fn verify(&self, expected: &Expected, rows: &[StartupRowView], server: &BenchNode, client: &BenchNode) -> anyhow::Result<()> {
        if let Some(order) = &expected.order {
            anyhow::ensure!(
                self.order.as_ref() == Some(order),
                "installed query has incorrect authoritative ordering: expected seeded positions {:?}, captured {:?}; expected {} states, captured {}",
                seeded_positions(order, rows),
                seeded_positions(self.order.iter().flatten(), rows),
                expected.members.len(),
                self.states.len()
            );
        }
        anyhow::ensure!(
            self.states.keys().copied().collect::<BTreeSet<_>>() == expected.members,
            "installed query has incorrect authoritative membership"
        );
        for id in &expected.members {
            let remote = server.storage.get_state(*id).await?;
            anyhow::ensure!(self.states[id] == remote.payload.state, "installation capture has incorrect applied state for {id}");
            // Persistence is checked after the landmark; these reads do not observe the client at
            // the landmark's moment.
            let local = client.storage.get_state(*id).await?;
            anyhow::ensure!(local.payload.state == remote.payload.state, "installed query has incorrect persisted state for {id}");
        }
        Ok(())
    }
}

/// Each id's index in the populated rows, for failure messages.
fn seeded_positions<'a>(ids: impl IntoIterator<Item = &'a proto::EntityId>, rows: &[StartupRowView]) -> Vec<Option<usize>> {
    ids.into_iter().map(|id| rows.iter().position(|row| row.id() == *id)).collect()
}
