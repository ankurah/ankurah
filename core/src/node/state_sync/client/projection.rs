use std::collections::BTreeMap;

use ankurah_digest_tree::leaf_address;
use ankurah_proto::{self as proto, EntityId, QueryMember, StateRound};
use anyhow::ensure;

use super::super::Snapshot;

type Members = BTreeMap<EntityId, QueryMember>;

/// Source membership is distinct from the predicate evaluated on merged local
/// entities. A round patches its frozen offer; live decisions never patch it.
pub(super) struct Projection {
    pub members: Members,
    pub revision: u64,
    pub round: Option<Round>,
}

pub(super) struct Round {
    pub descriptor: StateRound,
    pub snapshot: Snapshot,
    pub members: Members,
    addresses: BTreeMap<Vec<u8>, EntityId>,
    overlay: BTreeMap<EntityId, Option<QueryMember>>,
    pub complete: bool,
}

impl Round {
    pub fn new(descriptor: StateRound, snapshot: Snapshot) -> Self {
        let addresses = snapshot.members();
        let members = addresses
            .iter()
            .map(|(address, id)| {
                (
                    *id,
                    QueryMember {
                        key: address[..address.len() - EntityId::BYTE_LEN].to_vec(),
                        head: snapshot.states[id].payload.state.head.clone(),
                    },
                )
            })
            .collect();
        Self { descriptor, snapshot, members, addresses, overlay: BTreeMap::new(), complete: false }
    }

    /// Validation belongs at the wire boundary. Here pages replace only staging
    /// membership, including gaps, and move an identity out of its old address.
    pub fn replace(&mut self, page: proto::LeafPage) {
        let removed = self.addresses.range((page.bounds.start, page.bounds.end)).map(|(_, id)| *id).collect::<Vec<_>>();
        for id in removed {
            self.remove(id);
        }
        for leaf in page.leaves {
            let id = leaf.entity.entity_id;
            self.remove(id);
            self.addresses.insert(leaf_address(&leaf.key, id), id);
            self.members.insert(id, QueryMember { key: leaf.key, head: leaf.entity.head });
        }
    }

    fn remove(&mut self, id: EntityId) {
        if let Some(member) = self.members.remove(&id) {
            self.addresses.remove(&leaf_address(&member.key, id));
        }
    }
}

impl Projection {
    pub fn new(round: Round) -> Self { Self { members: round.members.clone(), revision: round.descriptor.revision, round: Some(round) } }

    pub fn begin(&mut self, round: Round) -> anyhow::Result<()> {
        ensure!(self.round.is_none(), "another comparison round is active");
        ensure!(round.descriptor.revision >= self.revision, "round precedes installed source publications");
        self.revision = round.descriptor.revision;
        self.round = Some(round);
        Ok(())
    }

    /// Call once per complete source publication, after collecting all its
    /// fragments. One publication may change several identities at one revision.
    pub fn publish(&mut self, revision: u64, changes: Vec<(EntityId, Option<QueryMember>)>) -> anyhow::Result<()> {
        ensure!(revision > self.revision, "source publication revision did not advance");
        if let Some(round) = &self.round {
            ensure!(revision > round.descriptor.revision, "live publication precedes round cut");
        }
        for (id, member) in changes {
            match &member {
                Some(member) => {
                    self.members.insert(id, member.clone());
                }
                None => {
                    self.members.remove(&id);
                }
            }
            if let Some(round) = &mut self.round {
                round.overlay.insert(id, member);
            }
        }
        self.revision = revision;
        Ok(())
    }

    pub fn has_live_decision(&self, id: &EntityId) -> bool { self.round.as_ref().is_some_and(|round| round.overlay.contains_key(id)) }

    pub fn retained_bytes(&self) -> anyhow::Result<usize> {
        Ok(self.round.as_ref().map(|round| bincode::serialized_size(&round.overlay)).transpose()?.unwrap_or(0) as usize)
    }

    /// Fold the overlay now, not at a future round. No causal completion writes
    /// membership, so removing a tombstone cannot let an old repair resurrect it.
    pub fn finish(&mut self, number: u64) -> anyhow::Result<Members> {
        let round = self.round.as_ref().ok_or_else(|| anyhow::anyhow!("no comparison round is active"))?;
        ensure!(round.descriptor.number == number && round.complete, "comparison round is not complete");
        let mut round = self.round.take().unwrap();
        let baseline = round.members.clone();
        for (id, member) in round.overlay {
            match member {
                Some(member) => {
                    round.members.insert(id, member);
                }
                None => {
                    round.members.remove(&id);
                }
            }
        }
        self.members = round.members;
        Ok(baseline)
    }

    pub fn cancel(&mut self, number: u64) -> anyhow::Result<()> {
        ensure!(self.round.as_ref().is_some_and(|round| round.descriptor.number == number), "comparison round changed");
        self.round = None;
        Ok(())
    }
}
