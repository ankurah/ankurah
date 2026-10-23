//! Structural checks a receiver applies to state-sync messages before acting on them.
//! They need no stream state; the stream-dependent checks stay with the receiver.

use anyhow::ensure;

use super::StateReply;

impl StateReply {
    /// The initial reply opens round zero with frame zero.
    pub fn validate(&self) -> anyhow::Result<()> {
        ensure!(self.round.number == 0 && self.frame.sequence == 0, "initial reply must open round zero with frame zero");
        Ok(())
    }
}
