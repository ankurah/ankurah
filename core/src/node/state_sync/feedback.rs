//! Coalesce cumulative credit while applying later frames; never impose one RTT per frame.
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};

use ankurah_proto as proto;
use anyhow::ensure;
use futures::FutureExt;

use super::Inbox;
use crate::{node::Node, policy::PolicyAgent, storage::StorageEngine};

#[derive(Default)]
pub(super) struct Feedback(Mutex<Pending>);

#[derive(Default)]
struct Pending {
    through: u64,
    applied_through: Option<u64>,
    answers: VecDeque<(Option<u64>, proto::StateAnswer)>,
    bytes: usize,
    dirty: bool,
    running: bool,
}

impl Feedback {
    pub fn cancel_round(&self, round: u64) {
        let mut pending = self.0.lock().unwrap();
        pending.answers.retain(|(owner, _)| *owner != Some(round));
        pending.bytes = pending
            .answers
            .iter()
            .map(|(_, answer)| super::budget::answer_bytes(answer).expect("queued answer already serialized") as usize)
            .sum();
    }

    pub fn queue<SE, PA>(
        self: &Arc<Self>,
        node: &Node<SE, PA>,
        peer: proto::EntityId,
        credentials: &[PA::ContextData],
        comparison: proto::StateStreamId,
        through: u64,
        round: Option<u64>,
        applied_through: Option<u64>,
        answers: Vec<proto::StateAnswer>,
        inbox: &Arc<Inbox>,
    ) -> anyhow::Result<()>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        {
            let mut pending = self.0.lock().unwrap();
            let bytes = super::budget::answers_bytes(&answers)? as usize;
            ensure!(pending.bytes + bytes <= super::WINDOW_BYTES, "state answer queue exceeded limit");
            pending.bytes += bytes;
            pending.answers.extend(answers.into_iter().map(|answer| (round, answer)));
            pending.applied_through = pending.applied_through.max(applied_through);
            pending.through = pending.through.max(through);
            pending.dirty = true;
            if pending.running {
                return Ok(());
            }
            pending.running = true;
        }
        let feedback = self.clone();
        let node = node.weak();
        let credentials = credentials.to_vec();
        let inbox = inbox.clone();
        crate::task::spawn(async move {
            let result: anyhow::Result<()> = async {
                loop {
                    let (processed_through, round, applied_through, answers) = {
                        let mut pending = feedback.0.lock().unwrap();
                        if !pending.dirty {
                            pending.running = false;
                            return Ok(());
                        }
                        let round = pending.answers.front().and_then(|(round, _)| *round);
                        let mut answers = Vec::new();
                        let mut bytes = 0;
                        while let Some((answer_round, answer)) = pending.answers.front() {
                            if *answer_round != round {
                                break;
                            }
                            let size = super::budget::answer_bytes(answer)? as usize;
                            ensure!(size <= super::PAGE_BYTES, "state answer exceeds limit");
                            if answers.len() == 128 || bytes + size > super::PAGE_BYTES {
                                break;
                            }
                            bytes += size;
                            answers.push(pending.answers.pop_front().unwrap().1);
                        }
                        pending.bytes = pending.bytes.saturating_sub(bytes);
                        pending.dirty = !pending.answers.is_empty();
                        if !pending.dirty {
                            pending.bytes = 0;
                        }
                        (pending.through, round, pending.applied_through, answers)
                    };
                    let Some(node) = node.upgrade() else { return Ok(()) };
                    let request = node
                        .request(
                            peer,
                            &credentials,
                            proto::NodeRequestBody::ContinueState { comparison, round, processed_through, applied_through, answers },
                        )
                        .fuse();
                    let timeout = futures_timer::Delay::new(std::time::Duration::from_secs(120)).fuse();
                    futures::pin_mut!(request, timeout);
                    let response = futures::select! {
                        response = request => response?,
                        _ = timeout => anyhow::bail!("state feedback timed out"),
                    };
                    ensure!(matches!(response, proto::NodeResponseBody::Success), "state feedback rejected: {response:?}");
                }
            }
            .await;
            if let Err(error) = result {
                tracing::warn!(%error, "state feedback failed");
                inbox.close();
            }
        });
        Ok(())
    }
}
