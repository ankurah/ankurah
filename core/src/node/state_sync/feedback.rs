//! Coalesce cumulative credit while applying later frames; never impose one RTT per frame.
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};

use ankurah_proto::{
    self as proto,
    state_sync::limits::{MAX_FEEDBACK_ANSWERS, MAX_PAGE_BYTES, STREAM_TTL, WINDOW_BYTES},
};
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
    answers: VecDeque<PendingAnswer>,
    bytes: usize,
    dirty: bool,
    running: bool,
}

/// One answer waiting to be sent, with what it responds to.
enum PendingAnswer {
    Round(u64, proto::StateAnswer),
    LiveRepair(proto::RepairRequest),
}

impl PendingAnswer {
    fn bytes(&self) -> bincode::Result<u64> {
        match self {
            Self::Round(_, answer) => answer.work_bytes(),
            Self::LiveRepair(request) => request.work_bytes(),
        }
    }
}

impl Pending {
    /// The next feedback's answers: the longest run of queued answers to one round,
    /// or of live repair requests, that fits one message.
    fn next_answers(&mut self) -> anyhow::Result<Option<proto::FeedbackAnswers>> {
        Ok(match self.answers.front() {
            None => None,
            Some(PendingAnswer::Round(round, _)) => {
                let round = *round;
                let answers = self.take_run(|answer| match answer {
                    PendingAnswer::Round(answer_round, answer) if answer_round == round => Ok(answer),
                    other => Err(other),
                })?;
                Some(proto::FeedbackAnswers::Round { round, answers })
            }
            Some(PendingAnswer::LiveRepair(_)) => {
                let requests = self.take_run(|answer| match answer {
                    PendingAnswer::LiveRepair(request) => Ok(request),
                    other => Err(other),
                })?;
                Some(proto::FeedbackAnswers::LiveRepair { requests })
            }
        })
    }

    /// Pops queued answers while `take` accepts them and they fit one message.
    fn take_run<T>(&mut self, mut take: impl FnMut(PendingAnswer) -> Result<T, PendingAnswer>) -> anyhow::Result<Vec<T>> {
        let mut run = Vec::new();
        let mut bytes = 0;
        while let Some(answer) = self.answers.front() {
            let size = answer.bytes()? as usize;
            ensure!(size <= MAX_PAGE_BYTES, "state answer exceeds limit");
            if run.len() == MAX_FEEDBACK_ANSWERS || bytes + size > MAX_PAGE_BYTES {
                break;
            }
            match take(self.answers.pop_front().unwrap()) {
                Ok(item) => {
                    bytes += size;
                    run.push(item);
                }
                Err(answer) => {
                    self.answers.push_front(answer);
                    break;
                }
            }
        }
        self.bytes = self.bytes.saturating_sub(bytes);
        Ok(run)
    }
}

impl Feedback {
    pub fn cancel_round(&self, round: u64) {
        let mut pending = self.0.lock().unwrap();
        pending.answers.retain(|answer| !matches!(answer, PendingAnswer::Round(answer_round, _) if *answer_round == round));
        pending.bytes = pending.answers.iter().map(|answer| answer.bytes().expect("queued answer already serialized") as usize).sum();
    }

    pub fn queue<SE, PA>(
        self: &Arc<Self>,
        node: &Node<SE, PA>,
        peer: proto::EntityId,
        credentials: &[PA::ContextData],
        stream_id: proto::StateStreamId,
        through: u64,
        applied_through: Option<u64>,
        answers: Option<proto::FeedbackAnswers>,
        inbox: &Arc<Inbox>,
    ) -> anyhow::Result<()>
    where
        SE: StorageEngine + Send + Sync + 'static,
        PA: PolicyAgent + Send + Sync + 'static,
    {
        {
            let mut pending = self.0.lock().unwrap();
            let answers: Vec<_> = match answers {
                Some(proto::FeedbackAnswers::Round { round, answers }) => {
                    answers.into_iter().map(|answer| PendingAnswer::Round(round, answer)).collect()
                }
                Some(proto::FeedbackAnswers::LiveRepair { requests }) => requests.into_iter().map(PendingAnswer::LiveRepair).collect(),
                None => Vec::new(),
            };
            let bytes = answers.iter().try_fold(0, |bytes, answer| answer.bytes().map(|size| bytes + size as usize))?;
            ensure!(pending.bytes + bytes <= WINDOW_BYTES, "state answer queue exceeded limit");
            pending.bytes += bytes;
            pending.answers.extend(answers);
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
                    let (processed_through, applied_through, answers) = {
                        let mut pending = feedback.0.lock().unwrap();
                        if !pending.dirty {
                            pending.running = false;
                            return Ok(());
                        }
                        let answers = pending.next_answers()?;
                        pending.dirty = !pending.answers.is_empty();
                        if !pending.dirty {
                            pending.bytes = 0;
                        }
                        (pending.through, pending.applied_through, answers)
                    };
                    let Some(node) = node.upgrade() else { return Ok(()) };
                    let request = node
                        .request(
                            peer,
                            &credentials,
                            proto::NodeRequestBody::StateFeedback { stream: stream_id, processed_through, applied_through, answers },
                        )
                        .fuse();
                    let timeout = futures_timer::Delay::new(STREAM_TTL).fuse();
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

#[cfg(test)]
mod tests {
    use super::*;

    fn request(n: u8) -> proto::RepairRequest {
        proto::RepairRequest { entity_id: proto::EntityId::from_bytes([n; 32]), target: proto::Clock::default() }
    }

    #[test]
    fn answers_leave_in_runs_of_one_round_or_of_live_requests() -> anyhow::Result<()> {
        let mut pending = Pending::default();
        pending.answers.extend([
            PendingAnswer::Round(1, proto::StateAnswer::NeedRepair(request(1))),
            PendingAnswer::Round(1, proto::StateAnswer::NeedRepair(request(2))),
            PendingAnswer::LiveRepair(request(3)),
            PendingAnswer::Round(2, proto::StateAnswer::NeedRepair(request(4))),
        ]);
        assert!(matches!(pending.next_answers()?, Some(proto::FeedbackAnswers::Round { round: 1, answers }) if answers.len() == 2));
        assert!(matches!(pending.next_answers()?, Some(proto::FeedbackAnswers::LiveRepair { requests }) if requests == vec![request(3)]));
        assert!(matches!(pending.next_answers()?, Some(proto::FeedbackAnswers::Round { round: 2, answers }) if answers.len() == 1));
        assert!(pending.next_answers()?.is_none());
        Ok(())
    }
}
