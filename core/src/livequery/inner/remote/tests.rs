use super::*;
use crate::livequery::inner::{tests::query, LocalActivation};
use crate::reactor::PreNotifyHook;

#[tokio::test]
async fn partial_install_blocks_stale_local_activation_without_marking_ready() {
    let query = query();
    let origin = proto::RequestId::new();
    let canceled = || false;
    query.0.bind_remote_stream(1, origin.clone(), &canceled);
    let partial = RemoteActivation { query: &query.0, origin: &origin, canceled: &canceled, complete: false };
    assert!(!partial.marks_loaded());
    partial.pre_notify(1);
    assert!(!LocalActivation(&query.0).is_current(1));
    assert!(query.0.remote_stream_pending(1));
    assert!(!query.resultset().is_loaded());
    let ready = query.wait_initialized();
    tokio::pin!(ready);
    assert!(futures::poll!(&mut ready).is_pending());
    RemoteActivation { query: &query.0, origin: &origin, canceled: &canceled, complete: true }.pre_notify(1);
    ready.await.unwrap();
}

#[tokio::test]
async fn admission_is_released_only_for_the_current_lost_failed_or_replaced_stream() {
    let query = query();
    let origin = proto::RequestId::new();
    let replacement = proto::RequestId::new();
    query.0.bind_remote_stream(1, origin.clone(), &|| false);
    let old_gate = query.0.remote_stream.lock().unwrap().as_ref().unwrap().admission.clone();
    assert!(old_gate.load(Ordering::Acquire));
    query.0.bind_remote_stream(1, replacement.clone(), &|| false);
    assert!(!old_gate.load(Ordering::Acquire));
    let current_gate = query.0.remote_stream.lock().unwrap().as_ref().unwrap().admission.clone();
    query.0.lose_remote_stream(1, &origin);
    assert!(current_gate.load(Ordering::Acquire), "old cleanup cannot release the replacement's restriction");
    query.0.lose_remote_stream(1, &replacement);
    assert!(!current_gate.load(Ordering::Acquire));
    let late = RemoteActivation { query: &query.0, origin: &replacement, canceled: &|| false, complete: true };
    assert!(!late.is_current(1));
    late.pre_notify(1);
    assert!(query.0.remote_stream_pending(1), "a late completion cannot restore lost readiness");
    query.0.bind_remote_stream(1, replacement, &|| false);
    let failed_gate = query.0.remote_stream.lock().unwrap().as_ref().unwrap().admission.clone();
    query.0.fail_initialization(1, RetrievalError::Other("unavailable".into()));
    assert!(!failed_gate.load(Ordering::Acquire));
}

#[tokio::test]
async fn a_failed_stream_clears_readiness_and_old_callbacks_cannot_restore_it() {
    let query = query();
    let original = proto::RequestId::new();
    let current = proto::RequestId::new();
    let canceled = || false;
    query.0.bind_remote_stream(1, original.clone(), &canceled);
    RemoteActivation { query: &query.0, origin: &original, canceled: &canceled, complete: true }.pre_notify(1);
    query.0.resultset.set_loaded(true);
    query.0.mark_durable_answered(1);
    query.wait_durable_answered().await.unwrap();

    let weak = query.weak();
    let guard = ankurah_signals::Signal::listen(
        &query.resultset(),
        Arc::new(move |_: ()| {
            let query = weak.upgrade().unwrap();
            query.0.mark_durable_answered(1);
            assert!(!query.resultset().is_loaded());
        }),
    );
    query.0.lose_remote_stream(1, &original);
    drop(guard);
    assert!(!query.resultset().is_loaded());
    let waiting = query.wait_durable_answered();
    tokio::pin!(waiting);
    assert!(futures::poll!(&mut waiting).is_pending());
    query.0.mark_durable_answered(1);
    assert!(futures::poll!(&mut waiting).is_pending(), "late readiness from the failed stream must be ignored");

    query.0.bind_remote_stream(1, current.clone(), &canceled);
    query.0.lose_remote_stream(1, &original);
    RemoteActivation { query: &query.0, origin: &original, canceled: &canceled, complete: true }.pre_notify(1);
    query.0.mark_durable_answered(1);
    assert!(futures::poll!(&mut waiting).is_pending());
    RemoteActivation { query: &query.0, origin: &current, canceled: &canceled, complete: true }.pre_notify(1);
    query.0.mark_durable_answered(1);
    waiting.await.unwrap();
    query.0.lose_remote_stream(1, &original);
    query.wait_durable_answered().await.unwrap();
}

#[tokio::test]
async fn a_canceled_activation_cannot_publish_readiness() {
    let query = query();
    let origin = proto::RequestId::new();
    query.0.bind_remote_stream(1, origin.clone(), &|| false);
    let canceled = || true;
    let activation = RemoteActivation { query: &query.0, origin: &origin, canceled: &canceled, complete: true };
    assert!(!activation.is_current(1));
    activation.pre_notify(1);
    query.0.mark_durable_answered(1);
    let waiting = query.wait_durable_answered();
    tokio::pin!(waiting);
    assert!(futures::poll!(&mut waiting).is_pending());
}

#[tokio::test]
async fn replacement_binding_invalidates_readiness_before_the_old_loss_callback() {
    let query = query();
    let original = proto::RequestId::new();
    let replacement = proto::RequestId::new();
    let canceled = || false;
    query.0.bind_remote_stream(1, original.clone(), &canceled);
    RemoteActivation { query: &query.0, origin: &original, canceled: &canceled, complete: true }.pre_notify(1);
    query.0.mark_durable_answered(1);
    query.wait_durable_answered().await.unwrap();
    query.0.bind_remote_stream(1, replacement.clone(), &canceled);
    query.0.lose_remote_stream(1, &original);
    let waiting = query.wait_durable_answered();
    tokio::pin!(waiting);
    assert!(futures::poll!(&mut waiting).is_pending());
    RemoteActivation { query: &query.0, origin: &replacement, canceled: &canceled, complete: true }.pre_notify(1);
    query.0.mark_durable_answered(1);
    waiting.await.unwrap();
}
