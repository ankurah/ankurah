//! Swift polls the future of an exported async call from its own threads, which run no Tokio
//! runtime, while the SQLite engine needs one: an export marked `async_runtime = "tokio"` polls its
//! future inside one. These tests call the exported C symbols as UniFFI's Swift bindings do,
//! polling from the test thread outside any runtime, so each fails if its export loses that
//! attribute.

mod common;

use std::{
    ffi::c_void,
    mem::ManuallyDrop,
    sync::{mpsc, Arc},
    time::Duration,
};

use ankurah::{transaction::Transaction, Context};
use ankurah_tests_uniffi::{Artist, ArtistInput, ArtistOps, ArtistRef, ArtistView, UniFfiTag};
use anyhow::{anyhow, Context as _, Result};
use common::context;
use tokio::runtime::Runtime;
use uniffi::{Handle, Lift, Lower, RustCallStatus, RustCallStatusCode, RustFutureContinuationCallback, RustFuturePoll};

// The scaffolding's symbols for the calls under test, as Swift's generated C header declares them
extern "C" {
    fn uniffi_ankurah_core_fn_method_transaction_commit(transaction: *const c_void) -> Handle;
    fn ffi_ankurah_core_rust_future_poll_void(handle: Handle, callback: RustFutureContinuationCallback, data: u64);
    fn ffi_ankurah_core_rust_future_complete_void(handle: Handle, out_status: &mut RustCallStatus);
    fn ffi_ankurah_core_rust_future_free_void(handle: Handle);

    fn uniffi_ankurah_tests_uniffi_fn_method_artistref_get(artist_ref: *const c_void, ctx: *const c_void) -> Handle;
    fn ffi_ankurah_tests_uniffi_rust_future_poll_pointer(handle: Handle, callback: RustFutureContinuationCallback, data: u64);
    fn ffi_ankurah_tests_uniffi_rust_future_complete_pointer(handle: Handle, out_status: &mut RustCallStatus) -> *const c_void;
    fn ffi_ankurah_tests_uniffi_rust_future_free_pointer(handle: Handle);
}

#[test]
fn commit_polled_outside_a_runtime_writes_to_sqlite() -> Result<()> {
    let runtime = Runtime::new()?;
    let ctx = runtime.block_on(context())?;
    let trx = Arc::new(ctx.begin());
    runtime.block_on(trx.create(&Artist { name: "Radiohead".into() }))?;

    unsafe {
        let commit = uniffi_ankurah_core_fn_method_transaction_commit(<Arc<Transaction> as Lower<ankurah::core::UniFfiTag>>::lower(trx));
        poll_to_completion(
            commit,
            ffi_ankurah_core_rust_future_poll_void,
            ffi_ankurah_core_rust_future_complete_void,
            ffi_ankurah_core_rust_future_free_void,
        )?;
    }

    // Fetching reads storage, so it finds the artist only if the commit wrote it to SQLite
    let artists = runtime.block_on(ctx.fetch::<ArtistView>("name = 'Radiohead'"))?;
    assert_eq!(artists.len(), 1);
    Ok(())
}

#[test]
fn ref_get_polled_outside_a_runtime_reads_from_sqlite() -> Result<()> {
    let runtime = Runtime::new()?;
    let ctx = runtime.block_on(context())?;
    let artist = runtime.block_on(ArtistOps::new().create_one(&ctx, ArtistInput { name: "Radiohead".into() }))?;
    let id = artist.id();
    let artist_ref = Arc::new(ArtistRef::from_view(&artist));
    // The ref holds only the id, so once the view is dropped the node keeps no copy of the artist
    // and get must read it from SQLite
    drop(artist);

    let artist = unsafe {
        let get = uniffi_ankurah_tests_uniffi_fn_method_artistref_get(
            <Arc<ArtistRef> as Lower<UniFfiTag>>::lower(artist_ref),
            <Arc<Context> as Lower<UniFfiTag>>::lower(Arc::new(ctx.clone())),
        );
        let view = poll_to_completion(
            get,
            ffi_ankurah_tests_uniffi_rust_future_poll_pointer,
            ffi_ankurah_tests_uniffi_rust_future_complete_pointer,
            ffi_ankurah_tests_uniffi_rust_future_free_pointer,
        )?;
        <Arc<ArtistView> as Lift<UniFfiTag>>::try_lift(view)?
    };
    assert_eq!(artist.id(), id);
    assert_eq!(artist.name()?, "Radiohead");
    Ok(())
}

/// Drives the future of an exported async call to its output as UniFFI's Swift bindings do: poll
/// it, wait for the continuation the poll hands it, and poll again until the continuation reports
/// it ready; then complete it and free it.
///
/// # Safety
/// `poll`, `complete` and `free` must be the scaffolding's functions for the FFI type the call
/// returns, from the crate that exports the call, and `future` must not have been freed.
unsafe fn poll_to_completion<T>(
    future: Handle,
    poll: unsafe extern "C" fn(Handle, RustFutureContinuationCallback, u64),
    complete: unsafe extern "C" fn(Handle, &mut RustCallStatus) -> T,
    free: unsafe extern "C" fn(Handle),
) -> Result<T> {
    assert!(tokio::runtime::Handle::try_current().is_err(), "a foreign thread polls outside any Tokio runtime");
    let (sender, outcomes) = mpsc::channel();
    loop {
        poll(future, continuation, Box::into_raw(Box::new(sender.clone())) as u64);
        match outcomes.recv_timeout(Duration::from_secs(30)).context("the future never called its continuation")? {
            RustFuturePoll::Ready => break,
            RustFuturePoll::MaybeReady => {}
        }
    }
    let mut status = RustCallStatus::default();
    let output = complete(future, &mut status);
    free(future);
    match status.code {
        RustCallStatusCode::Success => Ok(output),
        _ => Err(failure(status)),
    }
}

/// The continuation the scaffolding calls once per poll, from whichever thread wakes the future,
/// to tell the polling thread whether to poll again
extern "C" fn continuation(data: u64, outcome: RustFuturePoll) {
    let sender = unsafe { Box::from_raw(data as *mut mpsc::Sender<RustFuturePoll>) };
    // The send fails only when the polling thread has stopped waiting, having timed out
    let _ = sender.send(outcome);
}

/// What a failed call's status reports: a panic's message, or the Display text that a flat error
/// such as MutationError carries after its variant index
fn failure(status: RustCallStatus) -> anyhow::Error {
    let error_buf = ManuallyDrop::into_inner(status.error_buf);
    let message = match status.code {
        RustCallStatusCode::UnexpectedError => <String as Lift<UniFfiTag>>::try_lift(error_buf),
        RustCallStatusCode::Error => {
            let error = error_buf.destroy_into_vec();
            let mut error = error.as_slice();
            <i32 as Lift<UniFfiTag>>::try_read(&mut error).and_then(|_variant| <String as Lift<UniFfiTag>>::try_read(&mut error))
        }
        code => return anyhow!("the call ended {code:?}"),
    };
    match message {
        Ok(message) => anyhow!("the call ended {:?}: {message}", status.code),
        Err(error) => error.context("the call's error buffer holds no message"),
    }
}
