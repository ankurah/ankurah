use ankurah_signals::*;
mod common;
use common::watcher;
use tokio::{
    sync::mpsc::unbounded_channel,
    time::{Duration, timeout},
};

#[test]
fn test_basic_signal() {
    let mutable = Mut::new(42);
    let read = mutable.read();

    // closure subscription
    let (w, check) = watcher();
    let _handle = read.subscribe(w);

    mutable.set(43);
    mutable.set(44);
    assert_eq!(check(), vec![43, 44]); // Signals are only notified on updates, not initial value

    // channel subscription
    let (sender, mut receiver) = unbounded_channel();
    let _handle2 = read.subscribe(sender);

    mutable.set(45);

    // both should have received the update
    assert_eq!(check(), vec![45]);
    assert!(receiver.try_recv().is_ok(), "Should have received notification");
}

#[tokio::test]
async fn test_wait_value() {
    let mutable = Mut::new(1);
    let read = mutable.read();

    // Test immediate return when value already matches
    read.wait_value(1).await;

    // Test waiting for a future value
    let task = tokio::spawn(async move { read.wait_value(42).await });

    // Change the value after a short delay
    tokio::time::sleep(Duration::from_millis(10)).await;
    mutable.set(42);

    // The task should complete within a reasonable time
    timeout(Duration::from_millis(100), task).await.expect("wait_value should have completed within timeout").unwrap();
}

#[tokio::test]
async fn test_wait_predicate() {
    let mutable = Mut::new(1);
    let read = mutable.read();

    // Test waiting for a value matching a predicate
    let task = tokio::spawn(async move {
        read.wait_for(|v| *v > 10).await;
    });

    // Change the value to something that matches the predicate
    tokio::time::sleep(Duration::from_millis(10)).await;
    mutable.set(15);

    // The task should complete
    task.await.unwrap();
}

#[tokio::test]
async fn test_wait_for_result() {
    #[derive(Debug, Clone, PartialEq)]
    enum State {
        Loading,
        Success(String),
        Error(String),
    }

    let mutable = Mut::new(State::Loading);

    // Test waiting with Option<Result<>> return type
    let read = mutable.read();
    let success_task = tokio::spawn(async move {
        read.wait_for(|state| match state {
            State::Success(data) => Some(Ok(data.clone())),
            State::Error(msg) => Some(Err(format!("Failed: {}", msg))),
            State::Loading => None,
        })
        .await
    });

    // Change to success state
    tokio::time::sleep(Duration::from_millis(10)).await;
    mutable.set(State::Success("completed".to_string()));

    // Should get Ok result
    let result = success_task.await.unwrap();
    assert_eq!(result, Ok("completed".to_string()));

    // Test error case
    mutable.set(State::Loading); // Reset
    let read = mutable.read();
    let error_task = tokio::spawn(async move {
        read.wait_for(|state| match state {
            State::Success(data) => Some(Ok(data.clone())),
            State::Error(msg) => Some(Err(format!("Failed: {}", msg))),
            State::Loading => None,
        })
        .await
    });

    // Change to error state
    tokio::time::sleep(Duration::from_millis(10)).await;
    mutable.set(State::Error("network timeout".to_string()));

    // Should get Err result
    let result = error_task.await.unwrap();
    assert_eq!(result, Err("Failed: network timeout".to_string()));
}

#[tokio::test]
async fn test_wait_for_boolean() {
    let mutable = Mut::new(1);

    // Start a task that will wait for the condition
    let read = mutable.read();
    let task = tokio::spawn(async move {
        read.wait_for(|&value| value > 5).await;
    });

    // Change the value after a short delay
    tokio::time::sleep(Duration::from_millis(10)).await;
    mutable.set(10);

    // The task should complete within a reasonable time
    timeout(Duration::from_millis(100), task).await.expect("wait_for should complete within timeout").unwrap();
}

#[tokio::test]
async fn test_wait_for_option() {
    let mutable = Mut::new(5);
    let read = mutable.read();

    // Start a task that will wait for non-zero remainder when divided by 5
    let task = tokio::spawn(async move {
        read.wait_for(|&value| {
            let rem = value % 5;
            if rem == 0 { None } else { Some(rem) }
        })
        .await
    });

    // Change the value after a short delay
    tokio::time::sleep(Duration::from_millis(10)).await;
    mutable.set(7);

    // The task should complete and return the remainder
    let remainder = timeout(Duration::from_millis(100), task).await.expect("wait_for should complete within timeout").unwrap();

    assert_eq!(remainder, 2); // 7 % 5 = 2
}

#[tokio::test]
async fn test_wait_for_immediate_match() {
    let mutable = Mut::new(10);
    let read = mutable.read();

    // Should return immediately since condition is already met
    read.wait_for(|&value| value > 5).await;

    // Should return immediately with extracted value
    let remainder: usize = read
        .wait_for(|&value| {
            let rem = value % 7;
            if rem == 0 { None } else { Some(rem) }
        })
        .await;

    assert_eq!(remainder, 3); // 10 % 7 = 3
}
