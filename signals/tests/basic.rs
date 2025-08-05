use ankurah_signals::*;
mod common;
use common::watcher;
use std::sync::mpsc;
use tokio::time::{Duration, timeout};

#[tokio::test]
async fn test_basic_signal() {
    let mutable = Mut::new(42);
    let read = mutable.read();

    // closure subscription
    let (w, check) = watcher();
    let _handle = read.subscribe(w);

    mutable.set(43);
    tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
    mutable.set(44);

    // Sleep to allow async notification propagation
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

    assert_eq!(check(), vec![43, 44]); // Signals are only notified on updates, not initial value

    // channel subscription
    let (sender, receiver) = mpsc::channel();
    let _handle2 = read.subscribe(sender);

    mutable.set(45);

    // Sleep to allow async notification propagation
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

    // Channel should have received the update
    assert!(receiver.try_recv().is_ok(), "Should have received notification");
}

#[tokio::test]
async fn test_basic_subscriber() {
    let mutable = Mut::new(42);
    let read = mutable.read();

    let handle = read.subscribe(|value: i32| {
        println!("Signal value changed to: {}", value);
    });

    mutable.set(43);

    drop(handle); // unsubscribe
}

#[cfg(feature = "tokio")]
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

#[tokio::test]
async fn test_map_signal() {
    let mutable = Mut::new(10);
    let mapped = Map::new(mutable.read(), |x| *x * 2);

    // Test With trait
    mapped.with(|val| assert_eq!(*val, 20));

    // Test Get trait (requires Clone on Output)
    assert_eq!(mapped.get(), 20);

    // Test subscription
    let (w, check) = watcher();
    let _handle = mapped.subscribe(w);

    mutable.set(15);
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

    mutable.set(20);
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

    // Should receive transformed values
    assert_eq!(check(), vec![30, 40]); // 15*2=30, 20*2=40
}

#[tokio::test]
async fn test_map_signal_string_transform() {
    let mutable = Mut::new(5);
    let mapped = Map::new(mutable.read(), |x| format!("Value: {}", x));

    // Test With trait with type transformation
    mapped.with(|val| assert_eq!(val, "Value: 5"));

    // Test subscription with type transformation
    let (sender, receiver) = mpsc::channel();
    let _handle = mapped.subscribe(sender);

    mutable.set(10);
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

    mutable.set(15);
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

    // Should receive transformed string values
    let value1 = receiver.recv().unwrap();
    let value2 = receiver.recv().unwrap();
    assert_eq!(value1, "Value: 10");
    assert_eq!(value2, "Value: 15");
}

#[tokio::test]
async fn test_read_map_convenience_method() {
    let mutable = Mut::new(100);
    let read = mutable.read();

    // Use the convenience method to create a mapped signal
    let doubled = read.map(|x| *x * 2);
    let stringified = read.map(|x| format!("Number: {}", x));

    // Test both mapped signals
    doubled.with(|val| assert_eq!(*val, 200));
    stringified.with(|val| assert_eq!(val, "Number: 100"));

    // Test subscription on mapped signals
    let (sender1, receiver1) = mpsc::channel();
    let (sender2, receiver2) = mpsc::channel();
    let _handle1 = doubled.subscribe(sender1);
    let _handle2 = stringified.subscribe(sender2);

    mutable.set(50);
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

    // Should receive transformed values
    assert_eq!(receiver1.recv().unwrap(), 100); // 50 * 2
    assert_eq!(receiver2.recv().unwrap(), "Number: 50");
}
