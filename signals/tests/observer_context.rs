mod common;
use ankurah_signals::{observer::CallbackObserver, *};
use common::watcher;
use std::sync::{Arc, Mutex};

/// Test manual subscription to verify notification mechanism works
#[test]
fn test_manual_subscription_works() {
    let signal = Mut::new(42);
    let read_signal = signal.read();

    let results = Arc::new(Mutex::new(Vec::<String>::new()));

    // Manual subscription like in basic.rs test
    let results_clone = results.clone();
    let subscription_handle = read_signal.subscribe(move |value: i32| {
        results_clone.lock().unwrap().push(format!("notified: {}", value));
    });

    signal.set(100);
    std::thread::sleep(std::time::Duration::from_millis(1));

    println!("Manual subscription results: {:?}", *results.lock().unwrap());
    assert_eq!(*results.lock().unwrap(), vec!["notified: 100"]);

    drop(subscription_handle); // Clean up
}

/// Test basic Observer subscription and notification - simplified for debugging
#[test]
fn test_basic_observer_subscription() {
    let signal = Mut::new(42);
    let read_signal = signal.read();

    let results = Arc::new(Mutex::new(Vec::<String>::new()));

    let observer = {
        let results_clone = results.clone();
        CallbackObserver::new(Arc::new(move || {
            results_clone.lock().unwrap().push("observer callback triggered".to_string());
        }))
    };

    // Step 1: Initially, no context is set
    assert!(CurrentContext::current().is_none());

    // Step 2: Manually trigger observer with context to establish subscription
    observer.with_context(&|| {
        // Context should be set during callback
        assert!(CurrentContext::current().is_some());
        println!("About to call read_signal.get() within observer context...");
        let value = read_signal.get(); // This should subscribe the observer to the signal
        println!("read_signal.get() returned: {}", value);
        results.lock().unwrap().push(format!("manual read: {}", value));
    });

    // Context should be cleared after with_context
    assert!(CurrentContext::current().is_none());
    assert_eq!(*results.lock().unwrap(), vec!["manual read: 42"]);
    results.lock().unwrap().clear();

    // Step 3: Change the signal - this should trigger the observer's callback
    println!("About to set signal to 100...");
    signal.set(100);

    // Give some time for async notification
    std::thread::sleep(std::time::Duration::from_millis(10));

    println!("Results after signal change: {:?}", *results.lock().unwrap());
    assert_eq!(*results.lock().unwrap(), vec!["observer callback triggered"]);
}

/// Test multiple signals with single observer
#[test]
fn test_multiple_signals_single_observer() {
    let name = Mut::new("Alice");
    let age = Mut::new(25);
    let name_read = name.read();
    let age_read = age.read();

    let results = Arc::new(Mutex::new(Vec::<String>::new()));
    let observer = {
        let name_read_clone = name_read.clone();
        let age_read_clone = age_read.clone();
        let results_clone = results.clone();
        CallbackObserver::new(Arc::new(move || {
            let name_val = name_read_clone.get();
            let age_val = age_read_clone.get();
            results_clone.lock().unwrap().push(format!("{}: {}", name_val, age_val));
        }))
    };

    // Trigger observer to establish subscriptions
    observer.with_context(&|| {
        let name_val = name_read.get();
        let age_val = age_read.get();
        results.lock().unwrap().push(format!("init: {}: {}", name_val, age_val));
    });

    assert_eq!(*results.lock().unwrap(), vec!["init: Alice: 25"]);
    results.lock().unwrap().clear();

    // Change name - should trigger observer
    name.set("Bob");
    std::thread::sleep(std::time::Duration::from_millis(1));
    assert_eq!(*results.lock().unwrap(), vec!["Bob: 25"]);
    results.lock().unwrap().clear();

    // Change age - should also trigger observer
    age.set(30);
    std::thread::sleep(std::time::Duration::from_millis(1));
    assert_eq!(*results.lock().unwrap(), vec!["Bob: 30"]);
}

/// Test nested observer contexts (critical for React component trees)
#[test]
fn test_nested_observer_contexts() {
    let outer_signal = Mut::new("outer");
    let inner_signal = Mut::new("inner");
    let outer_read = outer_signal.read();
    let inner_read = inner_signal.read();

    let tracking_log = Arc::new(Mutex::new(Vec::<String>::new()));

    let outer_observer = {
        let tracking_log = tracking_log.clone();
        let outer_read = outer_read.clone();
        CallbackObserver::new(Arc::new(move || {
            tracking_log.lock().unwrap().push("outer callback".to_string());
        }))
    };

    let inner_observer = {
        let tracking_log = tracking_log.clone();
        let inner_read = inner_read.clone();
        CallbackObserver::new(Arc::new(move || {
            tracking_log.lock().unwrap().push("inner callback".to_string());
        }))
    };

    // Test nested context setup and restoration
    CurrentContext::set(outer_observer.clone());
    assert!(CurrentContext::current().is_some());

    // Access outer signal - should subscribe to outer observer
    outer_read.get();

    // Nest inner observer context
    CurrentContext::set(inner_observer.clone());

    // Access inner signal - should subscribe to inner observer (not outer)
    inner_read.get();

    // Restore outer context
    CurrentContext::unset();

    // Context should be restored to outer observer
    assert!(CurrentContext::current().is_some());

    // Clean up
    CurrentContext::unset();
    assert!(CurrentContext::current().is_none());

    // Test that signal changes trigger correct observers
    outer_signal.set("outer_changed");
    inner_signal.set("inner_changed");

    // Give callbacks time to execute
    std::thread::sleep(std::time::Duration::from_millis(1));

    let log = tracking_log.lock().unwrap();
    assert!(log.contains(&"outer callback".to_string()));
    assert!(log.contains(&"inner callback".to_string()));
}

/// Test context restoration with multiple nesting levels (React tree scenario)
#[test]
fn test_deep_nested_context_restoration() {
    let signals: Vec<Mut<i32>> = (0..5).map(|i| Mut::new(i)).collect();
    let reads: Vec<Read<i32>> = signals.iter().map(|s| s.read()).collect();

    let observers: Vec<CallbackObserver> = (0..5)
        .map(|i| {
            let read = reads[i].clone();
            CallbackObserver::new(Arc::new(move || {
                read.get(); // Subscribe this observer to this signal
            }))
        })
        .collect();

    // Build nested context stack: 0 -> 1 -> 2 -> 3 -> 4
    for i in 0..5 {
        CurrentContext::set(observers[i].clone());
        reads[i].get(); // Subscribe each observer to its signal

        // Verify current context is correct
        assert!(CurrentContext::current().is_some());
    }

    // Now unwind the stack - each unset should restore previous context
    for i in (0..5).rev() {
        CurrentContext::unset();

        if i > 0 {
            // Should still have a context (the previous one)
            assert!(CurrentContext::current().is_some());
        } else {
            // Final unset should leave no context
            assert!(CurrentContext::current().is_none());
        }
    }
}

/// Test observer cleanup and subscription handle management
#[test]
fn test_observer_cleanup() {
    let signal = Mut::new("test");
    let read_signal = signal.read();

    let notification_count = Arc::new(Mutex::new(0));

    let observer = {
        let notification_count = notification_count.clone();
        let read_signal = read_signal.clone();
        CallbackObserver::new(Arc::new(move || {
            read_signal.get(); // Re-subscribe on each notification
            *notification_count.lock().unwrap() += 1;
        }))
    };

    // Establish subscription
    observer.with_context(&|| {
        read_signal.get();
    });

    // Change signal - should trigger notification
    signal.set("changed1");
    std::thread::sleep(std::time::Duration::from_millis(1));
    assert_eq!(*notification_count.lock().unwrap(), 1);

    // Clear observer subscriptions
    observer.clear();

    // Change signal again - should NOT trigger notification
    signal.set("changed2");
    std::thread::sleep(std::time::Duration::from_millis(1));
    assert_eq!(*notification_count.lock().unwrap(), 1); // Should still be 1
}

/// Test that with_context properly clears previous subscriptions
#[test]
fn test_context_subscription_clearing() {
    let signal1 = Mut::new(1);
    let signal2 = Mut::new(2);
    let read1 = signal1.read();
    let read2 = signal2.read();

    let notification_count = Arc::new(Mutex::new(0));

    let observer = {
        let notification_count = notification_count.clone();
        CallbackObserver::new(Arc::new(move || {
            *notification_count.lock().unwrap() += 1;
        }))
    };

    // First context - subscribe to signal1 only
    observer.with_context(&|| {
        read1.get();
    });

    // Change signal1 - should trigger
    signal1.set(10);
    std::thread::sleep(std::time::Duration::from_millis(1));
    assert_eq!(*notification_count.lock().unwrap(), 1);

    // Second context - should clear previous subscriptions and subscribe to signal2
    observer.with_context(&|| {
        read2.get();
    });

    // Change signal1 - should NOT trigger (subscription was cleared)
    signal1.set(20);
    std::thread::sleep(std::time::Duration::from_millis(1));
    assert_eq!(*notification_count.lock().unwrap(), 1); // Still 1

    // Change signal2 - should trigger
    signal2.set(30);
    std::thread::sleep(std::time::Duration::from_millis(1));
    assert_eq!(*notification_count.lock().unwrap(), 2);
}

/// Test try/finally pattern that React will use  
#[test]
fn test_react_style_try_finally_pattern() {
    let signal = Mut::new("react_test");
    let read_signal = signal.read();

    let results = Arc::new(Mutex::new(Vec::<String>::new()));

    let observer = {
        let read_signal_clone = read_signal.clone();
        let results_clone = results.clone();
        CallbackObserver::new(Arc::new(move || {
            let value = read_signal_clone.get();
            results_clone.lock().unwrap().push(format!("react: {}", value));
        }))
    };

    // Simulate React useObserve pattern
    let simulate_react_component = || -> Result<String, &'static str> {
        CurrentContext::set(observer.clone());

        let result = {
            // This is where React component would render
            let value = read_signal.get();
            Ok(format!("rendered: {}", value))
        };

        // This must happen even if the component throws
        CurrentContext::unset();

        result
    };

    // Initial render
    let render_result = simulate_react_component().unwrap();
    assert_eq!(render_result, "rendered: react_test");
    assert!(CurrentContext::current().is_none()); // Context should be cleaned up

    // Change signal - should trigger observer
    signal.set("updated");
    std::thread::sleep(std::time::Duration::from_millis(1));
    assert_eq!(*results.lock().unwrap(), vec!["react: updated"]);
}
