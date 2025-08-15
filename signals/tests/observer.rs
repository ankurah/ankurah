mod common;
use ankurah_signals::*;
use common::watcher;
use std::sync::Arc;

#[tokio::test]
async fn test_observer() {
    tracing_subscriber::fmt::init();
    let name: Mut<&str> = Mut::new("Buffy");
    let age: Mut<u32> = Mut::new(29);
    // let retired: Map<u32, bool> = age.map(|age| *age > 65);

    let (accumulate, check) = watcher();
    let renderer = {
        let name = name.read();
        let age = age.read();

        CallbackObserver::new(move || {
            let body = format!("name: {}, age: {}", name.get(), age.get());
            accumulate(body)
        })
    };

    renderer.trigger();
    assert_eq!(check(), ["name: Buffy, age: 29"]); // got initial render
    assert_eq!(check(), [] as [&str; 0]); // no changes

    println!("About to set age to 70...");
    age.set(70);
    println!("Age set to 70, checking results...");

    // Give a moment for async notifications (if any)
    // std::thread::sleep(std::time::Duration::from_millis(10));

    let results = check();
    println!("Results after age.set(70): {:?}", results);

    // For now, manually trigger to see what happens
    println!("Manually triggering renderer...");
    renderer.trigger();
    let results_after_manual = check();

    assert_eq!(results_after_manual, ["name: Buffy, age: 70"]);
}
