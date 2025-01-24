use ankurah_signals::*;
mod common;
use common::changeset_watcher;
use std::sync::Arc;

// #[test]
// fn test_basic_signal() {
//     let signal = MutableSignal::new(42);
//     let (watcher, check) = changeset_watcher();

//     {
//         let watcher = watcher.clone();
//         signal.read().subscribe(move |value| watcher(format!("Read value: {}", value)));
//     }
//     {
//         let watcher = watcher.clone();
//         signal.map(|value| *value * 2).subscribe(move |value| watcher(format!("Mapped value: {}", value)));
//     }

//     assert_eq!(check(), vec!["Read value: 42".to_string(), "Mapped value: 84".to_string(),]);

//     signal.set(43);
//     assert_eq!(check(), vec!["Read value: 43".to_string(), "Mapped value: 86".to_string(),]);
// }

#[test]
fn test_observer() {
    let name = Mut::new("Buffy".to_string());
    let age = Mut::new(29);
    let retired = age.map(|age| *age > 65);

    let (watcher, check) = changeset_watcher();
    let render = {
        println!("Creating render closure");
        let name = name.read();
        let age = age.read();
        Arc::new(move || {
            println!("Render called");
            let msg = format!("name: {name}, age: {age}, retired: {retired}");
            println!("Formatted: {msg}");
            watcher(msg)
        })
    };

    let observer = Observer::new(render.clone());
    println!("Setting observer");
    observer.set();
    println!("Calling render");
    render();
    println!("Unsetting observer");
    observer.unset();

    println!("Checking first result");
    let result = check();
    println!("First result: {:?}", result);
    assert_eq!(result, vec!["name: Buffy, age: 29, retired: false".to_string(),]);

    println!("Setting age to 70");
    age.set(70);
    println!("Checking second result");
    let result = check();
    println!("Second result: {:?}", result);
    assert_eq!(result, vec!["name: Buffy, age: 70, retired: true".to_string(),]);
}
