use ankurah_signals::*;
mod common;
use common::change_watcher;
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
    let name: Mut<&str> = Mut::new("Buffy");
    let age: Mut<u32> = Mut::new(29);
    // let retired: Map<u32, bool> = age.map(|age| *age > 65);

    let (watcher, check) = change_watcher();
    let render = {
        let name = name.read();
        let age = age.read();

        Arc::new(move || {
            let msg = format!("name: {}, age: {}", name.get(), age.get());
            println!("Render: {msg}");
            watcher(msg)
        })
    };

    let observer = Observer::new(render.clone());
    CurrentContext::set(&observer);
    render(); // initial render
    CurrentContext::unset();

    assert_eq!(check(), ["name: Buffy, age: 29"]); // got initial render
    assert_eq!(check(), [] as [&str; 0]); // no changes

    age.set(70);

    assert_eq!(check(), ["name: Buffy, age: 70"]);
}
