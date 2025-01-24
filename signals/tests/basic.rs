use ankurah_signals::*;
mod common;
use common::change_watcher;

#[test]
fn test_basic_signal() {
    let signal = Mut::new(42);
    // cant subscribe to a mutable signal
    // TODO figure out how we can avoid this type annotation - for some reason the compiler thinks this is an FnOnce
    // but it is not
    signal.read().subscribe(|value: &i32| println!("Read value: {}", value));
    // signal.map(|value| *value * 2).subscribe(|value| println!("Mapped value: {value}"));
    signal.set(43);
}

#[test]
fn test_observer() {
    let name: Mut<&str> = Mut::new("Buffy");
    let age: Mut<u32> = Mut::new(29);
    // let retired: Map<u32, bool> = age.map(|age| *age > 65);

    let (watcher, check) = change_watcher();
    let renderer = {
        let name = name.read();
        let age = age.read();

        Renderer::new(move || {
            let msg = format!("name: {}, age: {}", name.get(), age.get());
            println!("Render: {msg}");
            watcher(msg)
        })
    };

    println!("DEBUG: Rendering");
    renderer.render();
    println!("DEBUG: Rendered");
    println!("DEBUG: Checking");
    assert_eq!(check(), ["name: Buffy, age: 29"]); // got initial render
    println!("DEBUG: Checked");
    println!("DEBUG: No changes");
    assert_eq!(check(), [] as [&str; 0]); // no changes

    println!("DEBUG: Setting age");
    age.set(70);

    println!("DEBUG: Rendering");
    renderer.render();
    println!("DEBUG: Rendered");
    println!("DEBUG: Checking");
    assert_eq!(check(), ["name: Buffy, age: 70"]);
    println!("DEBUG: Checked");
}
