mod common;
use ankurah_signals::*;
use common::watcher;

#[test]
fn test_observer() {
    let name: Mut<&str> = Mut::new("Buffy");
    let age: Mut<u32> = Mut::new(29);
    // let retired: Map<u32, bool> = age.map(|age| *age > 65);

    let (accumulate, check) = watcher();
    let renderer = {
        let name = name.read();
        let age = age.read();

        Renderer::new(move || {
            let body = format!("name: {}, age: {}", name.get(), age.get());
            println!("Render: {body}");
            accumulate(&body)
        })
    };

    renderer.render();
    assert_eq!(check(), ["name: Buffy, age: 29"]); // got initial render
    assert_eq!(check(), [] as [&str; 0]); // no changes

    age.set(70);

    // LEFT OFF HERE for observer - audit internals
    // this .render() should not be necessary - so something is wrong with the tracking logic (probably just never finished it)
    // be sure to validate that signals are un-tracked at the start of each render, and re-tracked based on usage
    // so that we are only observing signals relevant to the current conditional state
    // eg:
    // Renderer::new(move || {
    //     let doc = if(age.get() < 18) {
    //       // render function is not called subsequently if name changes
    //       "Jane Doe (minor)".to_string();
    //     }else{
    //        // name and age are observed
    //        format!("name: {}, age: {}", name.get(), age.get())
    //     }
    //    accumulate(doc);
    // });
    renderer.render();
    assert_eq!(check(), ["name: Buffy, age: 70"]);
}
