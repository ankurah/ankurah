use std::sync::{Arc, Mutex};

#[allow(unused)]
pub fn change_watcher<T: Send + Sync + 'static>() -> (Box<dyn Fn(T) + Send + Sync>, Box<dyn Fn() -> Vec<T> + Send + Sync>) {
    let changes = Arc::new(Mutex::new(Vec::new()));
    let watcher = {
        let changes = changes.clone();
        Box::new(move |value: T| {
            changes.lock().unwrap().push(value);
        })
    };

    let check = Box::new(move || {
        let changes: Vec<T> = changes.lock().unwrap().drain(..).collect();
        changes
    });

    (watcher, check)
}
