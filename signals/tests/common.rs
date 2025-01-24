use std::sync::{Arc, Mutex};

pub fn watcher<T: Clone + Send + 'static>() -> (Box<dyn Fn(&T) + Send + Sync>, Box<dyn Fn() -> Vec<T> + Send + Sync>) {
    let values = Arc::new(Mutex::new(Vec::new()));
    let accumulate = {
        let values = values.clone();
        Box::new(move |value: &T| {
            values.lock().unwrap().push(value.clone());
        })
    };

    let check = Box::new(move || values.lock().unwrap().drain(..).collect());

    (accumulate, check)
}
