use std::{rc::Weak, sync::Arc};

pub trait IntervalGuard {
    pub timer: Weak<T>;
    pub id: u64;
}

/// Abstraction for timing mechanisms to allow both simulated and real time
pub trait Timer: Send + Sync + Clone {
    /// Returns the current time in the timer's units
    fn now(&self) -> u64;

    // /// Schedules work to be done after a delay
    fn delay(&self, duration: u64, callback: Box<dyn FnOnce() + Send + 'static>);

    // /// Sets up a recurring interval that calls the closure every interval time units
    fn set_interval(&self, closure: Box<dyn FnMut() + Send + 'static>, interval: u64) -> IntervalGuard;
}

impl Drop for IntervalGuard {
    fn drop(&mut self) {
        if let Some(timer) = self.timer.upgrade() {
            let mut intervals = timer.intervals.lock().unwrap();
            if let Some(pos) = intervals.iter().position(|(id, _, _)| *id == self.id) {
                intervals.remove(pos);
            }
        }
    }
}
