use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
/// A simulated timer that uses fake time units
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

enum TimerEntry {
    OneShot { fire_time: u64, callback: Box<dyn FnOnce() + Send> },
    Interval { fire_time: u64, interval: u64, id: u64, cancelled: Arc<AtomicBool>, callback: Box<dyn FnOnce() + Send> },
}

impl TimerEntry {
    fn fire_time(&self) -> u64 {
        match self {
            TimerEntry::OneShot { fire_time, .. } => *fire_time,
            TimerEntry::Interval { fire_time, .. } => *fire_time,
        }
    }
}

pub struct IntervalGuard {
    cancelled: Arc<AtomicBool>,
}

impl Drop for IntervalGuard {
    fn drop(&mut self) { self.cancelled.store(true, Ordering::SeqCst); }
}

const WHEEL_SIZE: usize = 256;

pub struct SimulatedTimer {
    current_time: u64,
    wheel: Arc<Mutex<Vec<Vec<TimerEntry>>>>,
    next_interval_id: u64,
}

impl SimulatedTimer {
    pub fn new() -> Self {
        let mut wheel = Vec::with_capacity(WHEEL_SIZE);
        for _ in 0..WHEEL_SIZE {
            wheel.push(Vec::new());
        }
        Self { current_time: 0, wheel: Arc::new(Mutex::new(wheel)), next_interval_id: 0 }
    }

    pub fn now(&self) -> u64 { self.current_time }

    pub fn schedule_at(&self, time: u64, callback: Box<dyn FnOnce() + Send>) {
        let entry = TimerEntry::OneShot { fire_time: time, callback };
        let slot = (time % WHEEL_SIZE as u64) as usize;
        let mut wheel = self.wheel.lock().unwrap();

        // Binary search for insertion point in this slot
        let pos = wheel[slot].binary_search_by_key(&time, |entry| entry.fire_time()).unwrap_or_else(|e| e);

        wheel[slot].insert(pos, entry);
    }

    pub fn set_interval(&mut self, interval: u64, callback: Box<dyn FnOnce() + Send>) -> IntervalGuard {
        let interval_id = self.next_interval_id;
        self.next_interval_id += 1;

        let cancelled = Arc::new(AtomicBool::new(false));
        let start_time = self.current_time;
        let slot = (start_time % WHEEL_SIZE as u64) as usize;

        let entry = TimerEntry::Interval { fire_time: start_time, interval, id: interval_id, cancelled: cancelled.clone(), callback };

        let mut wheel = self.wheel.lock().unwrap();

        // Binary search for insertion point in this slot
        let pos = wheel[slot].binary_search_by_key(&start_time, |entry| entry.fire_time()).unwrap_or_else(|e| e);

        wheel[slot].insert(pos, entry);

        IntervalGuard { cancelled }
    }

    pub fn next_tick(&self) -> Option<u64> {
        let wheel = self.wheel.lock().unwrap();
        let current_slot = (self.current_time % WHEEL_SIZE as u64) as usize;

        // Check from current slot to end
        for slot in current_slot..WHEEL_SIZE {
            if !wheel[slot].is_empty() {
                let slot_time = (self.current_time / WHEEL_SIZE as u64) * WHEEL_SIZE as u64 + slot as u64;
                if slot_time >= self.current_time {
                    return Some(slot_time);
                }
            }
        }

        // Check from start to current slot
        for slot in 0..current_slot {
            if !wheel[slot].is_empty() {
                let slot_time = ((self.current_time / WHEEL_SIZE as u64) + 1) * WHEEL_SIZE as u64 + slot as u64;
                return Some(slot_time);
            }
        }

        None
    }

    pub fn advance(&mut self, not_after: Option<u64>) -> bool {
        let mut did_work = false;
        let target_time = not_after.unwrap_or(u64::MAX);

        while self.current_time <= target_time {
            let current_slot = (self.current_time % WHEEL_SIZE as u64) as usize;
            let mut wheel = self.wheel.lock().unwrap();

            // Since entries are sorted, we can check the first one
            if let Some(entry) = wheel[current_slot].first() {
                let fire_time = entry.fire_time();
                if fire_time > self.current_time {
                    // Skip ahead to next fire time
                    self.current_time = fire_time;
                    continue;
                }
            }

            let entries = std::mem::take(&mut wheel[current_slot]);

            for entry in entries {
                if entry.fire_time() == self.current_time {
                    match entry {
                        TimerEntry::OneShot { callback, .. } => {
                            (callback)();
                            did_work = true;
                        }
                        TimerEntry::Interval { interval, id, callback, cancelled, .. } => {
                            if !cancelled.load(Ordering::SeqCst) {
                                (callback)();
                                did_work = true;

                                let next_fire = self.current_time + interval;
                                if next_fire <= target_time {
                                    let next_slot = (next_fire % WHEEL_SIZE as u64) as usize;
                                    let next_entry = TimerEntry::Interval {
                                        fire_time: next_fire,
                                        interval,
                                        id,
                                        cancelled: cancelled.clone(),
                                        callback: Box::new(|| {}), // TODO: Still need to handle interval callbacks
                                    };

                                    // Binary search in next slot
                                    let pos =
                                        wheel[next_slot].binary_search_by_key(&next_fire, |entry| entry.fire_time()).unwrap_or_else(|e| e);

                                    wheel[next_slot].insert(pos, next_entry);
                                }
                            }
                        }
                    }
                } else if entry.fire_time() > self.current_time {
                    // Put back entries that fire later
                    let pos = wheel[current_slot].binary_search_by_key(&entry.fire_time(), |e| e.fire_time()).unwrap_or_else(|e| e);
                    wheel[current_slot].insert(pos, entry);
                }
            }

            self.current_time += 1;

            if did_work {
                break;
            }
        }

        did_work
    }

    pub fn delay(&self, duration: u64, callback: Box<dyn FnOnce() + Send>) { self.schedule_at(self.now() + duration, callback) }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    #[test]
    fn test_one_time_events() {
        let mut timer = SimulatedTimer::new();
        let counter: Arc<AtomicU64> = Arc::new(AtomicU64::new(0));
        let counter_clone = counter.clone();

        // Schedule one-time event at t=5
        timer.schedule_at(
            5,
            Box::new(move || {
                counter_clone.fetch_add(1, Ordering::SeqCst);
            }),
        );

        // Initially no work done
        assert_eq!(counter.load(Ordering::SeqCst), 0);
        assert_eq!(timer.next_tick(), Some(5));

        // Run to t=4, still no work
        timer.advance(Some(4));
        assert_eq!(counter.load(Ordering::SeqCst), 0);
        assert_eq!(timer.next_tick(), Some(5));

        // Run to t=5, work should be done
        timer.advance(Some(5));
        assert_eq!(counter.load(Ordering::SeqCst), 1);
        assert_eq!(timer.next_tick(), None);

        // Run to t=6, no more work
        timer.advance(Some(6));
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_multiple_one_time_events() {
        let mut timer = SimulatedTimer::new();
        let counter: Arc<AtomicU64> = Arc::new(AtomicU64::new(0));

        // Schedule multiple events
        for t in [5, 7, 10] {
            let counter_clone = counter.clone();
            timer.schedule_at(
                t,
                Box::new(move || {
                    counter_clone.fetch_add(1, Ordering::SeqCst);
                }),
            );
        }

        // Run to t=6, should trigger first event only
        timer.advance(Some(6));
        assert_eq!(counter.load(Ordering::SeqCst), 1);

        // Run to t=12, should trigger remaining events
        timer.advance(Some(12));
        assert_eq!(counter.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn test_intervals() {
        let mut timer = SimulatedTimer::new();
        let counter: Arc<AtomicU64> = Arc::new(AtomicU64::new(0));
        let counter_clone = counter.clone();

        // Set interval of 3 time units
        let _guard = timer.set_interval(
            3,
            Box::new(move || {
                counter_clone.fetch_add(1, Ordering::SeqCst);
            }),
        );

        // Initially no work done
        assert_eq!(counter.load(Ordering::SeqCst), 0);
        assert_eq!(timer.next_tick(), Some(0)); // First tick at start

        // Run to t=2, still no work
        timer.advance(Some(2));
        assert_eq!(counter.load(Ordering::SeqCst), 0);

        // Run to t=3, should have one tick
        timer.advance(Some(3));
        assert_eq!(counter.load(Ordering::SeqCst), 1);

        // Run to t=6, should have another tick
        timer.advance(Some(6));
        assert_eq!(counter.load(Ordering::SeqCst), 2);

        // Drop guard, interval should stop
        drop(_guard);
        timer.advance(Some(9));
        assert_eq!(counter.load(Ordering::SeqCst), 2);
    }
}
