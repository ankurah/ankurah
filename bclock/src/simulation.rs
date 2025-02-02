use rand::prelude::*;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};

use crate::timer::{IntervalGuard, Timer};
use crate::{Node, PING_INTERVAL, Position};

pub struct Simulation {
    pub nodes: Vec<Node<SimulatedTimer>>,
    pub timer: SimulatedTimer,
    pub rng: StdRng,
}

/// A simulated timer that uses fake time units
pub struct SimulatedTimer {
    current_time: Arc<AtomicU64>,
    next_ticks: Arc<Mutex<Vec<(u64, Box<dyn FnOnce() + Send + 'static>)>>>,
    intervals: Arc<Mutex<Vec<(u64, u64, Box<dyn FnMut() + Send + 'static>)>>>, // (id, next_tick, closure)
    next_interval_id: Arc<AtomicU64>,
}

impl Simulation {
    pub fn new(seed: u64) -> Self {
        let mut rng = StdRng::seed_from_u64(seed);
        let timer = SimulatedTimer::new();
        Self { nodes: Vec::new(), timer, rng }
    }

    /// Creates a deterministic set of nodes for testing.
    /// Uses a fixed seed unless specified, ensuring reproducible test scenarios.
    pub fn create_test_nodes(&mut self, count: usize) {
        let rng = &mut self.rng;
        let timer = &self.timer;
        self.nodes = (0..count)
            .map(|_| {
                let x = rng.gen_range(-500..500);
                let y = rng.gen_range(-500..500);
                let z = rng.gen_range(-500..500);
                let position = Position { x, y, z };
                let start_time = rng.gen_range(0..PING_INTERVAL);
                Node::new(position, start_time, timer.clone())
            })
            .collect()
    }

    /// Runs the simulation for the specified duration
    pub fn run(&mut self, duration: u64) -> usize {
        // Just advance time and let nodes handle their own scheduling
        while self.timer.now() < duration {
            if !self.timer.advance() {
                break;
            }
        }

        // Count total events by checking counters
        self.nodes.iter().map(|n| n.last_counter).sum::<u64>() as usize
    }
}

impl SimulatedTimer {
    pub fn new() -> Self {
        Self {
            current_time: Arc::new(AtomicU64::new(0)),
            next_ticks: Arc::new(Mutex::new(Vec::new())),
            intervals: Arc::new(Mutex::new(Vec::new())),
            next_interval_id: Arc::new(AtomicU64::new(0)),
        }
    }

    fn schedule_at(&self, at: u64, callback: Box<dyn FnOnce() + Send + 'static>) {
        let mut ticks = self.next_ticks.lock().unwrap();
        ticks.push((at, callback));
    }

    /// Advances time to the next scheduled tick
    pub fn advance(&self) -> bool {
        let mut ticks = self.next_ticks.lock().unwrap();
        let mut intervals = self.intervals.lock().unwrap();

        // Combine one-time ticks and interval ticks
        let mut all_ticks = ticks.iter().map(|(t, _)| *t).chain(intervals.iter().map(|(_, t, _)| *t)).collect::<Vec<_>>();

        if all_ticks.is_empty() {
            return false;
        }

        all_ticks.sort_unstable();
        let next_time = all_ticks[0];
        self.current_time.store(next_time, Ordering::SeqCst);

        // Handle one-time callbacks
        let mut i = 0;
        while i < ticks.len() && ticks[i].0 == next_time {
            i += 1;
        }
        let one_time_callbacks = ticks.drain(0..i).map(|(_, cb)| cb).collect::<Vec<_>>();

        // Handle interval callbacks and collect them
        let mut interval_callbacks = Vec::new();
        for (_, next_tick, closure) in intervals.iter_mut() {
            if *next_tick == next_time {
                // We can't call the closure here because we're holding the lock
                interval_callbacks.push((closure as *mut Box<dyn FnMut() + Send + 'static>));
                *next_tick += super::PING_INTERVAL;
            }
        }

        // Release locks before executing callbacks
        drop(ticks);
        drop(intervals);

        // Execute callbacks
        for callback in one_time_callbacks {
            callback();
        }
        // Now safe to call the interval callbacks
        for closure_ptr in interval_callbacks {
            unsafe {
                (&mut **closure_ptr)();
            }
        }

        true
    }
}

impl Timer for SimulatedTimer {
    fn now(&self) -> u64 { self.current_time.load(Ordering::SeqCst) }

    fn delay(&self, duration: u64, callback: Box<dyn FnOnce() + Send + 'static>) { self.schedule_at(self.now() + duration, callback) }

    fn set_interval(&self, closure: Box<dyn FnMut() + Send + 'static>, interval: u64) -> IntervalGuard {
        let id = self.next_interval_id.fetch_add(1, Ordering::Relaxed);
        let mut intervals = self.intervals.lock().unwrap();
        intervals.push((id, self.now() + interval, closure));
        IntervalGuard { timer: Arc::downgrade(&self) as Weak<dyn Timer>, id }
    }
}

impl Clone for SimulatedTimer {
    fn clone(&self) -> Self {
        Self {
            current_time: Arc::clone(&self.current_time),
            next_ticks: Arc::clone(&self.next_ticks),
            intervals: Arc::clone(&self.intervals),
            next_interval_id: Arc::clone(&self.next_interval_id),
        }
    }
}

// TODO create tests for the simulator and simulated timer
