//! Swarm-style fault configuration.
//!
//! The testing lit review (Groce et al., ISSTA 2012; adopted as delta C1-C)
//! found that enabling every fault kind on every run *suppresses* bugs: a run
//! that drops everything hides a reordering bug, a run that reorders
//! everything can mask a duplication bug. Each seed therefore enables a random
//! *subset* of the fault kinds, and the subset is part of the reproducible
//! configuration printed on failure.

use super::rng::SimRng;

/// Which fault kinds are live for a run, plus their tuning knobs. Derived from
/// the seed; printed verbatim in the seeded-failure artifact so a hit
/// reproduces exactly.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FaultConfig {
    /// Permute the order in which in-flight messages are delivered.
    pub reorder: bool,
    /// Hold some messages back for a later delivery round (delay).
    pub delay: bool,
    /// Deliver some messages more than once (at-least-once redelivery).
    pub duplicate: bool,
    /// Silently drop some messages. Convergence still requires the origin's
    /// events to reach every node, so the harness re-offers dropped
    /// *propagation* updates until delivered before quiescence (drops model
    /// transient loss, not permanent data loss); node-emitted acks may be
    /// dropped outright since they are advisory.
    pub drop: bool,
    /// Cut links between node pairs for a while, then heal them.
    pub partition: bool,

    /// Probability an individual eligible message is delayed this round.
    pub delay_p: f64,
    /// Probability an individual eligible message is duplicated.
    pub duplicate_p: f64,
    /// Probability an individual advisory message is dropped.
    pub drop_p: f64,
    /// Probability a partition is toggled at a scheduling boundary.
    pub partition_toggle_p: f64,
}

impl FaultConfig {
    /// The empty configuration: a perfectly reliable, in-order network. Used
    /// by the determinism-audit smoke path where we want the schedule shape
    /// fixed and by convergence baselines.
    pub fn none() -> Self {
        Self {
            reorder: false,
            delay: false,
            duplicate: false,
            drop: false,
            partition: false,
            delay_p: 0.0,
            duplicate_p: 0.0,
            drop_p: 0.0,
            partition_toggle_p: 0.0,
        }
    }

    /// The full union of fault kinds with moderate probabilities. Not used per
    /// run (swarm testing argues against the union); handy for a deliberately
    /// hostile scenario.
    pub fn all() -> Self {
        Self {
            reorder: true,
            delay: true,
            duplicate: true,
            drop: true,
            partition: true,
            delay_p: 0.3,
            duplicate_p: 0.2,
            drop_p: 0.2,
            partition_toggle_p: 0.15,
        }
    }

    /// Derive the swarm fault config from a seed via a dedicated RNG stream.
    /// Kept separate from the scheduler's RNG so the fault selection is a pure
    /// function of the seed (reproducible from the artifact line) and does not
    /// perturb the scheduling stream. The offset avoids sharing the exact seed
    /// value with the scheduler stream.
    pub fn swarm_from_seed(seed: u64) -> Self {
        let mut rng = SimRng::new(seed ^ 0x5741524d_5f4b4559); // "SWARM_KEY"
        Self::swarm(&mut rng)
    }

    /// Draw a random subset of fault kinds from the seed (swarm testing). At
    /// least one kind is guaranteed on, so a "faulty" run is never silently a
    /// no-fault run.
    pub fn swarm(rng: &mut SimRng) -> Self {
        let mut cfg = Self {
            reorder: rng.bool(),
            delay: rng.bool(),
            duplicate: rng.bool(),
            drop: rng.bool(),
            partition: rng.bool(),
            // Probabilities are themselves drawn so different seeds explore
            // different fault intensities, not just different kinds.
            delay_p: rng.range_inclusive(10, 50) as f64 / 100.0,
            duplicate_p: rng.range_inclusive(10, 40) as f64 / 100.0,
            drop_p: rng.range_inclusive(10, 40) as f64 / 100.0,
            partition_toggle_p: rng.range_inclusive(5, 25) as f64 / 100.0,
        };
        if !(cfg.reorder || cfg.delay || cfg.duplicate || cfg.drop || cfg.partition) {
            // Guarantee at least one kind is active.
            match rng.below(5) {
                0 => cfg.reorder = true,
                1 => cfg.delay = true,
                2 => cfg.duplicate = true,
                3 => cfg.drop = true,
                _ => cfg.partition = true,
            }
        }
        cfg
    }

    /// One-line, self-contained rendering for the seeded-failure artifact.
    pub fn summary(&self) -> String {
        let mut kinds = Vec::new();
        if self.reorder {
            kinds.push("reorder");
        }
        if self.delay {
            kinds.push("delay");
        }
        if self.duplicate {
            kinds.push("dup");
        }
        if self.drop {
            kinds.push("drop");
        }
        if self.partition {
            kinds.push("partition");
        }
        if kinds.is_empty() {
            kinds.push("none");
        }
        format!(
            "{{{}}} delay_p={:.2} dup_p={:.2} drop_p={:.2} part_p={:.2}",
            kinds.join(","),
            self.delay_p,
            self.duplicate_p,
            self.drop_p,
            self.partition_toggle_p
        )
    }
}
