/// Visitor trait for observing and responding to lineage traversal steps
///
/// The visitor is invoked during traversal with accumulated information about
/// the causal relationship between two clocks. The key method is `on_forward`,
/// which handles the ascending application phase after the meet is discovered.
pub trait LineageVisitor<Id: Clone> {
    /// Context type that can be passed through the visitor
    type Context;

    /// Result type returned from the visitor
    type Result;

    /// Called during the ascending phase after discovering a meet.
    ///
    /// # Parameters
    /// - `meet`: The common ancestor(s) where the two lineages converge
    /// - `subject_chain_forward`: Events to apply from meet to subject, in causal order (oldest → newest)
    /// - `known_descendants_of_meet`: Potentially concurrent branches that descend from the meet
    /// - `ctx`: Additional context for the visitor
    ///
    /// # Returns
    /// A new frontier that may include:
    /// - Single-member clock if no concurrency remains
    /// - Multi-member clock with bubbled concurrency if unsubsumed branches remain
    ///
    /// # Responsibilities
    /// 1. Apply subject_chain_forward in order (oldest → newest)
    /// 2. Prune descendants subsumed by the applied chain's tip
    /// 3. Bubble any remaining unsubsumed descendants into the returned frontier
    fn on_forward(
        &mut self,
        meet: &[Id],
        subject_chain_forward: &[Id],
        known_descendants_of_meet: &[Id],
        ctx: &mut Self::Context,
    ) -> Self::Result;
}

/// Accumulates events during lineage traversal for building event bridges
#[derive(Debug, Clone)]
pub struct EventAccumulator<Event> {
    events: Vec<Event>,
    maximum: Option<usize>,
}

impl<Event: Clone> EventAccumulator<Event> {
    pub fn new(maximum: Option<usize>) -> Self { Self { events: Vec::new(), maximum } }

    pub fn add(&mut self, event: &Event) -> bool {
        if let Some(max) = self.maximum {
            if self.events.len() >= max {
                return false; // Reached maximum
            }
        }
        self.events.push(event.clone());
        true
    }

    pub fn take_events(self) -> Vec<Event> { self.events }

    pub fn is_at_limit(&self) -> bool { self.maximum.map_or(false, |max| self.events.len() >= max) }
}
