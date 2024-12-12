/// Represents a change in the record set
#[derive(Debug)]
pub enum Operation<R, U> {
    /// A record has been added or now matches the subscription query
    Add {
        id: usize,
        record: R,
        updates: Vec<U>, // What changes caused this record to be added to the subscription
    },
    /// A record no longer matches the subscription query
    Remove {
        id: usize,
        old_record: R,
        current_record: R,
        updates: Vec<U>, // What changes caused this record to be removed from the subscription
    },
    /// A record was updated and still matches the subscription query
    Edit {
        id: usize,
        old_record: R,
        new_record: R,
        updates: Vec<U>,
    },
}
