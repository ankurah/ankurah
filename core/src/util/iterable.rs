use std::{collections::HashSet, hash::BuildHasher};

/// Trait for types that can provide an iterator over their contents without consuming self.
/// This allows uniform iteration over both single items (`&T`) and collections (`&HashSet<T>`).
pub trait Iterable<T> {
    type Iter<'a>: Iterator<Item = &'a T>
    where
        Self: 'a,
        T: 'a;

    fn iterable(&self) -> Self::Iter<'_>;
}

// Implementation for a single reference - treats it as a collection of one
impl<T> Iterable<T> for T {
    type Iter<'a>
        = std::iter::Once<&'a T>
    where
        Self: 'a,
        T: 'a;

    fn iterable(&self) -> Self::Iter<'_> { std::iter::once(self) }
}

// Implementation for a HashSet reference
impl<T, S: BuildHasher> Iterable<T> for HashSet<T, S> {
    type Iter<'a>
        = std::collections::hash_set::Iter<'a, T>
    where
        Self: 'a,
        T: 'a;

    fn iterable(&self) -> Self::Iter<'_> { HashSet::iter(self) }
}

// Implementation for a Vec reference
impl<T> Iterable<T> for Vec<T> {
    type Iter<'a>
        = std::slice::Iter<'a, T>
    where
        Self: 'a,
        T: 'a;

    fn iterable(&self) -> Self::Iter<'_> { (**self).iter() }
}
