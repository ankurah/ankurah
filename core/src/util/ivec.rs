use std::mem::MaybeUninit;

/// Inline vector - uses fixed array for small collections, Vec for larger ones
#[derive(Debug)]
pub enum IVec<T, const N: usize> {
    /// Small collections stored in fixed array with length tracker
    Small { data: [MaybeUninit<T>; N], len: usize },
    /// Large collections stored in Vec
    Large(Vec<T>),
}

impl<T, const N: usize> IVec<T, N> {
    /// Creates an empty IVec
    pub fn new() -> Self { Self::Small { data: unsafe { MaybeUninit::uninit().assume_init() }, len: 0 } }

    /// Returns the number of elements
    pub fn len(&self) -> usize {
        match self {
            Self::Small { len, .. } => *len,
            Self::Large(vec) => vec.len(),
        }
    }

    /// Returns true if empty
    pub fn is_empty(&self) -> bool { self.len() == 0 }

    /// Pushes a value to the collection
    pub fn push(&mut self, value: T) {
        match self {
            Self::Small { data, len } if *len < N => {
                data[*len].write(value);
                *len += 1;
            }
            Self::Small { data, len } => {
                // Transition to Large
                let mut vec = Vec::with_capacity(N + 1);
                for i in 0..*len {
                    // SAFETY: We know indices 0..len are initialized
                    unsafe {
                        vec.push(data[i].assume_init_read());
                    }
                }
                vec.push(value);
                *self = Self::Large(vec);
            }
            Self::Large(vec) => {
                vec.push(value);
            }
        }
    }

    /// Returns an iterator over the elements
    pub fn iter(&self) -> Iter<'_, T, N> { Iter { ivec: self, index: 0 } }

    /// Returns a slice view of the elements
    pub fn as_slice(&self) -> &[T] {
        match self {
            Self::Small { data, len } => {
                // SAFETY: We know indices 0..len are initialized
                unsafe { std::slice::from_raw_parts(data.as_ptr() as *const T, *len) }
            }
            Self::Large(vec) => vec.as_slice(),
        }
    }
}

impl<T: PartialEq, const N: usize> IVec<T, N> {
    /// Checks if the collection contains a value
    pub fn contains(&self, value: &T) -> bool {
        match self {
            Self::Small { data, len } => {
                for i in 0..*len {
                    // SAFETY: We know indices 0..len are initialized
                    unsafe {
                        if data[i].assume_init_ref() == value {
                            return true;
                        }
                    }
                }
                false
            }
            Self::Large(vec) => vec.contains(value),
        }
    }

    /// Adds a value if not already present, returns true if added
    pub fn add(&mut self, value: T) -> bool {
        if self.contains(&value) {
            false
        } else {
            self.push(value);
            true
        }
    }
}

impl<T, const N: usize> Default for IVec<T, N> {
    fn default() -> Self { Self::new() }
}

impl<T: Clone, const N: usize> Clone for IVec<T, N> {
    fn clone(&self) -> Self {
        match self {
            Self::Small { data, len } => {
                let mut new_data: [MaybeUninit<T>; N] = unsafe { MaybeUninit::uninit().assume_init() };
                for i in 0..*len {
                    // SAFETY: We know indices 0..len are initialized
                    unsafe {
                        new_data[i].write(data[i].assume_init_ref().clone());
                    }
                }
                Self::Small { data: new_data, len: *len }
            }
            Self::Large(vec) => Self::Large(vec.clone()),
        }
    }
}

impl<T, const N: usize> Drop for IVec<T, N> {
    fn drop(&mut self) {
        match self {
            Self::Small { data, len } => {
                // Drop initialized elements
                for i in 0..*len {
                    // SAFETY: We know indices 0..len are initialized
                    unsafe {
                        data[i].assume_init_drop();
                    }
                }
            }
            Self::Large(_) => {
                // Vec handles its own drop
            }
        }
    }
}

pub struct Iter<'a, T, const N: usize> {
    ivec: &'a IVec<T, N>,
    index: usize,
}

impl<'a, T, const N: usize> Iterator for Iter<'a, T, N> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        match self.ivec {
            IVec::Small { data, len } => {
                if self.index < *len {
                    // SAFETY: We know indices 0..len are initialized
                    let item = unsafe { data[self.index].assume_init_ref() };
                    self.index += 1;
                    Some(item)
                } else {
                    None
                }
            }
            IVec::Large(vec) => {
                if self.index < vec.len() {
                    let item = &vec[self.index];
                    self.index += 1;
                    Some(item)
                } else {
                    None
                }
            }
        }
    }
}

impl<'a, T, const N: usize> IntoIterator for &'a IVec<T, N> {
    type Item = &'a T;
    type IntoIter = Iter<'a, T, N>;

    fn into_iter(self) -> Self::IntoIter { self.iter() }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_small_push() {
        let mut ivec: IVec<i32, 4> = IVec::new();
        assert_eq!(ivec.len(), 0);
        assert!(ivec.is_empty());

        ivec.push(1);
        ivec.push(2);
        ivec.push(3);

        assert_eq!(ivec.len(), 3);
        assert!(!ivec.is_empty());
        assert!(matches!(ivec, IVec::Small { .. }));
    }

    #[test]
    fn test_transition_to_large() {
        let mut ivec: IVec<i32, 2> = IVec::new();
        ivec.push(1);
        ivec.push(2);
        assert!(matches!(ivec, IVec::Small { .. }));

        ivec.push(3);
        assert!(matches!(ivec, IVec::Large(_)));
        assert_eq!(ivec.len(), 3);
    }

    #[test]
    fn test_contains() {
        let mut ivec: IVec<i32, 4> = IVec::new();
        ivec.push(1);
        ivec.push(2);
        ivec.push(3);

        assert!(ivec.contains(&1));
        assert!(ivec.contains(&2));
        assert!(ivec.contains(&3));
        assert!(!ivec.contains(&4));
    }

    #[test]
    fn test_contains_large() {
        let mut ivec: IVec<i32, 2> = IVec::new();
        ivec.push(1);
        ivec.push(2);
        ivec.push(3);
        ivec.push(4);

        assert!(matches!(ivec, IVec::Large(_)));
        assert!(ivec.contains(&1));
        assert!(ivec.contains(&4));
        assert!(!ivec.contains(&5));
    }

    #[test]
    fn test_iter() {
        let mut ivec: IVec<i32, 4> = IVec::new();
        ivec.push(1);
        ivec.push(2);
        ivec.push(3);

        let items: Vec<_> = ivec.iter().copied().collect();
        assert_eq!(items, vec![1, 2, 3]);
    }

    #[test]
    fn test_iter_large() {
        let mut ivec: IVec<i32, 2> = IVec::new();
        ivec.push(1);
        ivec.push(2);
        ivec.push(3);
        ivec.push(4);

        let items: Vec<_> = ivec.iter().copied().collect();
        assert_eq!(items, vec![1, 2, 3, 4]);
    }

    #[test]
    fn test_drop() {
        use std::sync::Arc;

        // Test that Drop is called properly for Small variant
        let mut ivec: IVec<Arc<i32>, 4> = IVec::new();
        let a1 = Arc::new(1);
        let a2 = Arc::new(2);

        ivec.push(Arc::clone(&a1));
        ivec.push(Arc::clone(&a2));

        assert_eq!(Arc::strong_count(&a1), 2);
        assert_eq!(Arc::strong_count(&a2), 2);

        drop(ivec);

        assert_eq!(Arc::strong_count(&a1), 1);
        assert_eq!(Arc::strong_count(&a2), 1);
    }

    #[test]
    fn test_add() {
        let mut ivec: IVec<i32, 4> = IVec::new();

        assert!(ivec.add(1));
        assert!(ivec.add(2));
        assert!(ivec.add(3));
        assert_eq!(ivec.len(), 3);

        // Adding duplicate should return false
        assert!(!ivec.add(2));
        assert_eq!(ivec.len(), 3);

        // Verify contents
        let items: Vec<_> = ivec.iter().copied().collect();
        assert_eq!(items, vec![1, 2, 3]);
    }

    #[test]
    fn test_add_large() {
        let mut ivec: IVec<i32, 2> = IVec::new();

        assert!(ivec.add(1));
        assert!(ivec.add(2));
        assert!(ivec.add(3));
        assert!(matches!(ivec, IVec::Large(_)));

        // Adding duplicate should return false even in Large variant
        assert!(!ivec.add(1));
        assert!(!ivec.add(2));
        assert_eq!(ivec.len(), 3);

        // Can still add new items
        assert!(ivec.add(4));
        assert_eq!(ivec.len(), 4);
    }
}
