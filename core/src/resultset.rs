#[derive(Debug)]
pub struct ResultSet<T> {
    pub items: Vec<T>,
    pub loaded: bool,
}

impl<T: Clone> Clone for ResultSet<T> {
    fn clone(&self) -> Self { Self { items: self.items.clone(), loaded: self.loaded } }
}

impl<T> Default for ResultSet<T> {
    fn default() -> Self { Self { items: vec![], loaded: false } }
}

impl<T> From<ResultSet<T>> for Vec<T> {
    fn from(result_set: ResultSet<T>) -> Self { result_set.items }
}

impl<R> core::ops::Deref for ResultSet<R> {
    type Target = Vec<R>;

    fn deref(&self) -> &Self::Target { &self.items }
}

// impl<T> ResultSet<T> {
//     pub fn map<'a, U>(&'a self, f: impl Fn(&'a T) -> U + 'a) -> impl Iterator<Item = U> + 'a { self.items.iter().map(f) }
// }
