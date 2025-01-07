#[derive(Debug)]
pub struct ResultSet<T> {
    pub records: Vec<T>,
}

impl<T> From<ResultSet<T>> for Vec<T> {
    fn from(result_set: ResultSet<T>) -> Self {
        result_set.records
    }
}

impl<R> core::ops::Deref for ResultSet<R> {
    type Target = Vec<R>;

    fn deref(&self) -> &Self::Target {
        &self.records
    }
}
