use crate::model::ID;

#[derive(Debug)]
pub enum RetrievalError {
    NotFound(ID),
    StorageError(Box<dyn std::error::Error>),
    FailedUpdate(Box<dyn std::error::Error>),
    DeserializationError(bincode::Error),
}

impl From<bincode::Error> for RetrievalError {
    fn from(e: bincode::Error) -> Self {
        RetrievalError::DeserializationError(e)
    }
}

impl From<RetrievalError> for anyhow::Error {
    fn from(e: RetrievalError) -> Self {
        anyhow::anyhow!("{:?}", e)
    }
}
