use thiserror::Error;

pub type Result<T> = std::result::Result<T, JobError>;

#[derive(Debug, Error)]
pub enum JobError {
    #[error("job already registered: {0}")]
    DuplicateId(String),
    #[error("invalid schedule interval: {0:?}")]
    InvalidInterval(std::time::Duration),
    #[error("store: {0}")]
    Store(String),
    #[error("job failed: {0}")]
    JobFailed(String),
}
