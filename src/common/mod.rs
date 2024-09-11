use thiserror::Error;

/// `DatenLord` Result type
pub type DatenLordResult<T> = Result<T, DatenLordError>;
/// `DatenLord` error code
#[derive(Error, Debug)]
pub enum DatenLordError {
    /// Unimplemented error
    #[error("Unimplemented: {context:?}")]
    Unimplemented { context: Vec<String> },
    /// Invalid argument error
    #[error("Invalid argument: {context:?}")]
    InvalidArgument { context: Vec<String> },
    /// Internal error
    #[error("Internal error: {context:?}")]
    Internal { context: Vec<String> },
    /// I/O error
    #[error("I/O error: {context:?}")]
    Io { context: Vec<String> },
    /// Other error
    #[error("Other error: {context:?}")]
    Other { context: Vec<String> },
}