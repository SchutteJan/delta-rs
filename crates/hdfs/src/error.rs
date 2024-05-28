use deltalake_core::errors::DeltaTableError;

pub(crate) type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Generic HDFS Error")]
    HdfsError(String),
}

impl From<Error> for DeltaTableError {
    fn from(e: Error) -> Self {
        match e {
            Error::HdfsError(msg) => DeltaTableError::Generic(msg),
        }
    }
}
