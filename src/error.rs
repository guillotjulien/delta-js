use arrow_schema::ArrowError;
use deltalake::datafusion::error::DataFusionError;
use deltalake::protocol::ProtocolError;
use deltalake::{errors::DeltaTableError, ObjectStoreError};

#[derive(thiserror::Error, Debug)]
pub enum JsError {
  #[error("Error in delta table")]
  DeltaTable(#[from] DeltaTableError),
  #[error("Error in object store")]
  ObjectStore(#[from] ObjectStoreError),
  #[error("Error in arrow")]
  Arrow(#[from] ArrowError),
  #[error("Error in checkpoint")]
  Protocol(#[from] ProtocolError),
  #[error("Error in data fusion")]
  DataFusion(#[from] DataFusionError),
  #[error("Lock acquisition error")]
  ThreadingError(String),
  #[error("Serde error")]
  Serde(#[from] serde_json::Error),
  #[error("{0}")]
  Other(String),
}

impl std::convert::From<JsError> for napi::Error {
  fn from(err: JsError) -> Self {
    napi::Error::from_reason(err.to_string())
  }
}
