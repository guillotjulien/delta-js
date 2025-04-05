use arrow_schema::ArrowError;
use deltalake::datafusion::error::DataFusionError;
use deltalake::protocol::ProtocolError;
use deltalake::{errors::DeltaTableError, ObjectStoreError};
use napi::{Error, Status};

#[derive(thiserror::Error, Debug)]
pub enum JsError {
  #[error("Delta table error: {0}")]
  DeltaTable(#[from] DeltaTableError),
  #[error("Object store error: {0}")]
  ObjectStore(#[from] ObjectStoreError),
  #[error("Arrow error: {0}")]
  Arrow(#[from] ArrowError),
  #[error("Protocol error: {0}")]
  Protocol(#[from] ProtocolError),
  #[error("DataFusion error: {0}")]
  DataFusion(#[from] DataFusionError),
  #[error("Threading error: {0}")]
  ThreadingError(String),
  #[error("Serde error: {0}")]
  Serde(#[from] serde_json::Error),
  #[error("{0}")]
  Other(String),
}

// impl<T> From<std::sync::PoisonError<T>> for JsError {
//   fn from(value: std::sync::PoisonError<T>) -> Self {
//     JsError::ThreadingError(value.to_string())
//   }
// }

impl From<JsError> for Error {
  fn from(err: JsError) -> Self {
    match err {
      JsError::DeltaTable(e) => inner_to_napi_err(e),
      JsError::ObjectStore(e) => object_store_to_napi(e),
      JsError::Arrow(e) => arrow_to_napi(e),
      JsError::Protocol(e) => protocol_to_napi(e),
      JsError::DataFusion(e) => Error::new(Status::GenericFailure, e.to_string()),
      JsError::ThreadingError(e) => Error::new(Status::GenericFailure, e),
      _ => Error::new(Status::GenericFailure, err.to_string()),
    }
  }
}

fn inner_to_napi_err(err: DeltaTableError) -> Error {
  match err {
    DeltaTableError::NotATable(msg) | DeltaTableError::InvalidTableLocation(msg) => {
      Error::new(Status::InvalidArg, format!("Table not found: {}", msg))
    }

    DeltaTableError::InvalidJsonLog { .. } | DeltaTableError::InvalidStatsJson { .. } => {
      Error::new(Status::InvalidArg, format!("Protocol error: {}", err))
    }

    DeltaTableError::InvalidData { violations } => Error::new(
      Status::InvalidArg,
      format!("Invariant violations: {:?}", violations),
    ),

    DeltaTableError::Transaction { source } => {
      Error::new(Status::GenericFailure, format!("Commit failed: {}", source))
    }

    DeltaTableError::ObjectStore { source } => object_store_to_napi(source),
    DeltaTableError::Io { source } => {
      Error::new(Status::GenericFailure, format!("IO error: {}", source))
    }

    DeltaTableError::Arrow { source } => arrow_to_napi(source),

    _ => Error::new(Status::GenericFailure, err.to_string()),
  }
}

fn object_store_to_napi(err: ObjectStoreError) -> Error {
  match err {
    ObjectStoreError::NotFound { .. } => Error::new(Status::GenericFailure, err.to_string()),
    ObjectStoreError::Generic { source, .. }
      if source.to_string().contains("AWS_S3_ALLOW_UNSAFE_RENAME") =>
    {
      Error::new(Status::InvalidArg, source.to_string())
    }
    _ => Error::new(Status::GenericFailure, err.to_string()),
  }
}

fn arrow_to_napi(err: ArrowError) -> Error {
  match err {
    ArrowError::IoError(msg, _) => Error::new(Status::GenericFailure, msg),
    ArrowError::DivideByZero => Error::new(Status::InvalidArg, "division by zero"),
    ArrowError::InvalidArgumentError(msg) => Error::new(Status::InvalidArg, msg),
    ArrowError::NotYetImplemented(msg) => Error::new(Status::GenericFailure, msg),
    ArrowError::SchemaError(msg) => {
      Error::new(Status::InvalidArg, format!("Schema mismatch: {}", msg))
    }
    other => Error::new(Status::GenericFailure, other.to_string()),
  }
}

fn protocol_to_napi(err: ProtocolError) -> Error {
  match err {
    ProtocolError::Arrow { source } => arrow_to_napi(source),
    ProtocolError::ObjectStore { source } => object_store_to_napi(source),
    ProtocolError::EndOfLog => Error::new(Status::GenericFailure, "End of log".to_string()),
    ProtocolError::NoMetaData => {
      Error::new(Status::GenericFailure, "Table metadata missing".to_string())
    }
    ProtocolError::CheckpointNotFound => Error::new(Status::GenericFailure, err.to_string()),
    ProtocolError::InvalidField(e)
    | ProtocolError::InvalidRow(e)
    | ProtocolError::InvalidDeletionVectorStorageType(e) => Error::new(Status::InvalidArg, e),
    ProtocolError::SerializeOperation { source } => {
      Error::new(Status::GenericFailure, source.to_string())
    }
    ProtocolError::ParquetParseError { source } => {
      Error::new(Status::GenericFailure, source.to_string())
    }
    ProtocolError::IO { source } => Error::new(Status::GenericFailure, source.to_string()),
    ProtocolError::Generic(msg) => Error::new(Status::GenericFailure, msg),
    ProtocolError::Kernel { source } => Error::new(Status::GenericFailure, source.to_string()),
  }
}
