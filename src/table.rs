use std::collections::HashMap;
use std::sync::Arc;

use napi::{Result, Either};
use tokio::sync::Mutex;

#[napi(object)]
#[derive(Clone)]
pub struct DeltaTableOptions {
  /// Specify the version to load either as an integer or an ISO-8601/RFC-3339 timestamp.
  pub version: Option<Either<i64, String>>,

  /// Indicates whether DeltaTable should track files.
  /// This defaults to `true`
  ///
  /// Some append-only applications might have no need of tracking any files.
  /// Hence, DeltaTable will be loaded with significant memory reduction.
  pub without_files: Option<bool>,

  /// Set options used to initialize storage backend.
  pub storage_options: Option<Either<AWSConfigKeyCredentials, AWSConfigKeyProfile>>,
}

#[napi(object)]
#[derive(Clone)]
pub struct AWSConfigKeyCredentials {
  pub aws_region: String,
  pub aws_access_key_id: String,
  pub aws_secret_access_key: String,
  pub aws_session_token: Option<String>,
}

#[napi(object)]
#[derive(Clone)]
pub struct AWSConfigKeyProfile {
  pub aws_region: String,
  pub aws_profile: String,
}

#[napi]
pub struct DeltaTable {
  table: Arc<Mutex<deltalake::DeltaTable>>,
  storage_options: Option<HashMap<String, String>>,
}

#[napi]
impl DeltaTable {
  #[napi(constructor)]
  /// Create the Delta table from a path with an optional version.
  /// Multiple StorageBackends are currently supported: AWS S3 and local URI.
  /// Depending on the storage backend used, you could provide options values using the `options` parameter.
  /// 
  /// This will not load the log, i.e. the table is not initialized. To get an initialized
  /// table use the `load` function.
  /// 
  /// # Arguments
  /// 
  /// * `tableUri` - Path of the Delta table
  /// * `options` - an object of the options to use for the storage backend
  pub fn new(table_uri: String, options: Option<DeltaTableOptions>) -> Result<Self> {
    let mut builder = deltalake::DeltaTableBuilder::from_uri(table_uri.clone());
    let mut table_storage_options: Option<HashMap<String, String>> = None;

    if let Some(options) = options.clone() {
      if let Some(version) = options.version {
        match version {
          Either::A(version) => {
            builder = builder.with_version(version);
          },
          Either::B(version) => {
            builder = builder.with_datestring(version).map_err(|err| napi::Error::from_reason(err.to_string()))?;
          }
        }
      }

      if let Some(without_files) = options.without_files {
        if without_files {
          builder = builder.without_files();
        }
      }

      if let Some(storage_options) = options.storage_options {
        let options = get_storage_options(storage_options);

        table_storage_options = Some(options.clone());
        builder = builder.with_storage_options(options);
      }
    }

    let table = Arc::new(Mutex::new(builder.build().map_err(|err| napi::Error::from_reason(err.to_string()))?));
    let storage_options = table_storage_options.clone();

    Ok(DeltaTable { table, storage_options })
  }

  #[napi]
  /// Build the DeltaTable and load its state
  pub async fn load(&self) -> Result<()> {
    let mut table = self.table.lock().await;

    table.load().await.map_err(|err| napi::Error::from_reason(err.to_string()))?;

    Ok(())
  }

  #[napi]
  /// Get the version of the Delta table.
  pub fn version(&self) -> Result<i64> {
    let table = tokio::runtime::Handle::current().block_on(self.table.lock());

    Ok(table.version())
  }

  #[napi]
  /// Get the current schema of the Delta table.
  pub fn schema(&self) -> Result<String> {
    let table = tokio::runtime::Handle::current().block_on(self.table.lock());
    let schema = table.schema();

    match schema {
      Some(schema) => serde_json::to_string(schema).map_err(|err| napi::Error::from_reason(err.to_string())),
      None => Err(napi::Error::from_reason("Cannot read table schema. Table is not loaded")),
    }
  }

  pub fn raw_table(&self) -> Arc<Mutex<deltalake::DeltaTable>> {
    self.table.clone()
  }
}

fn get_storage_options(storage_options: Either<AWSConfigKeyCredentials, AWSConfigKeyProfile>) -> HashMap<String, String> {
  let mut options: HashMap<String, String> = HashMap::new();

  match storage_options {
    Either::A(credentials_options) => {
      options.insert("aws_region".to_string(), credentials_options.aws_region);
      options.insert("aws_access_key_id".to_string(), credentials_options.aws_access_key_id);
      options.insert("aws_secret_access_key".to_string(), credentials_options.aws_secret_access_key);

      if let Some(aws_session_token) = credentials_options.aws_session_token {
        options.insert("aws_session_token".to_string(), aws_session_token);
      }
    },
    Either::B(profile_options) => {
      options.insert("aws_region".to_string(), profile_options.aws_region);
      options.insert("aws_profile".to_string(), profile_options.aws_profile);
    },
  }

  options
}