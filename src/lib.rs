#![deny(clippy::all)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use arrow_json::writer::record_batches_to_json_rows;
use duckdb::{arrow::record_batch::RecordBatch, Connection};
use napi::{Task, Result, Env, JsUndefined, Either};
use napi::bindgen_prelude::{AsyncTask, Buffer};

#[macro_use]
extern crate napi_derive;

#[napi(object, js_name = "DeltaTableOptions")]
#[derive(Clone)]
pub struct JsDeltaTableOptions {
  /// Specify the version to load either as a number or an ISO-8601/RFC-3339 timestamp.
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

#[napi(object, js_name = "AWSConfigKeyCredentials")]
#[derive(Clone)]
pub struct AWSConfigKeyCredentials {
  pub aws_region: String,
  pub aws_access_key_id: String,
  pub aws_secret_access_key: String,
  pub aws_session_token: Option<String>,
}

#[napi(object, js_name = "AWSConfigKeyProfile")]
#[derive(Clone)]
pub struct AWSConfigKeyProfile {
  pub aws_region: String,
  pub aws_profile: String,
}

#[napi]
pub struct DeltaTable {
  // Might want to do something better later, but for now, the language just gets in the way of getting work done...
  table: Arc<Mutex<deltalake::DeltaTable>>,
  storage_options: Arc<Option<HashMap<String, String>>>,
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
  pub fn new(table_uri: String, options: Option<JsDeltaTableOptions>) -> Self {
    let mut builder = deltalake::DeltaTableBuilder::from_uri(table_uri.clone());
    let mut table_storage_options: Option<HashMap<String, String>> = None;

    if let Some(options) = options.clone() {
      if let Some(version) = options.version {
        match version {
          Either::A(version) => {
            println!("here");
            builder = builder.with_version(version);
          },
          Either::B(version) => {
            builder = builder.with_datestring(version).unwrap(); // FIXME: this is a result, how can we throw properly?
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

    let table = Arc::new(Mutex::new(builder.build().unwrap())); // FIXME: this is a result, how can we throw properly?
    let storage_options = Arc::new(table_storage_options.clone());

    DeltaTable { table, storage_options }
  }

  #[napi]
  /// Build the DeltaTable and load its state
  pub fn load(&self) -> AsyncTask<AsyncLoadTable> {
    let table_clone = Arc::clone(&self.table);

    AsyncTask::new(AsyncLoadTable { table: table_clone })
  }

  #[napi]
  /// Get the version of the Delta table.
  pub fn version(&self) -> Result<i64> {
    let table = self.table.lock().unwrap();

    Ok(table.version())
  }

  #[napi]
  /// Get the current schema of the Delta table.
  pub fn schema(&self) -> Result<String> {
    let table = self.table.lock().unwrap();
    let schema = table.schema();

    match schema {
      Some(schema) => serde_json::to_string(schema).map_err(|err| napi::Error::from_reason(err.to_string())),
      None => Err(napi::Error::from_reason("Cannot read table schema. Table is not loaded")),
    }
  }

  #[napi]
  pub async fn query(&self, query: String) -> Result<Buffer> {
    let table = self.table.lock().unwrap();
    let storage_options = self.storage_options.as_ref();

    let files = table.get_file_uris()
      .map(|f| format!("'{}'", f.to_string()))
      .collect::<Vec<String>>()
      .join(",");

    let db = Connection::open_in_memory().map_err(|err| napi::Error::from_reason(err.to_string()))?;

    if let Some(storage_options) = storage_options {
      if storage_options.len() > 0 {
        db.execute_batch(format!("SET s3_region='{}'; SET s3_access_key_id='{}'; SET s3_secret_access_key='{}'", storage_options.get("aws_region").unwrap(), storage_options.get("aws_access_key_id").unwrap(), storage_options.get("aws_secret_access_key").unwrap()).as_str()).map_err(|err| napi::Error::from_reason(err.to_string()))?;
      }
    }

    // Views are lazy evaluated, so we can use predicate pushdown and not read all parquet files
    db.execute_batch(format!("CREATE VIEW delta_table AS SELECT * from read_parquet([{}]);", files).as_str()).map_err(|err| napi::Error::from_reason(err.to_string()))?;

    let batches: Vec<RecordBatch> = db.prepare(query.as_str()).map_err(|err| napi::Error::from_reason(err.to_string()))?
      .query_arrow([]).map_err(|err| napi::Error::from_reason(err.to_string()))?
      .collect();

    // Would be better if we can skip the string part altogether and directly work with bytes - but for now that's good enough
    let t: Vec<&RecordBatch> = batches.iter().collect();
    let res = record_batches_to_json_rows(&t[..]).map_err(|err| napi::Error::from_reason(err.to_string()))?;

    let output = serde_json::to_vec(&res).map_err(|err| napi::Error::from_reason(err.to_string()))?;

    Ok(output.into())
  }
}

pub struct AsyncLoadTable {
  table: Arc<Mutex<deltalake::DeltaTable>>,
}

#[napi]
impl Task for AsyncLoadTable {
  type Output = ();
  type JsValue = JsUndefined;

  fn compute(&mut self) -> Result<Self::Output> {
    let mut table = self.table.lock().unwrap();

    rt().block_on(table.load()).map_err(|err| napi::Error::from_reason(err.to_string()))

    // FIXME: Do something w/ res in case of errors

    // Ok(())
  }

  fn resolve(&mut self, env: Env, _output: Self::Output) -> Result<Self::JsValue> {
    let undefined = env.get_undefined().unwrap();

    Ok(undefined)
  }
}

#[inline]
fn rt() -> tokio::runtime::Runtime {
  tokio::runtime::Runtime::new().unwrap()
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