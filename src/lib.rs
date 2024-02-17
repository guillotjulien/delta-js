#![deny(clippy::all)]

use std::sync::{Arc, Mutex};

use arrow_json::writer::record_batches_to_json_rows;
use duckdb::{arrow::record_batch::RecordBatch, Connection};
use napi::{Task, Result, Env, JsUndefined};
use napi::bindgen_prelude::{AsyncTask, Buffer};

#[macro_use]
extern crate napi_derive;

#[napi(object, js_name = "DeltaTableOptions")]
#[derive(Debug, Clone)]
pub struct JsDeltaTableOptions {
  // FIXME: How can we combine version<i64> and version<timestamp>? retry with an enum?
  pub version: Option<i64>,
  pub timestamp: Option<i64>, // Can't have a date
  pub without_files: Option<bool>,
}

#[napi]
pub struct DeltaTable {
  // FIXME: Is there a way to get rid of this Mutex?
  // Ideally we should be able to query the table in a loop, not sure if the Mutex will like this.
  table: Arc<Mutex<deltalake::DeltaTable>>,
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
  /// * `table_uri` - Path of the Delta table
  /// * `options` - an object of the options to use for the storage backend
  pub fn new(table_uri: String, options: Option<JsDeltaTableOptions>) -> Self {
    let mut builder = deltalake::DeltaTableBuilder::from_uri(table_uri.clone());

    if let Some(options) = options.clone() {
      if let Some(version) = options.version {
        builder = builder.with_version(version);
      }

      if let Some(without_files) = options.without_files {
        if without_files {
          builder = builder.without_files();
        }
      }
    }

    let table = Arc::new(Mutex::new(builder.build().unwrap())); // FIXME: this is a result, how can we throw properly?

    DeltaTable { table }
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

  // #[napi]
  // pub fn metadata(&self) -> Result<DeltaTableMetadata> {

  // }

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
    let files = table.get_file_uris()
      .map(|f| format!("'{}'", f.to_string()))
      .collect::<Vec<String>>()
      .join(",");

    let db = Connection::open_in_memory().map_err(|err| napi::Error::from_reason(err.to_string()))?;

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