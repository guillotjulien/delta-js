use std::sync::{Arc, Mutex};

use deltalake::{
  arrow::util::pretty::print_batches,
  datafusion::prelude::SessionContext,
  delta_datafusion::{DeltaScanConfigBuilder, DeltaSessionConfig, DeltaTableProvider},
};
use futures::TryStreamExt;
use napi::{CallContext, Env, JsFunction, JsObject, JsUnknown, Result};

use crate::table::DeltaTable;

#[napi]
#[derive(Clone)]
pub struct QueryBuilder {
  ctx: SessionContext,
}

#[napi]
pub struct QueryResult {
  query_builder: QueryBuilder,
  sql_query: String,
}

#[napi]
impl QueryBuilder {
  #[napi(constructor)]
  pub fn new() -> Self {
    let config = DeltaSessionConfig::default().into();
    let ctx = SessionContext::new_with_config(config);

    QueryBuilder { ctx }
  }

  #[napi]
  /// Register the given [DeltaTable] into the [SessionContext] using the provided `table_name`
  ///
  /// Once called, the provided `delta_table` will be referenceable in SQL queries so long as
  /// another table of the same name is not registered over it.
  pub fn register(&self, table_name: String, delta_table: &DeltaTable) -> Result<QueryBuilder> {
    let raw_table = delta_table.raw_table();
    let table = tokio::runtime::Handle::current().block_on(raw_table.lock());

    let snapshot = table
      .snapshot()
      .cloned()
      .map_err(|err| napi::Error::from_reason(err.to_string()))?;
    let log_store = table.log_store().clone();

    let scan_config = DeltaScanConfigBuilder::default()
      .build(&snapshot)
      .map_err(|err| napi::Error::from_reason(err.to_string()))?;

    let provider = Arc::new(
      DeltaTableProvider::try_new(snapshot, log_store, scan_config)
        .map_err(|err| napi::Error::from_reason(err.to_string()))?,
    );

    self
      .ctx
      .register_table(table_name, provider)
      .map_err(|err| napi::Error::from_reason(err.to_string()))?;

    Ok(self.clone())
  }

  #[napi]
  pub fn sql(&self, sql_query: String) -> QueryResult {
    QueryResult {
      query_builder: self.clone(),
      sql_query,
    }
  }
}

#[napi]
impl QueryResult {
  #[napi(constructor)]
  pub fn new(query_builder: &QueryBuilder, sql_query: String) -> Self {
    QueryResult {
      query_builder: query_builder.clone(),
      sql_query,
    }
  }

  #[napi]
  /// Print the first 25 rows returned by the SQL query
  pub async fn show(&self) -> Result<()> {
    let df = self
      .query_builder
      .ctx
      .sql(self.sql_query.as_str())
      .await
      .map_err(|err| napi::Error::from_reason(err.to_string()))?;

    let df = df
      .limit(0, Some(25))
      .map_err(|err| napi::Error::from_reason(err.to_string()))?;

    let results = df
      .collect()
      .await
      .map_err(|err| napi::Error::from_reason(err.to_string()))?;

    let _ = print_batches(&results);

    Ok(())
  }

  // FIXME: streams are stuck waiting for this: https://github.com/napi-rs/napi-rs/pull/2405
  // The important bit is here: https://github.com/napi-rs/napi-rs/blob/a976e9c3d97aff7b4f0760472368606e95c9a014/examples/napi/src/stream.rs#L42
  // #[napi]
  // pub fn stream(&self, env: Env) -> Result<JsObject> {
  //   let df = tokio::runtime::Handle::current().block_on(self.query_builder.ctx.sql(self.sql_query.as_str()))
  //     .map_err(|err| napi::Error::from_reason(err.to_string()))?;

  //   let stream = tokio::runtime::Handle::current().block_on(df.execute_stream())
  //     .map_err(|err| napi::Error::from_reason(err.to_string()))?;

  //   let stream = Arc::new(Mutex::new(stream));
  //   let _read_fn = env
  //     .create_function_from_closure("_read", move |ctx: CallContext<'_>| {
  //       let this: JsObject = ctx.this_unchecked();
  //       let mut stream_lock = stream.lock().unwrap();

  //       let batch = tokio::runtime::Handle::current()
  //         .block_on(stream_lock.try_next())
  //         .map_err(|err| napi::Error::from_reason(err.to_string()))?;

  //       let push_fn: JsFunction = this.get_named_property("push")?;
  //       if let Some(batch) = batch {
  //         println!("{:#?}", batch);

  //         // TODO: send batch once I'm able to use Arrow for that. For now simply print

  //         push_fn.call(Some(&this), &[ctx.env.create_string("")?])?;
  //       } else { // Close the stream
  //         push_fn.call(Some(&this), &[ctx.env.get_null()?])?;
  //       }

  //       Ok(())
  //     })?;

  //   let mut readable_stream = readable_constructor.new_instance::<JsUnknown>(&[])?;

  //   readable_stream.set_named_property("_read", _read_fn)?;

  //   Ok(readable_stream)
  // }

  #[napi]
  pub async fn fetch_all(&self) -> Result<()> {
    // TODO: Implement
    Ok(())
  }
}
