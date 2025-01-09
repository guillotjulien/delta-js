use std::sync::Arc;

use napi::Result;
use deltalake::{
  arrow::util::pretty::print_batches,
  datafusion::prelude::SessionContext,
  delta_datafusion::{DeltaScanConfigBuilder, DeltaSessionConfig, DeltaTableProvider},
};

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

    let snapshot = table.snapshot().cloned().map_err(|err| napi::Error::from_reason(err.to_string()))?;
    let log_store = table.log_store().clone();

    let scan_config = DeltaScanConfigBuilder::default()
      .build(&snapshot)
      .map_err(|err| napi::Error::from_reason(err.to_string()))?;

    let provider = Arc::new(
      DeltaTableProvider::try_new(snapshot, log_store, scan_config)
        .map_err(|err| napi::Error::from_reason(err.to_string()))?
    );

    self.ctx
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
    let df = self.query_builder.ctx
      .sql(self.sql_query.as_str())
      .await
      .map_err(|err| napi::Error::from_reason(err.to_string()))?;


    let df = df
      .limit(0, Some(25))
      .map_err(|err| napi::Error::from_reason(err.to_string()))?;

    let results = df.collect()
      .await
      .map_err(|err| napi::Error::from_reason(err.to_string()))?;

    let _ = print_batches(&results);

    Ok(())
  }

  #[napi]
  pub async fn fetch_all(&self) -> Result<()> {
    // TODO: Implement
    Ok(())
  }
}
