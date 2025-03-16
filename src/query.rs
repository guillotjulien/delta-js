use std::sync::Arc;

use deltalake::{
  arrow::{
    json::{
      self,
      writer::{JsonArray, LineDelimited},
    },
    util::pretty::print_batches,
  },
  datafusion::prelude::SessionContext,
  delta_datafusion::{DeltaScanConfigBuilder, DeltaSessionConfig, DeltaTableProvider},
};
use futures::TryStreamExt;
use napi::{
  bindgen_prelude::{Buffer, BufferSlice, ReadableStream},
  Env, Result,
};
use tokio::sync::mpsc::error::SendError;
use tokio_stream::wrappers::ReceiverStream;

use crate::{error::JsError, get_runtime, table::RawDeltaTable};

#[napi]
#[derive(Clone)]
pub struct RawQueryBuilder {
  ctx: SessionContext,
}

#[napi]
pub struct RawCursor {
  query_builder: RawQueryBuilder,
  sql_query: String,
}

#[napi]
impl RawQueryBuilder {
  #[napi(constructor)]
  pub fn new() -> Self {
    let config = DeltaSessionConfig::default().into();
    let ctx = SessionContext::new_with_config(config);

    RawQueryBuilder { ctx }
  }

  #[napi(catch_unwind)]
  /// Register the given [DeltaTable] into the [SessionContext] using the provided `table_name`
  ///
  /// Once called, the provided `delta_table` will be referenceable in SQL queries so long as
  /// another table of the same name is not registered over it.
  pub fn register(
    &self,
    table_name: String,
    delta_table: &RawDeltaTable,
  ) -> Result<RawQueryBuilder> {
    let snapshot = delta_table.clone_state()?;
    let log_store = delta_table.log_store()?;

    let scan_config = DeltaScanConfigBuilder::default()
      .build(&snapshot)
      .map_err(JsError::from)?;

    let provider = Arc::new(
      DeltaTableProvider::try_new(snapshot, log_store, scan_config).map_err(JsError::from)?,
    );

    self
      .ctx
      .register_table(table_name, provider)
      .map_err(JsError::from)?;

    Ok(self.clone())
  }

  #[napi(catch_unwind)]
  /// Prepares the sql query to be executed.
  pub fn sql(&self, sql_query: String) -> RawCursor {
    RawCursor {
      query_builder: self.clone(),
      sql_query,
    }
  }
}

#[napi]
impl RawCursor {
  #[napi(constructor)]
  pub fn new(query_builder: &RawQueryBuilder, sql_query: String) -> Self {
    RawCursor {
      query_builder: query_builder.clone(),
      sql_query,
    }
  }

  #[napi(catch_unwind)]
  /// Print the first 25 rows returned by the SQL query
  pub async fn show(&self) -> Result<()> {
    let df = self
      .query_builder
      .ctx
      .sql(self.sql_query.as_str())
      .await
      .map_err(JsError::from)?;

    let df = df.limit(0, Some(25)).map_err(JsError::from)?;

    let results = df.collect().await.map_err(JsError::from)?;

    let _ = print_batches(&results);

    Ok(())
  }

  #[napi(catch_unwind)]
  /// Execute the given SQL command within the [SessionContext] of this instance
  ///
  /// **NOTE:** The function returns the rows as a continuous, newline delimited, stream of JSON strings
  /// it is especially suited to deal with large results set.
  pub fn stream(&self, env: Env) -> Result<ReadableStream<BufferSlice>> {
    let stream = get_runtime()
      .block_on(async {
        let df = self.query_builder.ctx.sql(self.sql_query.as_str()).await?;
        df.execute_stream().await
      })
      .map_err(JsError::from)?;

    // FIXME: Make number of batches before blocking configurable
    let (tx, rx) = tokio::sync::mpsc::channel(100);

    // Spawn a dedicated thread for processing the stream
    std::thread::spawn(move || {
      // Use a local runtime for this thread
      let local_runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

      // Process the stream in this thread
      local_runtime.block_on(async {
        let mut stream = stream;

        while let Some(batch_result) = stream.try_next().await.transpose() {
          match batch_result {
            Ok(batch) => {
              // Process the batch in a separate task to avoid blocking
              let batch_tx = tx.clone();

              // Use spawn_blocking for CPU-intensive JSON serialization
              if let Err(err) = tokio::task::spawn_blocking(move || {
                let mut json_writer = json::Writer::<Vec<u8>, LineDelimited>::new(Vec::new());
                if let Err(e) = json_writer.write(&batch) {
                  return Err(napi::Error::from_reason(e.to_string()));
                }

                let json_bytes = json_writer.into_inner();
                if let Err(SendError(_)) = batch_tx.blocking_send(Ok(json_bytes)) {
                  // Channel closed, stream was likely aborted
                  return Err(napi::Error::from_reason("Stream aborted"));
                }

                Ok(())
              })
              .await
              .map_err(|err| napi::Error::from_reason(format!("Task panicked: {}", err)))
              {
                let _ = tx.send(Err(err)).await;
                break;
              }
            }
            Err(err) => {
              let _ = tx
                .send(Err(napi::Error::from_reason(err.to_string())))
                .await;
              break;
            }
          }
        }

        // Close the sender to signal the end of the stream
        drop(tx);
      });
    });

    ReadableStream::create_with_stream_bytes(&env, ReceiverStream::new(rx))
  }

  #[napi(catch_unwind)]
  /// Execute the given SQL command within the [SessionContext] of this instance
  ///
  /// **NOTE:** Since this function returns a materialized JS Buffer,
  /// it may result unexpected memory consumption for queries which return large data
  /// sets.
  pub async fn fetch_all(&self) -> Result<Buffer> {
    let df = self
      .query_builder
      .ctx
      .sql(self.sql_query.as_str())
      .await
      .map_err(JsError::from)?;

    let batches = df.collect().await.map_err(JsError::from)?;

    // Move the JSON serialization to a separate thread
    // This prevents blocking the event loop with CPU-intensive work
    let json_bytes = tokio::task::spawn_blocking(move || {
      let mut json_writer = json::Writer::<Vec<u8>, JsonArray>::new(Vec::new());

      for batch in batches {
        json_writer.write(&batch).map_err(JsError::from)?;
      }

      json_writer.finish().map_err(JsError::from)?;

      Ok::<_, napi::Error>(json_writer.into_inner())
    })
    .await
    .map_err(|err| napi::Error::from_reason(format!("Join error: {}", err)))?
    .map_err(|err| napi::Error::from_reason(err.to_string()))?;

    Ok(json_bytes.into())
  }
}
