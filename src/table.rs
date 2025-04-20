use std::collections::HashMap;
use std::future::IntoFuture;
use std::io::Cursor;
use std::sync::Arc;

use chrono::Duration;
use deltalake::arrow::ipc::reader::StreamReader;
use deltalake::datafusion::datasource::provider_as_source;
use deltalake::datafusion::logical_expr::LogicalPlanBuilder;
use deltalake::lakefs::LakeFSCustomExecuteHandler;
use deltalake::operations::vacuum::VacuumBuilder;
use deltalake::operations::write::WriteBuilder;
use deltalake::parquet::basic::Compression;
use deltalake::parquet::errors::ParquetError;
use deltalake::parquet::file::properties::{EnabledStatistics, WriterProperties};
use deltalake::{logstore::LogStoreRef, table::state::DeltaTableState};
use deltalake::{DeltaResult, DeltaTable, DeltaTableBuilder, DeltaTableError};
use napi::bindgen_prelude::Uint8Array;
use napi::{Either, Result};
use tokio::sync::Mutex;

use crate::error::JsError;
use crate::get_runtime;
use crate::transaction::{
  maybe_create_commit_properties, JsCommitProperties, JsPostCommitHookProperties,
};
use crate::writer::to_lazy_table;

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

#[napi(object, js_name = "DeltaTableMetadata")]
pub struct JsDeltaTableMetadata {
  pub id: String,
  pub name: Option<String>,
  pub description: Option<String>,
  pub partition_columns: Vec<String>,
  pub created_time: Option<i64>,
  pub configuration: HashMap<String, Option<String>>,
}

#[napi(object, js_name = "DeltaTableProtocolVersions")]
pub struct JsDeltaTableProtocolVersions {
  pub min_reader_version: i32,
  pub min_writer_version: i32,
  pub reader_features: Option<Vec<String>>,
  pub writer_features: Option<Vec<String>>,
}

#[napi(object)]
pub struct DeltaTableVacuumOptions {
  /// When true, list only the files, delete otherwise.
  /// This defaults to `false`.
  pub dry_run: Option<bool>,

  /// When falsed, accepts retention hours smaller than the value from
  /// `delta.deletedFileRetentionDuration`.
  /// This defaults to `true`.
  pub enforce_retention_duration: Option<bool>,

  /// the retention threshold in hours, if none then the value from
  /// `delta.deletedFileRetentionDuration` is used or default of 1 week otherwise.
  pub retention_hours: Option<i64>,

  /// Properties of the transaction commit. If null, default values are used.
  pub commit_properties: Option<JsCommitProperties>,

  /// Properties for the post commit hook. If null, default values are used.
  pub post_commithook_properties: Option<JsPostCommitHookProperties>,
}

#[napi(object)]
pub struct ColumnProperties {
  pub dictionary_enabled: Option<bool>,
  pub statistics_enabled: Option<String>,
  pub bloom_filter_properties: Option<BloomFilterProperties>,
}

#[napi(object, js_name = "WriterProperties")]
pub struct JsWriterProperties {
  pub data_page_size_limit: Option<u8>,
  pub dictionary_page_size_limit: Option<u8>,
  pub data_page_row_count_limit: Option<u8>,
  pub write_batch_size: Option<u8>,
  pub max_row_group_size: Option<u8>,
  pub statistics_truncate_length: Option<u8>,
  pub compression: Option<String>,
  pub default_column_properties: Option<ColumnProperties>,
  pub column_properties: Option<HashMap<String, Option<ColumnProperties>>>,
}

#[napi(object)]
pub struct DeltaTableWriteOptions {
  pub schema_mode: Option<String>,
  pub partition_by: Option<Vec<String>>,
  pub predicate: Option<String>,
  pub target_file_size: Option<u8>,
  pub name: Option<String>,
  pub description: Option<String>,
  pub configuration: Option<HashMap<String, Option<String>>>,
  pub writer_properties: Option<JsWriterProperties>,
  pub commit_properties: Option<JsCommitProperties>,
  pub post_commithook_properties: Option<JsPostCommitHookProperties>,
}

#[napi(object)]
pub struct BloomFilterProperties {
  pub set_bloom_filter_enabled: Option<bool>,
  pub fpp: Option<f64>,
  pub ndv: Option<i64>,
}

#[napi]
pub struct RawDeltaTable {
  table: Arc<Mutex<DeltaTable>>,
}

/// Those methods are internal and shouldn't be exposed to the JS API
impl RawDeltaTable {
  /// Internal helper method which allows for acquiring the lock on the underlying
  /// [deltalake::DeltaTable] and then executing the given function parameter with the guarded
  /// reference
  ///
  /// This should only be used for read-only accesses and callers that need to modify the
  /// underlying instance should acquire the lock themselves.
  pub fn with_table<T>(&self, func: impl Fn(&DeltaTable) -> Result<T>) -> Result<T> {
    let table = get_runtime().block_on(self.table.lock());
    func(&table)
  }

  pub fn clone_state(&self) -> Result<DeltaTableState> {
    self.with_table(|t| Ok(t.snapshot().cloned().map_err(JsError::from)?))
  }

  pub fn log_store(&self) -> Result<LogStoreRef> {
    self.with_table(|t| Ok(t.log_store().clone()))
  }

  pub async fn get_latest_version(&self) -> Result<i64> {
    let table = self.table.lock().await;
    Ok(table.get_latest_version().await.map_err(JsError::from)?)
  }

  pub async fn get_earliest_version(&self) -> Result<i64> {
    let table = self.table.lock().await;
    Ok(table.get_earliest_version().await.map_err(JsError::from)?)
  }

  pub fn get_stats_columns(&self) -> Result<Option<Vec<String>>> {
    self.with_table(|t| {
      Ok(
        t.snapshot()
          .map_err(JsError::from)?
          .table_config()
          .stats_columns()
          .map(|v| v.iter().map(|s| s.to_string()).collect::<Vec<String>>()),
      )
    })
  }

  pub fn get_num_index_cols(&self) -> Result<i32> {
    self.with_table(|t| {
      Ok(
        t.snapshot()
          .map_err(JsError::from)?
          .table_config()
          .num_indexed_cols(),
      )
    })
  }
}

#[napi]
impl RawDeltaTable {
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
    let mut builder = DeltaTableBuilder::from_uri(table_uri.clone());

    if let Some(options) = options.clone() {
      if let Some(version) = options.version {
        match version {
          Either::A(version) => {
            builder = builder.with_version(version);
          }
          Either::B(version) => {
            builder = builder.with_datestring(version).map_err(JsError::from)?;
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
        builder = builder.with_storage_options(options);
      }
    }

    let table = Arc::new(Mutex::new(builder.build().map_err(JsError::from)?));

    Ok(RawDeltaTable { table })
  }

  #[napi(catch_unwind)]
  pub async fn is_delta_table(
    table_uri: String,
    storage_options: Option<Either<AWSConfigKeyCredentials, AWSConfigKeyProfile>>,
  ) -> Result<bool> {
    let mut builder = DeltaTableBuilder::from_uri(table_uri.clone());
    if let Some(storage_options) = storage_options {
      let options = get_storage_options(storage_options);
      builder = builder.with_storage_options(options);
    }

    let table = builder.build().map_err(JsError::from)?;
    let is_delta_table = table
      .verify_deltatable_existence()
      .await
      .map_err(JsError::from)?;

    Ok(is_delta_table)
  }

  #[napi(catch_unwind)]
  /// Build the DeltaTable and load its state
  pub async fn load(&self) -> Result<()> {
    let mut table = self.table.lock().await;

    table.load().await.map_err(JsError::from)?;

    Ok(())
  }

  #[napi(catch_unwind)]
  pub fn table_uri(&self) -> Result<String> {
    self.with_table(|t| Ok(t.table_uri()))
  }

  #[napi(catch_unwind)]
  pub fn version(&self) -> Result<i64> {
    self.with_table(|t| Ok(t.version()))
  }

  #[napi(catch_unwind)]
  pub fn has_files(&self) -> Result<bool> {
    self.with_table(|t| Ok(t.config.require_files))
  }

  // FIXME: partition filters
  #[napi(catch_unwind)]
  pub async fn files(&self) -> Result<Vec<String>> {
    let table = self.table.lock().await;

    if !table.config.require_files {
      return Err(
        JsError::from(DeltaTableError::Generic(
          "Table is instantiated without files".into(),
        ))
        .into(),
      );
    }

    let files: Vec<String> = table
      .get_files_iter()
      .map_err(JsError::from)?
      .map(|f| f.to_string())
      .collect();

    Ok(files)
  }

  #[napi(catch_unwind)]
  pub fn metadata(&self) -> Result<JsDeltaTableMetadata> {
    let metadata = self.with_table(|t| Ok(t.metadata().cloned().map_err(JsError::from)?))?;

    Ok(JsDeltaTableMetadata {
      id: metadata.id.clone(),
      name: metadata.name.clone(),
      description: metadata.description.clone(),
      partition_columns: metadata.partition_columns.clone(),
      created_time: metadata.created_time,
      configuration: metadata.configuration.clone(),
    })
  }

  #[napi(catch_unwind)]
  pub fn protocol(&self) -> Result<JsDeltaTableProtocolVersions> {
    let table_protocol = self.with_table(|t| Ok(t.protocol().cloned().map_err(JsError::from)?))?;

    let reader_features = table_protocol
      .reader_features
      .as_ref()
      .and_then(|features| {
        let empty_set = !features.is_empty();
        empty_set.then(|| {
          features
            .iter()
            .map(|v| v.to_string())
            .collect::<Vec<String>>()
        })
      });

    let writer_features = table_protocol
      .writer_features
      .as_ref()
      .and_then(|features| {
        let empty_set = !features.is_empty();
        empty_set.then(|| {
          features
            .iter()
            .map(|v| v.to_string())
            .collect::<Vec<String>>()
        })
      });

    Ok(JsDeltaTableProtocolVersions {
      min_reader_version: table_protocol.min_reader_version,
      min_writer_version: table_protocol.min_writer_version,
      reader_features,
      writer_features,
    })
  }

  #[napi(catch_unwind)]
  /// Get the current schema of the Delta table.
  pub fn schema(&self) -> Result<String> {
    let schema = self.with_table(|t| {
      Ok(
        t.get_schema()
          .map_err(JsError::from)
          .map(|s| s.to_owned())?,
      )
    })?;

    // TODO: could return a JS object instead
    Ok(serde_json::to_string(&schema).map_err(JsError::from)?)
  }

  #[napi(catch_unwind)]
  pub async fn history(&self, limit: Option<u8>) -> Result<Vec<String>> {
    let table = self.table.lock().await;
    let history = table.history(limit.map(|l| l as usize))
      .await
      .map_err(JsError::from)
      .map(|s| s.to_owned())?;

    Ok(history
        .iter()
        .map(|c| serde_json::to_string(c).map_err(JsError::from).unwrap())
        .collect())
  }

  /// Run the Vacuum command on the Delta Table: list and delete files no longer
  /// referenced by the Delta table and are older than the retention threshold.
  #[napi(catch_unwind)]
  pub async fn vacuum(&self, options: Option<DeltaTableVacuumOptions>) -> Result<Vec<String>> {
    let mut table = self.table.lock().await;
    let log_store = table.log_store();
    let snapshot = table.snapshot().cloned().map_err(JsError::from)?;

    let mut cmd = VacuumBuilder::new(log_store.clone(), snapshot);
    if let Some(options) = options {
      if let Some(dry_run) = options.dry_run {
        cmd = cmd.with_dry_run(dry_run);
      }

      if let Some(enforce_retention_duration) = options.enforce_retention_duration {
        cmd = cmd.with_enforce_retention_duration(enforce_retention_duration);
      }

      if let Some(retention_period) = options.retention_hours {
        assert_napi!(retention_period >= 0, "retention hours should be positive");
        cmd = cmd.with_retention_period(Duration::hours(retention_period));
      }

      if let Some(commit_properties) = maybe_create_commit_properties(
        options.commit_properties,
        options.post_commithook_properties,
      ) {
        cmd = cmd.with_commit_properties(commit_properties);
      }
    }

    if log_store.clone().name() == "LakeFSLogStore" {
      cmd = cmd.with_custom_execute_handler(Arc::new(LakeFSCustomExecuteHandler {}))
    }

    // GenericError { source: InvalidVacuumRetentionPeriod { provided: 167, min: 168 } }
    let (updated_table, metrics) = cmd.into_future().await.map_err(JsError::from)?;

    table.state = updated_table.state;

    Ok(metrics.files_deleted)
  }

  #[napi]
  pub async fn write(
    &self,
    data: Uint8Array,
    mode: String,
    options: Option<DeltaTableWriteOptions>,
  ) -> Result<()> {
    let mut table = self.table.lock().await;
    let log_store = table.log_store();

    // FIXME: Doesn't this kinda defeat the purpose of the without_files flag?
    // When table isn't already loaded and exists, we need to load it first
    if table.version() == -1 {
      let is_delta_table = table
        .verify_deltatable_existence()
        .await
        .map_err(JsError::from)?;

      if is_delta_table {
        table.load().await.map_err(JsError::from)?;
      }
    }

    let mut builder = WriteBuilder::new(
      log_store.clone(),
      table.state.clone(),
      // Take the Option<state> since it might be the first write,
      // triggered through `write_to_deltalake`
    )
    .with_save_mode(mode.parse().map_err(JsError::from)?);

    let cursor = Cursor::new(data.to_vec());
    let reader = StreamReader::try_new(cursor, None).map_err(JsError::from)?;

    let table_provider = to_lazy_table(reader).map_err(JsError::from)?;

    let plan = LogicalPlanBuilder::scan("source", provider_as_source(table_provider), None)
      .map_err(JsError::from)?
      .build()
      .map_err(JsError::from)?;

    builder = builder.with_input_execution_plan(Arc::new(plan));

    if let Some(options) = options {
      if let Some(schema_mode) = options.schema_mode {
        builder = builder.with_schema_mode(schema_mode.parse().map_err(JsError::from)?);
      }

      if let Some(partition_columns) = options.partition_by {
        builder = builder.with_partition_columns(partition_columns);
      }

      if let Some(writer_props) = options.writer_properties {
        builder = builder
          .with_writer_properties(set_writer_properties(writer_props).map_err(JsError::from)?);
      }

      if let Some(name) = &options.name {
        builder = builder.with_table_name(name);
      };

      if let Some(description) = &options.description {
        builder = builder.with_description(description);
      };

      if let Some(predicate) = options.predicate {
        builder = builder.with_replace_where(predicate);
      };

      if let Some(target_file_size) = options.target_file_size {
        builder = builder.with_target_file_size(target_file_size as usize);
      };

      if let Some(config) = options.configuration {
        builder = builder.with_configuration(config);
      };

      if let Some(commit_properties) = maybe_create_commit_properties(
        options.commit_properties,
        options.post_commithook_properties,
      ) {
        builder = builder.with_commit_properties(commit_properties);
      };
    }

    if log_store.clone().name() == "LakeFSLogStore" {
      builder = builder.with_custom_execute_handler(Arc::new(LakeFSCustomExecuteHandler {}))
    }

    let updated_table = builder.into_future().await.map_err(JsError::from)?;

    table.state = updated_table.state;

    Ok(())
  }
}

fn get_storage_options(
  storage_options: Either<AWSConfigKeyCredentials, AWSConfigKeyProfile>,
) -> HashMap<String, String> {
  let mut options: HashMap<String, String> = HashMap::new();

  match storage_options {
    Either::A(credentials_options) => {
      options.insert("aws_region".to_string(), credentials_options.aws_region);
      options.insert(
        "aws_access_key_id".to_string(),
        credentials_options.aws_access_key_id,
      );
      options.insert(
        "aws_secret_access_key".to_string(),
        credentials_options.aws_secret_access_key,
      );

      if let Some(aws_session_token) = credentials_options.aws_session_token {
        options.insert("aws_session_token".to_string(), aws_session_token);
      }
    }
    Either::B(profile_options) => {
      options.insert("aws_region".to_string(), profile_options.aws_region);
      options.insert("aws_profile".to_string(), profile_options.aws_profile);
    }
  }

  options
}

fn set_writer_properties(writer_properties: JsWriterProperties) -> DeltaResult<WriterProperties> {
  let mut properties = WriterProperties::builder();

  if let Some(data_page_size) = writer_properties.data_page_size_limit {
    properties = properties.set_data_page_size_limit(data_page_size as usize);
  }

  if let Some(dictionary_page_size) = writer_properties.dictionary_page_size_limit {
    properties = properties.set_dictionary_page_size_limit(dictionary_page_size as usize);
  }

  if let Some(data_page_row_count) = writer_properties.data_page_row_count_limit {
    properties = properties.set_data_page_row_count_limit(data_page_row_count as usize);
  }

  if let Some(batch_size) = writer_properties.write_batch_size {
    properties = properties.set_write_batch_size(batch_size as usize);
  }

  if let Some(row_group_size) = writer_properties.max_row_group_size {
    properties = properties.set_max_row_group_size(row_group_size as usize);
  }

  if let Some(statistics_truncate_length) = writer_properties.statistics_truncate_length {
    properties =
      properties.set_statistics_truncate_length(Some(statistics_truncate_length as usize));
  }

  if let Some(compression) = writer_properties.compression {
    let compress: Compression = compression
      .parse()
      .map_err(|err: ParquetError| DeltaTableError::Generic(err.to_string()))?;

    properties = properties.set_compression(compress);
  }

  if let Some(default_column_properties) = writer_properties.default_column_properties {
    if let Some(dictionary_enabled) = default_column_properties.dictionary_enabled {
      properties = properties.set_dictionary_enabled(dictionary_enabled);
    }

    if let Some(statistics_enabled) = default_column_properties.statistics_enabled {
      let enabled_statistics: EnabledStatistics = statistics_enabled
        .parse()
        .map_err(|err: String| DeltaTableError::Generic(err))?;

      properties = properties.set_statistics_enabled(enabled_statistics);
    }

    if let Some(bloom_filter_properties) = default_column_properties.bloom_filter_properties {
      if let Some(set_bloom_filter_enabled) = bloom_filter_properties.set_bloom_filter_enabled {
        properties = properties.set_bloom_filter_enabled(set_bloom_filter_enabled);
      }

      if let Some(bloom_filter_fpp) = bloom_filter_properties.fpp {
        properties = properties.set_bloom_filter_fpp(bloom_filter_fpp);
      }

      if let Some(bloom_filter_ndv) = bloom_filter_properties.ndv {
        properties = properties.set_bloom_filter_ndv(bloom_filter_ndv as u64);
      }
    }
  }

  if let Some(column_properties) = writer_properties.column_properties {
    for (column_name, column_prop) in column_properties {
      if let Some(column_prop) = column_prop {
        if let Some(dictionary_enabled) = column_prop.dictionary_enabled {
          properties = properties
            .set_column_dictionary_enabled(column_name.clone().into(), dictionary_enabled);
        }

        if let Some(statistics_enabled) = column_prop.statistics_enabled {
          let enabled_statistics: EnabledStatistics = statistics_enabled
            .parse()
            .map_err(|err: String| DeltaTableError::Generic(err))?;

          properties = properties
            .set_column_statistics_enabled(column_name.clone().into(), enabled_statistics);
        }

        if let Some(bloom_filter_properties) = column_prop.bloom_filter_properties {
          if let Some(set_bloom_filter_enabled) = bloom_filter_properties.set_bloom_filter_enabled {
            properties = properties.set_column_bloom_filter_enabled(
              column_name.clone().into(),
              set_bloom_filter_enabled,
            );
          }

          if let Some(bloom_filter_fpp) = bloom_filter_properties.fpp {
            properties =
              properties.set_column_bloom_filter_fpp(column_name.clone().into(), bloom_filter_fpp);
          }

          if let Some(bloom_filter_ndv) = bloom_filter_properties.ndv {
            properties =
              properties.set_column_bloom_filter_ndv(column_name.into(), bloom_filter_ndv as u64);
          }
        }
      }
    }
  }

  Ok(properties.build())
}
