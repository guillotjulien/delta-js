use std::convert::TryFrom;

use deltalake::{PartitionFilter, DeltaTableError, protocol::Stats};
use neon::prelude::*;

struct RawDeltaTable {
    _table: deltalake::DeltaTable,
}

enum PartitionFilterValue<'a> {
    Single(&'a str),
    Multiple(Vec<&'a str>),
}

// Clean-up when RawDeltaTable is garbage collected.
impl Finalize for RawDeltaTable {}

// Internal Implementation
impl RawDeltaTable {
    fn new<'a, C>(_cx: &mut C, table_uri: String, version: Option<i64>/* , storage_options: Option<HashMap<String, String>> */) -> Result<Self, ()>
    where C: Context<'a> {
        let mut builder = deltalake::DeltaTableBuilder::from_uri(table_uri);

        // let options = storage_options.clone().unwrap_or_default();
        // if let Some(storage_options) = storage_options {
        //     builder = builder.with_storage_options(storage_options)
        // }
        if let Some(version) = version {
            builder = builder.with_version(version);
        }

        // FIXME: Likely, new shouldn't load the table in new DeltaTable, b/c that would be an async operation

        let table = rt().block_on(builder.load()).unwrap(); // TODO: Throw error properly instead of crashing in Rust: NotATable("No snapshot or version 0 found, perhaps /home/julien/projects/testing/delta-js/test-table-2/ is an empty dir?")

        Ok(RawDeltaTable { _table: table })
    }
}

// Methods exposed to JavaScript
// The `JsBox` boxed `RawDeltaTable` is expected as the `this` value on all methods except `js_new`
impl RawDeltaTable {
    /// Create the Delta table from a path with an optional version.
    /// Multiple StorageBackends are currently supported: AWS S3 and and local URI.
    /// Depending on the storage backend used, you could provide options values using the `storageOptions` parameter.
    fn js_new(mut cx: FunctionContext) -> JsResult<JsBox<RawDeltaTable>> {
        let table_uri: String = cx.argument::<JsString>(0)?.value(&mut cx);
        let maybe_version = cx.argument_opt(1);

        let mut version: Option<i64> = None;
        if let Some(handle) = maybe_version {
            let string_version = handle.to_string(&mut cx)?.value(&mut cx);
            version = string_version.parse::<i64>().ok();
        }

        let table = RawDeltaTable::new(&mut cx, table_uri, version).unwrap();

        Ok(cx.boxed(table))
    }

    /// Get the version of the Delta table.
    fn js_version(mut cx: FunctionContext) -> JsResult<JsNumber> {
        let table = cx.this().downcast_or_throw::<JsBox<RawDeltaTable>, _>(&mut cx).or_else(|err| cx.throw_error(err.to_string()))?;
        Ok(cx.number(table._table.version() as f64))
    }

    /// Get the .parquet files of the DeltaTable.
    /// 
    /// The paths are as they are saved in the delta log, which may either be
    /// relative to the table root or absolute URIs.
    fn js_files(mut cx: FunctionContext) -> JsResult<JsArray> {
        let table = cx.this().downcast_or_throw::<JsBox<RawDeltaTable>, _>(&mut cx).or_else(|err| cx.throw_error(err.to_string()))?;

        let files: Vec<String> = table._table
            .get_files_iter()
            .map(|f| f.to_string())
            .collect();

        let a = JsArray::new(&mut cx, files.len() as u32);
        for (i, s) in files.iter().enumerate() {       
            let v = cx.string(s);
            a.set(&mut cx, i as u32, v)?;
        }

        Ok(a)
    }

    /// Get the .parquet files of the DeltaTable.
    /// 
    /// The paths are as they are saved in the delta log, which may either be
    /// relative to the table root or absolute URIs.
    /// 
    /// Use the partition_filters parameter to retrieve a subset of files that match the
    /// given filters.
    fn js_file_uris(mut cx: FunctionContext) -> JsResult<JsArray> {
        let table = cx.this().downcast_or_throw::<JsBox<RawDeltaTable>, _>(&mut cx).or_else(|err| cx.throw_error(err.to_string()))?;
        let files: Vec<String> = table._table.get_file_uris().collect();

        let a = JsArray::new(&mut cx, files.len() as u32);
        for (i, s) in files.iter().enumerate() {       
            let v = cx.string(s);
            a.set(&mut cx, i as u32, v)?;
        }

        Ok(a)
    }

    fn js_get_stats(mut cx: FunctionContext) -> JsResult<JsArray> {
        let table = cx.this().downcast_or_throw::<JsBox<RawDeltaTable>, _>(&mut cx).or_else(|err| cx.throw_error(err.to_string()))?;
        let stats: Vec<Result<Option<Stats>, DeltaTableError>> = table._table.get_stats().collect();

        let a = JsArray::new(&mut cx, stats.len() as u32);

        let mut i = 0;
        for s in stats {
            let stat = s.unwrap().unwrap();
            
            let obj: Handle<JsObject> = cx.empty_object();

            let js_num_records = cx.number(stat.num_records as f64);

            let js_min_values = cx.empty_object();
            for (k, v) in stat.min_values {
                let key = &k[..];
                let value = cx.string(v.as_value().unwrap().as_str().unwrap());
                js_min_values.set(&mut cx, key, value).unwrap();
            }

            let js_max_values = cx.empty_object();
            for (k, v) in stat.max_values {
                let key = &k[..];
                let value = cx.string(v.as_value().unwrap().as_str().unwrap());
                js_max_values.set(&mut cx, key, value).unwrap();
            }

            let js_null_count = cx.empty_object();
            for (k, v) in stat.null_count {
                let key = &k[..];
                let value = cx.number(v.as_value().unwrap() as f64);
                js_null_count.set(&mut cx, key, value).unwrap();
            }

            obj.set(&mut cx, "numRecords", js_num_records).unwrap();
            obj.set(&mut cx, "minValues", js_min_values).unwrap();
            obj.set(&mut cx, "maxValues", js_max_values).unwrap();
            obj.set(&mut cx, "nullCount", js_null_count).unwrap();

            a.set(&mut cx, i as u32, obj).unwrap();

            i += 1;
        }

        Ok(a)
    }

    // fn js_schema(mut cx: FunctionContext) -> JsResult<JsObject> {
    //     let table = cx.this().downcast_or_throw::<JsBox<RawDeltaTable>, _>(&mut cx).or_else(|err| cx.throw_error(err.to_string()))?;
    //     let schema: &deltalake::Schema = table._table.get_schema().unwrap();

    //     let fields: Vec<SchemaField> = schema
    //         .get_fields()
    //         .iter()
    //         .map(|field| SchemaField {
    //             inner: field.clone(),
    //         })
    //         .collect();

    //     Ok()
    // }

    // fn js_to_array(mut cx: FunctionContext) -> JsResult<JsPromise> {
    //     let table = cx.this().downcast_or_throw::<JsBox<RawDeltaTable>, _>(&mut cx)?;
    //     let (deferred, promise) = cx.promise();

    //     table._table.get_files().

    //     // deltalake::DeltaOps::create(self).


    //     // table._table

    //     Ok()
    // }

    // Now we'll need to implement all the methods so that we can later wrap them in a clean TS facade
}

#[neon::main]
fn main(mut cx: ModuleContext) -> NeonResult<()> {
    // TODO: Can apparently export class:
    // https://github.com/katyo/ledb/blob/master/ledb-node/native/src/lib.rs
    // https://github.com/katyo/ledb/blob/master/ledb-node/native/src/documents.rs
    // 
    // Nice try macro: https://github.com/katyo/ledb/blob/master/ledb-node/native/src/helper.rs

    cx.export_function("rawDeltaTableNew", RawDeltaTable::js_new)?;
    cx.export_function("rawDeltaTableVersion", RawDeltaTable::js_version)?;
    // cx.export_function("rawDeltaTableSchema", RawDeltaTable::js_schema)?;
    // cx.export_function("rawDeltaTableToArray", RawDeltaTable::js_to_array)?;
    cx.export_function("rawDeltaTableFiles", RawDeltaTable::js_files)?;
    cx.export_function("rawDeltaTableFileUris", RawDeltaTable::js_file_uris)?;
    cx.export_function("rawDeltaTableStats", RawDeltaTable::js_get_stats)?;
    Ok(())
}

#[inline]
fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Runtime::new().unwrap()
}