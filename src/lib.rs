use std::collections::HashMap;

use deltalake::{DeltaTableError, protocol::Stats, Schema};
use neon::prelude::*;

struct RawDeltaTable {
    _table: deltalake::DeltaTable,
}

// Clean-up when RawDeltaTable is garbage collected.
impl Finalize for RawDeltaTable {}

// Internal Implementation
impl RawDeltaTable {
    fn new<'a, C>(_cx: &mut C, table_uri: String, version: Option<i64>, without_files: Option<bool>, storage_options: Option<HashMap<String, String>>) -> Result<Self, ()>
    where C: Context<'a> {
        let mut builder = deltalake::DeltaTableBuilder::from_uri(table_uri);

        if let Some(version) = version {
            builder = builder.with_version(version);
        }

        if let Some(_) = without_files {
            builder = builder.without_files();
        }

        if let Some(storage_options) = storage_options {
            builder = builder.with_storage_options(storage_options)
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
        let maybe_options = cx.argument_opt(1).map(|o| o.downcast_or_throw::<JsObject, _>(&mut cx).unwrap());
        
        let mut maybe_version: Option<i64> = None;
        let mut maybe_without_files: Option<bool> = None;
        let mut maybe_storage_options: Option<HashMap<String, String>> = None;
        if let Some(handle) = maybe_options {
            let keys = handle.get_own_property_names(&mut cx).unwrap();
            
            for i in 0..keys.len(&mut cx) {
                let key = keys.get::<JsString, _, _>(&mut cx, i).unwrap().value(&mut cx);
                let value_handle = handle.get::<JsValue, _, _>(&mut cx, key.as_str()).unwrap();
                
                match key.as_str() {
                    "version" => {
                        maybe_version = Some(value_handle.downcast_or_throw::<JsNumber, _>(&mut cx).unwrap().value(&mut cx) as i64);
                    },
                    "withoutFiles" => {
                        maybe_without_files = Some(value_handle.downcast_or_throw::<JsBoolean, _>(&mut cx).unwrap().value(&mut cx));
                    },
                    "storageOptions" => {
                        let handle = handle.get::<JsObject, _, _>(&mut cx, key.as_str()).unwrap();
                        let keys = handle.get_own_property_names(&mut cx).unwrap();
                        
                        let mut values: HashMap<String, String> = HashMap::new();
                        for i in 0..keys.len(&mut cx) {
                            let key = keys.get::<JsString, _, _>(&mut cx, i).unwrap().value(&mut cx);
                            let value_handle = handle.get::<JsValue, _, _>(&mut cx, key.as_str()).unwrap();
                            values.insert(key, value_handle.downcast_or_throw::<JsString, _>(&mut cx).unwrap().value(&mut cx));
                        }

                        maybe_storage_options = Some(values);
                    },
                    _ => {
                        // TODO: Unknown option, throw
                    }
                }
            }

        }

        let table = RawDeltaTable::new(&mut cx, table_uri, maybe_version, maybe_without_files, maybe_storage_options).unwrap();

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
                let value = cx.string(v.as_value().unwrap().as_str().unwrap()); // FIXME: v.as_value() doesn't work when we have Column({ "key": Value(String("")) }) instead of Value(String("")
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

    fn js_schema(mut cx: FunctionContext) -> JsResult<JsString> {
        let table = cx.this().downcast_or_throw::<JsBox<RawDeltaTable>, _>(&mut cx).or_else(|err| cx.throw_error(err.to_string()))?;
        let schema: &Schema = table._table.get_schema().unwrap();

        let schema_string = serde_json::to_string(schema).unwrap();

        Ok(cx.string(schema_string))
    }

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
    cx.export_function("rawDeltaTableSchema", RawDeltaTable::js_schema)?;
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