#[macro_use]
extern crate napi_derive;

use deltalake::init_client_version;
use napi::*;
use std::sync::Once;
use tokio::runtime::Runtime;

// Order of crate import matters for Typescript type definition generation!
#[rustfmt::skip]
#[macro_use]
mod utils;
#[rustfmt::skip]
mod transaction;
#[rustfmt::skip]
mod query;
#[rustfmt::skip]
mod table;

#[module_exports]
/// This function is executed when importing the module in JS
/// and registers the [ObjectStoreFactory] needed to handle common cloud providers URL schemes.
/// Only AWS is supported so far as I don't have access to other cloud providers for testing.
/// https://github.com/delta-io/delta-rs/blob/0b90a11383dce614be369032062e3e8e78cf95d9/python/src/lib.rs#L2197
fn init(_: JsObject) -> Result<()> {
  deltalake::aws::register_handlers(None);
  // deltalake::azure::register_handlers(None);
  // deltalake::gcp::register_handlers(None);
  // deltalake::hdfs::register_handlers(None);
  // deltalake_mount::register_handlers(None);

  init_client_version(format!("js-{}", env!("CARGO_PKG_VERSION")).as_str());

  Ok(())
}

static INIT: Once = Once::new();
static mut RUNTIME: Option<Runtime> = None;

// napi::tokio::runtime::Handle::current() is borked in napi-rs 3.0.0;
#[inline]
fn get_runtime() -> &'static tokio::runtime::Runtime {
  unsafe {
    INIT.call_once(|| {
      RUNTIME = Some(Runtime::new().unwrap());
    });
    RUNTIME.as_ref().unwrap()
  }
}
