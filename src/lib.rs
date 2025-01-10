use std::sync::Once;
use tokio::runtime::Runtime;

mod query;
mod stream;
mod table;

#[macro_use]
extern crate napi_derive;

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
