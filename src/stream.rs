use napi::bindgen_prelude::*;
use napi_derive::napi;

#[napi]
pub struct RustStream {
  current: u32,
  max: u32,
}

#[napi]
impl RustStream {
  #[napi(constructor)]
  pub fn new(max: u32) -> Self {
    RustStream { current: 0, max }
  }

  #[napi]
  pub fn read(&mut self) -> Result<Option<Buffer>> {
    if self.current >= self.max {
      return Ok(None);
    }

    let mut chunk = Vec::with_capacity(1024);
    for _ in 0..1024 {
      chunk.push(self.current as u8);
    }
    self.current += 1;

    Ok(Some(chunk.into()))
  }
}
