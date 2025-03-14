#[macro_export]
macro_rules! assert_napi {
  ($cond:expr) => {
    if !($cond) {
      return Err(::napi::Error::new(
        ::napi::Status::GenericFailure,
        format!("Assertion failed: {}", stringify!($cond)),
      ));
    }
  };

  ($cond:expr, $msg:expr) => {
    if !($cond) {
      return Err(::napi::Error::new(
        ::napi::Status::GenericFailure,
        $msg.to_string(),
      ));
    }
  };

  ($cond:expr, $status:expr, $msg:expr) => {
    if !($cond) {
      return Err(::napi::Error::new($status, $msg.to_string()));
    }
  };
}
