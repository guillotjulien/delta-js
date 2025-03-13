use std::collections::HashMap;

use deltalake::kernel::Transaction;
use deltalake::operations::transaction::CommitProperties;
use napi::{bindgen_prelude::FromNapiValue, JsObject, Result};
use serde_json::{Map, Value};

#[derive(Clone)]
#[napi(js_name = "Transaction")]
pub struct JsTransaction {
  pub app_id: String,
  pub version: i64,
  pub last_updated: Option<i64>,
}

#[napi]
impl JsTransaction {
  #[napi(constructor)]
  pub fn new(app_id: String, version: i64, last_updated: Option<i64>) -> Self {
    Self {
      app_id,
      version,
      last_updated,
    }
  }

  fn __repr__(&self) -> String {
    format!(
      "Transaction(app_id={}, version={}, last_updated={})",
      self.app_id,
      self.version,
      self
        .last_updated
        .map_or("None".to_owned(), |n| n.to_string())
    )
  }
}

impl From<Transaction> for JsTransaction {
  fn from(value: Transaction) -> Self {
    JsTransaction {
      app_id: value.app_id,
      version: value.version,
      last_updated: value.last_updated,
    }
  }
}

impl From<&JsTransaction> for Transaction {
  fn from(value: &JsTransaction) -> Self {
    Transaction {
      app_id: value.app_id.clone(),
      version: value.version,
      last_updated: value.last_updated,
    }
  }
}

impl FromNapiValue for JsTransaction {
  unsafe fn from_napi_value(
    env: napi::sys::napi_env,
    napi_val: napi::sys::napi_value,
  ) -> Result<Self> {
    let obj = JsObject::from_napi_value(env, napi_val)?;

    let app_id = obj.get_named_property("appId")?;
    let version = obj.get_named_property("version")?;
    let last_updated = obj.get_named_property("lastUpdated")?;

    Ok(Self {
      app_id,
      version,
      last_updated,
    })
  }
}

#[napi(object, js_name = "CommitProperties")]
pub struct JsCommitProperties {
  pub custom_metadata: Option<HashMap<String, String>>,
  pub max_commit_retries: Option<i8>,
  pub app_transactions: Option<Vec<JsTransaction>>,
}

#[napi(object, js_name = "PostCommitHookProperties")]
pub struct JsPostCommitHookProperties {
  pub create_checkpoint: bool,
  pub cleanup_expired_logs: Option<bool>,
}

pub fn maybe_create_commit_properties(
  maybe_commit_properties: Option<JsCommitProperties>,
  post_commithook_properties: Option<JsPostCommitHookProperties>,
) -> Option<CommitProperties> {
  if maybe_commit_properties.is_none() && post_commithook_properties.is_none() {
    return None;
  }
  let mut commit_properties = CommitProperties::default();

  if let Some(commit_props) = maybe_commit_properties {
    if let Some(metadata) = commit_props.custom_metadata {
      let json_metadata: Map<String, Value> =
        metadata.into_iter().map(|(k, v)| (k, v.into())).collect();
      commit_properties = commit_properties.with_metadata(json_metadata);
    };

    if let Some(max_retries) = commit_props.max_commit_retries {
      commit_properties = commit_properties.with_max_retries(max_retries as usize);
    };

    if let Some(app_transactions) = commit_props.app_transactions {
      let app_transactions = app_transactions.iter().map(Transaction::from).collect();
      commit_properties = commit_properties.with_application_transactions(app_transactions);
    }
  }

  if let Some(post_commit_hook_props) = post_commithook_properties {
    commit_properties = set_post_commithook_properties(commit_properties, post_commit_hook_props)
  }
  Some(commit_properties)
}

pub fn set_post_commithook_properties(
  mut commit_properties: CommitProperties,
  post_commithook_properties: JsPostCommitHookProperties,
) -> CommitProperties {
  commit_properties =
    commit_properties.with_create_checkpoint(post_commithook_properties.create_checkpoint);
  commit_properties =
    commit_properties.with_cleanup_expired_logs(post_commithook_properties.cleanup_expired_logs);
  commit_properties
}
