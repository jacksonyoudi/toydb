use serde_derive::{Deserialize, Serialize};
use std::fmt::{self, Display};

/// Result returning Error
/// 包装一个新的错误类型
pub type Result<T> = std::result::Result<T, Error>;

/// toyDB errors. All except Internal are considered user-facing.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Error {
    Abort,
    Config(String),
    Internal(String),
    Parse(String),
    ReadOnly,
    Serialization,
    Value(String),
}



