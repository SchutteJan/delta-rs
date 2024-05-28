use std::collections::HashMap;
use std::str::FromStr;

use crate::error::{Error, Result};

#[derive(PartialEq, Eq, Hash, Clone, Debug, Copy)]
#[non_exhaustive]
pub enum HdfsConfigKey {
    StorageUrl
}


impl AsRef<str> for HdfsConfigKey {
    fn as_ref(&self) -> &str {
        match self {
            // TODO: Define configuration options for HDFS
            Self::StorageUrl => "just a placeholder for now",
        }
    }
}

impl FromStr for HdfsConfigKey {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            // TODO: Define configuration options for HDFS
            "storageurl" => Ok(HdfsConfigKey::StorageUrl),
            _ => Err(Error::HdfsError(s.to_string())),
        }
    }
}


/// Helper struct to create full configuration from passed options and environment
pub(crate) struct HdfsConfigHelper {
    config: HashMap<HdfsConfigKey, String>
}

impl HdfsConfigHelper {
    /// Create a new [`ConfigHelper`]
    pub fn try_new(
        config: impl IntoIterator<Item = (impl AsRef<str>, impl Into<String>)>,
    ) -> Result<Self> {
        Ok(Self {
            config: config
                .into_iter()
                .map(|(key, value)| Ok((HdfsConfigKey::from_str(key.as_ref())?, value.into())))
                .collect::<Result<_, Error>>()?,
        })
    }
}
