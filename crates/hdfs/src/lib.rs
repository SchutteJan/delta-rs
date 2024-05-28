use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use deltalake_core::logstore::{default_logstore, logstores, LogStore, LogStoreFactory};
use deltalake_core::storage::{
    factories, ObjectStoreFactory, ObjectStoreRef, StorageOptions,
};
use deltalake_core::{DeltaResult, Path};
use url::Url;
use hdfs_native_object_store::HdfsObjectStore;
use hdfs_native::Client;
mod config;
pub mod error;


trait HdfsOptions {
    fn as_hdfs_options(&self) -> HashMap<config::HdfsConfigKey, String>;
}

impl HdfsOptions for StorageOptions {
    fn as_hdfs_options(&self) -> HashMap<config::HdfsConfigKey, String> {
        self.0
            .iter()
            .filter_map(|(key, value)| {
                Some((
                    config::HdfsConfigKey::from_str(&key.to_ascii_lowercase()).ok()?,
                    value.clone(),
                ))
            })
            .collect()
    }
}

#[derive(Clone, Default, Debug)]
pub struct HdfsFactory {}

impl ObjectStoreFactory for HdfsFactory {
    fn parse_url_opts(
        &self,
        url: &Url,
        options: &StorageOptions,
    ) -> DeltaResult<(ObjectStoreRef, Path)> {
        let _config = config::HdfsConfigHelper::try_new(options.as_hdfs_options())?;
        let client = Client::new(url.to_string().as_str()).unwrap();

        let store = Arc::new(HdfsObjectStore::new(client)) as ObjectStoreRef;
        Ok((store, Path::from("/")))
    }
}

impl LogStoreFactory for HdfsFactory {
    fn with_options(
        &self,
        store: ObjectStoreRef,
        location: &Url,
        options: &StorageOptions,
    ) -> DeltaResult<Arc<dyn LogStore>> {
        Ok(default_logstore(store, location, options))
    }
}

/// Register an [ObjectStoreFactory] for hdfs [Url] scheme
pub fn register_handlers(_additional_prefixes: Option<Url>) {
    let factory = Arc::new(HdfsFactory {});
    let scheme = &"hdfs";
    let url = Url::parse(&format!("{}://", scheme)).unwrap();
    factories().insert(url.clone(), factory.clone());
    logstores().insert(url.clone(), factory.clone());
}
