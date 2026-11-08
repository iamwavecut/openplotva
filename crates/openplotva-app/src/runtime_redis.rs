use openplotva_storage::redis_inspector::{RedisInspectorStore, prefix_groups_from_keys};

#[derive(Clone)]
pub(crate) struct RedisRuntimeInspector {
    store: RedisInspectorStore,
}
impl RedisRuntimeInspector {
    pub(crate) fn new(client: redis::Client) -> Self {
        Self {
            store: RedisInspectorStore::new(client),
        }
    }
}

pub(crate) fn runtime_redis_prefix_groups_from_keys(
    prefix: &str,
    keys: impl IntoIterator<Item = String>,
) -> Vec<openplotva_server::RuntimeRedisPrefixGroup> {
    let groups = prefix_groups_from_keys(prefix, keys);
    let mut groups = groups
        .into_iter()
        .map(|(prefix, count)| openplotva_server::RuntimeRedisPrefixGroup { prefix, count })
        .collect::<Vec<_>>();
    groups.sort_by(|a, b| a.prefix.cmp(&b.prefix));
    groups
}

pub(crate) fn runtime_redis_value_from_bytes(
    key: &str,
    bytes: &[u8],
    max_bytes: usize,
) -> openplotva_server::RuntimeRedisValue {
    let truncated = max_bytes > 0 && bytes.len() > max_bytes;
    let bytes = if truncated {
        &bytes[..max_bytes]
    } else {
        bytes
    };
    openplotva_server::RuntimeRedisValue {
        key: key.to_owned(),
        value: String::from_utf8_lossy(bytes).into_owned(),
        truncated,
    }
}

impl openplotva_server::RuntimeRedisInspector for RedisRuntimeInspector {
    fn prefix_groups<'a>(
        &'a self,
        prefix: &'a str,
        limit: usize,
    ) -> openplotva_server::RuntimeRedisInspectorFuture<
        'a,
        Vec<openplotva_server::RuntimeRedisPrefixGroup>,
    > {
        Box::pin(async move {
            let keys = self
                .store
                .scan_keys(&format!("{prefix}*"), Some(limit))
                .await
                .map_err(|error| error.to_string())?;
            Ok(runtime_redis_prefix_groups_from_keys(prefix, keys))
        })
    }
    fn keys<'a>(
        &'a self,
        pattern: &'a str,
        limit: usize,
    ) -> openplotva_server::RuntimeRedisInspectorFuture<'a, Vec<String>> {
        Box::pin(async move {
            self.store
                .scan_keys(pattern, Some(limit))
                .await
                .map_err(|error| error.to_string())
        })
    }
    fn value<'a>(
        &'a self,
        key: &'a str,
        max_bytes: usize,
    ) -> openplotva_server::RuntimeRedisInspectorFuture<
        'a,
        Option<openplotva_server::RuntimeRedisValue>,
    > {
        Box::pin(async move {
            self.store
                .value(key)
                .await
                .map(|value| {
                    value.map(|bytes| runtime_redis_value_from_bytes(key, &bytes, max_bytes))
                })
                .map_err(|error| error.to_string())
        })
    }
}
