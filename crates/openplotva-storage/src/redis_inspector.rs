//! Redis inspection and maintenance operations shared by HTTP and runtime APIs.
use std::collections::HashMap;

#[derive(Clone)]
pub struct RedisInspectorStore {
    client: redis::Client,
}

impl RedisInspectorStore {
    pub fn new(client: redis::Client) -> Self {
        Self { client }
    }

    pub async fn scan_keys(
        &self,
        pattern: &str,
        limit: Option<usize>,
    ) -> Result<Vec<String>, redis::RedisError> {
        if limit == Some(0) {
            return Ok(Vec::new());
        }
        let mut connection = self.client.get_multiplexed_async_connection().await?;
        let mut cursor = 0_u64;
        let mut keys = Vec::new();
        loop {
            let remaining = limit.map(|limit| limit.saturating_sub(keys.len()));
            if remaining == Some(0) {
                break;
            }
            let (next_cursor, batch): (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(pattern)
                .arg("COUNT")
                .arg(remaining.map(|n| n.min(100)).unwrap_or(1000))
                .query_async(&mut connection)
                .await?;
            keys.extend(batch.into_iter().take(remaining.unwrap_or(usize::MAX)));
            if next_cursor == 0 {
                break;
            }
            cursor = next_cursor;
        }
        Ok(keys)
    }

    pub async fn value(&self, key: &str) -> Result<Option<Vec<u8>>, redis::RedisError> {
        let mut connection = self.client.get_multiplexed_async_connection().await?;
        redis::cmd("GET")
            .arg(key)
            .query_async(&mut connection)
            .await
    }
    pub async fn delete_key(&self, key: &str) -> Result<(), redis::RedisError> {
        let mut connection = self.client.get_multiplexed_async_connection().await?;
        let _: () = redis::cmd("DEL")
            .arg(key)
            .query_async(&mut connection)
            .await?;
        Ok(())
    }

    pub async fn delete_by_prefix(&self, prefix: &str) -> Result<(), redis::RedisError> {
        let pattern = format!("{prefix}*");
        let mut connection = self.client.get_multiplexed_async_connection().await?;
        let mut cursor = 0_u64;

        loop {
            let (next_cursor, batch): (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(&pattern)
                .arg("COUNT")
                .arg(1000)
                .query_async(&mut connection)
                .await?;
            if !batch.is_empty() {
                let _: () = redis::cmd("DEL")
                    .arg(batch)
                    .query_async(&mut connection)
                    .await?;
            }
            if next_cursor == 0 {
                break;
            }
            cursor = next_cursor;
        }

        Ok(())
    }

    pub async fn flushdb(&self) -> Result<(), redis::RedisError> {
        let mut connection = self.client.get_multiplexed_async_connection().await?;
        redis::cmd("FLUSHDB").query_async(&mut connection).await
    }
}

pub fn prefix_groups_from_keys(
    prefix: &str,
    keys: impl IntoIterator<Item = String>,
) -> HashMap<String, i32> {
    let mut groups = HashMap::<String, i32>::new();
    for key in keys {
        let Some(rest) = key.strip_prefix(prefix) else {
            continue;
        };
        if rest.is_empty() {
            continue;
        };
        let segment = match rest.find(':') {
            Some(index) => &rest[..=index],
            None => rest,
        };
        *groups.entry(format!("{prefix}{segment}")).or_default() += 1;
    }
    groups
}
#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn live_redis_inspection_preserves_bytes_limits_and_prefix_scope()
    -> Result<(), Box<dyn std::error::Error>> {
        let Ok(url) = std::env::var("OPENPLOTVA_TEST_REDIS_URL") else {
            return Ok(());
        };
        let client = redis::Client::open(url)?;
        let store = RedisInspectorStore::new(client.clone());
        let suffix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos();
        let prefix = format!("openplotva:test:inspection:{suffix}:");
        let one = format!("{prefix}a:1");
        let two = format!("{prefix}a:2");
        let other = format!("{prefix}b:1");
        let mut connection = client.get_multiplexed_async_connection().await?;
        let result: Result<(), Box<dyn std::error::Error>> = async {
            let _: () = redis::cmd("MSET")
                .arg(&one)
                .arg([0xff_u8, b'a'].as_slice())
                .arg(&two)
                .arg("second")
                .arg(&other)
                .arg("other")
                .query_async(&mut connection)
                .await?;
            assert_eq!(store.value(&one).await?, Some(vec![0xff, b'a']));
            assert_eq!(
                store.scan_keys(&format!("{prefix}*"), Some(0)).await?,
                Vec::<String>::new()
            );
            assert_eq!(
                store.scan_keys(&format!("{prefix}*"), Some(1)).await?.len(),
                1
            );
            let keys = store.scan_keys(&format!("{prefix}*"), None).await?;
            assert_eq!(keys.len(), 3);
            assert_eq!(
                prefix_groups_from_keys(&prefix, keys),
                HashMap::from([(format!("{prefix}a:"), 2), (format!("{prefix}b:"), 1)])
            );
            store.delete_key(&one).await?;
            store.delete_key(&one).await?;
            assert_eq!(store.value(&one).await?, None);
            store.delete_by_prefix(&format!("{prefix}a:")).await?;
            assert_eq!(store.value(&two).await?, None);
            assert_eq!(store.value(&other).await?, Some(b"other".to_vec()));
            Ok(())
        }
        .await;
        store.delete_by_prefix(&prefix).await?;
        result
    }
}
