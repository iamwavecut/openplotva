use std::sync::{Arc, Mutex};

use openplotva_server::{RuntimeCacheInspector, RuntimeCacheSnapshotData, RuntimeCacheStatsData};
use openplotva_storage::{PostgresChatSettingsStore, RedisRateLimitStore};

use crate::{permissions::ChatPermissionPolicy, rate_limits::ChatRateLimitPolicy};

const GO_SERVER_CACHE_CAPACITY: i32 = 10_000;
const GO_PLANNER_CACHE_CAPACITY: i32 = 10_000;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct PolicyCacheStats {
    pub(crate) size: usize,
    pub(crate) hits: u64,
    pub(crate) misses: u64,
    pub(crate) mem_size: u64,
}

impl PolicyCacheStats {
    fn plus(self, other: Self) -> Self {
        Self {
            size: self.size.saturating_add(other.size),
            hits: self.hits.saturating_add(other.hits),
            misses: self.misses.saturating_add(other.misses),
            mem_size: self.mem_size.saturating_add(other.mem_size),
        }
    }
}

#[derive(Clone, Default)]
pub(crate) struct RuntimeCacheInspectorHandle {
    policy_caches: Arc<Mutex<Option<PolicyCacheSources>>>,
}

#[derive(Clone)]
struct PolicyCacheSources {
    rate_limits: Arc<ChatRateLimitPolicy<RedisRateLimitStore>>,
    permissions: Arc<ChatPermissionPolicy<PostgresChatSettingsStore>>,
}

impl RuntimeCacheInspectorHandle {
    pub(crate) fn set_policy_caches(
        &self,
        rate_limits: Arc<ChatRateLimitPolicy<RedisRateLimitStore>>,
        permissions: Arc<ChatPermissionPolicy<PostgresChatSettingsStore>>,
    ) {
        *self
            .policy_caches
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(PolicyCacheSources {
            rate_limits,
            permissions,
        });
    }
}

impl RuntimeCacheInspector for RuntimeCacheInspectorHandle {
    fn stats(&self) -> RuntimeCacheSnapshotData {
        let Some(caches) = self
            .policy_caches
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
        else {
            return RuntimeCacheSnapshotData::default();
        };

        let policy_stats = caches
            .rate_limits
            .cache_stats()
            .plus(caches.permissions.cache_stats());
        runtime_cache_snapshot_from_policy_stats(policy_stats)
    }
}

fn runtime_cache_snapshot_from_policy_stats(
    policy_stats: PolicyCacheStats,
) -> RuntimeCacheSnapshotData {
    RuntimeCacheSnapshotData {
        cache: RuntimeCacheStatsData {
            size: policy_stats.size.min(i32::MAX as usize) as i32,
            capacity: GO_SERVER_CACHE_CAPACITY,
            hits: policy_stats.hits,
            misses: policy_stats.misses,
            mem_size: policy_stats.mem_size,
        },
        planner_cache: RuntimeCacheStatsData {
            capacity: GO_PLANNER_CACHE_CAPACITY,
            ..RuntimeCacheStatsData::default()
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_cache_snapshot_reports_go_capacity_and_zero_planner_stats() {
        let snapshot = runtime_cache_snapshot_from_policy_stats(PolicyCacheStats {
            size: 12,
            hits: 34,
            misses: 56,
            mem_size: 78,
        });

        assert_eq!(snapshot.cache.size, 12);
        assert_eq!(snapshot.cache.capacity, GO_SERVER_CACHE_CAPACITY);
        assert_eq!(snapshot.cache.hits, 34);
        assert_eq!(snapshot.cache.misses, 56);
        assert_eq!(snapshot.cache.mem_size, 78);
        assert_eq!(snapshot.planner_cache.size, 0);
        assert_eq!(snapshot.planner_cache.capacity, GO_PLANNER_CACHE_CAPACITY);
        assert_eq!(snapshot.planner_cache.hits, 0);
        assert_eq!(snapshot.planner_cache.misses, 0);
        assert_eq!(snapshot.planner_cache.mem_size, 0);
    }
}
