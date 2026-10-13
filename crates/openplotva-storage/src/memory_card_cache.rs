//! Read-through RAM cache of visible memory-card lists.
//!
//! The memory pipeline lists the same chat and participant cards many times
//! while it sizes one run's batches. Entries are dropped when a write touches
//! their chat or user, and expire after a TTL that bounds staleness from
//! writers that do not invalidate.

use std::{
    collections::{BTreeMap, HashMap},
    fmt,
    sync::{Arc, Mutex, MutexGuard},
    time::{Duration, Instant},
};

use openplotva_memory::Card;

/// Bounds how long a list can miss writes made outside the invalidating paths.
pub const MEMORY_CARD_CACHE_TTL: Duration = Duration::from_secs(5 * 60);

/// Rough per-card overhead on top of its text, for the size estimate.
const CARD_OVERHEAD_BYTES: usize = 256;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) struct VisibleCardsKey {
    pub(crate) chat_id: i64,
    pub(crate) thread_id: i32,
    pub(crate) user_id: i64,
    pub(crate) own_dm: bool,
    pub(crate) limit: i32,
}

struct Entry {
    cards: Arc<Vec<Card>>,
    stored_at: Instant,
    recency: u64,
    bytes: usize,
}

#[derive(Default)]
struct CacheState {
    entries: HashMap<VisibleCardsKey, Entry>,
    by_recency: BTreeMap<u64, VisibleCardsKey>,
    next_recency: u64,
    generation: u64,
    bytes: usize,
    stats: MemoryCardCacheStats,
}

/// Counters for sizing the cache from production traffic.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct MemoryCardCacheStats {
    pub hits: u64,
    pub misses: u64,
    pub expired: u64,
    pub evictions: u64,
    pub invalidated: u64,
    pub entries: usize,
    pub bytes: usize,
}

pub struct MemoryCardCache {
    capacity: usize,
    ttl: Duration,
    state: Mutex<CacheState>,
}

impl fmt::Debug for MemoryCardCache {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemoryCardCache")
            .field("capacity", &self.capacity)
            .field("ttl", &self.ttl)
            .field("stats", &self.stats())
            .finish()
    }
}

impl MemoryCardCache {
    #[must_use]
    pub fn new(capacity: usize, ttl: Duration) -> Self {
        Self {
            capacity: capacity.max(1),
            ttl,
            state: Mutex::new(CacheState::default()),
        }
    }

    /// Return a fresh entry, or the generation a caller must pass to `put`
    /// after it reads the database.
    pub(crate) fn get(&self, key: &VisibleCardsKey) -> Result<Vec<Card>, u64> {
        self.get_at(key, Instant::now())
    }

    fn get_at(&self, key: &VisibleCardsKey, now: Instant) -> Result<Vec<Card>, u64> {
        let mut state = self.lock();
        let Some(entry) = state.entries.get(key) else {
            state.stats.misses += 1;
            return Err(state.generation);
        };
        if now.saturating_duration_since(entry.stored_at) > self.ttl {
            state.stats.expired += 1;
            state.stats.misses += 1;
            remove_entry(&mut state, key);
            return Err(state.generation);
        }
        let cards = Arc::clone(&entry.cards);
        let previous = entry.recency;
        let recency = state.next_recency;
        state.next_recency += 1;
        state.by_recency.remove(&previous);
        state.by_recency.insert(recency, *key);
        if let Some(entry) = state.entries.get_mut(key) {
            entry.recency = recency;
        }
        state.stats.hits += 1;
        drop(state);
        Ok(cards.as_ref().clone())
    }

    /// Store a list read at `generation`; a write that invalidated the cache
    /// since then makes the list possibly stale, so it is dropped.
    pub(crate) fn put(&self, key: VisibleCardsKey, cards: &[Card], generation: u64) {
        self.put_at(key, cards, generation, Instant::now());
    }

    fn put_at(&self, key: VisibleCardsKey, cards: &[Card], generation: u64, now: Instant) {
        let mut state = self.lock();
        if state.generation != generation {
            return;
        }
        remove_entry(&mut state, &key);
        let bytes = cards.iter().map(card_bytes).sum();
        let recency = state.next_recency;
        state.next_recency += 1;
        state.by_recency.insert(recency, key);
        state.bytes += bytes;
        state.entries.insert(
            key,
            Entry {
                cards: Arc::new(cards.to_vec()),
                stored_at: now,
                recency,
                bytes,
            },
        );
        while state.entries.len() > self.capacity {
            let Some((_, oldest)) = state.by_recency.pop_first() else {
                break;
            };
            if let Some(entry) = state.entries.remove(&oldest) {
                state.bytes -= entry.bytes;
                state.stats.evictions += 1;
            }
        }
    }

    /// Drop lists of `chat_id` and lists that include cards of `user_ids`.
    pub fn invalidate_scope(&self, chat_id: i64, user_ids: &[i64]) {
        let mut state = self.lock();
        state.generation += 1;
        let stale: Vec<VisibleCardsKey> = state
            .entries
            .keys()
            .filter(|key| {
                key.chat_id == chat_id || (key.user_id != 0 && user_ids.contains(&key.user_id))
            })
            .copied()
            .collect();
        state.stats.invalidated += stale.len() as u64;
        for key in &stale {
            remove_entry(&mut state, key);
        }
    }

    /// Drop every list, for writes whose chats and users are not known.
    pub fn clear(&self) {
        let mut state = self.lock();
        state.generation += 1;
        state.stats.invalidated += state.entries.len() as u64;
        state.entries.clear();
        state.by_recency.clear();
        state.bytes = 0;
    }

    #[must_use]
    pub fn stats(&self) -> MemoryCardCacheStats {
        let state = self.lock();
        MemoryCardCacheStats {
            entries: state.entries.len(),
            bytes: state.bytes,
            ..state.stats
        }
    }

    fn lock(&self) -> MutexGuard<'_, CacheState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

fn remove_entry(state: &mut CacheState, key: &VisibleCardsKey) {
    if let Some(entry) = state.entries.remove(key) {
        state.by_recency.remove(&entry.recency);
        state.bytes -= entry.bytes;
    }
}

fn card_bytes(card: &Card) -> usize {
    CARD_OVERHEAD_BYTES
        + card.fact_text.len()
        + card.subject.len()
        + card.predicate.len()
        + card.object.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(chat_id: i64, user_id: i64) -> VisibleCardsKey {
        VisibleCardsKey {
            chat_id,
            thread_id: 0,
            user_id,
            own_dm: false,
            limit: 80,
        }
    }

    fn card(id: i64) -> Card {
        Card {
            id,
            fact_text: format!("fact {id}"),
            ..Card::default()
        }
    }

    #[test]
    fn hit_returns_the_stored_list_until_the_ttl_passes() {
        let cache = MemoryCardCache::new(8, Duration::from_secs(60));
        let start = Instant::now();
        let generation = cache.get_at(&key(1, 0), start).expect_err("cold cache");
        cache.put_at(key(1, 0), &[card(10)], generation, start);

        let cards = cache
            .get_at(&key(1, 0), start + Duration::from_secs(30))
            .expect("fresh entry");
        assert_eq!(cards.iter().map(|card| card.id).collect::<Vec<_>>(), [10]);
        assert!(
            cache
                .get_at(&key(1, 0), start + Duration::from_secs(61))
                .is_err()
        );
        let stats = cache.stats();
        assert_eq!((stats.hits, stats.misses, stats.expired), (1, 2, 1));
        assert_eq!(stats.entries, 0);
    }

    #[test]
    fn capacity_evicts_the_least_recently_used_list() {
        let cache = MemoryCardCache::new(2, Duration::from_secs(60));
        let now = Instant::now();
        for chat_id in [1, 2] {
            let generation = cache.get_at(&key(chat_id, 0), now).expect_err("cold");
            cache.put_at(key(chat_id, 0), &[card(chat_id)], generation, now);
        }
        cache.get_at(&key(1, 0), now).expect("chat 1 cached");
        let generation = cache.get_at(&key(3, 0), now).expect_err("cold");
        cache.put_at(key(3, 0), &[card(3)], generation, now);

        assert!(cache.get_at(&key(1, 0), now).is_ok());
        assert!(cache.get_at(&key(2, 0), now).is_err());
        assert!(cache.get_at(&key(3, 0), now).is_ok());
        assert_eq!(cache.stats().evictions, 1);
    }

    #[test]
    fn invalidation_drops_the_chat_and_its_users_lists_in_other_chats() {
        let cache = MemoryCardCache::new(8, Duration::from_secs(60));
        let now = Instant::now();
        for (chat_id, user_id) in [(1, 0), (1, 7), (2, 7), (2, 8)] {
            let generation = cache.get_at(&key(chat_id, user_id), now).expect_err("cold");
            cache.put_at(key(chat_id, user_id), &[card(1)], generation, now);
        }

        cache.invalidate_scope(1, &[7]);

        assert!(cache.get_at(&key(1, 0), now).is_err());
        assert!(cache.get_at(&key(1, 7), now).is_err());
        assert!(cache.get_at(&key(2, 7), now).is_err());
        assert!(cache.get_at(&key(2, 8), now).is_ok());
        assert_eq!(cache.stats().invalidated, 3);
    }

    #[test]
    fn a_list_read_before_an_invalidation_is_not_stored() {
        let cache = MemoryCardCache::new(8, Duration::from_secs(60));
        let now = Instant::now();
        let generation = cache.get_at(&key(1, 0), now).expect_err("cold");
        cache.invalidate_scope(1, &[]);
        cache.put_at(key(1, 0), &[card(1)], generation, now);

        assert!(cache.get_at(&key(1, 0), now).is_err());
        assert_eq!(cache.stats().entries, 0);
    }
}
