//! Live, lock-free handle to the active routing table.
//!
//! The expensive work — reading the database snapshot and building a
//! [`RoutingTable`] — happens off to the side in the app loader. Publishing the
//! result is a single atomic [`ArcSwap::store`], the only synchronization point,
//! so request-path readers never block and never hold a lock across `.await`.

use std::sync::Arc;

use arc_swap::ArcSwap;

use super::table::RoutingTable;

/// Shared handle wrapping the current [`RoutingTable`] behind an `ArcSwap`.
pub struct RouterHandle {
    table: ArcSwap<RoutingTable>,
}

impl RouterHandle {
    #[must_use]
    pub fn new(table: RoutingTable) -> Arc<Self> {
        Arc::new(Self {
            table: ArcSwap::from_pointee(table),
        })
    }

    /// Cheap, lock-free read of the current table for one request.
    #[must_use]
    pub fn load(&self) -> arc_swap::Guard<Arc<RoutingTable>> {
        self.table.load()
    }

    /// Owned `Arc` snapshot of the current table. Unlike [`Self::load`], the
    /// returned handle is `Send` and safe to hold across an `.await`.
    #[must_use]
    pub fn snapshot(&self) -> Arc<RoutingTable> {
        self.table.load_full()
    }

    /// Atomically publish a freshly built table. The previous `Arc` is dropped
    /// once all in-flight readers release their guards.
    pub fn store(&self, table: RoutingTable) {
        self.table.store(Arc::new(table));
    }
}
