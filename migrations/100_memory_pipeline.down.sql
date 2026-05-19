-- Source SHA-256: 25fe2f5dd01129a76dd45362caeda581a8dd8af1ce0e0cefd66f42dfd0473bc8

DROP INDEX IF EXISTS memory_runs_chat_range_idx;
DROP INDEX IF EXISTS memory_runs_claim_idx;
DROP TABLE IF EXISTS memory_runs;
DROP INDEX IF EXISTS memory_links_to_idx;
DROP INDEX IF EXISTS memory_links_from_idx;
DROP TABLE IF EXISTS memory_links;
DROP INDEX IF EXISTS memory_sources_chat_time_idx;
DROP INDEX IF EXISTS memory_sources_episode_idx;
DROP INDEX IF EXISTS memory_sources_card_idx;
DROP TABLE IF EXISTS memory_sources;
DROP INDEX IF EXISTS memory_episodes_embedding_hnsw_idx;
DROP INDEX IF EXISTS memory_episodes_text_search_idx;
DROP INDEX IF EXISTS memory_episodes_scope_idx;
DROP TABLE IF EXISTS memory_episodes;
DROP INDEX IF EXISTS memory_cards_embedding_hnsw_idx;
DROP INDEX IF EXISTS memory_cards_text_search_idx;
DROP INDEX IF EXISTS memory_cards_scope_idx;
DROP INDEX IF EXISTS memory_cards_active_dedup_idx;
DROP TABLE IF EXISTS memory_cards;
