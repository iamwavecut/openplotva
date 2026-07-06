-- Backlog subject merge-pass bookkeeping. The 24/7 merge worker groups active
-- cards by (visibility, user_id, chat_id, thread_id, subject) and asks the model
-- to fold over-extracted near-duplicates into one card. `last_merge_pass_at`
-- records when a card's group was last LLM-reviewed so a group the model already
-- consolidated (and legitimately left large) is skipped until the cooldown
-- elapses or enough fresh cards accumulate. Nullable/additive: every existing row
-- gets NULL (never reviewed), so the first pass considers all groups; retrieval
-- behavior is unchanged. Metadata-only ADD COLUMN — no table rewrite.
ALTER TABLE memory_cards ADD COLUMN last_merge_pass_at TIMESTAMPTZ;
