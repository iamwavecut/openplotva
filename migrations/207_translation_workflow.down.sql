-- Dropping the workflow cascades to its assignments and triggers; translate_text
-- then has no route until the workflow is restored.
DELETE FROM workflows WHERE key = 'translation';
