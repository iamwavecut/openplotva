-- Existing songs retain NULL preparation data and remain readable.
ALTER TABLE generated_songs ADD COLUMN prepared_delivery jsonb;
