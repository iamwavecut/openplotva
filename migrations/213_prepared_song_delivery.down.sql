-- Prepared payloads cannot be reconstructed after rollback. Drain music delivery before rollback.
ALTER TABLE generated_songs DROP COLUMN prepared_delivery;
