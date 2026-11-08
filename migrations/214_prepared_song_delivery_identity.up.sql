-- no-transaction
CREATE UNIQUE INDEX CONCURRENTLY generated_songs_job_prepared_idx
    ON generated_songs (job_id)
    WHERE job_id IS NOT NULL AND prepared_delivery IS NOT NULL;
