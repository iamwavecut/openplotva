-- Source SHA-256: 0ab5e127e274b3d2a5c77e7e524985c29d35bc1ad56b108321e733f3938ec038

CREATE OR REPLACE FUNCTION ensure_chat_history_partition(bucket_day date)
RETURNS void AS $$
DECLARE
    partition_name text := 'chat_history_entries_' || to_char(bucket_day, 'YYYYMMDD');
    next_day date := bucket_day + 1;
BEGIN
    EXECUTE format(
        'CREATE TABLE IF NOT EXISTS %I PARTITION OF chat_history_entries FOR VALUES FROM (%L) TO (%L)',
        partition_name,
        bucket_day,
        next_day
    );
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION drop_expired_chat_history_partitions(cutoff_day date)
RETURNS text[] AS $$
DECLARE
    partition_record record;
    partition_day date;
    dropped text[] := '{}';
BEGIN
    FOR partition_record IN
        SELECT child.relname
        FROM pg_inherits
        JOIN pg_class parent ON parent.oid = pg_inherits.inhparent
        JOIN pg_class child ON child.oid = pg_inherits.inhrelid
        JOIN pg_namespace parent_ns ON parent_ns.oid = parent.relnamespace
        JOIN pg_namespace child_ns ON child_ns.oid = child.relnamespace
        WHERE parent.relname = 'chat_history_entries'
          AND parent_ns.nspname = current_schema()
          AND child_ns.nspname = current_schema()
    LOOP
        IF partition_record.relname !~ '^chat_history_entries_[0-9]{8}$' THEN
            CONTINUE;
        END IF;

        partition_day := to_date(right(partition_record.relname, 8), 'YYYYMMDD');
        IF partition_day < cutoff_day THEN
            EXECUTE format('DROP TABLE IF EXISTS %I', partition_record.relname);
            dropped := array_append(dropped, partition_record.relname);
        END IF;
    END LOOP;

    RETURN dropped;
END;
$$ LANGUAGE plpgsql;
