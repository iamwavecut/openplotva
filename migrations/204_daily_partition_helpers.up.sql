-- Daily RANGE partitions on a timestamptz key, named <parent>_YYYYMMDD and
-- bounded by UTC midnights. Retention drops whole partitions instead of deleting
-- rows; `keep_when` names rows that must not be dropped yet.
CREATE OR REPLACE FUNCTION ensure_daily_partition(parent text, partition_day date)
RETURNS void AS $$
BEGIN
    EXECUTE format(
        'CREATE TABLE IF NOT EXISTS %I PARTITION OF %I FOR VALUES FROM (%L) TO (%L)',
        parent || '_' || to_char(partition_day, 'YYYYMMDD'),
        parent,
        partition_day::timestamp AT TIME ZONE 'UTC',
        (partition_day + 1)::timestamp AT TIME ZONE 'UTC'
    );
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION drop_daily_partitions_before(
    parent text,
    cutoff_day date,
    keep_when text DEFAULT NULL
)
RETURNS text[] AS $$
DECLARE
    child record;
    partition_day date;
    busy boolean;
    dropped text[] := '{}';
BEGIN
    FOR child IN
        SELECT partition.relname
        FROM pg_inherits
        JOIN pg_class parent_table ON parent_table.oid = pg_inherits.inhparent
        JOIN pg_class partition ON partition.oid = pg_inherits.inhrelid
        JOIN pg_namespace parent_ns ON parent_ns.oid = parent_table.relnamespace
        WHERE parent_table.relname = parent
          AND parent_ns.nspname = current_schema()
        ORDER BY partition.relname
    LOOP
        IF child.relname !~ ('^' || parent || '_[0-9]{8}$') THEN
            CONTINUE;
        END IF;
        partition_day := to_date(right(child.relname, 8), 'YYYYMMDD');
        IF partition_day >= cutoff_day THEN
            CONTINUE;
        END IF;
        IF keep_when IS NOT NULL THEN
            EXECUTE format('SELECT EXISTS (SELECT 1 FROM %I WHERE %s)', child.relname, keep_when)
                INTO busy;
            IF busy THEN
                CONTINUE;
            END IF;
        END IF;
        EXECUTE format('DROP TABLE IF EXISTS %I', child.relname);
        dropped := array_append(dropped, child.relname::text);
    END LOOP;
    RETURN dropped;
END;
$$ LANGUAGE plpgsql;
