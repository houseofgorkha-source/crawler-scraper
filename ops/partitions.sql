-- Run monthly from cron. Creates next month's partition, drops anything
-- older than the retention window.
--
-- crawl_attempts is debugging history, not state. Unbounded retention makes
-- it the largest table in the system and slows every query around it.
DO $$
DECLARE
    start_date date := date_trunc('month', now() + interval '1 month')::date;
    end_date   date := date_trunc('month', now() + interval '2 month')::date;
    part_name  text := 'crawl_attempts_' || to_char(start_date, 'YYYY_MM');
BEGIN
    EXECUTE format(
        'CREATE TABLE IF NOT EXISTS %I PARTITION OF crawl_attempts
         FOR VALUES FROM (%L) TO (%L)', part_name, start_date, end_date);
END $$;

-- Retention: 30 days.
DO $$
DECLARE
    p record;
BEGIN
    FOR p IN
        SELECT c.relname FROM pg_class c
        JOIN pg_inherits i ON i.inhrelid = c.oid
        JOIN pg_class parent ON parent.oid = i.inhparent
        WHERE parent.relname = 'crawl_attempts'
          AND c.relname ~ '^crawl_attempts_\d{4}_\d{2}$'
          AND to_date(right(c.relname, 7), 'YYYY_MM') < date_trunc('month', now() - interval '1 month')
    LOOP
        EXECUTE format('DROP TABLE IF EXISTS %I', p.relname);
    END LOOP;
END $$;
