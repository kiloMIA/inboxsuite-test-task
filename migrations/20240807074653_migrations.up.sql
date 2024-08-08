DO $$
BEGIN
    IF NOT EXISTS (
        SELECT FROM pg_catalog.pg_database
        WHERE datname = 'db'
    ) THEN
        PERFORM dblink_exec('dbname=postgres', 'CREATE DATABASE db');
    END IF;
END
$$;

CREATE TABLE IF NOT EXISTS messages (
    id SERIAL PRIMARY KEY,
    profile_id BIGINT NOT NULL,
    class_id SMALLINT NOT NULL,
    roadmap_id SMALLINT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
