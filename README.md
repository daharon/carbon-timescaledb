# Carbon to TimescaleDB

## Database Schema
```sql
> CREATE DATABASE carbon_test; 
> \c carbon_test;
> CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;
> CREATE TABLE paths (
    id BIGSERIAL PRIMARY KEY,
    path TEXT NOT NULL UNIQUE
);
> CREATE TABLE metrics (
    path_id BIGINT REFERENCES paths(id),
    timestamp TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    value DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (path_id, timestamp)
);
> SELECT create_hypertable('metrics', 'timestamp'); 
> CREATE OR REPLACE FUNCTION
insert_metric(_path TEXT, _value DOUBLE PRECISION, unix_timestamp INT)
    RETURNS VOID AS $$
DECLARE
    path_id BIGINT;
BEGIN
    -- Get the path ID from the `paths` table.
    -- If it does not exist, then create it.
    BEGIN
        SELECT id INTO STRICT path_id FROM paths WHERE path = _path;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            INSERT INTO paths (path) VALUES (_path) 
                RETURNING id INTO path_id;
        WHEN TOO_MANY_ROWS THEN
            RAISE EXCEPTION 'Path `%` is not unique.', _path;
    END;

    -- Insert the metric.
    INSERT INTO metrics (path_id, value, timestamp)
        VALUES (path_id, _value, to_timestamp(unix_timestamp));
END;
$$ LANGUAGE plpgsql;
```
