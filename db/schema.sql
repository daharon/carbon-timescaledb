CREATE DATABASE carbon_test;
\c carbon_test;

-- Enable the TimescaleDB extension.
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

CREATE TABLE paths (
    id BIGSERIAL PRIMARY KEY,
    path TEXT NOT NULL UNIQUE
);
COMMENT ON TABLE paths IS 'Unique identifiers for metric series';

CREATE TABLE metrics (
    path_id BIGINT REFERENCES paths(id),
    timestamp TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    value DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (timestamp, path_id)
);
COMMENT ON TABLE metrics IS 'Time-series metrics';

-- Define the `metrics` table as a TimescaleDB Hypertable.
SELECT create_hypertable('metrics', 'timestamp');

--
-- Insert an array of metrics.
--
CREATE TYPE metric AS (
	path text,
	value double precision,
	unix_timestamp integer
);
CREATE OR REPLACE FUNCTION insert_metrics(VARIADIC _metrics metric[]) RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE
    path_id BIGINT;
    metric_data RECORD;
BEGIN
    FOR metric_data IN SELECT path, value, unix_timestamp FROM unnest(_metrics) LOOP
        -- Get the path ID from the `paths` table.
        -- If it does not exist, then create it.
        BEGIN
            SELECT id INTO STRICT path_id FROM paths WHERE path = metric_data.path;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                INSERT INTO paths (path) VALUES (metric_data.path)
                    RETURNING id INTO path_id;
            WHEN TOO_MANY_ROWS THEN
                RAISE EXCEPTION 'Path `%` is not unique.', metric_data.path;
        END;

        -- Insert the metric.
        INSERT INTO metrics (path_id, value, timestamp)
            VALUES (path_id, metric_data.value, to_timestamp(metric_data.unix_timestamp));
    END LOOP;
END;
$$;
COMMENT ON FUNCTION insert_metrics IS 'Insert an array or series of metrics.  SELECT insert_metrics((''x.y.z'', 13.0, 1544990204), (''a.b.c'', 14.5, 1544990232), ...);';

--
-- Insert a single metric.
--
CREATE OR REPLACE FUNCTION insert_metric(_path text, _value double precision, unix_timestamp integer) RETURNS void
    LANGUAGE plpgsql
    AS $$
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
$$;
COMMENT ON FUNCTION insert_metric IS 'Insert a single metric.  SELECT insert_metric(''x.y.z'', 12.0, 1544988880);';
