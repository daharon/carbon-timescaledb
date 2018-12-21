CREATE DATABASE carbon_test;
\c carbon_test;

-- Enable the TimescaleDB extension.
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

CREATE TABLE paths (
    id BIGSERIAL PRIMARY KEY,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
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
    err_detail TEXT;
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
        BEGIN
            INSERT INTO metrics (path_id, value, timestamp)
                VALUES (path_id, metric_data.value, to_timestamp(metric_data.unix_timestamp));
        EXCEPTION
            WHEN UNIQUE_VIOLATION THEN
                GET STACKED DIAGNOSTICS err_detail = PG_EXCEPTION_DETAIL;
                RAISE WARNING '%', err_detail;
        END;
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

CREATE VIEW metrics_view AS
    SELECT paths.path, extract(epoch from metrics.timestamp)::int AS timestamp, metrics.value
    FROM metrics
    JOIN paths ON paths.id = metrics.path_id;

CREATE OR REPLACE FUNCTION insert_metric_view() RETURNS TRIGGER
    LANGUAGE plpgsql
    AS $$
BEGIN
    EXECUTE format('SELECT insert_metric(%L, %L, %L)', NEW.path, NEW.value, NEW.timestamp);
    RETURN NULL;
END;
$$;

CREATE TRIGGER metrics_view_insert INSTEAD OF INSERT ON metrics_view
    FOR EACH ROW EXECUTE PROCEDURE insert_metric_view();

