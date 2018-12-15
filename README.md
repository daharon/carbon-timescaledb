# Carbon to TimescaleDB

## Database Schema
```sql
> CREATE DATABASE carbon_test; 
> \c carbon_test;
> CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;
> CREATE TABLE metrics (
    path TEXT NOT NULL,
    timestamp TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    value DOUBLE PRECISION NOT NULL
);
> SELECT create_hypertable('metrics', 'timestamp'); 
```
