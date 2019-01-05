# Carbon to TimescaleDB
[`carbon-cache`](https://github.com/graphite-project/carbon) daemon which writes to [TimescaleDB](https://www.timescale.com/) instead of the standard
[Whisper](https://github.com/graphite-project/whisper) file format.

This is an experiment and is not a feature-complete re-implementation of Carbon Cache.

## Database Schema
See the [schema.sql](db/schema.sql) file.

## Usage
```
Carbon Cache for TimescaleDB.

USAGE:
    carbon-timescaledb [OPTIONS] --dbname <NAME> --host <HOST> --password <PASSWORD> --username <NAME>

FLAGS:
        --help       Prints help information
    -V, --version    Prints version information

OPTIONS:
    -b, --batch-size <NUM>         Number of metric data points written by each thread per write operation. [default:
                                   100]
    -c, --concurrency <NUM>        Number of threads to spawn to perform writes to the database. [default: 1]
    -d, --dbname <NAME>            Database name to connect to.
    -h, --host <HOST>              Host address of the PostgreSQL database.
        --listen-address <ADDR>    Address to listen on. [default: 127.0.0.1]
        --listen-port <PORT>       Port to listen on. [default: 2003]
    -l, --log-level <LEVEL>        Log level (TRACE, DEBUG, ERROR, WARN, INFO). [default: info]
    -s, --max-cache-size <NUM>     Limit the cache size. Number of metric data points. [default: 100000]
    -W, --password <PASSWORD>      Password to connect to the PostgreSQL database.
    -p, --port <PORT>              Port number at which the PostgreSQL database is listening. [default: 5432]
    -u, --username <NAME>          Username to connect to the PostgreSQL database.
```
