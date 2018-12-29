use std::net::IpAddr;

use clap::ArgMatches;


pub struct Config {
    // Server
    pub listen_ip_addr: IpAddr,
    pub listen_port: u16,
    // Database
    pub db_host: String,
    pub db_port: u16,
    pub db_name: String,
    pub db_username: String,
    pub db_password: String,
    // Internal
    pub concurrency: u8,
    pub max_cache_size: usize,
    pub batch_size: u16,
}

impl Config {
    pub fn new() -> Self {
        use clap::value_t_or_exit;

        let matches = parse();
        let listen_ip_addr = value_t_or_exit!(matches.value_of("listen-address"), IpAddr);
        let listen_port = value_t_or_exit!(matches.value_of("listen-port"), u16);
        let db_port = value_t_or_exit!(matches.value_of("port"), u16);
        let concurrency = value_t_or_exit!(matches.value_of("concurrency"), u8);
        let max_cache_size = value_t_or_exit!(matches.value_of("max-cache-size"), usize);
        let batch_size = value_t_or_exit!(matches.value_of("batch-size"), u16);
        Self {
            listen_ip_addr,
            listen_port,
            db_host: matches.value_of("host").unwrap().to_string(),
            db_port,
            db_name: matches.value_of("dbname").unwrap().to_string(),
            db_username: matches.value_of("username").unwrap().to_string(),
            db_password: matches.value_of("password").unwrap().to_string(),
            concurrency,
            max_cache_size,
            batch_size,
        }
    }
}

fn parse() -> ArgMatches<'static> {
    use clap::{App, Arg, crate_name, crate_version};

    App::new(crate_name!())
        .about("Carbon Cache for TimescaleDB.")
        .version(crate_version!())
        .arg(Arg::with_name("listen-address")
            .long("listen-address")
            .help("Address to listen on.")
            .required(false)
            .takes_value(true)
            .default_value("127.0.0.1")
            .value_name("ADDR"))
        .arg(Arg::with_name("listen-port")
            .long("listen-port")
            .help("Port to listen on.")
            .required(false)
            .takes_value(true)
            .default_value("2003")
            .value_name("PORT"))
        .arg(Arg::with_name("host")
            .short("h")
            .long("host")
            .help("Host address of the PostgreSQL database.")
            .required(true)
            .takes_value(true)
            .value_name("HOST"))
        .arg(Arg::with_name("port")
            .short("p")
            .long("port")
            .help("Port number at which the PostgreSQL database is listening.")
            .takes_value(true)
            .required(false)
            .default_value("5432")
            .value_name("PORT"))
        .arg(Arg::with_name("username")
            .short("u")
            .long("username")
            .help("Username to connect to the PostgreSQL database.")
            .required(true)
            .takes_value(true)
            .value_name("NAME"))
        .arg(Arg::with_name("password")
            .short("W")
            .long("password")
            .help("Password to connect to the PostgreSQL database.")
            .required(true)
            .takes_value(true)
            .value_name("PASSWORD"))
        .arg(Arg::with_name("dbname")
            .short("d")
            .long("dbname")
            .help("Database name to connect to.")
            .required(true)
            .takes_value(true)
            .value_name("NAME"))
        .arg(Arg::with_name("concurrency")
            .short("c")
            .long("concurrency")
            .help("Number of threads to spawn to perform writes to the database.")
            .required(false)
            .takes_value(true)
            .default_value("1")
            .value_name("NUM"))
        .arg(Arg::with_name("max-cache-size")
            .short("s")
            .long("max-cache-size")
            .help("Limit the cache size. Number of metric data points.")
            .required(false)
            .takes_value(true)
            .default_value("100000")
            .value_name("NUM"))
        .arg(Arg::with_name("batch-size")
            .short("b")
            .long("batch-size")
            .help("Number of metric data points written by each thread per write operation.")
            .required(false)
            .takes_value(true)
            .default_value("100")
            .value_name("NUM"))
        .get_matches()
}
