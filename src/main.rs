use std::sync::Arc;
use std::net::IpAddr;
use std::str::FromStr;

use carbon_timescaledb::config::Config;
use carbon_timescaledb::server;


#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;


fn main() {
    let config = Config {
        listen_ip_addr: IpAddr::from_str("127.0.0.1").unwrap(),
        listen_port: 2003,
        db_host: String::from("127.0.0.1"),
        db_port: 5432,
        db_name: String::from("carbon_test"),
        db_username: String::from("postgres"),
        db_password: String::from("password"),
    };

    server::run(Arc::new(config));
}
