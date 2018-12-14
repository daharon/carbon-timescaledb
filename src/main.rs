use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;

use carbon_timescaledb::server;


fn main() {
    let ip_addr = IpAddr::from_str("127.0.0.1").unwrap();
    let port = 2003;
    let socket_addr = SocketAddr::new(ip_addr, port);

    server::run(&socket_addr);
}
