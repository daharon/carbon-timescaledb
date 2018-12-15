use std::net::IpAddr;


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
}


