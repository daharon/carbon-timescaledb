use std::io::prelude::*;
use std::io::BufReader;
use std::sync::Arc;
use std::thread;
use std::net::{
    SocketAddr, TcpListener, TcpStream
};

use chrono::NaiveDateTime;
use postgres::{
    Connection, TlsMode, params::ConnectParams, params::Host
};

use crate::carbon::parse::Metric;
use crate::server::config::Config;


pub mod config;


pub fn run(config: Arc<Config>) {
    let addr = SocketAddr::new(config.listen_ip_addr, config.listen_port);
    let listener = TcpListener::bind(addr).unwrap();
    println!("Listening on {}:{}...", addr.ip(), addr.port());

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                println!("Client connected from {}", stream.peer_addr().unwrap().ip());
                let thread_config = config.clone();
                thread::spawn(|| { handle_stream(thread_config, stream) });
            },
            Err(e) => eprintln!("Client connection failed:  {}", e),
        }
    }

}

fn handle_stream(config: Arc<Config>, stream: TcpStream) {
    println!("Connecting to database `{}` at {}:{}...", config.db_name, config.db_host, config.db_port);
    let db_params = ConnectParams::builder()
        .port(config.db_port)
        .database(&config.db_name)
        .user(&config.db_username, Some(&config.db_password))
        .build(Host::Tcp(config.db_host.clone()));
    let db = Connection::connect(db_params, TlsMode::None).unwrap();

    println!("Listening for metrics...");
    let buf = BufReader::new(stream);
    for entry in buf.lines() {
        match entry {
            Ok(entry) => {
                println!("Read the following bytes:  {}", entry);
                let metric = parse_entry(&entry);
                println!("Parsed metric:  {:?}", metric);
                write_to_db(&db, &metric);
            },
            Err(e) => eprintln!("Error reading stream:  {}", e),
        }
    }
}

fn write_to_db(db: &Connection, metric: &Metric) {
    let result = db.execute("INSERT INTO metrics (path, value, timestamp) VALUES ($1, $2, $3)",
        &[&metric.path, &metric.value, &NaiveDateTime::from_timestamp(metric.timestamp, 0)]);
    match result {
        Ok(rows_modified) => println!("Inserted {} metric(s).", rows_modified),
        Err(e) => eprintln!("Error from PostgreSQL:  {}", e),
    }
}

fn parse_entry(entry: & String) -> Metric {
    let mut parts = entry.split_whitespace();
    Metric {
        path: parts.next().unwrap(),
        value: parts.next().unwrap().parse::<f64>().unwrap(),
        timestamp: parts.next().unwrap().parse::<i64>().unwrap(),
    }
}
