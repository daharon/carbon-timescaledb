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

use crate::config::Config;
use crate::parse::Metric;


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
                let metric = Metric::parse(&entry);
                println!("Parsed metric:  {:?}", metric);
                write_to_db(&db, &metric);
            },
            Err(e) => eprintln!("Error reading stream:  {}", e),
        }
    }
}

fn write_to_db(db: &Connection, metric: &Metric) {
    let result = db.execute("SELECT insert_metric($1, $2, $3)",
        &[&metric.path, &metric.value, &metric.timestamp]);
    match result {
        Ok(rows_modified) => println!("Inserted {} metric(s).", rows_modified),
        Err(e) => eprintln!("Error from PostgreSQL:  {}", e),
    }
}
