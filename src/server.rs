use std::io::prelude::*;
use std::io::BufReader;
use std::sync::Arc;
use std::thread;
use std::net::{
    SocketAddr, TcpListener, TcpStream
};

use postgres::{
    Connection, TlsMode, params::ConnectParams, params::Host
};
use postgres::types::ToSql;
use postgres::stmt::Statement;

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
    let client_ip = stream.peer_addr().unwrap().ip().clone();
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
    println!("Client from {} disconnected.", client_ip);
}

fn write_to_db(db: &Connection, metric: &Metric) {
    /*
    // Example of converting variable into Postgres compatible input.
    let mut p_timestamp = Vec::<u8>::new();
    metric.timestamp.to_sql(&postgres::types::INT4, &mut p_timestamp);
    */
    let mut metrics = Vec::new();
    metrics.push(metric);
    let (query, params) = prepare_insert(db, &metrics);
    println!("Query:  {}", query);
    println!("Params:  {:?}", params);

    let result = db.execute(&query, &params);
    match result {
        Ok(rows_modified) => println!("Inserted {} metric(s).", rows_modified),
        Err(e) => eprintln!("Error from PostgreSQL:  {}", e),
    }
}

fn prepare_insert<'a>(db: &Connection, metrics: &Vec<&'a Metric>) -> (String, Vec<&'a ToSql>) {
    let mut tuples: Vec<String> = Vec::new();
    let mut params: Vec<&ToSql> = Vec::new();
    for (index, metric) in metrics.iter().enumerate() {
        tuples.push(format!("(${}, ${}, ${})", index + 1, index + 2, index + 3));

        params.push(&metric.path);
        params.push(&metric.value);
        params.push(&metric.timestamp);
    }
    let query = format!("SELECT insert_metrics({})", tuples.join(","));
    (query, params)
}
