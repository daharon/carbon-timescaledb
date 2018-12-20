use std::io::prelude::*;
use std::io::BufReader;
use std::sync::Arc;
use std::thread;
use std::net::{
    SocketAddr, TcpListener, TcpStream
};

use crossbeam_channel::{bounded, Receiver};
use itertools::Itertools;
use postgres::{
    Connection, TlsMode, params::ConnectParams, params::Host
};
use postgres::types::ToSql;

use crate::config::Config;
use crate::parse::Metric;


static CHUNK_SIZE: usize = 100;
static NUM_THREADS: usize = 1;
static CHANNEL_CAPACITY: usize = 1000;


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

    // Spawn worker threads.
    let (sender, receiver) = bounded::<String>(CHANNEL_CAPACITY);
    for i in 0..NUM_THREADS {
        let con_config = config.clone();
        let con_receiver = receiver.clone();
        thread::Builder::new()
            .name(i.to_string())
            .spawn(move || { metrics_consumer(con_config, con_receiver) })
            .expect("Error spawning worker thread.");
    }

    println!("Listening for metrics...");
    let buf = BufReader::new(stream);
    for line in buf.lines() {
        match line {
            Ok(data) => {
                println!("Read the following bytes:  {}", data);
                sender.send(data).unwrap();
            },
            Err(e) => {
                eprintln!("Error reading stream:  {}", e);
            },
        }
    }
    println!("Client from {} disconnected.", client_ip);
}

fn metrics_consumer(config: Arc<Config>, recv: Receiver<String>) {
    let thread_name = thread::current().name().unwrap().to_string();

    println!("Connecting to database `{}` at {}:{}...", config.db_name, config.db_host, config.db_port);
    let db_params = ConnectParams::builder()
        .port(config.db_port)
        .database(&config.db_name)
        .user(&config.db_username, Some(&config.db_password))
        .build(Host::Tcp(config.db_host.clone()));
    let db = Connection::connect(db_params, TlsMode::None).unwrap();

    println!("Consuming from channel...");
    for lines_iter in &recv.iter().chunks(CHUNK_SIZE) {
        let metrics: Vec<Option<Metric>> = lines_iter.map(|line| {
            let parsed = line.parse::<Metric>();
            match parsed {
                Ok(metric) => {
                    println!("{} - Parsed metric:  {:?}", &thread_name, metric);
                    Some(metric)
                },
                Err(e) => {
                    eprintln!("{} - Error reading stream:  {}", &thread_name, e);
                    None
                },
            }
        }).collect();
        println!("{} - Metrics collected:  {}", thread_name, metrics.len());
        write_to_db(&db, &metrics);
    }
}

fn write_to_db(db: &Connection, metrics: &[Option<Metric>]) {
    /*
    // Example of converting variable into Postgres compatible input.
    let mut p_timestamp = Vec::<u8>::new();
    metric.timestamp.to_sql(&postgres::types::INT4, &mut p_timestamp);
    */
    let (query, params) = prepare_insert(metrics);
    println!("Query:  {}", query);
    println!("Params:  {:?}", params);

    let result = db.execute(&query, &params);
    match result {
        Ok(rows_modified) => println!("Inserted {} metric(s).", rows_modified),
        Err(e) => eprintln!("Error from PostgreSQL:  {}", e),
    }
}

fn prepare_insert<'a>(metrics: &'a [Option<Metric>]) -> (String, Vec<&'a ToSql>) {
    let mut tuples: Vec<String> = Vec::new();
    let mut params: Vec<&ToSql> = Vec::new();
    let mut base: usize;
    for (index, metric) in metrics.iter().enumerate() {
        if let Some(metric) = metric {
            base = index * 3;
            tuples.push(format!("(${}, ${}, ${})", base + 1, base + 2, base + 3));

            params.push(&metric.path);
            params.push(&metric.value);
            params.push(&metric.timestamp);
        }
    }
    let query = format!("SELECT insert_metrics({})", tuples.join(","));
    (query, params)
}
