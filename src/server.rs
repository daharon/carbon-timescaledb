use std::io::prelude::*;
use std::io::BufReader;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use std::net::{
    SocketAddr, TcpListener, TcpStream
};

use crossbeam_channel::{bounded, Receiver, Sender};
use postgres::{
    Connection, TlsMode, params::ConnectParams, params::Host
};
use postgres::types::ToSql;

use crate::config::Config;
use crate::parse::Metric;


static BATCH_SIZE: usize = 1_000;
static NUM_THREADS: usize = 2;
static CHANNEL_CAPACITY: usize = 500_000;
static WRITE_TO_DB: fn(&Connection, &[Metric]) = write_to_db_multi_insert;


pub fn run(config: Arc<Config>) {
    // Create channels for stream-handler to worker threads.
    let (sender, receiver) = bounded::<String>(CHANNEL_CAPACITY);

    // Start worker thread-pool.
    let pool = threadpool::Builder::new()
        .num_threads(NUM_THREADS)
        .thread_name("worker-thread".into())
        .build();
    for _ in 0..NUM_THREADS {
        let w_config = config.clone();
        let w_receiver = receiver.clone();
        pool.execute(move || metrics_consumer(w_config, w_receiver));
    }

    // Listen on socket.
    let addr = SocketAddr::new(config.listen_ip_addr, config.listen_port);
    let listener = TcpListener::bind(addr).unwrap();
    println!("Listening on {}:{}...", addr.ip(), addr.port());

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                println!("Client connected from {}", stream.peer_addr().unwrap().ip());
                let c_sender = sender.clone();
                thread::spawn(move || handle_stream(c_sender, stream));
            },
            Err(e) => eprintln!("Client connection failed:  {}", e),
        }
    }
}

fn handle_stream(sender: Sender<String>, stream: TcpStream) {
    let client_ip = stream.peer_addr().unwrap().ip().clone();

    println!("Listening for metrics...");
    let buf = BufReader::new(stream);
    for line in buf.lines() {
        match line {
            Ok(data) => {
//                println!("Read the following bytes:  {}", data);
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
    let mut batch = Vec::<Metric>::with_capacity(BATCH_SIZE);
    fn flush_batch(db: &Connection, batch: &mut Vec<Metric>) {
        println!("Metrics collected:  {}", batch.len());
        if !batch.is_empty() {
            WRITE_TO_DB(&db, &batch);
            batch.clear();
        }
    }
    loop {
        println!("Messages remaining in channel:  {}", recv.len());
        let payload = recv.recv_timeout(Duration::from_secs(1));
        match payload {
            Ok(line) => {
                let parsed = line.parse::<Metric>();
                match parsed {
                    Ok(metric) => {
                        batch.push(metric);
                        if batch.len() == batch.capacity() {
                            flush_batch(&db, &mut batch);
                        }
                    },
                    Err(e) => eprintln!("{} - Error parsing metric:  {}", &thread_name, e),
                }
            },
            Err(e) => {
                if e.is_timeout() { flush_batch(&db, &mut batch); }
                if e.is_disconnected() { break; }
            },
        }
    }
}

/// Batch-write metrics using the `COPY FROM STDIN` statement provided by [postgres::stmt::Statement::copy_in].
fn write_to_db_copy_in(db: &Connection, metrics: &[Metric]) {
    let stmt = db.prepare("COPY metrics_view FROM STDIN").unwrap();
    let input = metrics.iter().map(|metric| {
        format!("{}\t{}\t{}\n", metric.path, metric.timestamp, metric.value)
    }).fold(String::new(), |out, line| { out + line.as_str() } );

//    println!("Input bytes:  {}", input);
    let result = stmt.copy_in(&[], &mut input.as_bytes());
    match result {
        Ok(rows_modified) => println!("Inserted {} metric(s).", rows_modified),
        Err(e) => eprintln!("Error from PostgreSQL:  {}", e),
    }
}

/// Batch-write metrics using the variadic `insert_metrics` stored procedure.
/// `insert_metrics` can handle 100 arguments, which is a limitation of
/// PostgreSQL.
fn write_to_db_multi_sp(db: &Connection, metrics: &[Metric]) {
    let (query, params) = prepare_multi_sp(metrics);
//    println!("Query:  {}", query);
//    println!("Params:  {:?}", params);

    let result = db.execute(&query, &params);
    match result {
        Ok(rows_modified) => println!("Inserted {} metric(s).", rows_modified),
        Err(e) => eprintln!("Error from PostgreSQL:  {}", e),
    }
}

/// Batch-write metrics using a normal `INSERT` statement.
fn write_to_db_multi_insert(db: &Connection, metrics: &[Metric]) {
    let (query, params) = prepare_multi_insert(metrics);
//    println!("Query:  {}", query);
//    println!("Params:  {:?}", params);

    let result = db.execute(&query, &params);
    match result {
        Ok(rows_modified) => println!("Inserted {} metric(s).", rows_modified),
        Err(e) => eprintln!("Error from PostgreSQL:  {}", e),
    }
}

/// Write metrics using the `insert_metric` stored procedure.
/// Metrics will be written one at a time.
fn write_to_db_single(db: &Connection, metrics: &[Metric]) {
    for metric in metrics {
        let result = db.execute("SELECT insert_metric($1, $2, $3)",
            &[&metric.path, &metric.value, &metric.timestamp]);
        match result {
            Ok(rows_modified) => println!("Inserted {} metric(s).", rows_modified),
            Err(e) => eprintln!("Error from PostgreSQL:  {}", e),
        }
    }
}

fn prepare_multi_sp(metrics: &[Metric]) -> (String, Vec<&ToSql>) {
    let (tuples, params) = prepare_multi_params(metrics);
    let query = format!("SELECT insert_metrics({})", tuples.join(","));
    (query, params)
}

fn prepare_multi_insert(metrics: &[Metric]) -> (String, Vec<&ToSql>) {
    let (tuples, params) = prepare_multi_params(metrics);
    let query = format!("INSERT INTO metrics_view (path, value, \"timestamp\") VALUES {}", tuples.join(","));
    (query, params)
}

fn prepare_multi_params(metrics: &[Metric]) -> (Vec<String>, Vec<&ToSql>) {
    let mut tuples: Vec<String> = Vec::new();
    let mut params: Vec<&ToSql> = Vec::new();
    let mut base: usize;
    for (index, metric) in metrics.iter().enumerate() {
        base = index * 3;
        tuples.push(format!("(${}, ${}, ${})", base + 1, base + 2, base + 3));

        params.push(&metric.path);
        params.push(&metric.value);
        params.push(&metric.timestamp);
    }
    (tuples, params)
}
