use std::io::{BufRead, BufReader};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use std::net::{
    SocketAddr, TcpListener, TcpStream
};

use crossbeam_channel::{bounded, Receiver, Sender};
use log::{info, debug, error};
use postgres::{
    Connection, TlsMode, params::ConnectParams, params::Host
};
use rayon::ThreadPoolBuilder;

use crate::config::Config;
use crate::parse::Metric;
use crate::write_to_db;


// Used to easily switch between the database writer implementations for testing.
static WRITE_TO_DB: fn(&Connection, &[Metric]) = write_to_db::multi_insert;


pub fn run(config: Arc<Config>) {
    // Config clone to be moved into the thread-pool.
    let consumer_config = config.clone();

    // Create channels for stream-handler to worker threads.
    let (sender, receiver) = bounded::<String>(config.max_cache_size);

    // Start worker thread-pool.
    let _pool = ThreadPoolBuilder::new()
        .num_threads(config.concurrency as usize)
        .thread_name(|index| format!("worker-thread-{}", index))
        .start_handler(move |_| {
            let w_config = consumer_config.clone();
            let w_receiver = receiver.clone();
            metrics_consumer(w_config, w_receiver);
        })
        .build()
        .expect("Failed to instantiate thread-pool.");

    // Listen on socket.
    let addr = SocketAddr::new(config.listen_ip_addr, config.listen_port);
    let listener = TcpListener::bind(addr).unwrap();
    info!("Listening on {}:{}...", addr.ip(), addr.port());

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                info!("Client connected from {}", stream.peer_addr().unwrap().ip());
                let c_sender = sender.clone();
                thread::spawn(move || handle_stream(c_sender, stream));
            },
            Err(e) => error!("Client connection failed:  {}", e),
        }
    }
}

fn handle_stream(sender: Sender<String>, stream: TcpStream) {
    let client_ip = stream.peer_addr().unwrap().ip().clone();

    info!("Listening for metrics...");
    let buf = BufReader::new(stream);
    for line in buf.lines() {
        match line {
            Ok(data) => {
                debug!("Read the following bytes:  {}", data);
                sender.send(data).unwrap();
            },
            Err(e) => {
                error!("Error reading stream:  {}", e);
            },
        }
    }
    info!("Client from {} disconnected.", client_ip);
}

fn metrics_consumer(config: Arc<Config>, recv: Receiver<String>) {
    let thread_name = thread::current().name().unwrap().to_string();

    info!("{} - Connecting to database `{}` at {}:{}...", thread_name, config.db_name, config.db_host, config.db_port);
    let db_params = ConnectParams::builder()
        .port(config.db_port)
        .database(&config.db_name)
        .user(&config.db_username, Some(&config.db_password))
        .build(Host::Tcp(config.db_host.clone()));
    let db = Connection::connect(db_params, TlsMode::None).unwrap();

    debug!("{} - Consuming from channel...", thread_name);
    let mut batch = Vec::<Metric>::with_capacity(config.batch_size.into());
    fn flush_batch(db: &Connection, batch: &mut Vec<Metric>) {
        debug!("Metrics collected:  {}", batch.len());
        if !batch.is_empty() {
            WRITE_TO_DB(&db, &batch);
            batch.clear();
        }
    }

    loop {
        debug!("{} - Messages remaining in channel:  {}", thread_name, recv.len());
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
                    Err(e) => error!("{} - Error parsing metric:  {}", thread_name, e),
                }
            },
            Err(e) => {
                if e.is_timeout() { flush_batch(&db, &mut batch); }
                if e.is_disconnected() { break; }
            },
        }
    }
}
