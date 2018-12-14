use std::io::prelude::*;
use std::io::BufReader;
use std::thread;
use std::net::{
    SocketAddr, TcpListener, TcpStream
};

use crate::carbon::parse::Metric;


pub fn run(addr: &SocketAddr) {
    let listener = TcpListener::bind(addr).unwrap();
    println!("Listening on {}:{}...", addr.ip(), addr.port());

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                println!("Client connected from {}", stream.peer_addr().unwrap().ip());
                thread::spawn(|| { handle_stream(stream) });
            },
            Err(e) => eprintln!("Client connection failed:  {}", e),
        }
    }

}

fn handle_stream(stream: TcpStream) {
    println!("Stream details:\n{:?}", stream);
    let buf = BufReader::new(stream);
    for entry in buf.lines() {
        match entry {
            Ok(entry) => {
                println!("Read the following bytes:  {}", entry);
                let metric = parse_entry(&entry);
                println!("Parsed metric:  {:?}", metric);
            },
            Err(e) => eprintln!("Error reading stream:  {}", e),
        }
    }
}

fn parse_entry(entry: & String) -> Metric {
    let mut parts = entry.split_whitespace();
    Metric {
        path: parts.next().unwrap(),
        value: parts.next().unwrap().parse::<f64>().unwrap(),
        timestamp: parts.next().unwrap().parse::<u64>().unwrap(),
    }
}
