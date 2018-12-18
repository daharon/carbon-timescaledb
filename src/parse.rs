//! Parse Carbon metrics.
//!
//! See https://graphite.readthedocs.io/en/latest/feeding-carbon.html#step-3-understanding-the-graphite-message-format

use nom::{named, do_parse};


#[derive(Debug)]
pub struct Metric {
    pub path: String,
    pub value: f64,
    pub timestamp: i32,
}


impl Metric {
    pub fn parse(data: &str) -> Self {
        let mut parts = data.split_whitespace();
        Self {
            path: String::from(parts.next().unwrap()),
            value: parts.next().unwrap().parse::<f64>().unwrap(),
            timestamp: parts.next().unwrap().parse::<i32>().unwrap(),
        }
    }
}
