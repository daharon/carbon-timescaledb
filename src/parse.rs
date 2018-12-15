//! Parse Carbon metrics.
//!
//! See https://graphite.readthedocs.io/en/latest/feeding-carbon.html#step-3-understanding-the-graphite-message-format

use nom::{named, do_parse};


#[derive(Debug)]
pub struct Metric<'a> {
    pub path: &'a str,
    pub value: f64,
    pub timestamp: i64,
}


impl<'a> Metric<'a> {
    pub fn parse(data: &'a str) -> Self {
        let mut parts = data.split_whitespace();
        Self {
            path: parts.next().unwrap(),
            value: parts.next().unwrap().parse::<f64>().unwrap(),
            timestamp: parts.next().unwrap().parse::<i64>().unwrap(),
        }
    }
}
