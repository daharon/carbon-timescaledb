//! Parse Carbon metrics.
//!
//! See https://graphite.readthedocs.io/en/latest/feeding-carbon.html#step-3-understanding-the-graphite-message-format

use nom::{named, do_parse};


#[derive(Debug)]
pub struct Metric<'a> {
    pub path: &'a str,
    pub value: f64,
    pub timestamp: u64,
}


/*
pub fn parse(data: &str) -> Result<Metric, nom::Err::Error> {

}
*/
