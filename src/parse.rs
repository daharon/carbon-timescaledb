//! Parse Carbon metrics.
//!
//! See https://graphite.readthedocs.io/en/latest/feeding-carbon.html#step-3-understanding-the-graphite-message-format

use std::error::Error;
use std::fmt;
use std::str::FromStr;


#[derive(Debug)]
pub struct Metric {
    pub path: String,
    pub value: f64,
    pub timestamp: i32,
}

#[derive(Debug)]
pub struct MetricParseError {
    pub description: String,
}

impl Error for MetricParseError { }

impl fmt::Display for MetricParseError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Error parsing string: {}", self.description)
    }
}

impl FromStr for Metric {
    type Err = MetricParseError;

    // TODO:  Fix up this parsing.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut parts = s.split_whitespace();
        let path = String::from(parts.next().unwrap());
        let value = parts.next().unwrap().parse::<f64>()
            .map_err(|e| { MetricParseError { description: e.description().to_string() } })?;
        let timestamp = parts.next().unwrap().parse::<i32>()
            .map_err(|e| { MetricParseError { description: e.description().to_string() } })?;
        let metric = Self {
            path,
            value,
            timestamp,
        };
        Ok(metric)
    }
}
