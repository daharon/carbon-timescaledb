//! Functions for implementing different strategies for inserting metrics into
//! the database.

use log::{debug, error, trace};
use postgres::Connection;
use postgres::types::ToSql;

use crate::parse::Metric;


/// Batch-write metrics using the `COPY FROM STDIN` statement provided by [postgres::stmt::Statement::copy_in].
#[allow(unused)]
pub fn copy_in(db: &Connection, metrics: &[Metric]) {
    let stmt = db.prepare("COPY metrics_view FROM STDIN").unwrap();
    let input = metrics.iter().map(|metric| {
        format!("{}\t{}\t{}\n", metric.path, metric.timestamp, metric.value)
    }).fold(String::new(), |out, line| { out + line.as_str() } );

    trace!("Input bytes:  {}", input);
    let result = stmt.copy_in(&[], &mut input.as_bytes());
    match result {
        Ok(rows_modified) => debug!("Inserted {} metric(s).", rows_modified),
        Err(e) => error!("Error from PostgreSQL:  {}", e),
    }
}

/// Batch-write metrics using the variadic `insert_metrics` stored procedure.
/// `insert_metrics` can handle 100 arguments, which is a limitation of
/// PostgreSQL.
#[allow(unused)]
pub fn multi_sp(db: &Connection, metrics: &[Metric]) {
    let (query, params) = prepare_multi_sp(metrics);
    trace!("Query:  {}", query);
    trace!("Params:  {:?}", params);

    let result = db.execute(&query, &params);
    match result {
        Ok(rows_modified) => debug!("Inserted {} metric(s).", rows_modified),
        Err(e) => error!("Error from PostgreSQL:  {}", e),
    }
}

/// Batch-write metrics using a normal `INSERT` statement.
#[allow(unused)]
pub fn multi_insert(db: &Connection, metrics: &[Metric]) {
    let (query, params) = prepare_multi_insert(metrics);
    trace!("Query:  {}", query);
    trace!("Params:  {:?}", params);

    let result = db.execute(&query, &params);
    match result {
        Ok(rows_modified) => debug!("Inserted {} metric(s).", rows_modified),
        Err(e) => error!("Error from PostgreSQL:  {}", e),
    }
}

/// Write metrics using the `insert_metric` stored procedure.
/// Metrics will be written one at a time.
#[allow(unused)]
pub fn single(db: &Connection, metrics: &[Metric]) {
    for metric in metrics {
        let result = db.execute("SELECT insert_metric($1, $2, $3)",
                                &[&metric.path, &metric.value, &metric.timestamp]);
        match result {
            Ok(rows_modified) => debug!("Inserted {} metric(s).", rows_modified),
            Err(e) => error!("Error from PostgreSQL:  {}", e),
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
