use std::sync::Arc;

use simplelog::SimpleLogger;

use carbon_timescaledb::config::Config;
use carbon_timescaledb::server;


#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;


fn main() {
    let config = Arc::new(Config::new());
    SimpleLogger::init(config.log_level, simplelog::Config::default())
        .expect("Failed to initialize logging.");

    server::run(config);
}
