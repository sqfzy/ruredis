// #![allow(clippy::needless_return)]

mod cli;
mod cmd;
mod config;
mod db;
mod frame;
mod init;
mod master_server;
mod replicaof;
mod stream;
mod util;

// static CONFIG: Lazy<Arc<config::RedisConfig>> = Lazy::new(|| Arc::new(RedisConfig::new()));

// TODO:
// 1. more reasonable error handling
// 2. more commands: ttl, exist

// asd asd
// (error) ERR unknown command 'asd', with args beginning with: 'asd'
// get foo a
// (error) ERR wrong number of arguments for 'get' command
// set foo bar bar2
// (error) ERR syntax error

#[tokio::main]
async fn main() {
    init::init();

    master_server::run().await;
}
