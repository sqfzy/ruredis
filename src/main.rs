// #![allow(clippy::needless_return)]

mod cli;
mod cmd;
mod config;
mod db;
mod frame;
mod init;
mod replicaof;
mod server;
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

// remote: [build] > error: package `clap_lex v0.7.0` cannot be built because it requires rustc 1.74 or newer, while the currently active rustc version is 1.70.0
// remote: [build] > Either upgrade to rustc 1.74 or newer, or use
// remote: [build] > cargo update -p clap_lex@0.7.0 --precise ver
// remote: [build] > where `ver` is the latest version of `clap_lex` supporting rustc 1.70.0
// remote: [build] Build failed. Check the logs above for the reason.
// remote: [build] If you think this is a CodeCrafters error, please contact us at hello@codecrafters.io.
// remote:
#[tokio::main]
async fn main() {
    init::init();

    server::run().await;
}
