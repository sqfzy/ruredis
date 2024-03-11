use std::str::FromStr;
use tracing::Level;

pub fn init() {
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "debug");
    }

    let level = Level::from_str(&std::env::var("RUST_LOG").unwrap()).unwrap();
    tracing_subscriber::fmt()
        // .pretty()
        .with_max_level(level)
        .init();
}
