pub fn init() {
    if std::env::var("CARGO_PKG_NAME").is_err() {
        std::env::set_var("CARGO_PKG_NAME", "rutinose");
    }
}
