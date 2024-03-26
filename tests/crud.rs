use redis::Commands;
use std::thread;
mod common;

#[test]
fn test_get_and_set() {
    common::init();

    thread::spawn(move || {
        assert_cmd::Command::cargo_bin(env!("CARGO_PKG_NAME"))
            .unwrap()
            .unwrap();
    });

    thread::spawn(|| {
        let mut conn = loop {
            if let Ok(conn) = redis::Client::open("redis://127.0.0.1")
                .unwrap()
                .get_connection()
            {
                break conn;
            }
        };

        let _: () = conn.set("key1", "value1").unwrap();
        let _: () = conn.set("key2", "value2").unwrap();
        let _: () = conn.set("key3", "value3").unwrap();

        let value1: String = conn.get("key1").unwrap();
        let value2: String = conn.get("key2").unwrap();
        let value3: String = conn.get("key3").unwrap();

        assert_eq!(value1, "value1");
        assert_eq!(value2, "value2");
        assert_eq!(value3, "value3");
    })
    .join()
    .unwrap();
}
