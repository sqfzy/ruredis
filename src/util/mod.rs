mod aof;
mod rdb;

use crate::db::Db;
use anyhow::{anyhow, Result};
use bytes::Bytes;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time::Duration,
};

pub use aof::*;
pub use rdb::*;

// 测试客户端，向服务端发送指定命令
#[allow(dead_code)]
pub async fn client_test(cmd: &'static str) {
    let mut stream = TcpStream::connect("127.0.0.1:6379").await.unwrap();
    stream.write_all(cmd.as_bytes()).await.unwrap();
    let mut buf = [0u8; 1024];
    loop {
        let n = stream.read(&mut buf).await.unwrap();
        if n == 0 {
            break;
        }
        println!("{:?}", String::from_utf8(buf[0..n].to_vec()).unwrap());
    }
}

// 测试服务端，接收客户端的命令并打印
#[allow(dead_code)]
pub async fn server_test(stream: &mut TcpStream) {
    let mut buf = [0u8; 1024];
    let n = stream.read(&mut buf).await.unwrap();
    println!("{:?}", String::from_utf8(buf[0..n].to_vec()).unwrap());
}

pub fn bytes_to_string(bytes: Bytes) -> Result<String> {
    String::from_utf8(bytes.into()).map_err(|_| anyhow!("ERR syntax error"))
}

pub fn bytes_to_u64(bytes: Bytes) -> Result<u64> {
    String::from_utf8(bytes.into())
        .map_err(|_| anyhow!("ERR syntax error"))?
        .parse::<u64>()
        .map_err(|_| anyhow!("ERR syntax error"))
}

pub async fn check_expiration_periodical(period: Duration, db: &Db) {
    let db = db.clone();
    tokio::spawn(async move {
        // 循环检查字符串，列表，集合，有序集合，哈希表的键
        loop {
            {
                tokio::time::sleep(period).await;

                let mut writer_guard = db.inner.write().await;
                let keys_to_check: Vec<_> = writer_guard.string_kvs.0.keys().cloned().collect();

                for key in keys_to_check {
                    writer_guard.string_kvs.check_exist(key);
                }
            }
            {
                // TODO: list
                // tokio::time::sleep(period).await;
            }
        }
    });
}

#[tokio::test]
async fn test_check_expiration_periodical() {
    let db = Db::new();
    check_expiration_periodical(Duration::from_millis(500), &db).await;
    {
        let string_db = &mut db.inner.write().await.string_kvs;
        string_db.set("foo", "bar", Some(Duration::from_millis(300)));
        assert_eq!(Some("bar".into()), string_db.get("foo"));
    }
    tokio::time::sleep(Duration::from_secs(1)).await;

    let string_db = &mut db.inner.write().await.string_kvs;
    // "foo"过期后应当被检查程序删除
    if string_db.0.get(&Bytes::from("foo")).is_some() {
        panic!("key foo should be deleted");
    }
}
