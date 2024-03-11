use crate::cli::Cli;
use clap::Parser;
use rand::Rng;
use std::{
    net::{Ipv4Addr, Ipv6Addr, SocketAddr},
    str::FromStr,
};

#[derive(Debug, Default)]
pub struct RedisConfig {
    pub port: u16,
    pub replicaof: Option<SocketAddr>, // 主服务器的地址
    pub replid: String,                // 服务器的运行ID。由40个随机字符组成
    pub repl_offset: u64,              // 主服务器的复制偏移量
    pub max_replication: u64,          // 最多允许多少个从服务器连接到当前服务器
    pub expire_check_interval: tokio::time::Duration, // 检查过期键的周期
}

impl RedisConfig {
    pub fn new() -> Self {
        let cli = Cli::parse();

        let replid: String = rand::thread_rng()
            .sample_iter(&rand::distributions::Alphanumeric)
            .take(40)
            .map(char::from) // 将u8转换为char
            .collect(); // 直接收集到String中

        #[allow(unused_assignments)]
        let mut replicaof = None;
        if let Some(addr) = cli.replicaof {
            let port = addr[1].parse().expect("Port should be a number");
            if addr[0] == "localhost" {
                replicaof = Some(
                    SocketAddr::from_str(&format!("127.0.0.1:{}", port))
                        .expect("localhost should be valid"),
                );
            } else if let Ok(ipv4) = Ipv4Addr::from_str(&addr[0]) {
                replicaof = Some(SocketAddr::from((ipv4, port)));
            } else if let Ok(ipv6) = Ipv6Addr::from_str(&addr[0]) {
                replicaof = Some(SocketAddr::from((ipv6, port)));
            } else {
                panic!("Replicaof should be a valid address");
            }
        } else {
            replicaof = None
        };

        RedisConfig {
            port: cli.port,
            replicaof,
            replid,
            repl_offset: 0,
            max_replication: 16,
            expire_check_interval: tokio::time::Duration::from_secs(60),
        }
    }
}
