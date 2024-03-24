/// 项目配置最佳实践
/// 1. 首先配置结构体从默认值开始构造
/// 2. 如果提供了配置文件，读取配置文件并更新
/// 3. 如果命令行参数有 --server.addr 之类的配置项，merge 该配置
/// 4. 如果环境变量有 SERVER_ADDR 之类配置，进行 merge
mod serialize;

use crate::{
    cli::Cli,
    db::{Db, DbInner},
    frame::Frame,
    replicaof::enable_replicaof,
    util,
};
use bytes::Bytes;
use clap::Parser;
use crossbeam::{atomic::AtomicCell, queue::ArrayQueue, sync::ShardedLock};
use rand::Rng;
use serde_with::serde_as;
use std::{
    collections::VecDeque,
    fmt::Display,
    sync::{atomic::AtomicU64, Arc},
};
use tokio::{
    select,
    sync::{broadcast::Sender, Mutex, RwLockWriteGuard},
};

pub static CONFIG: once_cell::sync::Lazy<Arc<Conf>> =
    once_cell::sync::Lazy::new(|| Arc::new(Conf::new()));

/// 用于记录当前服务器的复制偏移量。当从服务器发送 PSYNC
/// 命令给主服务器时，比较从服务器和主服务器的ACK_OFFSET，从而判断主从是否一致。
pub static OFFSET: AtomicU64 = AtomicU64::new(0);

/// 缓存最近master发送给replicate的命令
pub static REPLI_BACKLOG: once_cell::sync::Lazy<util::RepliBackLog> =
    once_cell::sync::Lazy::new(|| util::RepliBackLog::new(1024));

#[derive(Debug, serde::Deserialize)]
pub struct Conf {
    #[serde(rename = "server")]
    pub server: ServerConf,
    #[serde(rename = "security")]
    pub security: SecurityConf,
    #[serde(rename = "replication")]
    pub replication: ReplicationConf,
    #[serde(rename = "rdb")]
    pub rdb: RDBConf,
    #[serde(rename = "aof")]
    pub aof: AOFConf,
}

#[derive(Debug, serde::Deserialize)]
pub struct ServerConf {
    pub port: u16,
    pub expire_check_interval_secs: u64, // 检查过期键的周期
}

#[derive(Debug, serde::Deserialize)]
pub struct SecurityConf {
    pub requirepass: Option<String>, // 访问密码
}

#[derive(Debug)]
pub struct ReplicationConf {
    pub replicaof: Option<ShardedLock<String>>, // 主服务器的地址
    pub replid: String,                         // 服务器的运行ID。由40个随机字符组成
    pub max_replicate: u64,                     // 最多允许多少个从服务器连接到当前服务器
    pub masterauth: Option<String>, // 主服务器密码，设置该值之后，当从服务器连接到主服务器时会发送该值
}

#[derive(Debug, serde::Deserialize)]
pub struct RDBConf {
    pub enable: bool,          // 是否启用RDB持久化
    pub file_path: String,     // RDB文件路径
    pub version: u32,          // RDB版本号
    pub enable_checksum: bool, // 是否启用RDB校验和
}

#[derive(Debug, serde::Deserialize)]
pub struct AOFConf {
    pub enable: bool, // 是否启用AOF持久化
    pub file_path: String,
    #[serde(rename = "append_fsync")]
    pub append_fsync: AppendFSync,
}

#[derive(Debug, serde::Deserialize)]
pub enum AppendFSync {
    EverySec,
    Always,
    No,
}

impl Conf {
    pub fn new() -> Self {
        // 1. 从默认配置文件中加载配置
        let config_builder = config::Config::builder().add_source(config::File::new(
            "config/default.toml",
            config::FileFormat::Toml,
        ));
        // 2. 从用户自定义配置文件中加载配置
        let config_builder = config_builder.add_source(config::File::new(
            "config/custom.toml",
            config::FileFormat::Toml,
        ));

        // TODO: 改进加载的方式，不然每次修改cli都需要修改这里
        // 3. 从命令行中加载配置
        let cli = Cli::parse();
        let config_builder = config_builder
            .set_override_option("replication.replicaof", cli.replicaof)
            .expect("Failed to set replicaof")
            .set_override_option("server.port", cli.port)
            .expect("Failed to set port")
            .set_override_option("rdb.file_path", cli.rdb_path)
            .expect("Failed to set rdb file path");

        // 4. 运行时配置
        let replid: String = rand::thread_rng()
            .sample_iter(&rand::distributions::Alphanumeric)
            .take(40)
            .map(char::from) // 将u8转换为char
            .collect(); // 直接收集到String中

        config_builder
            .set_override("replication.replid", replid)
            .expect("Failed to set replid")
            .build()
            .expect("Failed to load config")
            .try_deserialize()
            .expect("Failed to deserialize config")
    }

    pub fn may_enable_replicaof(
        &self,
        db: Db,
        psync_to_others_sender: Sender<Frame>,
        others_to_psync_sender: Sender<Frame>,
    ) {
        if let Some(master_addr) = &self.replication.replicaof {
            let master_addr = master_addr
                .read()
                .expect("Failed to read master_addr")
                .clone();
            tokio::spawn(async move {
                enable_replicaof(
                    master_addr,
                    db,
                    psync_to_others_sender,
                    others_to_psync_sender,
                )
                .await;
            });
        }
    }

    pub fn may_enable_rdb(&self, db: &mut RwLockWriteGuard<DbInner>) {
        // AOF持久化优先级高于RDB持久化，当AOF持久化开启时，不加载RDB文件
        if !self.rdb.enable || self.aof.enable {
            return;
        }

        match util::rdb_load(db) {
            Ok(_) => {
                tracing::info!("RDB file loaded successfully!!!");
            }
            Err(e) => {
                tracing::error!("Failed to load RDB file: {:?}", e);
            }
        }
    }

    pub async fn may_enable_aof(
        &self,
        write_cmd_sender: Sender<Frame>,
        finished_notify: Arc<tokio::sync::Notify>,
    ) {
        if !self.aof.enable {
            return;
        }

        let aof = util::Aof::new(write_cmd_sender.subscribe()).await.unwrap();
        // 载入AOF文件
        std::thread::spawn(move || {
            if let Err(e) = aof.load() {
                tracing::error!("Failed to load AOF file: {:?}", e);
            } else {
                tracing::info!("AOF file loaded successfully!!!");
            }
        });

        let mut aof = util::Aof::new(write_cmd_sender.subscribe()).await.unwrap();
        match self.aof.append_fsync {
            AppendFSync::Always => {
                // receive命令, append入buffer，
                tokio::spawn(async move {
                    loop {
                        select! {
                            // 每次事件循环（即当一条连接结束时），将buffer中的内容写入AOF文件并同步
                            _ = finished_notify.notified() => {
                                if let Err(e) = aof.write_in().await {
                                    tracing::error!("Failed to write AOF file: {:?}", e);
                                }
                                if let Err(e) = aof.fsync().await {
                                    tracing::error!("Failed to fsync AOF file: {:?}", e);
                                }
                            }
                            cmd = aof.write_cmd_receiver.recv() => {
                                if let Ok(cmd) = cmd {
                                    aof.append(&format!("{}\n", cmd));
                                }
                            }
                        }
                    }
                });
            }
            AppendFSync::EverySec => {
                tokio::spawn(async move {
                    loop {
                        select! {
                            // 每次事件循环，将buffer中的内容写入AOF文件
                            _ = finished_notify.notified() => {
                                if let Err(e) = aof.write_in().await {
                                    tracing::error!("Failed to write AOF file: {:?}", e);
                                }
                            }
                            // 每过一秒，同步AOF文件
                            _ = tokio::time::sleep(tokio::time::Duration::from_secs(1)) => {
                                if let Err(e) = aof.fsync().await {
                                    tracing::error!("Failed to fsync AOF file: {:?}", e);
                                }
                            }
                            cmd = aof.write_cmd_receiver.recv() => {
                                if let Ok(cmd) = cmd {
                                    aof.append(&format!("{}\n", cmd));
                                }
                            }
                        }
                    }
                });
            }
            AppendFSync::No => {
                tokio::spawn(async move {
                    select! {
                        // 每次事件循环，将buffer中的内容写入AOF文件
                        _ = finished_notify.notified() => {
                            if let Err(e) = aof.write_in().await {
                                tracing::error!("Failed to write AOF file: {:?}", e);
                            }
                        }
                        cmd = aof.write_cmd_receiver.recv() => {
                            if let Ok(cmd) = cmd {
                                aof.append(&format!("{}\n", cmd));
                            }
                        }
                    }
                });
            }
        }
    }
}
