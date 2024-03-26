use super::CmdExecutor;
use crate::{
    conf::{CONFIG, OFFSET},
    connection::Connection,
    db::Db,
    frame::Frame,
    util,
};
use anyhow::{anyhow, Error, Result};
use bytes::Bytes;
use tokio::sync::broadcast::Sender;
use tracing::debug;

// 当执行客户端redis-cli命令时，会执行该命令
// *2\r\n$7\r\nCOMMAND\r\n$4\r\nDOCS\r\n
pub struct Command;

#[async_trait::async_trait]
impl CmdExecutor for Command {
    async fn execute(&self, _db: &Db) -> Result<Option<Frame>> {
        debug!("executing command 'COMMAND'");
        Ok(Some(Frame::Array(vec![])))
    }
}

// *1\r\n$4\r\nping\r\n
// return: +PONG\r\n
pub struct Ping;

#[async_trait::async_trait]
impl CmdExecutor for Ping {
    async fn execute(&self, _db: &Db) -> Result<Option<Frame>> {
        debug!("executing command 'PING'");
        Ok(Some(Frame::Simple("PONG".to_string())))
    }
}

// *2\r\n$4\r\necho\r\n$3\r\nhey\r\n
// return: $3\r\nhey\r\n
pub struct Echo {
    pub msg: Bytes,
}

#[async_trait::async_trait]
impl CmdExecutor for Echo {
    async fn execute(&self, _db: &Db) -> Result<Option<Frame>> {
        debug!("executing command 'ECHO'");
        Ok(Some(Frame::Bulk(self.msg.clone())))
    }
}

// 该命令用于获取Redis服务器的各种信息和统计数值
// *1\r\n$4\r\ninfo\r\n
// *2\r\n$4\r\ninfo\r\n$11\r\nreplication\r\n
pub struct Info {
    pub sections: Section,
}

#[allow(dead_code)]
pub enum Section {
    Array(Vec<Section>),
    // all: Return all sections (excluding module generated ones)
    All,
    // default: Return only the default set of sections
    Default,
    // everything: Includes all and modules
    Everything,
    // server: General information about the Redis server
    Server,
    // clients: Client connections section
    Clients,
    // memory: Memory consumption related information
    Memory,
    // persistence: RDB and AOF related information
    Persistence,
    // stats: General statistics
    Stats,
    // replication: Master/replica replication information
    Replication,
    // cpu: CPU consumption statistics
    Cpu,
    // commandstats: Redis command statistics
    CommandStats,
    // latencystats: Redis command latency percentile distribution statistics
    LatencyStats,
    // sentinel: Redis Sentinel section (only applicable to Sentinel instances)
    Sentinel,
    // cluster: Redis Cluster section
    Cluster,
    // modules: Modules section
    Modules,
    // keyspace: Database related statistics
    Keyspace,
    // errorstats: Redis error statistics
    ErrorStats,
}
impl TryFrom<Bytes> for Section {
    type Error = Error;

    fn try_from(value: Bytes) -> Result<Self, Self::Error> {
        let value = value.to_ascii_lowercase();
        match value.as_slice() {
            b"replication" => Ok(Section::Replication),
            // TODO:
            _ => Err(anyhow!("Incomplete")),
        }
    }
}
impl TryFrom<Vec<Bytes>> for Section {
    type Error = Error;

    fn try_from(value: Vec<Bytes>) -> Result<Self, Self::Error> {
        let mut sections = Vec::with_capacity(value.len());
        for section in value {
            sections.push(section.try_into()?);
        }
        Ok(Section::Array(sections))
    }
}

#[async_trait::async_trait]
impl CmdExecutor for Info {
    async fn execute(&self, _db: &Db) -> Result<Option<Frame>> {
        debug!("executing command 'INFO'");

        match self.sections {
            Section::Replication => {
                let res = if CONFIG.replication.replicaof.is_none() {
                    format!(
                        "role:master\r\nmaster_replid:{}\r\nmaster_repl_offset:{}\r\n",
                        CONFIG.server.run_id,
                        OFFSET.load(std::sync::atomic::Ordering::SeqCst)
                    )
                } else {
                    format!(
                        "role:slave\r\nmaster_replid:{}\r\nmaster_repl_offset:{}\r\n",
                        CONFIG.server.run_id,
                        OFFSET.load(std::sync::atomic::Ordering::SeqCst)
                    )
                };
                Ok(Some(Frame::Bulk(res.into())))
            }
            // TODO:
            _ => Err(anyhow!("Incomplete")),
        }
    }
}

// 该命令用于在后台异步保存当前数据库的数据到磁盘
pub struct BgSave;

#[async_trait::async_trait]
impl CmdExecutor for BgSave {
    async fn execute(&self, db: &Db) -> Result<Option<Frame>> {
        let db = db.inner.read().await.clone();
        std::thread::spawn(|| match util::rdb_save(db) {
            Ok(_) => tracing::info!("RDB file generated successfully!!!"),
            Err(e) => tracing::error!("Failed to save RDB file: {:?}", e),
        });

        Ok(Some(Frame::Simple(
            "Background saving scheduled".to_string(),
        )))
    }
}

// pub struct BgRewriteAof;

pub struct Auth {
    pub username: Option<String>,
    pub password: String,
}

#[async_trait::async_trait]
impl CmdExecutor for Auth {
    async fn execute(&self, _db: &Db) -> Result<Option<Frame>> {
        Ok(None)
    }

    async fn hook(
        &self,
        conn: &mut Connection,
        _replacate_msg_sender: &Sender<Frame>,
        _write_cmd_sender: &Sender<Frame>,
        _db: &Db,
        _cmd_from_client: Frame,
    ) -> anyhow::Result<()> {
        if let Some(passwd) = &CONFIG.security.requirepass {
            if &self.password != passwd {
                conn.write_frame(Frame::Error("ERR invalid password".to_string()))
                    .await?;
                return Ok(());
            }
        }
        conn.authed.store(true, std::sync::atomic::Ordering::SeqCst);
        conn.write_frame(Frame::Simple("OK".to_string())).await?;
        Ok(())
    }
}
