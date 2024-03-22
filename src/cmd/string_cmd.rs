use super::CmdExecutor;
use crate::{conf::CONFIG, db::Db, frame::Frame};
use anyhow::Result;
use bytes::Bytes;
use std::time::Duration;
use tokio::sync::broadcast::Sender;
use tracing::debug;

// https://redis.io/commands/get/
// *2\r\n$3\r\nget\r\n$3\r\nkey\r\n
// return(the key exesits): $5\r\nvalue\r\n
// return(the key doesn't exesit): $-1\r\n
pub struct Get {
    pub key: Bytes,
}

#[async_trait::async_trait]
impl CmdExecutor for Get {
    async fn execute(&self, db: &Db) -> Result<Option<Frame>> {
        debug!("executing command 'GET'");
        let frame = match db.inner.write().await.string_kvs.get(self.key.clone()) {
            Some(value) => Frame::Bulk(value),
            // 键不存在，或已过期
            None => {
                //
                Frame::Null
            }
        };
        Ok(Some(frame))
    }
}

pub struct Set {
    pub key: Bytes,
    pub value: Bytes,
    pub expire: Option<Duration>,
    pub keep_ttl: bool,
}

#[async_trait::async_trait]
impl CmdExecutor for Set {
    async fn execute(&self, db: &Db) -> Result<Option<Frame>> {
        debug!("executing command 'SET'");
        db.inner
            .write()
            .await
            .string_kvs
            .set(self.key.clone(), self.value.clone(), self.expire);
        Ok(Some(Frame::Simple("OK".to_string())))
    }

    async fn hook(
        &self,
        _stream: &mut tokio::net::TcpStream,
        _replacate_msg_sender: &Sender<Frame>,
        write_cmd_sender: &Sender<Frame>,
        frame: Frame,
    ) -> anyhow::Result<()> {
        // 如果该节点是主节点，则向其它节点广播
        if CONFIG.replication.replicaof.is_none() {
            write_cmd_sender.send(frame)?;
        }
        Ok(())
    }
}
