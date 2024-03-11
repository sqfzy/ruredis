use super::CmdExecutor;
use crate::{
    db::Db,
    frame::Frame,
    server::{ACKOFFSET, CONFIG},
};
use anyhow::Result;
use bytes::Bytes;
use std::{sync::atomic::Ordering, time::Duration};
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
        let frame = match db.inner.write().await.string_db.get(self.key.clone()) {
            Some(value) => Frame::Bulk(value),
            None => Frame::Null,
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
            .string_db
            .set(self.key.clone(), self.value.clone(), self.expire);
        Ok(Some(Frame::Simple("OK".to_string())))
    }

    async fn hook(
        &self,
        _stream: &mut tokio::net::TcpStream,
        _db: &Db,
        propagate_tx: &Sender<Frame>,
        frame: Frame,
    ) -> anyhow::Result<()> {
        ACKOFFSET.fetch_add(frame.num_of_bytes(), Ordering::SeqCst);
        // 如果配置了主从复制，则将命令发送给所有从服务器
        if CONFIG.replicaof.is_none() {
            propagate_tx.send(frame)?;
        }
        Ok(())
    }
}
