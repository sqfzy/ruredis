//! Use by relication to handle the REPLCONF and PSYNC commands from the master server.

use std::{future, sync::atomic::Ordering, time::Duration};

use super::CmdExecutor;
use crate::{
    db::Db,
    frame::Frame,
    server::{ACKOFFSET, CONFIG},
    stream::FrameHandler,
    util::bytes_to_u64,
};
use anyhow::Result;
use tokio::{io::AsyncWriteExt, sync::broadcast::Sender, task::block_in_place};

// 该命令用于配置主从复制
#[derive(Default)]
pub enum Replconf {
    #[default]
    Default,
    GetAck, // *3\r\n$8\r\nreplconf\r\n$6\r\ngetack\r\n$1\r\n*\r\n
}

#[async_trait::async_trait]
impl CmdExecutor for Replconf {
    async fn execute(&self, _db: &Db) -> Result<Option<Frame>> {
        Ok(None)
    }

    async fn hook(
        &self,
        stream: &mut tokio::net::TcpStream,
        _db: &Db,
        _frame_sender: &Sender<Frame>,
        frame: Frame,
    ) -> anyhow::Result<()> {
        let res = match self {
            Replconf::Default => Frame::Simple("OK".to_string()),
            Replconf::GetAck => Frame::from(vec![
                "REPLCONF".into(),
                "ACK".into(),
                ACKOFFSET.load(Ordering::SeqCst).to_string().into(),
            ]),
        };
        ACKOFFSET.fetch_add(frame.num_of_bytes(), Ordering::SeqCst);
        stream.write_frame(res).await?;
        Ok(())
    }
}

// 该命令用于同步主从服务器的数据
// *2\r\n$5\r\npsync\r\n$1\r\n0\r\n
pub struct Psync;

#[async_trait::async_trait]
impl CmdExecutor for Psync {
    async fn execute(&self, _db: &Db) -> Result<Option<Frame>> {
        Ok(Some(Frame::Simple(format!(
            "FULLRESYNC {} 0",
            CONFIG.replid
        ))))
    }

    async fn hook(
        &self,
        stream: &mut tokio::net::TcpStream,
        _db: &Db,
        propagate_tx: &Sender<Frame>,
        _frame: Frame,
    ) -> anyhow::Result<()> {
        // RDB is a snapshoot of datas
        let empty_rdb = [
            0x52, 0x45, 0x44, 0x49, 0x53, 0x30, 0x30, 0x31, 0x31, 0xfa, 0x09, 0x72, 0x65, 0x64,
            0x69, 0x73, 0x2d, 0x76, 0x65, 0x72, 0x05, 0x37, 0x2e, 0x32, 0x2e, 0x30, 0xfa, 0x0a,
            0x72, 0x65, 0x64, 0x69, 0x73, 0x2d, 0x62, 0x69, 0x74, 0x73, 0xc0, 0x40, 0xfa, 0x05,
            0x63, 0x74, 0x69, 0x6d, 0x65, 0xc2, 0x6d, 0x08, 0xbc, 0x65, 0xfa, 0x08, 0x75, 0x73,
            0x65, 0x64, 0x2d, 0x6d, 0x65, 0x6d, 0xc2, 0xb0, 0xc4, 0x10, 0x00, 0xfa, 0x08, 0x61,
            0x6f, 0x66, 0x2d, 0x62, 0x61, 0x73, 0x65, 0xc0, 0x00, 0xff, 0xf0, 0x6e, 0x3b, 0xfe,
            0xc0, 0xff, 0x5a, 0xa2,
        ];
        let buf = [b"$88\r\n", empty_rdb.as_ref()].concat();
        // $<length_of_file>\r\n<contents_of_file>
        stream.write_all(&buf).await?;
        stream.flush().await?;

        let mut propagate_rx = propagate_tx.subscribe();
        // 对每个握手后的replication都进行持久化连接。每当收到一个"write"
        // command通知，则发送给所有的replication
        loop {
            // TODO: 命令缓冲区
            let frame = propagate_rx.recv().await?;
            stream.write_frame(frame).await?;
        }
    }
}

// 该命令用于等待从服务器同步数据
// *2\r\n$4\r\nwait\r\n$1\r\n1\r\n
pub struct Wait {
    pub numreplicas: u64,
    pub timeout: Duration,
}

#[async_trait::async_trait]
impl CmdExecutor for Wait {
    async fn execute(&self, _db: &Db) -> Result<Option<Frame>> {
        Ok(None)
    }

    async fn hook(
        &self,
        stream: &mut tokio::net::TcpStream,
        _db: &Db,
        propagate_tx: &Sender<Frame>,
        _frame: Frame,
    ) -> anyhow::Result<()> {
        // 向所有的replication发送"REPLCONF GETACK *"
        propagate_tx.send(Frame::from(vec![
            "REPLCONF".into(),
            "GETACK".into(),
            "*".into(),
        ]))?;
        let synchronized_replica = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        block_in_place(|| async {
            tokio::select! {
                _ = tokio::time::sleep(self.timeout) => (),
                _ = future::ready(()) => {
                    while let Some(frame) = stream
                        .read_frame()
                        .await
                        .expect("Master server responds invaildly")
                    {
                        if let Frame::Array(resp) = frame {
                            if let Some(Frame::Bulk(ack_offset)) = resp.get(2) {
                                let ack_offset =
                                    bytes_to_u64(ack_offset.clone()).expect("Invalid ack offset");
                                if ack_offset >= ACKOFFSET.load(Ordering::SeqCst) {
                                    // ctx.ack_offset.store(ack_offset, Ordering::SeqCst);
                                    synchronized_replica.fetch_add(1, Ordering::SeqCst);
                                    let sync_repl = synchronized_replica.load(Ordering::SeqCst);
                                    if  sync_repl >= self.numreplicas {
                                        return;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        })
        .await;
        stream
            .write_frame(Frame::Integer(synchronized_replica.load(Ordering::SeqCst)))
            .await?;

        Ok(())
    }
}
