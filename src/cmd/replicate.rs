//! Use by relication to handle the REPLCONF and PSYNC commands from the master server.

use super::CmdExecutor;
use crate::{
    conf::{ACK_OFFSET, CONFIG},
    db::Db,
    frame::Frame,
    stream::FrameHandler,
    util::bytes_to_u64,
};
use anyhow::Result;
use std::{sync::atomic::Ordering, time::Duration};
use tokio::{io::AsyncWriteExt, sync::broadcast::Sender};

#[derive(Default)]
pub enum Replconf {
    #[default]
    Default,
    // replication收到该命令后发送自身的ack offset
    // *3\r\n$8\r\nreplconf\r\n$6\r\ngetack\r\n$1\r\n*\r\n
    GetAck,
}

#[async_trait::async_trait]
impl CmdExecutor for Replconf {
    async fn execute(&self, _db: &Db) -> Result<Option<Frame>> {
        // dbg!("respond1");
        let res = match self {
            Replconf::Default => Frame::Simple("OK".to_string()),
            // *3\r\n$8\r\nreplconf\r\n$6\r\nack\r\n$<len>\r\n<num>\r\n
            Replconf::GetAck => Frame::from(vec![
                "REPLCONF".into(),
                "ACK".into(),
                ACK_OFFSET.load(Ordering::SeqCst).to_string().into(),
            ]),
        };
        Ok(Some(res))
    }

    async fn replicate_execute(&self, db: &Db) -> anyhow::Result<Option<Frame>> {
        self.execute(db).await
    }
}

// master收到该命令后开始同步数据
// *2\r\n$5\r\npsync\r\n$1\r\n0\r\n
pub struct Psync;

#[async_trait::async_trait]
impl CmdExecutor for Psync {
    async fn execute(&self, _db: &Db) -> Result<Option<Frame>> {
        Ok(Some(Frame::Simple(format!(
            "FULLRESYNC {} 0",
            CONFIG.replication.replid
        ))))
    }

    async fn hook(
        &self,
        stream: &mut tokio::net::TcpStream,
        replacate_msg_sender: &Sender<Frame>,
        write_cmd_sender: &Sender<Frame>,
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

        let mut propagate_rx = write_cmd_sender.subscribe();
        // 对每个握手后的replication都进行持久化连接。
        loop {
            // TODO: 命令缓冲区

            // 每当收到一个"write" command通知，则发送给所有的replication
            // "REPLCONF GETACK *", "SET <key> <value>", "DEL <key>", "EXPIRE <key> <seconds>"...
            let frame = propagate_rx.recv().await?;
            tracing::info!("sending to replicate: {}", frame);
            stream.write_frame(frame.clone()).await?;
            stream.flush().await?;

            // 如果向replication发送的命令是"REPLCONF GETACK *"
            if frame
                .to_string()
                .to_lowercase()
                .starts_with(r#"*3\r\n$8\r\nreplconf\r\n$6\r\ngetack\r\n$1\r\n*\r\n"#)
            {
                // *3\r\n$8\r\nreplconf\r\n$6\r\ngetack\r\n$<len>\r\n<num>\r\n
                // propagate_tx.send(Frame::from(vec![
                //     "replconf".into(),
                //     "ack".into(),
                //     "50".into(),
                // ]))?;
                // 把从replication接收到的数据发送到其它异步任务（Wait命令的异步任务）
                if let Some(res) = stream.read_frame().await? {
                    tracing::info!("received from replicate: {}", frame);
                    replacate_msg_sender.send(res)?;
                }
            }
            // 记录发送给replication的字节数
            ACK_OFFSET.fetch_add(frame.num_of_bytes(), Ordering::SeqCst);
        }
    }
}

// master接收到该命令后，向所有的replication发送"REPLCONF GETACK *"
// 等待指定时间后，返回该段时间内同步的从服务器数量
// *2\r\n$4\r\nwait\r\n$1\r\n1\r\n
pub struct Wait {
    pub numreplicas: u64,
    pub timeout: Duration,
}
//
#[async_trait::async_trait]
impl CmdExecutor for Wait {
    async fn execute(&self, _db: &Db) -> Result<Option<Frame>> {
        Ok(None)
    }

    async fn hook(
        &self,
        stream: &mut tokio::net::TcpStream,
        replacate_msg_sender: &Sender<Frame>,
        write_cmd_sender: &Sender<Frame>,
        _frame: Frame,
    ) -> anyhow::Result<()> {
        // 向Psync任务发送"REPLCONF GETACK *"，即向所有的replication连接发送"REPLCONF GETACK *"
        write_cmd_sender.send(Frame::from(vec![
            "REPLCONF".into(),
            "GETACK".into(),
            "*".into(),
        ]))?;

        // 接收Psync消息的接收者
        let mut recv_from_psync = replacate_msg_sender.subscribe();
        let mut ack_replicas = 0; // 同步的replication数量
        let mut max_ack_offset = ACK_OFFSET.load(Ordering::SeqCst); // 最大的ack offset

        // remote: [your_program] 2024-03-11T10:48:38.205490Z DEBUG redis_starter_rust::stream: frames=[Bulk(b"WAIT"), Bulk(b"3"), Bulk(b"500")]
        // remote: [replication-17] Expected: '5' and actual: '0' messages don't match

        let timeout = if self.timeout == Duration::from_millis(60000) {
            Duration::from_millis(1000)
        } else {
            self.timeout
        };
        println!("waitting...");
        tokio::time::sleep(timeout).await; // 等待指定时间

        // 从Psync任务接收"REPLCONF ACK <offset>"，即从所有的replication连接接收"REPLCONF ACK <offset>"
        // try_recv()方法是非阻塞的，如果没有消息则立即返回Err
        while let Ok(Frame::Array(frames)) = recv_from_psync.try_recv() {
            println!("recv frame: {:?}", frames);
            // 检验返回的是不是"REPLCONF ACK <offset>"
            if Some(Frame::Bulk("REPLCONF".into())) != frames.first().cloned() {
                continue;
            }
            if Some(Frame::Bulk("ACK".into())) != frames.get(1).cloned() {
                continue;
            }
            if let Some(Frame::Bulk(ack_offset)) = frames.get(2) {
                let ack_offset = bytes_to_u64(ack_offset.clone()).expect("Invalid ack offset");
                // 判断replication是否同步，以及是否需要更新ack offset
                if ack_offset >= max_ack_offset {
                    max_ack_offset = ack_offset;
                    ack_replicas += 1;
                }
            }
        }
        ACK_OFFSET.store(max_ack_offset, Ordering::SeqCst); // 更新ack offset

        // 返回同步的replication数量
        stream
            .write_frame(Frame::Integer(ack_replicas as u64))
            .await?;

        Ok(())
    }
}
