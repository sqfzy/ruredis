use std::{io::Write, time::Duration};

use crate::{conf::CONFIG, frame::Frame};
use bytes::BytesMut;
use tokio::{fs::File, io::AsyncWriteExt, sync::broadcast::Receiver};

pub struct Aof {
    pub file: File,
    pub buffer: BytesMut,
    pub write_cmd_receiver: Receiver<Frame>, // 从写命令的通道中接收命令, 并写入到buffer中
}

impl Aof {
    pub async fn new(write_cmd_receiver: Receiver<Frame>) -> anyhow::Result<Self> {
        Ok(Aof {
            file: tokio::fs::OpenOptions::new()
                .append(true)
                .create(true)
                .open(&CONFIG.aof.file_path)
                .await?,
            buffer: BytesMut::with_capacity(1024 * 1024 * 1024),
            write_cmd_receiver,
        })
    }

    pub fn append(&mut self, cmd: &str) {
        self.buffer.extend_from_slice(cmd.as_bytes());
    }

    pub async fn write_in(&mut self) -> anyhow::Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        // 清空buffer, 并将buffer中的数据写入到文件中
        let buffer = std::mem::take(&mut self.buffer);
        self.file.write_all(&buffer).await?;

        Ok(())
    }

    pub async fn fsync(&mut self) -> anyhow::Result<()> {
        self.file.sync_all().await?;
        Ok(())
    }

    pub fn load(&self) -> anyhow::Result<()> {
        let mut client = loop {
            std::thread::sleep(Duration::from_millis(500)); // 等待server启动
            if let Ok(client) =
                std::net::TcpStream::connect(format!("127.0.0.1:{}", CONFIG.server.port))
            {
                break client;
            }
        };

        let cmds = std::fs::read_to_string(&CONFIG.aof.file_path)?;
        for cmd in cmds.lines() {
            if cmd.is_empty() {
                break;
            }
            client.write_all(cmd.replace("\\r\\n", "\r\n").as_bytes())?;
        }
        client.shutdown(std::net::Shutdown::Both)?;
        Ok(())
    }
}
