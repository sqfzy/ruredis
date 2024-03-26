use crate::{
    frame::Frame,
    util::{bytes_to_string, bytes_to_u64},
};
use anyhow::{bail, Result};
use bytes::{BufMut, Bytes};
use std::{sync::atomic::AtomicBool, usize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::{debug, error};

#[derive(Debug)]
pub struct Connection {
    pub stream: TcpStream,
    pub peer_addr: String,
    pub authed: AtomicBool,
}

impl Connection {
    pub fn new(socket: TcpStream, peer_addr: String) -> Self {
        Self {
            stream: socket,
            peer_addr,
            authed: AtomicBool::new(false),
        }
    }

    pub async fn connect(socket: &str) -> anyhow::Result<Self> {
        let stream = TcpStream::connect(socket).await?;
        let peer_addr = stream.peer_addr()?.to_string();
        Ok(Self {
            stream,
            peer_addr,
            authed: AtomicBool::new(false),
        })
    }

    #[inline]
    pub async fn write_all(&mut self, buf: &[u8]) -> Result<()> {
        self.stream.write_all(buf).await?;
        Ok(())
    }

    #[inline]
    pub async fn flush(&mut self) -> Result<()> {
        self.stream.flush().await?;
        Ok(())
    }

    pub async fn read_frame(&mut self) -> Result<Option<Frame>> {
        let mut prefix = [0u8; 1];
        if self.stream.peek(&mut prefix).await? == 0 {
            return Ok(None);
        }
        match prefix[0] {
            b'*' => {
                debug!("reading array");

                self.stream.read_u8().await?;
                let len = self.read_decimal().await? as usize;
                let mut frames: Vec<Frame> = Vec::with_capacity(len);

                for _ in 0..len {
                    let frame = read_value(self).await?;
                    frames.push(frame);
                }

                debug!(?frames);

                Ok(Some(Frame::Array(frames)))
            }
            _ => read_value(self).await.map(Some),
        }
    }

    pub async fn write_frame(&mut self, frame: Frame) -> Result<()> {
        match frame {
            // *<len>\r\n<Frame>...
            Frame::Array(frames) => {
                let header = format!("*{}\r\n", frames.len());
                self.stream.write_all(header.as_bytes()).await?;

                for frame in frames {
                    write_value(self, frame).await?;
                }
            }
            _ => write_value(self, frame).await?,
        }

        Ok(())
    }

    pub async fn read_line(&mut self) -> Result<Bytes> {
        let mut buf = vec![];
        loop {
            let byte = self.stream.read_u8().await?;
            if byte == b'\r' {
                let byte = self.stream.read_u8().await?;
                if byte == b'\n' {
                    break;
                }
                buf.put_u8(b'\r');
                buf.put_u8(byte);
            }
            buf.put_u8(byte);
        }

        Ok(buf.into())
    }

    pub async fn read_decimal(&mut self) -> Result<u64> {
        let len = self.read_line().await?;
        bytes_to_u64(len)
    }

    pub async fn read_line_exact(&mut self, n: usize) -> Result<Bytes> {
        let mut buf = vec![0u8; n];
        self.stream.read_exact(&mut buf).await?;

        let mut new_line = [0u8; 2];
        self.stream.read_exact(&mut new_line).await?;
        if new_line != "\r\n".as_bytes() {
            bail!("ERR syntax error")
        }

        Ok(buf.into())
    }
}

// // #[async_trait::async_trait]
// pub trait FrameHandler {
//     async fn read_frame(&mut self) -> Result<Option<Frame>>;
//     async fn write_frame(&mut self, frame: Frame) -> Result<()>;
//     async fn read_line(&mut self) -> Result<Bytes>;
//     async fn read_decimal(&mut self) -> Result<u64>;
//     async fn read_line_exact(&mut self, n: usize) -> Result<Bytes>;
// }

async fn read_value(conn: &mut Connection) -> Result<Frame> {
    match conn.stream.read_u8().await? {
        b'+' => {
            debug!("reading simple");

            let line = conn.read_line().await?;
            let res = Frame::Simple(bytes_to_string(line)?);

            debug!(?res);

            Ok(res)
        }
        b'-' => {
            debug!("reading error");

            let line = conn.read_line().await?;
            let res = Frame::Error(bytes_to_string(line)?);

            debug!(?res);

            Ok(res)
        }
        b':' => {
            debug!("reading integer");

            let res = conn.read_decimal().await?;

            debug!(?res);

            Ok(Frame::Integer(res))
        }
        b'$' => {
            debug!("reading bulk");

            let len = conn.read_decimal().await? as usize;
            let bytes = conn.read_line_exact(len).await?;
            let res = Frame::Bulk(bytes);

            debug!(?res);

            Ok(res)
        }
        b'*' => unreachable!(),
        somthing => {
            error!("read invaild prefix {}", somthing);
            bail!("ERR syntax error")
        }
    }
}

async fn write_value(conn: &mut Connection, frame: Frame) -> Result<()> {
    match frame {
        // +<str>\r\n
        Frame::Simple(s) => {
            let msg = format!("+{}\r\n", s);
            conn.stream.write_all(msg.as_bytes()).await?;
            conn.stream.flush().await?;
        }
        // -<err>\r\n
        Frame::Error(e) => {
            let msg = format!("-{}\r\n", e);
            conn.stream.write_all(msg.as_bytes()).await?;
            conn.stream.flush().await?;
        }
        // :<num>\r\n
        Frame::Integer(n) => {
            let msg = format!(":{}\r\n", n);
            conn.stream.write_all(msg.as_bytes()).await?;
            conn.stream.flush().await?;
        }
        // $<len>\r\n<bytes>\r\n
        Frame::Bulk(b) => {
            let header = format!("${}\r\n", b.len());
            conn.stream.write_all(header.as_bytes()).await?;
            conn.stream.write_all(&b).await?;
            conn.stream.write_all(b"\r\n").await?;
            conn.stream.flush().await?;
        }
        // $-1\r\n
        Frame::Null => {
            conn.stream.write_all(b"$-1\r\n").await?;
            conn.stream.flush().await?;
        }
        Frame::Array(_) => unreachable!(),
    }

    Ok(())
}

#[cfg(test)]
mod test_connection {
    use super::*;

    #[tokio::test]
    async fn test_read_and_write_frame() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        // 模拟服务器
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();

            // 测试简单字符串
            let _ = socket.write_all(b"+OK\r\n").await;
            // 测试错误消息
            let _ = socket.write_all(b"-Error message\r\n").await;
            // 测试整数
            let _ = socket.write_all(b":1000\r\n").await;
            // 测试大容量字符串
            let _ = socket.write_all(b"$6\r\nfoobar\r\n").await;
            // 测试空字符串
            let _ = socket.write_all(b"$0\r\n\r\n").await;
            // 测试数组
            let _ = socket.write_all(b"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n").await;
            // 测试空数组
            let _ = socket.write_all(b"*0\r\n").await;
        });

        let stream = TcpStream::connect(addr).await.unwrap();
        let mut conn = Connection::new(stream, "".to_string());

        // 测试简单字符串
        assert_eq!(
            conn.read_frame().await.unwrap(),
            Some(Frame::Simple("OK".to_string()))
        );
        // 测试错误消息
        assert_eq!(
            conn.read_frame().await.unwrap(),
            Some(Frame::Error("Error message".to_string()))
        );
        // 测试整数
        assert_eq!(conn.read_frame().await.unwrap(), Some(Frame::Integer(1000)));
        // 测试大容量字符串
        assert_eq!(
            conn.read_frame().await.unwrap(),
            Some(Frame::Bulk(Bytes::from("foobar")))
        );
        // 测试空字符串
        assert_eq!(
            conn.read_frame().await.unwrap(),
            Some(Frame::Bulk(Bytes::from("")))
        );
        // 测试数组
        assert_eq!(
            conn.read_frame().await.unwrap(),
            Some(Frame::Array(vec![
                Frame::Bulk(Bytes::from("foo")),
                Frame::Bulk(Bytes::from("bar"))
            ]))
        );
        // 测试空数组
        assert_eq!(conn.read_frame().await.unwrap(), Some(Frame::Array(vec![])));
    }
}
