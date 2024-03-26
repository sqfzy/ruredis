use std::usize;

use crate::{
    frame::Frame,
    util::{bytes_to_string, bytes_to_u64},
};
use anyhow::{bail, Result};
use bytes::{BufMut, Bytes};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};
use tracing::{debug, error};

// #[async_trait::async_trait]
pub trait FrameHandler {
    async fn read_frame(&mut self) -> Result<Option<Frame>>;
    async fn write_frame(&mut self, frame: Frame) -> Result<()>;
    async fn read_line(&mut self) -> Result<Bytes>;
    async fn read_decimal(&mut self) -> Result<u64>;
    async fn read_line_exact(&mut self, n: usize) -> Result<Bytes>;
}

// #[async_trait::async_trait]
impl FrameHandler for TcpStream {
    async fn read_frame(&mut self) -> Result<Option<Frame>> {
        let mut prefix = [0u8; 1];
        if self.peek(&mut prefix).await? == 0 {
            return Ok(None);
        }
        match prefix[0] {
            b'*' => {
                debug!("reading array");

                self.read_u8().await?;
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

    async fn write_frame(&mut self, frame: Frame) -> Result<()> {
        match frame {
            // *<len>\r\n<Frame>...
            Frame::Array(frames) => {
                let header = format!("*{}\r\n", frames.len());
                self.write_all(header.as_bytes()).await?;

                for frame in frames {
                    write_value(self, frame).await?;
                }
            }
            _ => write_value(self, frame).await?,
        }

        Ok(())
    }

    async fn read_line(&mut self) -> Result<Bytes> {
        let mut buf = vec![];
        loop {
            let byte = self.read_u8().await?;
            if byte == b'\r' {
                let byte = self.read_u8().await?;
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

    async fn read_decimal(&mut self) -> Result<u64> {
        let len = self.read_line().await?;
        bytes_to_u64(len)
    }

    async fn read_line_exact(&mut self, n: usize) -> Result<Bytes> {
        let mut buf = vec![0u8; n];
        self.read_exact(&mut buf).await?;

        let mut new_line = [0u8; 2];
        self.read_exact(&mut new_line).await?;
        if new_line != "\r\n".as_bytes() {
            bail!("ERR syntax error")
        }

        Ok(buf.into())
    }
}

async fn read_value(stream: &mut TcpStream) -> Result<Frame> {
    match stream.read_u8().await? {
        b'+' => {
            debug!("reading simple");

            let line = stream.read_line().await?;
            let res = Frame::Simple(bytes_to_string(line)?);

            debug!(?res);

            Ok(res)
        }
        b'-' => {
            debug!("reading error");

            let line = stream.read_line().await?;
            let res = Frame::Error(bytes_to_string(line)?);

            debug!(?res);

            Ok(res)
        }
        b':' => {
            debug!("reading integer");

            let res = stream.read_decimal().await?;

            debug!(?res);

            Ok(Frame::Integer(res))
        }
        b'$' => {
            debug!("reading bulk");

            let len = stream.read_decimal().await? as usize;
            let bytes = stream.read_line_exact(len).await?;
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

async fn write_value(stream: &mut TcpStream, frame: Frame) -> Result<()> {
    match frame {
        // +<str>\r\n
        Frame::Simple(s) => {
            let msg = format!("+{}\r\n", s);
            stream.write_all(msg.as_bytes()).await?;
            stream.flush().await?;
        }
        // -<err>\r\n
        Frame::Error(e) => {
            let msg = format!("-{}\r\n", e);
            stream.write_all(msg.as_bytes()).await?;
            stream.flush().await?;
        }
        // :<num>\r\n
        Frame::Integer(n) => {
            let msg = format!(":{}\r\n", n);
            stream.write_all(msg.as_bytes()).await?;
            stream.flush().await?;
        }
        // $<len>\r\n<bytes>\r\n
        Frame::Bulk(b) => {
            let header = format!("${}\r\n", b.len());
            stream.write_all(header.as_bytes()).await?;
            stream.write_all(&b).await?;
            stream.write_all(b"\r\n").await?;
            stream.flush().await?;
        }
        // $-1\r\n
        Frame::Null => {
            stream.write_all(b"$-1\r\n").await?;
            stream.flush().await?;
        }
        Frame::Array(_) => unreachable!(),
    }

    Ok(())
}
