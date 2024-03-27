use crate::{
    cmd::{self, CmdExecutor, Section},
    util::{bytes_to_string, bytes_to_u64},
};
use anyhow::{anyhow, bail, Error, Result};
use bytes::Bytes;
use std::{fmt::Display, time::Duration, u64};

#[derive(Clone, Debug, Default, PartialEq)]
pub enum Frame {
    Simple(String), // +<str>\r\n
    Error(String),  // -<err>\r\n
    Integer(u64),   // :<num>\r\n
    Bulk(Bytes),    // $<len>\r\n<bytes>\r\n
    #[default]
    Null, // $-1\r\n
    Array(Vec<Frame>), // *<len>\r\n<Frame>...
}

impl Frame {
    pub fn parse_cmd(self) -> Result<Box<dyn CmdExecutor>> {
        let bulks: Vec<Bytes> = self.try_into()?;
        let len = bulks.len();
        if len == 0 {
            bail!("ERR command syntax error")
        }

        Ok(match bulks[0].to_ascii_uppercase().as_slice() {
            b"COMMAND" => Box::new(cmd::Command),
            b"PING" => Box::new(cmd::Ping),
            b"PONG" => Box::new(cmd::Echo::try_from(bulks)?),
            b"GET" => Box::new(cmd::Get::try_from(bulks)?),
            b"SET" => Box::new(cmd::Set::try_from(bulks)?),
            b"INFO" => Box::new(cmd::Info::try_from(bulks)?),
            b"REPLCONF" => Box::new(cmd::Replconf::try_from(bulks)?),
            b"PSYNC" => Box::new(cmd::Psync::try_from(bulks)?),
            b"WAIT" => Box::new(cmd::Wait::try_from(bulks)?),
            b"BGSAVE" => Box::new(cmd::BgSave),
            _ => {
                bail!(
                    "ERR unknown command '{}'",
                    bytes_to_string(bulks[0].clone())?
                )
            }
        })
    }

    // Frame的字节长度
    pub fn num_of_bytes(&self) -> u64 {
        match self {
            Frame::Simple(s) => s.len() as u64 + 3,
            Frame::Error(e) => e.len() as u64 + 3,
            Frame::Integer(n) => n.to_string().len() as u64 + 3,
            Frame::Bulk(b) => b.len() as u64 + b.len().to_string().len() as u64 + 5,
            Frame::Null => 5,
            Frame::Array(frames) => {
                frames.iter().map(|f| f.num_of_bytes()).sum::<u64>()
                    + 3
                    + frames.len().to_string().len() as u64
            }
        }
    }

    pub fn to_bytes(&self) -> Bytes {
        self.to_string().replace("\\r\\n", "\r\n").into()
    }
}

impl Display for Frame {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Frame::Simple(s) => write!(f, r#"+{}\r\n"#, s),
            Frame::Error(e) => write!(f, r#"-{}\r\n"#, e),
            Frame::Integer(n) => write!(f, r#":{}\r\n"#, n),
            Frame::Bulk(b) => write!(
                f,
                r#"${}\r\n{}\r\n"#,
                b.len(),
                bytes_to_string(b.clone()).unwrap()
            ),
            Frame::Null => write!(f, r#"$-1\r\n"#),
            Frame::Array(frames) => {
                let mut s = String::new();
                s.push_str(&format!(r#"*{}\r\n"#, frames.len()));
                for frame in frames {
                    s.push_str(&frame.to_string());
                }
                write!(f, "{}", s)
            }
        }
    }
}

impl TryFrom<Vec<Bytes>> for cmd::Info {
    type Error = Error;
    fn try_from(value: Vec<Bytes>) -> Result<Self, Self::Error> {
        let len = value.len();
        if len == 1 {
            return Ok(cmd::Info {
                sections: Section::Default,
            });
        }
        if len == 2 {
            let section = value[1].clone();
            return Ok(cmd::Info {
                sections: section.try_into()?,
            });
        }
        if len > 2 && len <= 14 {
            let sections = value[1..].to_vec();
            return Ok(cmd::Info {
                sections: sections.try_into()?,
            });
        }

        Err(anyhow!("ERR syntax error"))
    }
}

impl TryInto<Vec<Bytes>> for Frame {
    type Error = Error;

    fn try_into(self) -> Result<Vec<Bytes>, Error> {
        if let Frame::Array(frames) = self {
            frames
                .into_iter()
                .map(|frame| match frame {
                    Frame::Bulk(bytes) => Ok(bytes),
                    _ => panic!("invaild frame"),
                })
                .collect()
        } else {
            panic!("invaild frame");
        }
    }
}

impl From<Vec<Bytes>> for Frame {
    fn from(value: Vec<Bytes>) -> Self {
        Frame::Array(value.into_iter().map(Frame::Bulk).collect())
    }
}

#[cfg(test)]
mod test_frame {
    #[test]
    fn test_num_of_bytes() {
        use crate::frame::Frame;
        let frame = Frame::Simple("OK".to_string()); // +OK\r\n
        assert_eq!(frame.num_of_bytes(), 5);

        let frame = Frame::Error("ERR".to_string()); // -ERR\r\n
        assert_eq!(frame.num_of_bytes(), 6);

        let frame = Frame::Integer(100); // :100\r\n
        assert_eq!(frame.num_of_bytes(), 6);

        let frame = Frame::Bulk("Hello".into()); // $5\r\nHello\r\n
        assert_eq!(frame.num_of_bytes(), 11);

        let frame = Frame::Null; // $-1\r\n
        assert_eq!(frame.num_of_bytes(), 5);

        // *5\r\n+OK\r\n-ERR\r\n:100\r\n$5\r\nHello\r\n$-1\r\n
        let frame = Frame::Array(vec![
            Frame::Simple("OK".to_string()),
            Frame::Error("ERR".to_string()),
            Frame::Integer(100),
            Frame::Bulk("Hello".into()),
            Frame::Null,
        ]);
        assert_eq!(frame.num_of_bytes(), 37);
    }
}
