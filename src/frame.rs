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
        let cmd_name = bytes_to_string(bulks[0].clone())?;
        match cmd_name.to_lowercase().as_str() {
            "command" => return Ok(Box::new(cmd::Command)),
            "ping" => {
                if len == 1 {
                    return Ok(Box::new(cmd::Ping));
                }
            }
            "echo" => {
                if len == 2 {
                    return Ok(Box::new(cmd::Echo {
                        msg: bulks[1].clone(),
                    }));
                }
            }
            "get" => {
                if len == 2 {
                    return Ok(Box::new(cmd::Get {
                        key: bulks[1].clone(),
                    }));
                }
                bail!("ERR wrong number of arguments for 'get' command")
            }
            "set" => return Ok(Box::new(cmd::Set::try_from(bulks)?) as Box<dyn CmdExecutor>),
            "info" => return Ok(Box::new(cmd::Info::try_from(bulks)?) as Box<dyn CmdExecutor>),
            "replconf" => match bulks[1].to_ascii_lowercase().as_slice() {
                b"getack" => {
                    // if bulks[2].as_ref() == b"*" {
                    return Ok(Box::new(cmd::Replconf::GetAck));
                    // }
                }
                _ => return Ok(Box::<cmd::Replconf>::default()),
            },
            "psync" => return Ok(Box::new(cmd::Psync)),
            "wait" => {
                if let Some(numreplicas) = bulks.get(1) {
                    let numreplicas = bytes_to_u64(numreplicas.clone())?;
                    if let Some(timeout) = bulks.get(2) {
                        let timeout = Duration::from_millis(bytes_to_u64(timeout.clone())?);
                        return Ok(Box::new(cmd::Wait {
                            numreplicas,
                            timeout,
                        }));
                    }
                }
            }
            "bgsave" => return Ok(Box::new(cmd::BgSave)),
            _ => {}
        }

        Err(anyhow!(
            // "ERR unknown command {}, with args beginning with:",
            "ERR unknown command {}",
            cmd_name
        ))
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

impl TryFrom<Vec<Bytes>> for cmd::Set {
    type Error = Error;
    fn try_from(bulks: Vec<Bytes>) -> Result<Self, Self::Error> {
        let len = bulks.len();
        if len >= 3 {
            let key = bulks[1].clone();
            let value = bulks[2].clone();

            if len == 3 {
                return Ok(cmd::Set {
                    key,
                    value,
                    expire: None,
                    keep_ttl: false,
                });
            }
            if len == 4 {
                #[allow(clippy::single_match)]
                match bulks[4].to_ascii_lowercase().as_slice() {
                    b"keepttl" => {
                        return Ok(cmd::Set {
                            key,
                            value,
                            expire: None,
                            keep_ttl: true,
                        });
                    }
                    _ => {}
                }
            }
            if len == 5 {
                let expire_unit = bulks[3].to_ascii_lowercase();
                let expire = bytes_to_u64(bulks[4].clone())?;

                if expire == 0 {
                    bail!("ERR invalid expire time in 'set' command")
                }

                match expire_unit.as_slice() {
                    b"ex" => {
                        return Ok(cmd::Set {
                            key,
                            value,
                            expire: Some(Duration::from_secs(expire)),
                            keep_ttl: false,
                        });
                    }
                    b"px" => {
                        return Ok(cmd::Set {
                            key,
                            value,
                            expire: Some(Duration::from_millis(expire)),
                            keep_ttl: false,
                        });
                    }
                    _ => {}
                }
            }
        }

        Err(anyhow!("ERR syntax error"))
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
