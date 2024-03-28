use super::CmdExecutor;
use crate::{
    conf::CONFIG,
    connection::Connection,
    db::{Db, Str},
    frame::Frame,
    util,
};
use anyhow::{anyhow, bail, Error, Result};
use bytes::{Bytes, BytesMut};
use std::{
    ops::RangeFull,
    time::{Duration, SystemTime},
};
use tokio::sync::broadcast::Sender;
use tracing::debug;

// 1	SET key value
// 设置指定 key 的值。
// 2	GET key
// 获取指定 key 的值。
// 3	GETRANGE key start end
// 返回 key 中字符串值的子字符
// 4	GETSET key value
// 将给定 key 的值设为 value ，并返回 key 的旧值(old value)。
// 5	GETBIT key offset
// 对 key 所储存的字符串值，获取指定偏移量上的位(bit)。
// 6	MGET key1 [key2..]
// 获取所有(一个或多个)给定 key 的值。
// 7	SETBIT key offset value
// 对 key 所储存的字符串值，设置或清除指定偏移量上的位(bit)。
// 8	SETEX key seconds value
// 将值 value 关联到 key ，并将 key 的过期时间设为 seconds (以秒为单位)。
// 9	SETNX key value
// 只有在 key 不存在时设置 key 的值。
// 10	SETRANGE key offset value
// 用 value 参数覆写给定 key 所储存的字符串值，从偏移量 offset 开始。
// 11	STRLEN key
// 返回 key 所储存的字符串值的长度。
// 12	MSET key value [key value ...]
// 同时设置一个或多个 key-value 对。
// 13	MSETNX key value [key value ...]
// 同时设置一个或多个 key-value 对，当且仅当所有给定 key 都不存在。
// 14	PSETEX key milliseconds value
// 这个命令和 SETEX 命令相似，但它以毫秒为单位设置 key 的生存时间，而不是像 SETEX 命令那样，以秒为单位。
// 15	INCR key
// 将 key 中储存的数字值增一。
// 16	INCRBY key increment
// 将 key 所储存的值加上给定的增量值（increment） 。
// 17	INCRBYFLOAT key increment
// 将 key 所储存的值加上给定的浮点增量值（increment） 。
// 18	DECR key
// 将 key 中储存的数字值减一。
// 19	DECRBY key decrement
// key 所储存的值减去给定的减量值（decrement） 。
// 20	APPEND key value
// 如果 key 已经存在并且是一个字符串， APPEND 命令将指定的 value 追加到该 key 原来值（value）的末尾。

// https://redis.io/commands/get/
// *2\r\n$3\r\nget\r\n$3\r\nkey\r\n
// return(the key exesits): $5\r\nvalue\r\n
// return(the key doesn't exesit): $-1\r\n
#[derive(Debug)]
pub struct Get {
    pub key: Bytes,
}

impl TryFrom<Vec<Bytes>> for Get {
    type Error = Error;

    fn try_from(bulks: Vec<Bytes>) -> std::prelude::v1::Result<Self, Self::Error> {
        if bulks.len() != 3 {
            bail!("ERR wrong number of arguments for 'get' command")
        }
        Ok(Get {
            key: bulks[2].clone(),
        })
    }
}

#[async_trait::async_trait]
impl CmdExecutor for Get {
    async fn execute(&self, db: &Db) -> Result<Option<Frame>> {
        debug!("executing command 'GET'");
        let frame = match db.inner.string_kvs.get(&self.key) {
            Some(value) => Frame::Bulk(value),
            // 键不存在，或已过期
            None => Frame::Null,
        };
        Ok(Some(frame))
    }
}

#[derive(Debug)]
pub struct Set {
    pub key: Bytes,
    pub value: Bytes,
    pub expire: Option<SystemTime>,
}

impl TryFrom<Vec<Bytes>> for Set {
    type Error = Error;

    fn try_from(bulks: Vec<Bytes>) -> Result<Self, Self::Error> {
        let mut bulks = bulks.into_iter().skip(1);

        let key = if let Some(key) = bulks.next() {
            key
        } else {
            bail!("ERR wrong number of arguments for 'set' command")
        };

        let value = if let Some(value) = bulks.next() {
            value
        } else {
            bail!("ERR wrong number of arguments for 'set' command")
        };

        let expire = if let Some(arg) = bulks.next() {
            let val = if let Some(val) = bulks.next() {
                val
            } else {
                bail!("ERR syntax error")
            };
            match arg.to_ascii_uppercase().as_slice() {
                b"EX" => Some(SystemTime::now() + Duration::from_secs(util::bytes_to_u64(val)?)),
                b"PX" => Some(SystemTime::now() + Duration::from_millis(util::bytes_to_u64(val)?)),
                b"EXAT" => {
                    Some(SystemTime::UNIX_EPOCH + Duration::from_secs(util::bytes_to_u64(val)?))
                }
                b"PXAT" => {
                    Some(SystemTime::UNIX_EPOCH + Duration::from_millis(util::bytes_to_u64(val)?))
                }
                b"KEEPTTL" => {
                    // None代表保持该键原有的过期时间
                    None
                }
                _ => {
                    bail!("ERR syntax error")
                }
            }
        } else {
            // UNIX_EPOCH设置该键为代表永不过期
            Some(SystemTime::UNIX_EPOCH)
        };

        Ok(Set { key, value, expire })
    }
}

#[async_trait::async_trait]
impl CmdExecutor for Set {
    async fn execute(&self, db: &Db) -> Result<Option<Frame>> {
        debug!("executing command 'SET'");
        db.clone().inner.string_kvs.set(
            self.key.clone(),
            BytesMut::from(self.value.as_ref()),
            self.expire,
        );
        Ok(Some(Frame::Simple("OK".to_string())))
    }

    async fn hook(
        &self,
        _conn: &mut Connection,
        _replacate_msg_sender: &Sender<Frame>,
        write_cmd_sender: &Sender<Frame>,
        _db: &Db,
        frame: Frame,
    ) -> anyhow::Result<()> {
        // 如果该节点是主节点，则向其它节点广播
        if CONFIG.replication.replicaof.is_none() {
            write_cmd_sender.send(frame)?;
        }
        Ok(())
    }
}

pub struct GetRange {
    pub key: Bytes,
    pub range: std::ops::Range<usize>,
}

impl TryFrom<Vec<Bytes>> for GetRange {
    type Error = Error;

    fn try_from(bulks: Vec<Bytes>) -> Result<Self, Self::Error> {
        if bulks.len() != 4 {
            bail!("ERR wrong number of arguments for 'getrange' command")
        }
        let key = bulks[2].clone();
        let start = util::bytes_to_u64(bulks[3].clone())? as usize;
        let end = util::bytes_to_u64(bulks[4].clone())? as usize;
        Ok(GetRange {
            key,
            range: start..end,
        })
    }
}

#[async_trait::async_trait]
impl CmdExecutor for GetRange {
    async fn execute(&self, db: &Db) -> Result<Option<Frame>> {
        debug!("executing command 'GETRANGE'");
        if let Some(value) = db.inner.string_kvs.get(&self.key) {
            let value = value.slice(self.range.clone());
            Ok(Some(Frame::Bulk(value)))
        } else {
            Ok(Some(Frame::Null))
        }
    }
}

pub struct GetSet {
    pub key: Bytes,
    pub value: Bytes,
}

impl TryFrom<Vec<Bytes>> for GetSet {
    type Error = Error;

    fn try_from(bulks: Vec<Bytes>) -> Result<Self, Self::Error> {
        if bulks.len() != 3 {
            bail!("ERR wrong number of arguments for 'getset' command")
        }
        Ok(GetSet {
            key: bulks[2].clone(),
            value: bulks[3].clone(),
        })
    }
}

#[async_trait::async_trait]
impl CmdExecutor for GetSet {
    async fn execute(&self, db: &Db) -> Result<Option<Frame>> {
        debug!("executing command 'GETSET'");
        let old_value = match db.inner.string_kvs.get(&self.key) {
            Some(value) => Frame::Bulk(value),
            None => Frame::Null,
        };
        db.inner
            .string_kvs
            .set(self.key.clone(), BytesMut::from(self.value.as_ref()), None);
        Ok(Some(old_value))
    }
}

pub struct MGet {
    pub keys: Vec<Bytes>,
}

impl TryFrom<Vec<Bytes>> for MGet {
    type Error = Error;

    fn try_from(bulks: Vec<Bytes>) -> Result<Self, Self::Error> {
        if bulks.len() < 2 {
            bail!("ERR wrong number of arguments for 'mget' command")
        }
        Ok(MGet {
            keys: bulks.into_iter().skip(1).collect(),
        })
    }
}

#[async_trait::async_trait]
impl CmdExecutor for MGet {
    async fn execute(&self, db: &Db) -> Result<Option<Frame>> {
        debug!("executing command 'MGET'");
        let mut frames = Vec::with_capacity(self.keys.len());
        for key in &self.keys {
            if let Some(value) = db.inner.string_kvs.get(key) {
                frames.push(Frame::Bulk(value));
            } else {
                frames.push(Frame::Null);
            }
        }
        Ok(Some(Frame::Array(frames)))
    }
}

pub struct SetEx {
    pub key: Bytes,
    pub expire: Duration,
    pub value: Bytes,
}

impl TryFrom<Vec<Bytes>> for SetEx {
    type Error = Error;

    fn try_from(bulks: Vec<Bytes>) -> Result<Self, Self::Error> {
        let mut bulks = bulks.into_iter().skip(1);

        let key = if let Some(key) = bulks.next() {
            key
        } else {
            bail!("ERR wrong number of arguments for 'setex' command")
        };

        let expire = if let Some(val) = bulks.next() {
            Duration::from_secs(util::bytes_to_u64(val)?)
        } else {
            bail!("ERR wrong number of arguments for 'setex' command")
        };

        let value = if let Some(value) = bulks.next() {
            value
        } else {
            bail!("ERR wrong number of arguments for 'setex' command")
        };

        Ok(SetEx { key, value, expire })
    }
}

#[async_trait::async_trait]
impl CmdExecutor for SetEx {
    async fn execute(&self, db: &Db) -> Result<Option<Frame>> {
        debug!("executing command 'SETEX'");
        db.inner.string_kvs.set_ttl(&self.key, self.expire);

        Ok(Some(Frame::Simple("OK".to_string())))
    }
}

pub struct SetNx {
    pub key: Bytes,
    pub value: Bytes,
}

impl TryFrom<Vec<Bytes>> for SetNx {
    type Error = Error;

    fn try_from(bulks: Vec<Bytes>) -> Result<Self, Self::Error> {
        if bulks.len() != 3 {
            bail!("ERR wrong number of arguments for 'setnx' command")
        }
        Ok(SetNx {
            key: bulks[2].clone(),
            value: bulks[3].clone(),
        })
    }
}

#[async_trait::async_trait]
impl CmdExecutor for SetNx {
    async fn execute(&self, db: &Db) -> Result<Option<Frame>> {
        debug!("executing command 'SETNX'");
        if db.inner.string_kvs.get(&self.key).is_some() {
            Ok(Some(Frame::Integer(0)))
        } else {
            db.inner.string_kvs.set(
                self.key.clone(),
                BytesMut::from(self.value.as_ref()),
                Some(SystemTime::UNIX_EPOCH),
            );
            Ok(Some(Frame::Integer(1)))
        }
    }
}

pub struct Incr {
    pub key: Bytes,
}

impl TryFrom<Vec<Bytes>> for Incr {
    type Error = Error;

    fn try_from(bulks: Vec<Bytes>) -> Result<Self, Self::Error> {
        if bulks.len() != 2 {
            bail!("ERR wrong number of arguments for 'incr' command")
        }
        Ok(Incr {
            key: bulks[2].clone(),
        })
    }
}

#[async_trait::async_trait]
impl CmdExecutor for Incr {
    async fn execute(&self, db: &Db) -> Result<Option<Frame>> {
        debug!("executing command 'INCR'");
        if let Some(obj) = db.inner.string_kvs.0.get_mut(&self.key) {
            match obj.value {
                Str::Int(i) => {
                    let value = i + 1;
                    Ok(Some(Frame::Integer(value)))
                }
                Str::Raw(_) => {
                    return Ok(Some(Frame::Error(
                        "ERR value is not an integer or out of range".to_string(),
                    )))
                }
            }
        } else {
            Ok(Some(Frame::Null))
        }
    }
}

// pub struct IncrBy {
//     pub key: Bytes,
//     pub increment: i64,
// }
//
// impl TryFrom<Vec<Bytes>> for IncrBy {
//     type Error = Error;
//
//     fn try_from(bulks: Vec<Bytes>) -> Result<Self, Self::Error> {
//         if bulks.len() != 3 {
//             bail!("ERR wrong number of arguments for 'incrby' command")
//         }
//         Ok(IncrBy {
//             key: bulks[2].clone(),
//             increment: util::bytes_to_i64(bulks[3].clone())?,
//         })
//     }
// }
//
// #[async_trait::async_trait]
// impl CmdExecutor for IncrBy {
//     async fn execute(&self, db: &Db) -> Result<Option<Frame>> {
//         debug!("executing command 'INCRBY'");
//         let value = match db.inner.string_kvs.get(&self.key) {
//             Some(value) => {
//                 let value = util::bytes_to_i64(value)?;
//                 value + self.increment
//             }
//             None => self.increment,
//         };
//         db.inner.string_kvs.set(
//             self.key.clone(),
//             BytesMut::from(value.to_string().as_bytes()),
//             Some(SystemTime::UNIX_EPOCH),
//         );
//         Ok(Some(Frame::Integer(value))
//     }
// }
//
// pub struct Decr {
//     pub key: Bytes,
// }
//
// impl TryFrom<Vec<Bytes>> for Decr {
//     type Error = Error;
//
//     fn try_from(bulks: Vec<Bytes>) -> Result<Self, Self::Error> {
//         if bulks.len() != 2 {
//             bail!("ERR wrong number of arguments for 'decr' command")
//         }
//         Ok(Decr {
//             key: bulks[2].clone(),
//         })
//     }
// }
//
// #[async_trait::async_trait]
// impl CmdExecutor for Decr {
//     async fn execute(&self, db: &Db) -> Result<Option<Frame>> {
//         debug!("executing command 'DECR'");
//         let value = match db.inner.string_kvs.get(&self.key) {
//             Some(value) => {
//                 let value = util::bytes_to_i64(value)?;
//                 value - 1
//             }
//             None => -1,
//         };
//         db.inner.string_kvs.set(
//             self.key.clone(),
//             BytesMut::from(value.to_string().as_bytes()),
//             Some(SystemTime::UNIX_EPOCH),
//         );
//         Ok(Some(Frame::Integer(value))
//     }
// }
//
// pub struct DecrBy {
//     pub key: Bytes,
//     pub decrement: i64,
// }
//
// impl TryFrom<Vec<Bytes>> for DecrBy {
//     type Error = Error;
//
//     fn try_from(bulks: Vec<Bytes>) -> Result<Self, Self::Error> {
//         if bulks.len() != 3 {
//             bail!("ERR wrong number of arguments for 'decrby' command")
//         }
//         Ok(DecrBy {
//             key: bulks[2].clone(),
//             decrement: util::bytes_to_i64(bulks[3].clone())?,
//         })
//     }
// }
//
// #[async_trait::async_trait]
// impl CmdExecutor for DecrBy {
//     async fn execute(&self, db: &Db) -> Result<Option<Frame>> {
//         debug!("executing command 'DECRBY'");
//         let value = match db.inner.string_kvs.get(&self.key) {
//             Some(value) => {
//                 let value = util::bytes_to_i64(value)?;
//                 value - self.decrement
//             }
//             None => -self.decrement,
//         };
//         db.inner.string_kvs.set(
//             self.key.clone(),
//             BytesMut::from(value.to_string().as_bytes()),
//             Some(SystemTime::UNIX_EPOCH),
//         );
//         Ok(Some(Frame::Integer(value))
//     }
// }
