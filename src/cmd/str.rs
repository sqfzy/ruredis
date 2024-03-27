use super::CmdExecutor;
use crate::{
    conf::CONFIG,
    connection::Connection,
    db::{Db, IndexRange},
    frame::Frame,
    util,
};
use anyhow::{anyhow, bail, Error, Result};
use bytes::Bytes;
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
        let frame = match db.inner.string_kvs.get(&self.key, RangeFull.into()) {
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
        db.clone()
            .inner
            .string_kvs
            .set(self.key.clone(), self.value.clone(), self.expire);
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
    pub keys: Vec<Bytes>,
}
