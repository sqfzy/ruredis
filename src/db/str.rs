use super::{KvPairs, ObjValueCODEC, Object};
use bytes::Bytes;
use std::{
    ops::Deref,
    time::{Duration, SystemTime},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Str {
    Int(i64),
    Raw(Bytes),
}

impl ObjValueCODEC for Str {
    fn decode(&self) -> Bytes {
        match self {
            Str::Raw(b) => b.clone(),
            Str::Int(i) => Bytes::from(i.to_string()),
        }
    }

    fn encode(bytes: Bytes) -> Self {
        if let Ok(s) = std::str::from_utf8(&bytes) {
            if let Ok(i) = s.parse::<i64>() {
                return Str::Int(i);
            }
        }
        Str::Raw(bytes)
    }
}

impl KvPairs<Str> {
    pub fn get(&self, key: &[u8]) -> Option<Bytes> {
        // 找到key对应的obj则继续，否则返回None
        if let Some(obj) = self.0.get(key) {
            // 检查键是否过期了，如果键已经过期则移除并返回None，否则返回Some(value)
            if let Some(expire_at) = obj.expire_at {
                if expire_at < SystemTime::now() {
                    self.0.remove(key);
                    return None;
                }
            }
            return Some(obj.deref().value());
        };

        None
    }

    pub fn set(&self, key: impl Into<Bytes>, value: impl Into<Bytes>, expire: Option<Duration>) {
        let key = key.into();
        let value = value.into();
        // 找到key对应的obj则继续，否则写入该键值对
        if let Some(mut obj) = self.0.get_mut(&key) {
            // 开始修改旧obj为新的obj
            obj.set_value(value); // 修改旧值为新值
            match expire {
                // 如果expire为0则obj永不过期
                Some(e) if e.as_secs() == 0 => {
                    obj.expire_at = None;
                }
                Some(e) => {
                    obj.expire_at = Some(SystemTime::now() + e);
                }
                // 如果exipre为None则不修改obj的过期时间
                None => {}
            }
        } else {
            self.0.insert(
                key,
                Object::new(value, expire.map(|e| SystemTime::now() + e)),
            );
        }
    }

    pub fn del(&self, key: impl Into<Bytes>) {
        self.0.remove(&key.into());
    }

    pub fn check_exist(&self, key: impl Into<Bytes>) -> bool {
        let key = key.into();
        // 找到key对应的obj则继续，否则返回false
        if let Some(value) = self.0.get(&key.clone()) {
            // 检查键是否过期了，如果键已经过期则移除并返回false，否则返回true
            if let Some(expire_at) = value.expire_at {
                if expire_at < SystemTime::now() {
                    self.0.remove(&key);
                    return false;
                }
            }
            return true;
        }

        false
    }

    pub fn get_ttl(&mut self, key: impl Into<Bytes>) -> Option<Duration> {
        let key = key.into();
        // 找到key对应的obj则继续，否则返回None
        if let Some(value) = self.0.get(&key) {
            // 检查键是否过期了，如果键永不过期则返回Some(0)
            if let Some(expire_at) = value.expire_at {
                let now = SystemTime::now();
                // 如果键已经过期则移除并返回None，否则返回Some(可存活时间)
                if let Ok(ttl) = expire_at.duration_since(now) {
                    return Some(ttl);
                }
                self.0.remove(&key);
                return None;
            }
            return Some(Duration::from_secs(0));
        }

        None
    }
}
