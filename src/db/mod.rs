mod hash;
mod list;
mod set;
mod str;
mod zset;

use bytes::Bytes;
use dashmap::DashMap;
use skiplist::SkipList;
use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};

pub use str::Str;

// pub const MAX_KVPAIRS_NUMS: u64 = u64::MAX;

#[derive(Debug, Clone)]
pub struct Db {
    pub inner: Arc<DbInner>,
}

#[derive(Debug, Clone)]
pub struct DbInner {
    pub string_kvs: KvPairs<Str>,
}

impl Db {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(DbInner {
                string_kvs: KvPairs(DashMap::new()),
            }),
        }
    }
}

#[derive(Debug, Clone)]
pub struct KvPairs<T: ObjValueCODEC>(pub DashMap<Bytes, Object<T>>);

impl<T: ObjValueCODEC + PartialEq + Eq> KvPairs<T> {
    pub fn get(&self, key: &[u8], index: T::Index) -> Option<T::Output> {
        // 找到key对应的obj则继续，否则返回None
        if let Some(obj) = self.0.get(key) {
            // 检查键是否过期了，如果键已经过期则移除并返回None，否则返回Some(value)
            if let Some(expire_at) = obj.expire {
                if expire_at < SystemTime::now() {
                    self.0.remove(key);
                    return None;
                }
            }
            return Some((*obj).value(index));
        };

        None
    }

    pub fn set(&self, key: impl Into<Bytes>, value: T::Input, expire: Option<SystemTime>) {
        let key = key.into();
        // 找到key对应的obj则继续，否则写入该键值对
        if let Some(mut obj) = self.0.get_mut(&key) {
            // 开始修改旧obj为新的obj
            obj.set_value(value); // 修改旧值为新值
            match expire {
                // 如果exipre为UNIX_EPOCH则设置该键为永不过期
                Some(e) if e == SystemTime::UNIX_EPOCH => {
                    obj.expire = None;
                }
                Some(e) => {
                    obj.expire = Some(e);
                }
                // 如果exipre为None则不修改obj的过期时间
                None => {}
            }
        } else {
            self.0.insert(key, Object::new(value, expire));
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
            if let Some(expire_at) = value.expire {
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
            if let Some(expire_at) = value.expire {
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

#[derive(Debug, Clone, Eq)]
pub struct Object<T: ObjValueCODEC> {
    pub value: T,
    pub expire: Option<SystemTime>, // None代表永不过期
}

impl<T: ObjValueCODEC> Object<T> {
    pub fn new(value: T::Input, expire_at: Option<SystemTime>) -> Self {
        Self {
            value: T::encode(value),
            expire: expire_at,
        }
    }

    pub fn value(&self, index: T::Index) -> T::Output {
        self.value.decode(index)
    }

    pub fn set_value(&mut self, value: T::Input) {
        self.value = T::encode(value);
    }
}

impl<T: PartialEq + Eq + ObjValueCODEC> PartialEq for Object<T> {
    fn eq(&self, other: &Self) -> bool {
        if self.expire.is_none() && other.expire.is_none() {
            self.value == other.value
        } else if self.expire.is_some() && other.expire.is_some() {
            let time1 = self.expire.unwrap();
            let time2 = other.expire.unwrap();
            let cmp_res = match time1.duration_since(time2) {
                Ok(duration) => duration.as_secs() < 1,
                Err(e) => e.duration().as_secs() < 1,
            };
            cmp_res && self.value == other.value
        } else {
            false
        }
    }
}

#[derive(Debug)]
pub enum IndexRange {
    Range(std::ops::Range<usize>),
    RangeFrom(std::ops::RangeFrom<usize>),
    RangeTo(std::ops::RangeTo<usize>),
    RangeFull(std::ops::RangeFull),
}

impl From<std::ops::Range<usize>> for IndexRange {
    fn from(range: std::ops::Range<usize>) -> Self {
        Self::Range(range)
    }
}
impl From<std::ops::RangeFrom<usize>> for IndexRange {
    fn from(range: std::ops::RangeFrom<usize>) -> Self {
        Self::RangeFrom(range)
    }
}
impl From<std::ops::RangeTo<usize>> for IndexRange {
    fn from(range: std::ops::RangeTo<usize>) -> Self {
        Self::RangeTo(range)
    }
}
impl From<std::ops::RangeFull> for IndexRange {
    fn from(range: std::ops::RangeFull) -> Self {
        Self::RangeFull(range)
    }
}

pub trait ObjValueCODEC {
    type Index;
    type Input;
    type Output;

    fn encode(input: Self::Input) -> Self;
    fn decode(&self, index: Self::Index) -> Self::Output;
}
