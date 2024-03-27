mod str;

use bytes::Bytes;
use dashmap::DashMap;
use std::{collections::VecDeque, sync::Arc, time::SystemTime};

pub use str::Str;

pub const MAX_KVPAIRS_NUMS: u64 = u64::MAX;

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
pub struct KvPairs<T: ObjValueCODEC + PartialEq + Eq>(pub DashMap<Bytes, Object<T>>);

#[derive(Debug, Clone, Eq)]
pub struct Object<T: ObjValueCODEC> {
    pub value: T,
    pub expire_at: Option<SystemTime>, // None代表永不过期
}

impl<T: ObjValueCODEC> Object<T> {
    pub fn new(value: Bytes, expire_at: Option<SystemTime>) -> Self {
        Self {
            value: T::encode(value),
            expire_at,
        }
    }

    pub fn value(&self) -> Bytes {
        self.value.decode()
    }

    pub fn set_value(&mut self, value: Bytes) {
        self.value = T::encode(value);
    }
}

impl<T: PartialEq + Eq + ObjValueCODEC> PartialEq for Object<T> {
    fn eq(&self, other: &Self) -> bool {
        if self.expire_at.is_none() && other.expire_at.is_none() {
            self.value == other.value
        } else if self.expire_at.is_some() && other.expire_at.is_some() {
            let time1 = self.expire_at.unwrap();
            let time2 = other.expire_at.unwrap();
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum List {
    LinkedList(VecDeque<Bytes>),
    // ZipList(Bytes),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ZSet {
    // SkipList(SkipList<Bytes>),
    // ZipList(Bytes),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Hash {
    // HashMap<Bytes, Bytes>,
    // ZipList(Bytes),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Set {
    // HashSet<Bytes>,
    // IntSet
}

pub trait ObjValueCODEC {
    fn encode(bytes: Bytes) -> Self;
    fn decode(&self) -> Bytes;
}
