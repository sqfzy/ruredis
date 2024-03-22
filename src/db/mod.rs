#![allow(dead_code)]

// mod list_db;
mod string_kvs;

use bytes::Bytes;
// use skiplist::SkipList;
use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
    time::SystemTime,
};

use tokio::sync::RwLock;

pub const MAX_KVPAIRS_NUMS: u64 = u64::MAX;

#[derive(Debug, Clone)]
pub struct Db {
    pub inner: Arc<RwLock<DbInner>>,
}

#[derive(Debug, Clone)]
pub struct DbInner {
    pub string_kvs: KvPairs<String>,
}

impl Db {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(DbInner {
                string_kvs: KvPairs::<String>(HashMap::new()),
            })),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct String;
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct List;
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ZSet;
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Hash;
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Set;

#[derive(Debug, Clone)]
pub struct KvPairs<T: PartialEq + Eq>(pub HashMap<Bytes, Object<T>>);

#[derive(Debug, Clone, Eq)]
pub struct Object<T> {
    pub value: ObjValue<T>,
    pub expire_at: Option<SystemTime>, // None代表永不过期
}

impl<T: PartialEq + Eq> PartialEq for Object<T> {
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

// struct ZSetDb(RwLock<HashMap<String, ObjValueCODEC>>);
//
// struct HashDb(RwLock<HashMap<String, ObjValueCODEC>>);
//
// struct SetDb(RwLock<HashMap<String, ObjValueCODEC>>);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObjValue<T> {
    Int(i64),
    Raw(Bytes),
    LinkedList(VecDeque<Bytes>),
    // SkipList(SkipList<Bytes>),
    // TODO:  ZipList()
    HT(HashMap<Bytes, Bytes>),
    IntSet(VecDeque<i64>),
    PhantomData(std::marker::PhantomData<T>),
}

// impl ObjValueCODEC<StringObj> {
//     pub fn decode(value: &ObjValueCODEC<StringObj>) -> Bytes {
//         match value {
//             ObjValueCODEC::Raw(raw) => raw.clone() ,
//             ObjValueCODEC::Int(i) =>  i.to_string().into(),
//             _ => unreachable!("Server error: cann't get stringobj value because stringobj was encoded in wrong type")
//
//         }
//     }
//
//     pub fn encode(value: Bytes) -> ObjValueCODEC<StringObj> {
//         if let Ok(s) = std::str::from_utf8(&value) {
//             if let Ok(i) = s.parse::<i64>() {
//                 return ObjValueCODEC::Int(i);
//             }
//         }
//         ObjValueCODEC::Raw(value)
//     }
// }

// impl ObjValueEncoding {
//     pub fn stringobj_decode_value(&self) -> Option<Bytes> {
//         match self {
//             ObjValueEncoding::Raw(raw) => Some(raw.clone()),
//             ObjValueEncoding::Int(i) => Some(i.to_string().into()),
//             _ => {
//                 tracing::error!("Server error: cann't get stringobj value because stringobj was encoded in wrong type");
//                 None
//             }
//         }
//     }
//
//     pub fn stringobj_encoding_value(value: Bytes) -> ObjValueEncoding {
//         if let Ok(s) = std::str::from_utf8(&value) {
//             if let Ok(i) = s.parse::<i64>() {
//                 return ObjValueEncoding::Int(i);
//             }
//         }
//         ObjValueEncoding::Raw(value)
//     }
// }
