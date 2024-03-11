mod list_db;
mod string_db;

use bytes::Bytes;
use skiplist::SkipList;
use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};
pub use string_db::StringDb;
use tokio::sync::RwLock;

use self::string_db::StringObj;

#[derive(Debug, Clone)]
pub struct Db {
    pub inner: Arc<RwLock<DbInner>>,
}

#[derive(Debug)]
pub struct DbInner {
    pub string_db: StringDb,
}

impl Db {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(DbInner {
                string_db: StringDb::new(),
            })),
        }
    }
}

// struct ZSetDb(RwLock<HashMap<String, ObjValueCODEC>>);
//
// struct HashDb(RwLock<HashMap<String, ObjValueCODEC>>);
//
// struct SetDb(RwLock<HashMap<String, ObjValueCODEC>>);

#[derive(Debug)]
enum ObjValueCODEC<T = StringObj> {
    Int(i64),
    Raw(Bytes),
    LinkedList(VecDeque<Bytes>),
    SkipList(SkipList<Bytes>),
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
