#![allow(dead_code)]

use super::ObjValueCODEC;
use bytes::Bytes;
use std::{
    collections::{HashMap, VecDeque},
    time::{Duration, Instant},
};

#[derive(Debug)]
pub struct ListDb(HashMap<String, ListObj>);

impl ListDb {
    pub fn new() -> Self {
        Self(HashMap::new())
    }

    // 获取Stringobj的值
    pub fn get(&mut self, key: &str) -> Option<Vec<Bytes>> {
        // 找到key对应的obj则继续，否则返回None
        if let Some(obj) = self.0.get(key) {
            // 检查键是否过期了，如果键已经过期则移除并返回None，否则返回Some(value)
            if let Some(expire_at) = obj.expire_at {
                if expire_at < Instant::now() {
                    // if the entry is expired, remove it and return None
                    self.0.remove(key);
                    return None;
                }
            }
            return Some(obj.value());
        };

        None
    }

    pub fn set(
        &mut self,
        key: impl Into<String> + std::hash::Hash + std::cmp::Eq,
        value: Vec<impl Into<Bytes>>,
        expire: Option<Duration>,
    ) {
        let key = key.into();
        let value = value.into_iter().map(|to_bytes| to_bytes.into()).collect();
        // 找到key对应的obj则继续，否则写入该键值对
        if let Some(obj) = self.0.get_mut(&key) {
            // 开始修改旧obj为新的obj
            obj.set_value(value); // 修改旧值为新值
            match expire {
                // 如果expire为0则obj永不过期
                Some(e) if e.as_secs() == 0 => {
                    obj.expire_at = None;
                }
                Some(e) => {
                    obj.expire_at = Some(Instant::now() + e);
                }
                // 如果exipre为None则不修改obj的过期时间
                None => {}
            }
        } else {
            self.0
                .insert(key, ListObj::new(value, expire.map(|e| Instant::now() + e)));
        }
    }

    pub fn del(&mut self, key: &str) {
        self.0.remove(key);
    }

    pub fn check_exist(&mut self, key: &str) -> bool {
        // 找到key对应的obj则继续，否则返回false
        if let Some(value) = self.0.get(key) {
            // 检查键是否过期了，如果键已经过期则移除并返回false，否则返回true
            if let Some(expire_at) = value.expire_at {
                if expire_at < Instant::now() {
                    self.0.remove(key);
                    return false;
                }
            }
            return true;
        }

        false
    }

    pub fn get_ttl(&mut self, key: &str) -> Option<Duration> {
        // 找到key对应的obj则继续，否则返回None
        if let Some(value) = self.0.get(key) {
            // 检查键是否过期了，如果键永不过期则返回Some(0)
            if let Some(expire_at) = value.expire_at {
                // 如果键已经过期则移除并返回None，否则返回Some(可存活时间)
                if expire_at < Instant::now() {
                    // if the entry is expired, remove it and return None
                    self.0.remove(key);
                    return None;
                }
                return Some(expire_at - Instant::now());
            }
            return Some(Duration::from_secs(0));
        }

        None
    }
}

#[derive(Debug)]
struct ListObj {
    value: ObjValueCODEC,       // 可能是LinkedList或Ziplist
    expire_at: Option<Instant>, // None代表永不过期
}

impl ListObj {
    pub fn new(value: Vec<Bytes>, expire_at: Option<Instant>) -> Self {
        Self {
            value: ObjValueCODEC::LinkedList(VecDeque::from(value)),
            expire_at,
        }
    }

    pub fn value(&self) -> Vec<Bytes> {
        match &self.value {
            ObjValueCODEC::LinkedList(list) => list.clone().into(),
            _ => unreachable!(
                "Cann't get listobj value because listobj was encoded in wrong type!!!"
            ),
        }
    }

    pub fn set_value(&mut self, value: Vec<Bytes>) {
        self.value = ObjValueCODEC::LinkedList(value.into());
    }
}

#[cfg(test)]
mod list_db_test {
    use super::*;

    #[test]
    fn test_get_and_set_and_del() {
        let mut db = ListDb::new();

        db.set("key1", vec!["value1"], None);
        db.set("key2", vec!["value2"], None);
        db.set("key3", vec!["value3"], Some(Duration::from_secs(1)));

        // 测试get，获取值
        assert_eq!(Some(vec!["value1".into()]), db.get("key1"));
        assert_eq!(Some(vec!["value2".into()]), db.get("key2"));
        assert_eq!(Some(vec!["value3".into()]), db.get("key3"));

        // 测试set，修改值
        db.set("key1", vec!["value11"], None);
        db.set("key2", vec!["value22"], None);
        assert_eq!(Some(vec!["value11".into()]), db.get("key1"));
        assert_eq!(Some(vec!["value22".into()]), db.get("key2"));

        // 测试del，删除值
        db.del("key1");
        db.del("key2");
        assert_eq!(None, db.get("key1"));
        assert_eq!(None, db.get("key2"));

        // 测试get，等待key3过期
        std::thread::sleep(Duration::from_secs(1)); // waiting for key3 expire
        assert_eq!(None, db.get("key3"));
    }

    #[test]
    fn test_check_exist() {
        let mut db = ListDb::new();

        db.set("key1", vec!["value1"], None);
        db.set("key2", vec!["value2"], None);
        db.set("key3", vec!["value3"], Some(Duration::from_secs(1)));

        assert!(db.check_exist("key1"));
        assert!(db.check_exist("key2"));
        std::thread::sleep(Duration::from_secs(1)); // waiting for key3 expire
        assert!(!db.check_exist("key3"));
    }

    #[test]
    fn test_get_ttl() {
        let mut db = ListDb::new();

        db.set("key1", vec!["value1"], None);
        db.set("key2", vec!["value2"], Some(Duration::from_secs(1)));

        assert_eq!(Duration::ZERO, db.get_ttl("key1").expect("should be Some"));
        let ttl = db.get_ttl("key2").expect("should be Some");
        assert!(Duration::from_secs(1) - ttl < Duration::from_millis(10));
    }
}
