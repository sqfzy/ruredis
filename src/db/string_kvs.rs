use super::{KvPairs, ObjValue, Object, String};
use bytes::Bytes;
use std::{
    collections::HashMap,
    time::{Duration, SystemTime},
};

impl KvPairs<String> {
    pub fn new() -> Self {
        Self(HashMap::new())
    }

    pub fn get(&mut self, key: impl Into<Bytes>) -> Option<Bytes> {
        let key = key.into();
        // 找到key对应的obj则继续，否则返回None
        if let Some(obj) = self.0.get(&key) {
            // 检查键是否过期了，如果键已经过期则移除并返回None，否则返回Some(value)
            if let Some(expire_at) = obj.expire_at {
                if expire_at < SystemTime::now() {
                    // if the entry is expired, remove it and return None
                    self.0.remove(&key);
                    return None;
                }
            }
            return Some(obj.value());
        };

        None
    }

    pub fn set(
        &mut self,
        key: impl Into<Bytes>,
        value: impl Into<Bytes>,
        expire: Option<Duration>,
    ) {
        let key = key.into();
        let value = value.into();
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

    pub fn del(&mut self, key: impl Into<Bytes>) {
        self.0.remove(&key.into());
    }

    pub fn check_exist(&mut self, key: impl Into<Bytes>) -> bool {
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

// 字符串对象的值的编码类型可能为Int或Raw
impl Object<String> {
    pub fn new(value: Bytes, expire_at: Option<SystemTime>) -> Self {
        if let Ok(s) = std::str::from_utf8(&value) {
            if let Ok(i) = s.parse::<i64>() {
                return Self {
                    value: ObjValue::Int(i),
                    expire_at,
                };
            }
        }

        Self {
            value: ObjValue::Raw(value),
            expire_at,
        }
    }

    pub fn value_type(&self) -> u8 {
        match &self.value {
            ObjValue::Int(_) => 0,
            ObjValue::Raw(_) => 1,
            _ => unreachable!(
                "Cann't get stringobj value type because stringobj was encoded in wrong type!!!"
            ),
        }
    }

    // 解码并获取字符串对象的值
    pub fn value(&self) -> Bytes {
        match &self.value {
            ObjValue::Raw(raw) => raw.clone(),
            ObjValue::Int(i) => i.to_string().into(),
            _ => unreachable!(
                "Cann't get stringobj value because stringobj was encoded in wrong type!!!"
            ),
        }
    }

    // 编码并保存字符串对象的值
    pub fn set_value(&mut self, value: Bytes) {
        if let Ok(s) = std::str::from_utf8(&value) {
            if let Ok(i) = s.parse::<i64>() {
                self.value = ObjValue::Int(i);
                return;
            }
        }
        self.value = ObjValue::Raw(value);
    }
}

#[cfg(test)]
mod string_db_test {
    use super::*;

    #[test]
    fn test_get_and_set_and_del() {
        let mut db = KvPairs::new();

        db.set("key1", "value1", None);
        db.set("key2", "value2", None);
        db.set("key3", "value3", Some(Duration::from_secs(1)));

        // 测试get，获取值
        assert_eq!(Some("value1".into()), db.get("key1"));
        assert_eq!(Some("value2".into()), db.get("key2"));
        assert_eq!(Some("value3".into()), db.get("key3"));

        // 测试set，修改值
        db.set("key1", "value11", None);
        db.set("key2", "value22", None);
        assert_eq!(Some("value11".into()), db.get("key1"));
        assert_eq!(Some("value22".into()), db.get("key2"));

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
        let mut db = KvPairs::new();

        db.set("key1", "value1", None);
        db.set("key2", "value2", None);
        db.set("key3", "value3", Some(Duration::from_secs(1)));

        assert!(db.check_exist("key1"));
        assert!(db.check_exist("key2"));
        std::thread::sleep(Duration::from_secs(1)); // waiting for key3 expire
        assert!(!db.check_exist("key3"));
    }

    #[test]
    fn test_get_ttl() {
        let mut db = KvPairs::new();

        db.set("key1", "value1", None);
        db.set("key2", "value2", Some(Duration::from_secs(1)));

        assert_eq!(Duration::ZERO, db.get_ttl("key1").expect("should be Some"));
        let ttl = db.get_ttl("key2").expect("should be Some");
        assert!(Duration::from_secs(1) - ttl < Duration::from_millis(10));
    }
}
