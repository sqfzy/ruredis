#![allow(dead_code)]
mod lzf;
mod rdb_load;
mod rdb_save;

pub use rdb_load::rdb_load;
pub use rdb_save::{_rdb_save, rdb_save};

// Opcode
const RDB_OPCODE_AUX: u8 = 0xfa;
const RDB_OPCODE_RESIZEDB: u8 = 0xfb;
const RDB_OPCODE_EXPIRETIME_MS: u8 = 0xfc;
const RDB_OPCODE_EXPIRETIME: u8 = 0xfd;
const RDB_OPCODE_SELECTDB: u8 = 0xfe; // 只允许一个数据库
const RDB_OPCODE_EOF: u8 = 0xff;

// 进行类型编码时，如果是252(EXPIRETIME_MS)，则后面的数据是过期时间，如果是以下值，则后面的数据是该类型的kv编码
const RDB_TYPE_STRING: u8 = 0;
const RDB_TYPE_LIST: u8 = 1;
const RDB_TYPE_SET: u8 = 2;
const RDB_TYPE_ZSET: u8 = 3;
const RDB_TYPE_HASH: u8 = 4;
const RDB_TYPE_ZIPMAP: u8 = 9;
const RDB_TYPE_ZIPLIST: u8 = 10;
const RDB_TYPE_INTSET: u8 = 11;
const RDB_TYPE_ZSET_ZIPLIST: u8 = 12;
const RDB_TYPE_HASH_ZIPLIST: u8 = 13;
const RDB_TYPE_LIST_QUICKLIST: u8 = 14;

// 进行长度编码时，如果开头2bit是11，则后面的数据不是字符串，而是特殊的编码格式
const RDB_SPECTIAL_FORMAT_INT8: u8 = 0;
const RDB_SPECTIAL_FORMAT_INT16: u8 = 1;
const RDB_SPECTIAL_FORMAT_INT32: u8 = 2;
const RDB_SPECTIAL_FORMAT_LZF: u8 = 3;

#[cfg(test)]
mod test_rdb {
    use crate::db::{Db, Object, Str};

    use super::{rdb_load::*, rdb_save::*};
    use bytes::Bytes;
    use std::io::Cursor;
    use std::time::SystemTime;

    #[test]
    fn test_rdb_length_codec() {
        let mut buf = Vec::new();
        encode_length(&mut buf, 0, None);
        assert_eq!(buf, [0]);
        let len = decode_length(&mut Cursor::new(buf.clone())).unwrap();
        if let Length::Len(len) = len {
            assert_eq!(len, 0);
        } else {
            panic!("decode length failed");
        }
        buf.clear();

        encode_length(&mut buf, 63, None);
        assert_eq!(buf, [63]);
        let len = decode_length(&mut Cursor::new(buf.clone())).unwrap();
        if let Length::Len(len) = len {
            assert_eq!(len, 63);
        } else {
            panic!("decode length failed");
        }
        buf.clear();

        encode_length(&mut buf, 64, None);
        assert_eq!(buf, [0x40, 64]);
        let len = decode_length(&mut Cursor::new(buf.clone())).unwrap();
        if let Length::Len(len) = len {
            assert_eq!(len, 64);
        } else {
            panic!("decode length failed");
        }
        buf.clear();

        encode_length(&mut buf, 16383, None);
        assert_eq!(buf, [0x7f, 0xff]);
        let len = decode_length(&mut Cursor::new(buf.clone())).unwrap();
        if let Length::Len(len) = len {
            assert_eq!(len, 16383);
        } else {
            panic!("decode length failed");
        }
        buf.clear();

        encode_length(&mut buf, 16384, None);
        assert_eq!(buf, [0x80, 0, 0, 0x40, 0]);
        let len = decode_length(&mut Cursor::new(buf.clone())).unwrap();
        if let Length::Len(len) = len {
            assert_eq!(len, 16384);
        } else {
            panic!("decode length failed");
        }
        buf.clear();
    }

    #[test]
    fn test_rdb_key_codec() {
        let mut buf = Vec::new();
        encode_key(&mut buf, "key".into());
        assert_eq!(buf, [3, 107, 101, 121]);
        let key = decode_key(&mut Cursor::new(buf.clone())).unwrap();
        assert_eq!(key, "key".as_bytes());
        buf.clear();
    }

    #[test]
    fn test_rdb_str_kv() {
        let mut buf = Vec::new();
        let key: Bytes = "key".into();
        let object = Object {
            value: Str::Raw("hello".into()),
            expire: None,
        };

        encode_str_kv(&mut buf, key.clone(), &object);
        let (k, obj) = decode_kv(&mut Cursor::new(buf.clone())).unwrap();
        assert_eq!(k, key);
        assert_eq!(obj, object);
        buf.clear();

        let expire_at = Some(SystemTime::now() + std::time::Duration::from_secs(10));
        let object = Object {
            value: Str::Raw("hello".into()),
            expire: expire_at,
        };
        encode_str_kv(&mut buf, key.clone(), &object);
        let (k, obj) = decode_kv(&mut Cursor::new(buf.clone())).unwrap();
        assert_eq!(k, key);
        assert_eq!(obj, object);
        buf.clear();

        let object = Object {
            value: Str::Int(10),
            expire: None,
        };
        encode_str_kv(&mut buf, key.clone(), &object);
        let (k, obj) = decode_kv(&mut Cursor::new(buf.clone())).unwrap();
        assert_eq!(k, key);
        assert_eq!(obj, object);
        buf.clear();

        let expire_at = Some(SystemTime::now() + std::time::Duration::from_secs(10));
        let object = Object {
            value: Str::Int(10),
            expire: expire_at,
        };
        encode_str_kv(&mut buf, key.clone(), &object);
        let (k, obj) = decode_kv(&mut Cursor::new(buf.clone())).unwrap();
        assert_eq!(k, key);
        assert_eq!(obj, object);
    }

    #[test]
    fn test_rdb_save_and_load() {
        let db = Db::new();
        let db_inner = db.inner.clone();
        let obj1 = Object {
            value: Str::Raw("hello".into()),
            expire: None,
        };
        let obj2 = Object {
            value: Str::Int(10),
            expire: None,
        };
        let obj3 = Object {
            value: Str::Int(200),
            expire: Some(SystemTime::now() + std::time::Duration::from_secs(10)),
        };
        let obj4 = Object {
            value: Str::Raw("hello".into()),
            expire: Some(SystemTime::now() + std::time::Duration::from_secs(10)),
        };
        db_inner.string_kvs.0.insert("key".into(), obj1.clone());
        db_inner.string_kvs.0.insert("key1".into(), obj2.clone());
        db_inner.string_kvs.0.insert("key2".into(), obj3.clone());
        db_inner.string_kvs.0.insert("key3".into(), obj4.clone());
        _rdb_save(&db, "dump.rdb", true).unwrap();

        let db = Db::new();
        let db_inner = db.inner.clone();
        _rdb_load(&db, "dump.rdb", true).unwrap();
        assert_eq!(
            *db_inner.string_kvs.0.get(&Bytes::from("key")).unwrap(),
            obj1
        );
        assert_eq!(
            *db_inner.string_kvs.0.get(&Bytes::from("key1")).unwrap(),
            obj2
        );
        assert_eq!(
            *db_inner.string_kvs.0.get(&Bytes::from("key2")).unwrap(),
            obj3
        );
        assert_eq!(
            *db_inner.string_kvs.0.get(&Bytes::from("key3")).unwrap(),
            obj4
        );
    }
}
