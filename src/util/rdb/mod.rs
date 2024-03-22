#![allow(dead_code)]
mod lzf;
mod rdb_load;
mod rdb_save;

pub use rdb_load::rdb_load;
pub use rdb_save::rdb_save;

const EOF: u8 = 0xff;
// const SELECTDB: u8 = 0xfe; // 不启用，因为只有一个db
const EXPIRETIME: u8 = 0xfd;
const EXPIRETIME_MS: u8 = 0xfc;
const RESIZEDB: u8 = 0xfb;
const AUX: u8 = 0xfa;

// 进行类型编码时，如果是252(EXPIRETIME_MS)，则后面的数据是过期时间，如果是以下值，则后面的数据是该类型的kv编码
const RUREDIS_RDB_TYPE_STRING: u8 = 0;
const RUREDIS_RDB_TYPE_LIST: u8 = 1;
const RUREDIS_RDB_TYPE_SET: u8 = 2;
const RUREDIS_RDB_TYPE_ZSET: u8 = 3;
const RUREDIS_RDB_TYPE_HASH: u8 = 4;
const RUREDIS_RDB_TYPE_ZIPMAP: u8 = 9;
const RUREDIS_RDB_TYPE_ZIPLIST: u8 = 10;
const RUREDIS_RDB_TYPE_INTSET: u8 = 11;
const RUREDIS_RDB_TYPE_ZSET_ZIPLIST: u8 = 12;
const RUREDIS_RDB_TYPE_HASH_ZIPLIST: u8 = 13;
const RUREDIS_RDB_TYPE_LIST_QUICKLIST: u8 = 14;

// 进行长度编码时，如果开头2bit是11，则后面的数据不是字符串，而是特殊的编码格式
const RUREDIS_RDB_SPECTIAL_FORMAT_INT8: u8 = 0;
const RUREDIS_RDB_SPECTIAL_FORMAT_INT16: u8 = 1;
const RUREDIS_RDB_SPECTIAL_FORMAT_INT32: u8 = 2;

#[cfg(test)]
mod test_rdb {
    use crate::db::{Db, ObjValue, Object};

    use super::{rdb_load::*, rdb_save::*};
    use bytes::Bytes;
    use std::io::Cursor;
    use std::time::SystemTime;

    #[test]
    fn test_rdb_length_codec() {
        let mut buf = Vec::new();
        encode_length(&mut buf, 0, None);
        assert_eq!(buf, [0]);
        let len = decode_length(&mut Cursor::new(buf.clone()));
        assert_eq!(len, 0);
        buf.clear();

        encode_length(&mut buf, 63, None);
        assert_eq!(buf, [63]);
        let len = decode_length(&mut Cursor::new(buf.clone()));
        assert_eq!(len, 63);
        buf.clear();

        encode_length(&mut buf, 64, None);
        assert_eq!(buf, [0x40, 64]);
        let len = decode_length(&mut Cursor::new(buf.clone()));
        assert_eq!(len, 64);
        buf.clear();

        encode_length(&mut buf, 16383, None);
        assert_eq!(buf, [0x7f, 0xff]);
        let len = decode_length(&mut Cursor::new(buf.clone()));
        assert_eq!(len, 16383);
        buf.clear();

        encode_length(&mut buf, 16384, None);
        assert_eq!(buf, [0x80, 0, 0, 0x40, 0]);
        let len = decode_length(&mut Cursor::new(buf.clone()));
        assert_eq!(len, 16384);
        buf.clear();
    }

    #[test]
    fn test_rdb_key_codec() {
        let mut buf = Vec::new();
        encode_key(&mut buf, "key".into());
        assert_eq!(buf, [3, 107, 101, 121]);
        let key = decode_key(&mut Cursor::new(buf.clone()));
        assert_eq!(key, "key".as_bytes());
        buf.clear();
    }

    #[test]
    fn test_rdb_int_type_codec() {
        let mut buf = Vec::new();

        encode_int(&mut buf, 0);
        assert_eq!(buf, [0xc0, 0]);
        let i = decode_int(&mut Cursor::new(buf.clone()));
        assert_eq!(i, 0);
        buf.clear();

        encode_int(&mut buf, -10);
        assert_eq!(buf, [0xc0, (-10_i8) as u8]);
        let i = decode_int(&mut Cursor::new(buf.clone()));
        assert_eq!(i, -10);
        buf.clear();

        encode_int(&mut buf, 10);
        assert_eq!(buf, [0xc0, 10]);
        let i = decode_int(&mut Cursor::new(buf.clone()));
        assert_eq!(i, 10);
        buf.clear();

        encode_int(&mut buf, 200);
        assert_eq!(buf, [0xc1, 0, 200]);
        let i = decode_int(&mut Cursor::new(buf.clone()));
        assert_eq!(i, 200);
        buf.clear();

        encode_int(&mut buf, 200000);
        assert_eq!(buf, [0xc2, 0, 3, 13, 64]);
        let i = decode_int(&mut Cursor::new(buf.clone()));
        assert_eq!(i, 200000);
    }

    #[test]
    fn test_rdb_raw_type_codec() {
        let mut buf = Vec::new();
        encode_raw(&mut buf, "hello".into());
        assert_eq!(buf, [5, 104, 101, 108, 108, 111]);
        let raw = decode_raw(&mut Cursor::new(buf.clone()));
        assert_eq!(raw, "hello".as_bytes());
    }

    #[test]
    fn test_rdb_string_kv() {
        let mut buf = Vec::new();
        let key: Bytes = "key".into();
        let object = Object {
            value: ObjValue::Raw("hello".into()),
            expire_at: None,
        };

        encode_string_kv(&mut buf, key.clone(), &object);
        assert_eq!(buf, [0, 3, 107, 101, 121, 5, 104, 101, 108, 108, 111]);
        let (k, obj) = decode_kv(&mut Cursor::new(buf.clone()));
        assert_eq!(k, key);
        assert_eq!(obj, object);
        buf.clear();

        let object = Object {
            value: ObjValue::Raw("hello".into()),
            expire_at: Some(SystemTime::now() + std::time::Duration::from_secs(10)),
        };
        encode_string_kv(&mut buf, key.clone(), &object);
        assert_eq!(
            buf,
            [252, 0, 0, 0, 0, 0, 0, 39, 15, 0, 3, 107, 101, 121, 5, 104, 101, 108, 108, 111]
        );
        let (k, obj) = decode_kv(&mut Cursor::new(buf.clone()));
        assert_eq!(k, key);
        assert_eq!(obj, object);
        buf.clear();

        let object = Object {
            value: ObjValue::Int(10),
            expire_at: None,
        };
        encode_string_kv(&mut buf, key.clone(), &object);
        assert_eq!(buf, [0, 3, 107, 101, 121, 192, 10]);
        let (k, obj) = decode_kv(&mut Cursor::new(buf.clone()));
        assert_eq!(k, key);
        assert_eq!(obj, object);
        buf.clear();

        let object = Object {
            value: ObjValue::Int(10),
            expire_at: Some(SystemTime::now() + std::time::Duration::from_secs(10)),
        };
        encode_string_kv(&mut buf, key.clone(), &object);
        assert_eq!(
            buf,
            [252, 0, 0, 0, 0, 0, 0, 39, 15, 0, 3, 107, 101, 121, 192, 10]
        );
        let (k, obj) = decode_kv(&mut Cursor::new(buf.clone()));
        assert_eq!(k, key);
        assert_eq!(obj, object);
    }

    #[test]
    fn test_rdb_save_and_load() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let db = Db::new();
        let mut db_inner = runtime.block_on(db.inner.write());
        let obj1 = Object {
            value: ObjValue::Raw("hello".into()),
            expire_at: None,
        };
        let obj2 = Object {
            value: ObjValue::Int(10),
            expire_at: None,
        };
        let obj3 = Object {
            value: ObjValue::Int(200),
            expire_at: Some(SystemTime::now() + std::time::Duration::from_secs(10)),
        };
        let obj4 = Object {
            value: ObjValue::Raw("hello".into()),
            expire_at: Some(SystemTime::now() + std::time::Duration::from_secs(10)),
        };
        db_inner.string_kvs.0.insert("key".into(), obj1.clone());
        db_inner.string_kvs.0.insert("key1".into(), obj2.clone());
        db_inner.string_kvs.0.insert("key2".into(), obj3.clone());
        db_inner.string_kvs.0.insert("key3".into(), obj4.clone());
        rdb_save(db_inner.clone()).unwrap();

        let db = Db::new();
        let mut db_inner = runtime.block_on(db.inner.write());
        rdb_load(&mut db_inner).unwrap();
        assert_eq!(
            db_inner.string_kvs.0.get(&Bytes::from("key")).unwrap(),
            &obj1
        );
        assert_eq!(
            db_inner.string_kvs.0.get(&Bytes::from("key1")).unwrap(),
            &obj2
        );
        assert_eq!(
            db_inner.string_kvs.0.get(&Bytes::from("key2")).unwrap(),
            &obj3
        );
        assert_eq!(
            db_inner.string_kvs.0.get(&Bytes::from("key3")).unwrap(),
            &obj4
        );
    }
}
