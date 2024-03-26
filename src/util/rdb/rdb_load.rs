use std::{
    io::{Cursor, Read},
    time::SystemTime,
    usize,
};

use super::*;
use crate::{
    conf::CONFIG,
    db::{self, Db, DbInner, ObjValue, Object},
};
use bytes::{Buf, Bytes};
use tokio::sync::RwLockWriteGuard;

pub fn rdb_load(db: &mut RwLockWriteGuard<DbInner>) -> anyhow::Result<()> {
    let mut rdb = std::fs::File::open(&CONFIG.rdb.file_path)?;

    let mut buf = Vec::with_capacity(1024);
    rdb.read_to_end(&mut buf)?;

    let mut cursor = Cursor::new(buf);
    let mut magic = [0; 5];
    cursor.read_exact(&mut magic)?;
    if magic != b"REDIS"[..] {
        anyhow::bail!("Failed to load RDB file: magic string should be RUREDIS, but got {magic:?}");
    }
    let _rdb_version = cursor.get_u32();

    // Database Selector
    if cursor.get_ref()[cursor.position() as usize] == RDB_OPCODE_SELECTDB {
        let _db_num = decode_length(&mut cursor);
        tracing::debug!("Select database: {}", _db_num);
    }

    // Resizedb information
    if cursor.get_ref()[cursor.position() as usize] == RDB_OPCODE_RESIZEDB {
        let _db_size = decode_length(&mut cursor);
        let _expires_size = decode_length(&mut cursor);
        tracing::debug!(
            "Resizedb: db_size: {}, expires_size: {}",
            _db_size,
            _expires_size
        );
    }
    // Auxiliary fields
    while let Some((_key, _value)) = decode_aux(&mut cursor) {
        tracing::debug!("Auxiliary fields: key: {:?}, value: {:?}", _key, _value);
    }

    let len = cursor.get_ref().len();
    while cursor.get_ref()[cursor.position() as usize] != RDB_OPCODE_EOF {
        let (key, obj) = decode_kv(&mut cursor);
        db.string_kvs.0.insert(key, obj);
    }

    cursor.advance(1);
    if CONFIG.rdb.enable_checksum {
        let mut checksum = [0; 8];
        cursor.read_exact(&mut checksum)?;
        let checksum = u64::from_be_bytes(checksum);
        let crc = crc::Crc::<u64>::new(&crc::CRC_64_REDIS);
        if checksum != crc.checksum(&cursor.get_ref()[..len - 8]) {
            anyhow::bail!("Failed to load RDB file: checksum failed");
        }
    }

    Ok(())
}

pub(super) fn decode_kv(cursor: &mut Cursor<Vec<u8>>) -> (Bytes, Object<db::String>) {
    match cursor.get_u8() {
        RDB_OPCODE_EXPIRETIME_MS => {
            let ms = cursor.get_u64();
            let expire_at = Some(SystemTime::now() + std::time::Duration::from_millis(ms));
            match cursor.get_u8() {
                RDB_TYPE_STRING => decode_string(cursor, expire_at),
                _ => unimplemented!(),
            }
        }
        RDB_TYPE_STRING => decode_string(cursor, None),
        other => unimplemented!("Unknow type: {}", other),
    }
}

pub(super) fn decode_string(
    cursor: &mut Cursor<Vec<u8>>,
    expire_at: Option<SystemTime>,
) -> (Bytes, Object<db::String>) {
    let key = decode_key(cursor);
    let pos = cursor.position();
    match cursor.get_ref()[pos as usize] >> 6 {
        0..=2 => {
            let value = decode_raw(cursor);
            (
                key,
                Object {
                    value: ObjValue::Raw(value),
                    expire_at,
                },
            )
        }
        3 => {
            let value = decode_int(cursor);
            (
                key,
                Object {
                    value: ObjValue::Int(value),
                    expire_at,
                },
            )
        }
        _ => unimplemented!(),
    }
}

pub(super) fn decode_raw(cursor: &mut Cursor<Vec<u8>>) -> Bytes {
    let len = decode_length(cursor);
    if len == 0 {
        return Bytes::new();
    }
    let mut raw = vec![0; len];
    let _ = cursor.read_exact(&mut raw);
    raw.into()
}

pub(super) fn decode_int(cursor: &mut Cursor<Vec<u8>>) -> i64 {
    match cursor.get_u8() & 0x3f {
        RDB_SPECTIAL_FORMAT_INT8 => cursor.get_i8() as i64,
        RDB_SPECTIAL_FORMAT_INT16 => cursor.get_i16() as i64,
        RDB_SPECTIAL_FORMAT_INT32 => cursor.get_i32() as i64,
        _ => unreachable!(),
    }
}

pub(super) fn decode_key(cursor: &mut Cursor<Vec<u8>>) -> Bytes {
    let len = decode_length(cursor);
    let mut key = vec![0; len];
    let _ = cursor.read_exact(&mut key);
    key.into()
}

pub(super) fn decode_length(cursor: &mut Cursor<Vec<u8>>) -> usize {
    let ctrl = cursor.get_u8();
    match ctrl >> 6 {
        // 00
        0 => ctrl as usize,
        // 01
        1 => {
            let mut res = 0_usize;
            res |= ((ctrl & 0x3f) as usize) << 8; // ctrl & 0011 1111
            res |= (cursor.get_u8()) as usize;
            res
        }
        // 10
        2 => <u32>::from_be_bytes([
            cursor.get_u8(),
            cursor.get_u8(),
            cursor.get_u8(),
            cursor.get_u8(),
        ]) as usize,
        // 11
        // TODO:
        3 => ctrl as usize & 0x3f,
        _ => unreachable!(),
    }
}

pub(super) fn decode_aux(cursor: &mut Cursor<Vec<u8>>) -> Option<(Bytes, Bytes)> {
    if cursor.get_ref()[cursor.position() as usize] != RDB_OPCODE_AUX {
        println!("aux: {:?}", cursor.get_ref()[cursor.position() as usize]);
        return None;
    }
    cursor.advance(1);
    Some((decode_key(cursor), decode_raw(cursor)))
}
