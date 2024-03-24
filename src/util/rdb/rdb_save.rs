use super::*;
use crate::{
    conf::CONFIG,
    db::{self, DbInner, ObjValue, Object},
};
use bytes::{BufMut, Bytes};
use std::{io::Write, time::SystemTime};

// REDIS  kvpair* EOF checksum
// kvpair:
// 1. EXPIRETIME_MS(252), ms(8B), TYPE(0~14), key(string), value(根据类型不同而不同)
// 2. TYPE(0~14), key(string), value(根据类型不同而不同)
// string:
// 1. int8|int16|int32(1B), num
// 2. len, string

pub fn rdb_save(db: DbInner) -> anyhow::Result<()> {
    let mut buf = Vec::with_capacity(1024);
    buf.extend_from_slice(b"REDIS");
    buf.put_u32(1); // 版本号
    buf.put_u8(SELECTDB); // 选择数据库
    buf.put_u32(0); // 选择0号数据库

    // 保存string_kv{kvs_with_expire[ObjValue::Raw_nums expire len key len data ObjValue::Int_nums expire len key int8/int16/int32 data]}
    db.string_kvs.0.iter().for_each(|(k, obj)| {
        encode_string_kv(&mut buf, k.clone(), obj);
    });

    buf.put_u8(EOF); // 结束标志
    let checksum = if CONFIG.rdb.enable_checksum {
        crc::Crc::<u64>::new(&crc::CRC_64_REDIS).checksum(&buf)
    } else {
        0
    };
    buf.put_u64(checksum); // checksum 8 bytes 使用crc64

    // 保存到文件
    let mut file = std::fs::File::create("dump.rdb")?;
    file.write_all(&buf)?;

    Ok(())
}

pub(super) fn encode_string_kv(buf: &mut Vec<u8>, key: Bytes, obj: &Object<db::String>) {
    let expire_at = obj.expire_at;
    if let Some(expire_at) = expire_at {
        if let Ok(expire) = expire_at.duration_since(SystemTime::now()) {
            buf.put_u8(EXPIRETIME_MS);
            buf.put_u64(expire.as_millis() as u64);
        } else {
            // 过期则忽略
            return;
        }
    }
    buf.put_u8(RUREDIS_RDB_TYPE_STRING);
    encode_key(buf, key);
    match &obj.value {
        ObjValue::Int(i) => encode_int(buf, *i as i32),
        ObjValue::Raw(s) => encode_raw(buf, s.clone()),
        _ => unreachable!(
            "Cann't get stringobj value type because stringobj was encoded in wrong type!!!"
        ),
    }
}

pub(super) fn encode_raw(buf: &mut Vec<u8>, value: Bytes) {
    encode_length(buf, value.len() as u32, None);
    buf.extend(value);
}

pub(super) fn encode_int(buf: &mut Vec<u8>, value: i32) {
    // PERF: 还能优化RUREDIS_RDB_ENC_INT8..的占用空间
    if value >= i8::MIN as i32 && value <= i8::MAX as i32 {
        encode_length(buf, 0, Some(RUREDIS_RDB_SPECTIAL_FORMAT_INT8));
        buf.put_i8(value as i8);
    } else if value >= i16::MIN as i32 && value <= i16::MAX as i32 {
        encode_length(buf, 0, Some(RUREDIS_RDB_SPECTIAL_FORMAT_INT16));
        buf.put_i16(value as i16);
    } else {
        encode_length(buf, 0, Some(RUREDIS_RDB_SPECTIAL_FORMAT_INT32));
        buf.put_i32(value);
    }
}

pub(super) fn encode_key(buf: &mut Vec<u8>, key: Bytes) {
    encode_length(buf, key.len() as u32, None);
    buf.extend(key);
}

pub(super) fn encode_length(buf: &mut Vec<u8>, len: u32, special_format: Option<u8>) {
    if let Some(special_format) = special_format {
        // 11000000
        buf.put_u8(0xc0 | special_format);
        return;
    }
    if len < 1 << 6 {
        // 00xxxxxx
        buf.put_u8(len as u8);
    } else if len < 1 << 14 {
        // 01xxxxxx(高6位) xxxxxxxx(低8位)
        buf.put_u8((len >> 8 | 0x40) as u8);
        buf.put_u8(len as u8);
    } else {
        // 10xxxxxx(丢弃) xxxxxxxx xxxxxxxx xxxxxxxx xxxxxxxx
        buf.put_u8(0x80);
        buf.put_u32(len);
    }
}
