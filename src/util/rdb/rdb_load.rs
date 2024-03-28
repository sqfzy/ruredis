use std::{
    io::{Cursor, Read},
    time::{SystemTime, UNIX_EPOCH},
    usize,
};

use super::*;
use crate::{
    conf::CONFIG,
    db::{Db, Object, Str},
};
use anyhow::bail;
use bytes::{Buf, Bytes, BytesMut};

pub fn rdb_load(db: &Db) -> anyhow::Result<()> {
    _rdb_load(db, &CONFIG.rdb.file_path, CONFIG.rdb.enable_checksum)
}

pub fn _rdb_load(db: &Db, path: &str, enable_checksum: bool) -> anyhow::Result<()> {
    let mut rdb = std::fs::File::open(path)?;

    let mut buf = Vec::with_capacity(1024);
    rdb.read_to_end(&mut buf)?;

    let mut cursor = Cursor::new(buf);
    let mut magic = [0; 5];
    cursor.read_exact(&mut magic)?;
    if magic != b"REDIS"[..] {
        anyhow::bail!("Failed to load RDB file: magic string should be RUREDIS, but got {magic:?}");
    }
    let _rdb_version = cursor.get_u32();

    // Auxiliary fields
    while let Some((_key, _value)) = decode_aux(&mut cursor)? {
        // println!("Auxiliary fields: key: {:?}, value: {:?}", _key, _value);
    }

    // Database Selector
    if cursor.get_ref()[cursor.position() as usize] == RDB_OPCODE_SELECTDB {
        cursor.advance(1);
        let _db_num = decode_length(&mut cursor)?;
        // println!("Select database: {:?}", _db_num);
    }

    // Resizedb information
    if cursor.get_ref()[cursor.position() as usize] == RDB_OPCODE_RESIZEDB {
        cursor.advance(1);
        let _db_size = decode_length(&mut cursor)?;
        let _expires_size = decode_length(&mut cursor)?;
        // println!(
        //     "Resizedb: db_size: {:?}, expires_size: {:?}",
        //     _db_size, _expires_size
        // );
    }

    let len = cursor.get_ref().len();
    while cursor.get_ref()[cursor.position() as usize] != RDB_OPCODE_EOF {
        let (key, obj) = decode_kv(&mut cursor)?;
        // println!("Key: {:?}, Value: {:?}", key, obj);
        db.inner.string_kvs.0.insert(key, obj);
    }

    cursor.advance(1);
    if enable_checksum {
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

pub(super) fn decode_aux(
    cursor: &mut Cursor<Vec<u8>>,
) -> anyhow::Result<Option<(Bytes, Object<Str>)>> {
    if cursor.get_ref()[cursor.position() as usize] != RDB_OPCODE_AUX {
        return Ok(None);
    }
    cursor.advance(1);
    Ok(Some(decode_str_kv(cursor, None)?))
}

pub(super) fn decode_kv(cursor: &mut Cursor<Vec<u8>>) -> anyhow::Result<(Bytes, Object<Str>)> {
    match cursor.get_u8() {
        RDB_OPCODE_EXPIRETIME_MS => {
            let ms = cursor.get_u64_le();
            let expire_at = Some(UNIX_EPOCH + std::time::Duration::from_millis(ms));
            match cursor.get_u8() {
                RDB_TYPE_STRING => decode_str_kv(cursor, expire_at),
                _ => unimplemented!(),
            }
        }
        RDB_OPCODE_EXPIRETIME => {
            let sec = cursor.get_u32_le();
            let expire_at = Some(UNIX_EPOCH + std::time::Duration::from_secs(sec as u64));
            match cursor.get_u8() {
                RDB_TYPE_STRING => decode_str_kv(cursor, expire_at),
                _ => unimplemented!(),
            }
        }
        RDB_TYPE_STRING => decode_str_kv(cursor, None),
        other => unimplemented!("Unknow type: {}", other),
    }
}

pub(super) fn decode_str_kv(
    cursor: &mut Cursor<Vec<u8>>,
    expire_at: Option<SystemTime>,
) -> anyhow::Result<(Bytes, Object<Str>)> {
    let key = decode_key(cursor)?;
    let str = match decode_length(cursor)? {
        Length::Len(len) => {
            let mut raw = BytesMut::from(vec![0; len].as_slice());
            let _ = cursor.read_exact(&mut raw);
            Object {
                value: Str::Raw(bytes::BytesMut::from(raw)),
                expire: expire_at,
            }
        }
        Length::Int8 => Object {
            value: Str::Int(cursor.get_i8() as i64),
            expire: expire_at,
        },
        Length::Int16 => Object {
            value: Str::Int(cursor.get_i16() as i64),
            expire: expire_at,
        },
        Length::Int32 => Object {
            value: Str::Int(cursor.get_i32() as i64),
            expire: expire_at,
        },
        Length::Lzf => {
            if let (Length::Len(compressed_len), Length::Len(_uncompressed_len)) =
                (decode_length(cursor)?, decode_length(cursor)?)
            {
                let mut buf = vec![0; compressed_len];
                cursor.read_exact(&mut buf)?;
                let raw = lzf::lzf_decompress(&buf);
                Object {
                    value: Str::Raw(bytes::BytesMut::from(raw.as_ref())),
                    expire: expire_at,
                }
            } else {
                bail!("Invalid LZF length")
            }
        }
    };

    Ok((key, str))
}

pub(super) fn decode_key(cursor: &mut Cursor<Vec<u8>>) -> anyhow::Result<Bytes> {
    if let Length::Len(len) = decode_length(cursor)? {
        let mut key = vec![0; len];
        let _ = cursor.read_exact(&mut key);
        Ok(key.into())
    } else {
        bail!("Invalid key length")
    }
}

#[derive(Debug)]
pub enum Length {
    Len(usize),
    Int8,
    Int16,
    Int32,
    Lzf,
}

pub(super) fn decode_length(cursor: &mut Cursor<Vec<u8>>) -> anyhow::Result<Length> {
    let ctrl = cursor.get_u8();
    let len = match ctrl >> 6 {
        // 00
        0 => Length::Len(ctrl as usize),
        // 01
        1 => {
            let mut res = 0_usize;
            res |= ((ctrl & 0x3f) as usize) << 8; // ctrl & 0011 1111
            res |= (cursor.get_u8()) as usize;
            Length::Len(res)
        }
        // 10
        2 => Length::Len(<u32>::from_be_bytes([
            cursor.get_u8(),
            cursor.get_u8(),
            cursor.get_u8(),
            cursor.get_u8(),
        ]) as usize),
        // 11
        3 => match ctrl & 0x3f {
            0 => Length::Int8,
            1 => Length::Int16,
            2 => Length::Int32,
            3 => Length::Lzf,
            _ => bail!("Invalid length encoding"),
        },
        _ => bail!("Invalid length encoding"),
    };

    Ok(len)
}
