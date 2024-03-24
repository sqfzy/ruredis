use super::ReplicationConf;

use crossbeam::atomic::AtomicCell;
use crossbeam::sync::ShardedLock;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;

// 实现Serialize
impl Serialize for ReplicationConf {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("ReplicationConf", 4)?;
        let replicaof = if let Some(replicaof) = &self.replicaof {
            Some(
                replicaof
                    .read()
                    .map_err(|e| serde::ser::Error::custom(e))?
                    .clone(),
            )
        } else {
            None
        };
        state.serialize_field("replicaof", &replicaof)?;
        state.serialize_field("replid", &self.replid)?;
        state.serialize_field("max_replicate", &self.max_replicate)?;
        state.serialize_field("masterauth", &self.masterauth)?;
        state.end()
    }
}

// 实现Deserialize
struct ReplicationConfVisitor;

impl<'de> Deserialize<'de> for ReplicationConf {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        const FIELDS: &'static [&'static str] =
            &["replicaof", "replid", "max_replicate", "masterauth"];

        impl<'de> serde::de::Visitor<'de> for ReplicationConfVisitor {
            type Value = ReplicationConf;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("struct ReplicationConf")
            }

            fn visit_map<V>(self, mut map: V) -> Result<ReplicationConf, V::Error>
            where
                V: serde::de::MapAccess<'de>,
            {
                let mut replicaof = None;
                let mut replid = None;
                let mut max_replicate = None;
                let mut masterauth = None;
                while let Some(key) = map.next_key()? {
                    match key {
                        "replicaof" => {
                            if replicaof.is_some() {
                                return Err(serde::de::Error::duplicate_field("replicaof"));
                            }
                            replicaof = map.next_value::<Option<String>>()?.map(ShardedLock::new);
                        }
                        "replid" => {
                            if replid.is_some() {
                                return Err(serde::de::Error::duplicate_field("replid"));
                            }
                            replid = Some(map.next_value()?);
                        }
                        "max_replicate" => {
                            if max_replicate.is_some() {
                                return Err(serde::de::Error::duplicate_field("max_replicate"));
                            }
                            max_replicate = Some(map.next_value()?);
                        }
                        "masterauth" => {
                            if masterauth.is_some() {
                                return Err(serde::de::Error::duplicate_field("masterauth"));
                            }
                            masterauth = Some(map.next_value()?);
                        }
                        _ => {
                            return Err(serde::de::Error::unknown_field(key, FIELDS));
                        }
                    }
                }
                Ok(ReplicationConf {
                    replicaof,
                    replid: replid.ok_or_else(|| serde::de::Error::missing_field("replid"))?,
                    max_replicate: max_replicate
                        .ok_or_else(|| serde::de::Error::missing_field("max_replicate"))?,
                    masterauth,
                })
            }
        }

        deserializer.deserialize_struct("ReplicationConf", FIELDS, ReplicationConfVisitor)
    }
}
