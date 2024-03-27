use super::ObjValueCODEC;
use bytes::Bytes;
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Hash {
    HT(HashMap<Bytes, Bytes>),
    ZipList,
}

impl ObjValueCODEC for Hash {
    type Index = Vec<Bytes>;
    type Input = Vec<(Bytes, Bytes)>;
    type Output = Vec<(Bytes, Bytes)>;

    fn encode(input: Self::Input) -> Self {
        let mut ht = HashMap::new();
        for (k, v) in input {
            ht.insert(k, v);
        }
        Self::HT(ht)
    }

    /// 如果keys为空，则返回所有的键值对
    fn decode(&self, keys: Self::Index) -> Self::Output {
        match self {
            Self::HT(ht) => {
                if keys.is_empty() {
                    ht.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
                } else {
                    keys.iter()
                        .filter_map(|k| ht.get(k).map(|v| (k.clone(), v.clone())))
                        .collect()
                }
            }
            Self::ZipList => unimplemented!(),
        }
    }
}
