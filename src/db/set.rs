use super::{IndexRange, ObjValueCODEC};
use bytes::Bytes;
use std::collections::HashSet;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Set {
    HT(HashSet<Bytes>),
    IntSet,
}

impl ObjValueCODEC for Set {
    type Index = ();
    type Input = Vec<Bytes>;
    type Output = Vec<Bytes>;

    fn encode(input: Self::Input) -> Self {
        let mut ht = HashSet::new();
        for v in input {
            ht.insert(v);
        }
        Self::HT(ht)
    }

    fn decode(&self, _index: Self::Index) -> Self::Output {
        match self {
            Self::HT(ht) => ht.iter().cloned().collect(),
            Self::IntSet => unimplemented!(),
        }
    }
}
