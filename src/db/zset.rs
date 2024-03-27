use super::{IndexRange, ObjValueCODEC};
use bytes::Bytes;
use skiplist::SkipList;
use std::{collections::HashMap, ops::Range};

#[derive(Debug, PartialEq)]
pub enum ZSet {
    SkipList(SkipList<(Bytes, f64)>),
    ZipList,
}

impl ObjValueCODEC for ZSet {
    type Index = Range<f64>;
    type Input = Vec<(Bytes, f64)>;
    type Output = Vec<(Bytes, f64)>;

    fn encode(mut input: Self::Input) -> Self {
        input.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));

        Self::SkipList(input.into_iter().collect())
    }

    fn decode(&self, score_range: Self::Index) -> Self::Output {
        match self {
            // 在score_range之内的元素
            Self::SkipList(list) => list
                .iter()
                .take_while(|(_, v)| score_range.contains(v))
                .cloned()
                .collect(),
            Self::ZipList => unimplemented!(),
        }
    }
}
