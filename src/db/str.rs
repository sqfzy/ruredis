use super::{IndexRange, ObjValueCODEC};
use bytes::Bytes;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Str {
    Int(i64),
    Raw(Bytes),
}

impl ObjValueCODEC for Str {
    type Index = IndexRange;
    type Input = Bytes;
    type Output = Bytes;

    fn encode(input: Self::Input) -> Self {
        if let Ok(s) = std::str::from_utf8(&input) {
            if let Ok(i) = s.parse::<i64>() {
                return Str::Int(i);
            }
        }
        Self::Raw(input)
    }

    fn decode(&self, index: Self::Index) -> Self::Output {
        match self {
            Self::Raw(b) => match index {
                IndexRange::Range(range) => b.slice(range),
                IndexRange::RangeFrom(range) => b.slice(range.start..),
                IndexRange::RangeTo(range) => b.slice(..range.end),
                IndexRange::RangeFull(_) => b.clone(),
            },
            Self::Int(i) => Bytes::from(i.to_string()),
        }
    }
}
