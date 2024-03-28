use super::ObjValueCODEC;
use bytes::{Bytes, BytesMut};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Str {
    Raw(BytesMut),
    Int(i64),
}

impl ObjValueCODEC for Str {
    type Input = BytesMut;
    type Output = Bytes;

    fn encode(input: Self::Input) -> Self {
        if let Ok(s) = std::str::from_utf8(&input) {
            if let Ok(i) = s.parse::<i64>() {
                return Str::Int(i);
            }
        }
        Self::Raw(input)
    }

    fn decode(&self) -> Self::Output {
        match self {
            Self::Raw(b) => b.clone().freeze(),
            Self::Int(i) => Bytes::from(i.to_string()),
        }
    }
}

impl Str {
    pub fn len(&self) -> usize {
        match self {
            Self::Raw(b) => b.len(),
            Self::Int(i) => i.to_string().len(),
        }
    }
}
