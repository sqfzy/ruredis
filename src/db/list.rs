use super::ObjValueCODEC;
use bytes::Bytes;
use std::collections::VecDeque;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum List {
    LinkedList(VecDeque<Bytes>),
    ZipList,
}

impl ObjValueCODEC for List {
    type Index = std::ops::Range<usize>;
    type Input = Vec<Bytes>;
    type Output = Vec<Bytes>;

    fn encode(input: Self::Input) -> Self {
        Self::LinkedList(input.into())
    }

    fn decode(&self, range: Self::Index) -> Self::Output {
        match self {
            List::LinkedList(list) => list
                .iter()
                .skip(range.start)
                .take(range.end - range.start)
                .cloned()
                .collect(),
            // For ZipList or other cases, you need to provide a suitable iterator that matches the lifetime 'a
            List::ZipList => unimplemented!(),
        }
    }
}
