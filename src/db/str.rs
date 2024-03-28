use super::ObjValueCODEC;
use bytes::Bytes;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Str {
    Raw(Bytes),
    Int(i64),
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

impl<'a> IntoIterator for &'a Str {
    type Item = StrIterItem<'a>;
    type IntoIter = StrIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        match self {
            Str::Raw(b) => StrIter::Raw(b.iter()),
            Str::Int(i) => StrIter::Int(std::iter::once(i)),
        }
    }
}

#[derive(Debug)]
pub enum StrIter<'a> {
    Raw(std::slice::Iter<'a, u8>),
    Int(std::iter::Once<&'a i64>),
}

#[derive(Debug)]
pub enum StrIterMut<'a> {
    Raw(std::slice::IterMut<'a, u8>),
    Int(std::iter::Once<&'a mut i64>),
}

#[derive(Debug)]
pub enum StrIntoIter {
    Raw(std::vec::IntoIter<u8>),
    Int(std::iter::Once<i64>),
}

impl<'a> Iterator for StrIter<'a> {
    type Item = StrIterItem<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            StrIter::Raw(iter) => iter.next().map(StrIterItem::Raw),
            StrIter::Int(iter) => iter.next().map(StrIterItem::Int),
        }
    }
}

impl<'a> Iterator for StrIterMut<'a> {
    type Item = StrIterItemMut<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            StrIterMut::Raw(iter) => iter.next().map(StrIterItemMut::Raw),
            StrIterMut::Int(iter) => iter.next().map(StrIterItemMut::Int),
        }
    }
}

impl Iterator for StrIntoIter {
    type Item = StrIntoIterItem;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            StrIntoIter::Raw(iter) => iter.next().map(StrIntoIterItem::Raw),
            StrIntoIter::Int(iter) => iter.next().map(StrIntoIterItem::Int),
        }
    }
}

#[derive(Debug)]
pub enum StrIterItem<'a> {
    Raw(&'a u8),
    Int(&'a i64),
}

#[derive(Debug)]
pub enum StrIterItemMut<'a> {
    Raw(&'a mut u8),
    Int(&'a mut i64),
}

#[derive(Debug)]
pub enum StrIntoIterItem {
    Raw(u8),
    Int(i64),
}

#[derive(Debug)]
pub enum IndexRange {
    Range(std::ops::Range<usize>),
    RangeFrom(std::ops::RangeFrom<usize>),
    RangeTo(std::ops::RangeTo<usize>),
    RangeFull(std::ops::RangeFull),
}

impl From<std::ops::Range<usize>> for IndexRange {
    fn from(range: std::ops::Range<usize>) -> Self {
        Self::Range(range)
    }
}
impl From<std::ops::RangeFrom<usize>> for IndexRange {
    fn from(range: std::ops::RangeFrom<usize>) -> Self {
        Self::RangeFrom(range)
    }
}
impl From<std::ops::RangeTo<usize>> for IndexRange {
    fn from(range: std::ops::RangeTo<usize>) -> Self {
        Self::RangeTo(range)
    }
}
impl From<std::ops::RangeFull> for IndexRange {
    fn from(range: std::ops::RangeFull) -> Self {
        Self::RangeFull(range)
    }
}
