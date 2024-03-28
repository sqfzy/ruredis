use super::ObjValueCODEC;
use bytes::Bytes;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Str {
    Raw(Bytes),
    Int(i64),
}

impl ObjValueCODEC for Str {
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

    fn decode(&self) -> Self::Output {
        match self {
            Self::Raw(b) => b.clone(),
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

impl<'a> IntoIterator for &'a mut Str {
    type Item = StrIterItemMut<'a>;
    type IntoIter = StrIterMut<'a>;

    fn into_iter(self) -> Self::IntoIter {
        match self {
            Str::Raw(b) => StrIterMut::Raw(b.iter_mut()),
            Str::Int(i) => StrIterMut::Int(std::iter::once(i)),
        }
    }
}

impl IntoIterator for Str {
    type Item = StrIntoIterItem;
    type IntoIter = StrIntoIter;

    fn into_iter(self) -> Self::IntoIter {
        match self {
            Str::Raw(b) => StrIntoIter::Raw(b.into_iter()),
            Str::Int(i) => StrIntoIter::Int(std::iter::once(i)),
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
    Raw(bytes::buf::IntoIter<bytes::Bytes>),
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
