use std::fmt::{Display, Formatter, Result as FmtResult};

use feldera_storage::file::reader::Cursor as FileCursor;

use crate::{trace::layers::Cursor, DBData, DBWeight};

use super::FileColumnLayer;

/// A cursor for walking through a [`FileColumnLayer`].
#[derive(Clone, Debug)]
pub struct FileColumnLayerCursor<'s, K, R>
where
    K: DBData,
    R: DBWeight,
{
    storage: &'s FileColumnLayer<K, R>,
    item: Option<(K, R)>,
    cursor: FileCursor<'s, K, R>,
}

impl<'s, K, R> FileColumnLayerCursor<'s, K, R>
where
    K: DBData,
    R: DBWeight,
{
    pub fn new(pos: usize, storage: &'s FileColumnLayer<K, R>, bounds: (usize, usize)) -> Self {
        let cursor = storage
            .file
            .rows()
            .subset(bounds.0 as u64..bounds.1 as u64)
            .nth((pos - bounds.0) as u64)
            .unwrap();
        let item = unsafe { cursor.item() };
        Self {
            cursor,
            storage,
            item,
        }
    }

    pub fn current_key(&self) -> &K {
        &self.item.as_ref().unwrap().0
    }

    pub fn current_diff(&self) -> &R {
        &self.item.as_ref().unwrap().1
    }

    pub fn current_item(&self) -> &(K, R) {
        self.item.as_ref().unwrap()
    }

    pub fn take_current_item(&mut self) -> Option<(K, R)> {
        let item = self.item.take();
        self.step();
        item
    }

    pub fn seek_with<P>(&mut self, predicate: P)
    where
        P: Fn(&K) -> bool + Clone,
    {
        todo!()
    }

    pub fn seek_with_reverse<P>(&mut self, predicate: P)
    where
        P: Fn(&K) -> bool + Clone,
    {
        todo!()
    }

    pub fn move_to_row(&mut self, row: usize) {
        self.cursor.move_to_row(row as u64).unwrap();
    }
}

impl<'s, K, R> Cursor<'s> for FileColumnLayerCursor<'s, K, R>
where
    K: DBData,
    R: DBWeight,
{
    type Item<'k> = (&'k K, &'k R)
        where
            Self: 'k;

    type Key = K;

    type ValueCursor = ();

    fn keys(&self) -> usize {
        self.cursor.n_rows() as usize
    }

    fn item(&self) -> Self::Item<'_> {
        (self.current_key(), self.current_diff())
    }

    fn values(&self) {}

    fn step(&mut self) {
        self.cursor.move_next().unwrap();
        self.item = unsafe { self.cursor.item() };
    }

    fn step_reverse(&mut self) {
        self.cursor.move_prev().unwrap();
        self.item = unsafe { self.cursor.item() };
    }

    fn seek(&mut self, key: &Self::Key) {
        unsafe { self.cursor.advance_to_value_or_larger(key) }.unwrap();
        self.item = unsafe { self.cursor.item() };
    }

    fn seek_reverse(&mut self, key: &Self::Key) {
        unsafe { self.cursor.rewind_to_value_or_smaller(key) }.unwrap();
        self.item = unsafe { self.cursor.item() };
    }

    fn valid(&self) -> bool {
        self.cursor.has_value()
    }

    fn rewind(&mut self) {
        self.cursor.move_first().unwrap();
        self.item = unsafe { self.cursor.item() };
    }

    fn fast_forward(&mut self) {
        self.cursor.move_last().unwrap();
        self.item = unsafe { self.cursor.item() };
    }

    fn position(&self) -> usize {
        self.cursor.position() as usize
    }

    fn reposition(&mut self, lower: usize, upper: usize) {
        self.cursor = self
            .storage
            .file
            .rows()
            .subset(lower as u64..upper as u64)
            .first()
            .unwrap();
        self.item = unsafe { self.cursor.item() };
    }
}

impl<'a, K, R> Display for FileColumnLayerCursor<'a, K, R>
where
    K: DBData,
    R: DBWeight,
{
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        let mut cursor: FileColumnLayerCursor<K, R> = self.clone();

        while cursor.valid() {
            let (key, val) = cursor.item();
            writeln!(f, "{key:?} -> {val:?}")?;
            cursor.step();
        }

        Ok(())
    }
}
