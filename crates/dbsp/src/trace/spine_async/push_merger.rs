use std::marker::PhantomData;

use crate::{trace::Builder, Batch};

pub struct PushMerger<B>
where
    B: Batch,
{
    _phantom: PhantomData<B>,
}

pub enum NoData {
    Pending,
    Eof,
}

pub trait PushCursor<K, V, T, R>
where
    K: ?Sized,
    V: ?Sized,
    R: ?Sized,
{
    fn key(&self) -> Result<&K, NoData>;
    fn val(&self) -> Result<&V, NoData>;
    fn map_times(&mut self, logic: &mut dyn FnMut(&T, &R));
    fn weight(&mut self) -> &R
    where
        T: PartialEq<()>;
    fn step_key(&mut self);
    fn step_val(&mut self);
}

impl<B> PushMerger<B>
where
    B: Batch,
{
    fn work() {}
}
