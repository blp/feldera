use std::io;
use std::path::Path;

use feldera_storage::backend::{Backend, StorageBackend};
use rkyv::{Archive, Infallible, Serialize};

use crate::trace::{Deserializable, Serializer};
use crate::{DBData, DBWeight};

pub mod column_layer;
pub mod ordered_layer;
mod utils;

pub trait Persistence {
    type Persist: Archive + Serialize<Serializer> + Deserializable;

    fn data(&self) -> &<<Self as Persistence>::Persist as Archive>::Archived;
    fn read<P>(path: P, backend: Backend) -> io::Result<Self>
    where
        P: AsRef<Path>,
        Self: Sized;
    fn write<P>(backend: Box<dyn StorageBackend>, path: P, cl: &Self::Persist) -> io::Result<()>
    where
        P: AsRef<Path>;
}

#[cfg(test)]
pub(crate) mod tests {
    use crate::trace::layers::column_layer::{ColumnLayer, ColumnLayerBuilder};
    use crate::trace::layers::ordered::OrderedLayer;
    use crate::trace::layers::{Builder, TupleBuilder};
    use crate::{DBData, DBWeight};

    pub(crate) fn mkcl<T: DBData, R: DBWeight>(keys: Vec<T>, diffs: Vec<R>) -> ColumnLayer<T, R> {
        let mut clb = ColumnLayerBuilder::new();
        for (k, d) in keys.into_iter().zip(diffs.into_iter()) {
            clb.push_tuple((k, d));
        }
        clb.done()
    }

    pub(crate) fn mkol<K: DBData, L>(
        keys: Vec<K>,
        offs: Vec<usize>,
        vals: L,
        lower_bound: usize,
    ) -> OrderedLayer<K, L> {
        unsafe { OrderedLayer::from_parts(keys, offs, vals, lower_bound) }
    }
}
