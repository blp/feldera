use super::super::Factories;
use crate::dynamic::{DataTrait, DynVec, WeightTrait};
use crate::storage::file::reader::{
    decompress, ColumnSpec, DataBlock, Error, Reader, TreeBlock, TreeNode,
};
use crate::storage::{
    backend::StorageError,
    buffer_cache::{BufferCache, FBuf},
};
use crate::trace::ord::vec::wset_batch::VecWSetBuilder;
use crate::trace::{BatchReaderFactories, Builder, VecWSet, VecWSetFactories};
use smallvec::SmallVec;
use std::{collections::BTreeMap, fmt::Debug, marker::PhantomData, ops::Range, sync::Arc};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

pub struct FetchZSet<'a, 'b, K, A, N, T>
where
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
{
    reader: &'a Reader<T>,
    keys: &'b DynVec<K>,
    cache: Arc<BufferCache>,
    factories: Factories<K, A>,

    receiver: UnboundedReceiver<FetchZSetReadResults>,
    sender: UnboundedSender<FetchZSetReadResults>,

    tmp_key: Box<K>,
    tmp_key2: Box<K>,

    output_blocks: BTreeMap<u64, (Range<usize>, Arc<DataBlock<K, A>>)>,
    pending: usize,

    _phantom: PhantomData<fn(&N)>,
}

impl<'a, 'b, K, A, N, T> FetchZSet<'a, 'b, K, A, N, T>
where
    K: DataTrait + ?Sized,
    A: WeightTrait + ?Sized,
    T: ColumnSpec,
{
    pub fn new(reader: &'a Reader<T>, keys: &'b DynVec<K>) -> Result<Self, Error> {
        debug_assert!(keys.is_sorted_by(&|a, b| a.cmp(b)));
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        let factories = reader.columns[0].factories.factories();
        let tmp_key = factories.key_factory.default_box();
        let tmp_key2 = factories.key_factory.default_box();
        let mut this = Self {
            reader,
            keys,
            cache: (reader.file.cache)(),
            factories,
            sender,
            receiver,
            tmp_key,
            tmp_key2,
            output_blocks: BTreeMap::new(),
            pending: 0,
            _phantom: PhantomData,
        };
        if !keys.is_empty() {
            if let Some(node) = &reader.columns[0].root {
                let mut reads = Vec::new();
                this.try_read(FetchZSetRead::new(0..keys.len(), node.clone()), &mut reads)?;
                this.start_reads(reads);
            }
        }
        Ok(this)
    }

    pub fn is_done(&self) -> bool {
        self.pending == 0
    }

    pub fn results(self, factories: VecWSetFactories<K, A>) -> VecWSet<K, A> {
        debug_assert!(self.is_done());
        let mut builder = VecWSetBuilder::<K, A>::new_builder(&factories);
        let mut tmp_diff = factories.weight_factory().default_box();
        let mut index_stack = SmallVec::<[usize; 10]>::new();
        let mut key_stack = factories.layer_factories.keys.default_box();
        key_stack.reserve_exact(10);
        for (key_range, data_block) in self.output_blocks.into_values() {
            key_stack.clear();
            index_stack.clear();
            for i in key_range {
                let key = &self.keys[i];
                if let Some(child_index) = unsafe {
                    data_block.find_with_cache(
                        &self.factories,
                        &mut *key_stack,
                        &mut index_stack,
                        key,
                    )
                } {
                    unsafe { data_block.aux(&self.factories, child_index, &mut tmp_diff) };
                    builder.push_val_diff(&(), &mut tmp_diff);

                    builder.push_key_mut(key_stack.last_mut().unwrap());
                    index_stack.pop().unwrap();
                    key_stack.truncate(index_stack.len());
                }
            }
        }
        builder.done()
    }

    pub async fn async_results(
        mut self,
        factories: VecWSetFactories<K, A>,
    ) -> Result<VecWSet<K, A>, Error> {
        while !self.is_done() {
            let mut reads = Vec::new();
            let msg = self.receiver.recv().await.unwrap();
            self.process_results(msg, &mut reads)?;
            self.run_(reads)?;
        }
        Ok(self.results(factories))
    }

    pub fn wait(&mut self) -> Result<(), Error> {
        if !self.is_done() {
            let mut reads = Vec::new();
            let msg = self.receiver.blocking_recv().unwrap();
            self.process_results(msg, &mut reads)?;
            self.run_(reads)?;
        }
        Ok(())
    }

    pub fn run(&mut self) -> Result<(), Error> {
        self.run_(Vec::new())
    }

    fn run_(&mut self, mut reads: Vec<FetchZSetRead>) -> Result<(), Error> {
        while let Ok(results) = self.receiver.try_recv() {
            self.process_results(results, &mut reads)?;
        }
        self.start_reads(reads);
        Ok(())
    }

    fn process_results(
        &mut self,
        results: FetchZSetReadResults,
        reads: &mut Vec<FetchZSetRead>,
    ) -> Result<(), Error> {
        self.pending -= 1;
        for (read, result) in results.reads.into_iter().zip(results.results.into_iter()) {
            let raw = result?;
            let tree_block = TreeBlock::from_raw_with_cache(
                decompress(self.reader.file.compression, read.node.location, raw)?,
                &read.node,
                &self.cache,
                self.reader.file_handle().file_id(),
            )
            .unwrap();
            self.process_read(&read.keys, tree_block, reads)?;
        }
        Ok(())
    }

    fn start_reads(&mut self, reads: Vec<FetchZSetRead>) {
        if !reads.is_empty() {
            self.reader.file.file_handle.read_async(
                reads.iter().map(|read| read.node.location).collect(),
                {
                    let sender = self.sender.clone();
                    Box::new(move |results| {
                        let _ = sender.send(FetchZSetReadResults { reads, results });
                    })
                },
            );
            self.pending += 1;
        }
    }

    fn try_read(
        &mut self,
        read: FetchZSetRead,
        reads: &mut Vec<FetchZSetRead>,
    ) -> Result<(), Error> {
        if let Some(tree_block) =
            TreeBlock::from_cache(&read.node, &self.cache, &*self.reader.file.file_handle).unwrap()
        {
            self.process_read(&read.keys, tree_block, reads)?;
        } else {
            reads.push(read);
        }
        Ok(())
    }

    fn process_read(
        &mut self,
        key_range: &Range<usize>,
        tree_block: TreeBlock<K, A>,
        reads: &mut Vec<FetchZSetRead>,
    ) -> Result<(), Error> {
        match tree_block {
            TreeBlock::Data(data_block) => {
                let _existing = self
                    .output_blocks
                    .insert(data_block.first_row, (key_range.clone(), data_block));
                debug_assert!(_existing.is_none());
            }
            TreeBlock::Index(index_block) => {
                let mut child_idx = 0;
                let mut i = key_range.start;
                while i < key_range.end {
                    if let Some((child_index, n_keys)) = unsafe {
                        index_block.find_next(
                            &mut self.tmp_key,
                            &mut self.tmp_key2,
                            self.keys,
                            i..key_range.end,
                            &mut child_idx,
                        )
                    } {
                        let read =
                            FetchZSetRead::new(i..i + n_keys, index_block.get_child(child_index)?);
                        self.try_read(read, reads)?;
                        i += n_keys;
                    } else {
                        i += 1;
                    }
                    if child_idx >= index_block.n_children() {
                        break;
                    }
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
struct FetchZSetRead {
    keys: Range<usize>,
    node: TreeNode,
}

impl FetchZSetRead {
    fn new(keys: Range<usize>, node: TreeNode) -> Self {
        Self { keys, node }
    }
}

struct FetchZSetReadResults {
    reads: Vec<FetchZSetRead>,
    results: Vec<Result<Arc<FBuf>, StorageError>>,
}
