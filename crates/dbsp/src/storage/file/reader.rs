//! Layer file reader.
//!
//! [`Reader`] is the top-level interface for reading layer files.

use super::format::{Compression, FileTrailer};
use super::{AnyFactories, Factories};
use crate::circuit::runtime::ThreadType;
use crate::dynamic::DynVec;
use crate::storage::buffer_cache::AsyncCacheContext;
use crate::storage::buffer_cache::{CacheAccess, CacheEntry};
use crate::storage::file::format::FilterBlock;
use crate::storage::{
    backend::StorageError,
    buffer_cache::{BufferCache, FBuf},
    file::format::{
        DataBlockHeader, FileTrailerColumn, IndexBlockHeader, NodeType, Varint, VERSION_NUMBER,
    },
    file::item::ArchivedItem,
};
use crate::{
    dynamic::{DataTrait, DeserializeDyn, Factory},
    storage::{
        backend::{BlockLocation, FileReader, InvalidBlockLocation, StorageBackend},
        buffer_cache::{AtomicCacheStats, CacheStats},
    },
};
use binrw::{
    io::{self},
    BinRead,
};
use crc32c::crc32c;
use fastbloom::BloomFilter;
use feldera_storage::file::FileId;
use feldera_storage::StoragePath;
use futures::future::Either;
use itertools::Itertools;
use smallvec::{smallvec, SmallVec};
use snap::raw::{decompress_len, Decoder};
use std::collections::{BTreeMap, VecDeque};
use std::mem::replace;
use std::sync::mpsc::{self, channel, Receiver, Sender};
use std::time::Instant;
use std::{
    cmp::{
        max, min,
        Ordering::{self, *},
    },
    fmt::{Debug, Formatter, Result as FmtResult},
    marker::PhantomData,
    mem::size_of,
    ops::{Bound, Range, RangeBounds},
    sync::Arc,
};
use thiserror::Error as ThisError;

/// Any kind of error encountered reading a layer file.
#[derive(ThisError, Clone, Debug)]
pub enum Error {
    /// Errors that indicate a problem with the layer file contents.
    #[error("Corrupt layer file: {0}")]
    Corruption(#[from] CorruptionError),

    /// Errors reading the layer file.
    #[error("Error accessing storage: {0}")]
    Storage(#[from] StorageError),

    /// File has unexpected number of columns.
    #[error("File has {actual} column(s) but should have {expected}.")]
    WrongNumberOfColumns {
        /// Number of columns in file.
        actual: usize,
        /// Expected number of columns in file.
        expected: usize,
    },

    /// The invocation is not supported.
    #[error("The requested operation is not supported.")]
    Unsupported,
}

impl From<io::Error> for Error {
    fn from(source: io::Error) -> Self {
        Error::Storage(StorageError::StdIo(source.kind()))
    }
}

/// Errors that indicate a problem with the layer file contents.
#[derive(ThisError, Clone, Debug)]
pub enum CorruptionError {
    /// File size must be a positive multiple of 512.
    #[error("File size {0} must be a positive multiple of 512")]
    InvalidFileSize(
        /// Actual file size.
        u64,
    ),

    /// Block has invalid checksum.
    #[error(
        "Block ({location}) with magic {magic:?} has invalid checksum {checksum:#x} (expected {computed_checksum:#x})"
    )]
    InvalidChecksum {
        /// Block location
        location: BlockLocation,
        /// Block magic,
        magic: [u8; 4],
        /// Checksum in block.
        checksum: u32,
        /// Checksum that block should have.
        computed_checksum: u32,
    },

    /// Invalid version number in file trailer.
    #[error("File has invalid version {version} (expected {expected_version})")]
    InvalidVersion {
        /// Version in file.
        version: u32,
        /// Expected version ([`VERSION_NUMBER`]).
        expected_version: u32,
    },

    /// [`mod@binrw`] reported a format violation.
    #[error("Binary read/write error reading {block_type} block ({location}): {inner}")]
    Binrw {
        /// Block location.
        location: BlockLocation,

        /// Block type.
        block_type: &'static str,

        /// Underlying error.
        inner: String,
    },

    /// Array overflows block bounds.
    #[error("{count}-element array of {each}-byte elements starting at offset {offset} within block overflows {block_size}-byte block")]
    InvalidArray {
        /// Block size.
        block_size: usize,
        /// Starting byte offset in block.
        offset: usize,
        /// Number of array elements.
        count: usize,
        /// Array element size.
        each: usize,
    },

    /// Strides overflow block bounds.
    #[error("{count} strides of {stride} bytes each starting at offset {start} overflows {block_size}-byte block")]
    InvalidStride {
        /// Block size.
        block_size: usize,
        /// Starting byte offset in block.
        start: usize,
        /// Size of each stride in bytes.
        stride: usize,
        /// Number of strides.
        count: usize,
    },

    /// Index is too deep.
    #[error("Index nesting depth {depth} exceeds maximum ({max_depth}).")]
    TooDeep {
        /// Depth.
        depth: usize,
        /// Maximum depth.
        max_depth: usize,
    },

    /// File has no columns.
    #[error("File has no columns.")]
    NoColumns,

    /// Index block has no children.
    #[error("Index block ({0}) is empty")]
    EmptyIndex(BlockLocation),

    /// Data block contains unexpected rows.
    #[error("Data block ({location}) contains rows {rows:?} but {expected_rows:?} were expected.")]
    DataBlockWrongRows {
        /// Block location.
        location: BlockLocation,
        /// Rows actually in block.
        rows: Range<u64>,
        /// Expected rows in block.
        expected_rows: Range<u64>,
    },

    /// Index block requires unexpected number of rows.
    #[error("Index block ({location}) contains {n_rows} rows but {expected_rows} were expected.")]
    IndexBlockWrongNumberOfRows {
        /// Block location.
        location: BlockLocation,
        /// Number of rows in block.
        n_rows: u64,
        /// Expected number of rows in block.
        expected_rows: u64,
    },

    /// Index row totals aren't strictly increasing.
    #[error("Index block ({location}) has nonmonotonic row totals ({prev} then {next}).")]
    NonmonotonicIndex {
        /// Block location.
        location: BlockLocation,
        /// Previous row total.
        prev: u64,
        /// Next row total (which should be bigger than `prev`).
        next: u64,
    },

    /// Each column must have at least at many rows as the previous, that is, we
    /// should have `prev_n_rows <= this_n_rows`.
    #[error(
        "Column {column} has fewer rows ({this_n_rows}) than the previous column ({prev_n_rows})."
    )]
    DecreasingRowCount {
        /// 0-based column index.
        column: usize,
        /// Number of rows in `column`.
        this_n_rows: u64,
        /// Number of rows in `column - 1`.
        prev_n_rows: u64,
    },

    /// Row should be present but isn't.
    #[error("Unexpectedly missing row {0} in column 1 (or later)")]
    MissingRow(
        /// Row number of missing row.
        u64,
    ),

    /// Invalid child in index block.  At least one of `child_offset` or
    /// `child_size` is invalid.
    #[error("Index block ({location}) has child {index} with invalid offset {child_offset} or size {child_size}.")]
    InvalidChild {
        /// Block location.
        location: BlockLocation,
        /// Index of child within block.
        index: usize,
        /// Child offset.
        child_offset: u64,
        /// Child size.
        child_size: usize,
    },

    /// Invalid node pointer in file trailer block.
    #[error("File trailer column specification has invalid node offset {node_offset} or size {node_size}.")]
    InvalidColumnRoot {
        /// Block offset in bytes.
        node_offset: u64,
        /// Block size in bytes.
        node_size: u32,
    },

    /// Invalid row group in data block.
    #[error("Row group {index} in data block ({location}) has invalid row range {start}..{end}.")]
    InvalidRowGroup {
        /// Block location.
        location: BlockLocation,
        /// Row group index inside block.
        index: usize,
        /// Row number of start of range.
        start: u64,
        /// Row number of end of range (exclusive).
        end: u64,
    },

    /// Bad block type.
    #[error("Block ({0}) is wrong type of block.")]
    BadBlockType(BlockLocation),

    /// Bad compressed length.
    #[error("Compressed block ({location}) claims compressed length {compressed_len} but at most {max_compressed_len} would fit.")]
    BadCompressedLen {
        /// Block location.
        location: BlockLocation,
        /// Compressed length.
        compressed_len: usize,
        /// Maximum compressed length.
        max_compressed_len: usize,
    },

    /// Unexpected decompressed length.
    #[error("Compressed block ({location}) decompressed to {length} bytes instead of the expected {expected_length} bytes")]
    UnexpectedDecompressionLength {
        /// Block location.
        location: BlockLocation,
        /// Actual length.
        length: usize,
        /// Expected length.
        expected_length: usize,
    },

    /// Snappy decompression failed.
    #[error("Compressed block ({location}) failed Snappy decompression: {error}.")]
    Snappy {
        /// Block location.
        location: BlockLocation,
        /// Snappy error.
        error: snap::Error,
    },

    /// Multiple paths to block.
    #[error("Multiple paths to block ({0}).")]
    MultiplePaths(BlockLocation),

    /// Invalid filter block location.
    #[error("Invalid file block location ({0}).")]
    InvalidFilterLocation(InvalidBlockLocation),
}

#[derive(Clone, Debug)]
struct VarintReader {
    varint: Varint,
    start: usize,
    count: usize,
}

impl VarintReader {
    fn new(buf: &FBuf, varint: Varint, start: usize, count: usize) -> Result<Self, Error> {
        let block_size = buf.len();
        match varint
            .len()
            .checked_mul(count)
            .and_then(|len| len.checked_add(start))
        {
            Some(end) if end <= block_size => Ok(Self {
                varint,
                start,
                count,
            }),
            _ => Err(CorruptionError::InvalidArray {
                block_size,
                offset: start,
                count,
                each: varint.len(),
            }
            .into()),
        }
    }
    fn new_opt(
        buf: &FBuf,
        varint: Option<Varint>,
        start: usize,
        count: usize,
    ) -> Result<Option<Self>, Error> {
        varint
            .map(|varint| VarintReader::new(buf, varint, start, count))
            .transpose()
    }
    fn get(&self, src: &FBuf, index: usize) -> u64 {
        debug_assert!(index < self.count);
        self.varint.get(src, self.start + self.varint.len() * index)
    }
}

#[derive(Clone, Debug)]
struct StrideReader {
    start: usize,
    stride: usize,
    count: usize,
}

impl StrideReader {
    fn new(raw: &FBuf, start: usize, stride: usize, count: usize) -> Result<Self, Error> {
        let block_size = raw.len();
        if count > 0 {
            if let Some(last) = stride
                .checked_mul(count - 1)
                .and_then(|len| len.checked_add(start))
            {
                if last < block_size {
                    return Ok(Self {
                        start,
                        stride,
                        count,
                    });
                }
            }
        }
        Err(CorruptionError::InvalidStride {
            block_size,
            start,
            stride,
            count,
        }
        .into())
    }
    fn get(&self, index: usize) -> usize {
        debug_assert!(index < self.count);
        self.start + index * self.stride
    }
}

#[derive(Clone, Debug)]
enum ValueMapReader {
    VarintMap(VarintReader),
    StrideMap(StrideReader),
}

impl ValueMapReader {
    fn new(raw: &FBuf, varint: Option<Varint>, offset: u32, n_values: u32) -> Result<Self, Error> {
        let offset = offset as usize;
        let n_values = n_values as usize;
        if let Some(varint) = varint {
            Ok(Self::VarintMap(VarintReader::new(
                raw, varint, offset, n_values,
            )?))
        } else {
            let stride_map = VarintReader::new(raw, Varint::B32, offset, 2)?;
            let start = stride_map.get(raw, 0) as usize;
            let stride = stride_map.get(raw, 1) as usize;
            Ok(Self::StrideMap(StrideReader::new(
                raw, start, stride, n_values,
            )?))
        }
    }
    fn len(&self) -> usize {
        match self {
            ValueMapReader::VarintMap(ref varint_reader) => varint_reader.count,
            ValueMapReader::StrideMap(ref stride_reader) => stride_reader.count,
        }
    }
    fn get(&self, raw: &FBuf, index: usize) -> usize {
        match self {
            ValueMapReader::VarintMap(ref varint_reader) => varint_reader.get(raw, index) as usize,
            ValueMapReader::StrideMap(ref stride_reader) => stride_reader.get(index),
        }
    }
}

#[derive(Debug)]
pub(super) struct DataBlock<K, A>
where
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
{
    location: BlockLocation,
    raw: Arc<FBuf>,
    value_map: ValueMapReader,
    row_groups: Option<VarintReader>,
    first_row: u64,
    _phantom: PhantomData<fn(&K, &A)>,
}

impl<K, A> CacheEntry for DataBlock<K, A>
where
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
{
    fn cost(&self) -> usize {
        size_of::<Self>() + self.raw.capacity()
    }
}

struct DataBlockReader<'a> {
    file: &'a ImmutableFileRef,
    node: &'a TreeNode,
    start: Instant,
    cache: Arc<BufferCache>,
    access: CacheAccess,
}

impl<'a> DataBlockReader<'a> {
    fn new<K, A>(
        file: &'a ImmutableFileRef,
        node: &'a TreeNode,
    ) -> Result<Either<Self, Arc<DataBlock<K, A>>>, Error>
    where
        K: DataTrait + ?Sized,
        A: DataTrait + ?Sized,
    {
        let start = Instant::now();
        let cache = (file.cache)();
        #[allow(clippy::borrow_deref_ref)]
        let (access, entry) = match cache.get(&*file.file_handle, node.location) {
            Some(entry) => (CacheAccess::Hit, Some(entry)),
            None => (CacheAccess::Miss, None),
        };
        let this = Self {
            file,
            node,
            start,
            cache,
            access,
        };
        match entry {
            Some(entry) => Ok(Either::Right(this.complete(entry)?)),
            None => Ok(Either::Left(this)),
        }
    }
    fn complete<K, A>(self, cache_entry: Arc<dyn CacheEntry>) -> Result<Arc<DataBlock<K, A>>, Error>
    where
        K: DataTrait + ?Sized,
        A: DataTrait + ?Sized,
    {
        let data_block = DataBlock::from_cache_entry(cache_entry, self.node.location)?;
        self.file
            .stats
            .record(self.access, self.start.elapsed(), self.node.location);

        if data_block.rows() != self.node.rows {
            return Err(CorruptionError::DataBlockWrongRows {
                location: self.node.location,
                rows: data_block.rows(),
                expected_rows: self.node.rows.clone(),
            }
            .into());
        }

        Ok(data_block)
    }
}

impl<K, A> DataBlock<K, A>
where
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
{
    pub(super) fn from_raw(
        raw: Arc<FBuf>,
        location: BlockLocation,
        first_row: u64,
    ) -> Result<Self, Error> {
        let header =
            DataBlockHeader::read_le(&mut io::Cursor::new(raw.as_slice())).map_err(|e| {
                Error::Corruption(CorruptionError::Binrw {
                    location,
                    block_type: "data",
                    inner: e.to_string(),
                })
            })?;
        Ok(Self {
            location,
            value_map: ValueMapReader::new(
                &raw,
                header.value_map_varint,
                header.value_map_ofs,
                header.n_values,
            )?,
            row_groups: VarintReader::new_opt(
                &raw,
                header.row_group_varint,
                header.row_groups_ofs as usize,
                header.n_values as usize + 1,
            )?,
            raw,
            first_row,
            _phantom: PhantomData,
        })
    }
    pub(super) fn from_raw_with_cache(
        raw: Arc<FBuf>,
        node: &TreeNode,
        cache: &BufferCache,
        file_id: FileId,
    ) -> Result<Arc<Self>, Error> {
        let block = Arc::new(Self::from_raw(raw, node.location, node.rows.start)?);
        cache.insert(file_id, node.location.offset, block.clone(), false);
        Ok(block)
    }
    fn from_cache_entry(
        cache_entry: Arc<dyn CacheEntry>,
        location: BlockLocation,
    ) -> Result<Arc<Self>, Error> {
        cache_entry
            .downcast()
            .ok_or(Error::Corruption(CorruptionError::BadBlockType(location)))
    }
    fn new_blocking(file: &ImmutableFileRef, node: &TreeNode) -> Result<Arc<Self>, Error> {
        match DataBlockReader::new(file, node)? {
            Either::Left(data_block_reader) => {
                let entry = Self::from_raw_with_cache(
                    file.read_blocking(node.location)?,
                    node,
                    &data_block_reader.cache,
                    file.file_handle.file_id(),
                )?;
                data_block_reader.complete(entry)
            }
            Either::Right(data_block) => Ok(data_block),
        }
    }
    async fn new_async(
        file: &ImmutableFileRef,
        context: &AsyncCacheContext,
        node: TreeNode,
    ) -> Result<Arc<Self>, Error> {
        match DataBlockReader::new(file, &node)? {
            Either::Left(data_block_reader) => {
                let compression = file.compression;
                let entry = context
                    .read(
                        node.location,
                        move |raw| {
                            Ok(Arc::new(Self::from_raw(
                                decompress(compression, node.location, raw)?,
                                node.location,
                                node.rows.start,
                            )?))
                        },
                        false,
                    )
                    .await?;
                data_block_reader.complete(entry)
            }
            Either::Right(data_block) => Ok(data_block),
        }
    }

    fn n_values(&self) -> usize {
        self.value_map.len()
    }
    fn rows(&self) -> Range<u64> {
        self.first_row..(self.first_row + self.n_values() as u64)
    }
    fn row_group(&self, index: usize) -> Result<Range<u64>, Error> {
        let row_groups = self.row_groups.as_ref().unwrap();
        let start = row_groups.get(&self.raw, index);
        let end = row_groups.get(&self.raw, index + 1);
        if start < end {
            Ok(start..end)
        } else {
            Err(CorruptionError::InvalidRowGroup {
                location: self.location,
                index,
                start,
                end,
            }
            .into())
        }
    }
    fn row_group_for_row(&self, row: u64) -> Result<Range<u64>, Error> {
        let index = (row - self.first_row) as usize;
        self.row_group(index)
    }
    unsafe fn archived_item(
        &self,
        factories: &Factories<K, A>,
        index: usize,
    ) -> &dyn ArchivedItem<K, A> {
        factories
            .item_factory
            .archived_value(&self.raw, self.value_map.get(&self.raw, index))
    }
    unsafe fn archived_item_for_row(
        &self,
        factories: &Factories<K, A>,
        row: u64,
    ) -> &dyn ArchivedItem<K, A> {
        let index = (row - self.first_row) as usize;

        self.archived_item(factories, index)
    }

    unsafe fn item(&self, factories: &Factories<K, A>, index: usize, item: (&mut K, &mut A)) {
        let archived_item = self.archived_item(factories, index);
        DeserializeDyn::deserialize(archived_item.fst(), item.0);
        DeserializeDyn::deserialize(archived_item.snd(), item.1);
    }
    unsafe fn item_for_row(&self, factories: &Factories<K, A>, row: u64, item: (&mut K, &mut A)) {
        let index = (row - self.first_row) as usize;
        self.item(factories, index, item)
    }
    unsafe fn key(&self, factories: &Factories<K, A>, index: usize, key: &mut K) {
        let item = self.archived_item(factories, index);
        DeserializeDyn::deserialize(item.fst(), key)
    }
    unsafe fn aux(&self, factories: &Factories<K, A>, index: usize, aux: &mut A) {
        let item = self.archived_item(factories, index);
        DeserializeDyn::deserialize(item.snd(), aux)
    }
    unsafe fn key_for_row(&self, factories: &Factories<K, A>, row: u64, key: &mut K) {
        let index = (row - self.first_row) as usize;
        self.key(factories, index, key)
    }
    unsafe fn aux_for_row(&self, factories: &Factories<K, A>, row: u64, aux: &mut A) {
        let index = (row - self.first_row) as usize;
        self.aux(factories, index, aux)
    }

    unsafe fn find_best_match<C>(
        &self,
        factories: &Factories<K, A>,
        target_rows: &Range<u64>,
        compare: &C,
        bias: Ordering,
    ) -> Option<usize>
    where
        C: Fn(&K) -> Ordering,
    {
        let block_rows = self.rows();
        if block_rows.start >= target_rows.end || block_rows.end <= target_rows.start {
            return None;
        }
        let mut best = None;
        factories.key_factory.with(&mut |key| {
            let mut start = (max(block_rows.start, target_rows.start) - self.first_row) as usize;
            let mut end = (min(block_rows.end, target_rows.end) - self.first_row) as usize;
            while start < end {
                let mid = start.midpoint(end);
                self.key(factories, mid, key);
                let cmp = compare(key);

                match cmp {
                    Less => end = mid,
                    Equal => {
                        best = Some(mid);
                        break;
                    }
                    Greater => start = mid + 1,
                };
                if cmp == bias {
                    best = Some(mid);
                }
            }
        });
        best
    }

    unsafe fn find_exact<C>(
        &self,
        factories: &Factories<K, A>,
        target_rows: &Range<u64>,
        compare: &C,
    ) -> Option<usize>
    where
        C: Fn(&K) -> Ordering,
    {
        self.find_best_match(factories, target_rows, compare, Equal)
    }

    unsafe fn find_next(
        &self,
        factories: &Factories<K, A>,
        tmp: &mut K,
        target: &K,
        start: &mut usize,
    ) -> Option<usize> {
        let mut end = self.n_values();
        while *start < end {
            let mid = start.midpoint(end);
            self.key(factories, mid, tmp);
            match target.cmp(tmp) {
                Less => end = mid,
                Equal => return Some(mid),
                Greater => *start = mid + 1,
            };
        }
        None
    }

    /// Returns the comparison of the key in `row` using `compare`.
    unsafe fn compare_row<C>(&self, factories: &Factories<K, A>, row: u64, compare: &C) -> Ordering
    where
        C: Fn(&K) -> Ordering,
    {
        let mut ordering = Equal;
        factories.key_factory.with(&mut |key| {
            self.key_for_row(factories, row, key);
            ordering = compare(key);
        });
        ordering
    }
}

fn range_compare<T>(range: &Range<T>, target: T) -> Ordering
where
    T: Ord,
{
    if target < range.start {
        Greater
    } else if target >= range.end {
        Less
    } else {
        Equal
    }
}

/// Metadata for reading an index or data node.
///
/// # Naming convention
///
/// In this API, functions that can block on I/O have names that end in
/// `_blocking`. Thus, an `async` function should not call a `_blocking`
/// function.
#[derive(Clone, Debug)]
pub(super) struct TreeNode {
    pub location: BlockLocation,
    pub node_type: NodeType,
    pub rows: Range<u64>,
}

impl TreeNode {
    fn read_blocking<K, A>(&self, file: &ImmutableFileRef) -> Result<TreeBlock<K, A>, Error>
    where
        K: DataTrait + ?Sized,
        A: DataTrait + ?Sized,
    {
        match self.node_type {
            NodeType::Data => Ok(TreeBlock::Data(DataBlock::new_blocking(file, self)?)),
            NodeType::Index => Ok(TreeBlock::Index(IndexBlock::new_blocking(file, self)?)),
        }
    }
    fn read_blocking_multiple<K, A, const N: usize>(
        mut nodes: SmallVec<[TreeNode; N]>,
        file: &ImmutableFileRef,
    ) -> Result<TreeBlock<K, A>, Error>
    where
        K: DataTrait + ?Sized,
        A: DataTrait + ?Sized,
    {
        if nodes.len() > 1 {
            let cache = (file.cache)();
            let mut missing =
                cache.missing(&*file.file_handle, nodes.iter().map(|node| node.location));
            if (missing & 1) == 0 {
                return nodes[0].read_blocking(file);
            }
            if missing != 0 {
                let mut retval = if (missing & 1) == 0 {
                    Some(nodes[0].read_blocking(file)?)
                } else {
                    None
                };
                nodes.retain(|_| {
                    let retain = (missing & 1) != 0;
                    missing >>= 1;
                    retain
                });
                let (sender, receiver) = mpsc::channel();
                file.file_handle.read_async(
                    nodes.iter().map(|node| node.location).collect(),
                    Box::new(move |result| sender.send(result).unwrap()),
                );
                for (result, node) in receiver.recv().unwrap().into_iter().zip(nodes.iter()) {
                    let raw = decompress(file.compression, node.location, result?)?;
                    let entry = TreeBlock::from_raw_with_cache(
                        raw,
                        node,
                        &cache,
                        file.file_handle.file_id(),
                    )?;
                    if retval.is_none() {
                        retval = Some(entry);
                    }
                }
                Ok(retval.unwrap())
            } else {
                nodes[0].read_blocking(file)
            }
        } else {
            nodes[0].read_blocking(file)
        }
    }
    async fn read_async<K, A>(
        self,
        file: &ImmutableFileRef,
        context: &AsyncCacheContext,
    ) -> Result<TreeBlock<K, A>, Error>
    where
        K: DataTrait + ?Sized,
        A: DataTrait + ?Sized,
    {
        match self.node_type {
            NodeType::Data => Ok(TreeBlock::Data(
                DataBlock::new_async(file, context, self).await?,
            )),
            NodeType::Index => Ok(TreeBlock::Index(
                IndexBlock::new_async(file, context, self).await?,
            )),
        }
    }
}

enum TreeBlock<K: DataTrait + ?Sized, A: DataTrait + ?Sized> {
    Data(Arc<DataBlock<K, A>>),
    Index(Arc<IndexBlock<K>>),
}

impl<K, A> TreeBlock<K, A>
where
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
{
    fn from_cache(
        node: &TreeNode,
        cache: &BufferCache,
        file: &dyn FileReader,
    ) -> Result<Option<Self>, Error> {
        match cache.get(file, node.location) {
            Some(cache_entry) => match node.node_type {
                NodeType::Data => Ok(Some(Self::Data(DataBlock::from_cache_entry(
                    cache_entry,
                    node.location,
                )?))),
                NodeType::Index => Ok(Some(Self::Index(IndexBlock::from_cache_entry(
                    cache_entry,
                    node.location,
                )?))),
            },
            None => Ok(None),
        }
    }

    pub(super) fn from_raw_with_cache(
        raw: Arc<FBuf>,
        node: &TreeNode,
        cache: &BufferCache,
        file_id: FileId,
    ) -> Result<Self, Error> {
        match node.node_type {
            NodeType::Data => Ok(Self::Data(DataBlock::from_raw_with_cache(
                raw, node, cache, file_id,
            )?)),
            NodeType::Index => Ok(Self::Index(IndexBlock::from_raw_with_cache(
                raw, node, cache, file_id,
            )?)),
        }
    }

    fn lookup_row(&self, row: u64) -> Result<Option<TreeNode>, Error> {
        self.lookup_row_and_successors::<1>(row)
            .map(|mut vec| vec.pop())
    }

    fn lookup_row_and_successors<const N: usize>(
        &self,
        row: u64,
    ) -> Result<SmallVec<[TreeNode; N]>, Error> {
        match self {
            Self::Data(data_block) => {
                if data_block.rows().contains(&row) {
                    Ok(SmallVec::new())
                } else {
                    Err(CorruptionError::MissingRow(row).into())
                }
            }
            Self::Index(index_block) => Ok(index_block.get_children_by_row(row)?),
        }
    }
}

/// Index block.
///
/// # Naming convention
///
/// In this API, functions that can block on I/O have names that end in
/// `_blocking`. Thus, an `async` function should not call a `_blocking`
/// function.
pub(super) struct IndexBlock<K>
where
    K: DataTrait + ?Sized,
{
    location: BlockLocation,
    raw: Arc<FBuf>,
    child_type: NodeType,
    bounds: VarintReader,
    row_totals: VarintReader,
    child_offsets: VarintReader,
    child_sizes: VarintReader,
    first_row: u64,
    _phantom: PhantomData<K>,
}

impl<K> CacheEntry for IndexBlock<K>
where
    K: DataTrait + ?Sized,
{
    fn cost(&self) -> usize {
        size_of::<Self>() + self.raw.capacity()
    }
}

struct IndexBlockReader<'a> {
    file: &'a ImmutableFileRef,
    node: &'a TreeNode,
    start: Instant,
    cache: Arc<BufferCache>,
    access: CacheAccess,
}

impl<'a> IndexBlockReader<'a> {
    fn new<K>(
        file: &'a ImmutableFileRef,
        node: &'a TreeNode,
    ) -> Result<Either<Self, Arc<IndexBlock<K>>>, Error>
    where
        K: DataTrait + ?Sized,
    {
        let start = Instant::now();
        let cache = (file.cache)();
        #[allow(clippy::borrow_deref_ref)]
        let (access, entry) = match cache.get(&*file.file_handle, node.location) {
            Some(entry) => (CacheAccess::Hit, Some(entry)),
            None => (CacheAccess::Miss, None),
        };
        let this = Self {
            file,
            node,
            start,
            cache,
            access,
        };
        match entry {
            Some(entry) => Ok(Either::Right(this.complete(entry)?)),
            None => Ok(Either::Left(this)),
        }
    }
    fn complete<K>(self, cache_entry: Arc<dyn CacheEntry>) -> Result<Arc<IndexBlock<K>>, Error>
    where
        K: DataTrait + ?Sized,
    {
        let index_block = IndexBlock::from_cache_entry(cache_entry, self.node.location)?;
        if index_block.first_row != self.node.rows.start {
            return Err(Error::Corruption(CorruptionError::MultiplePaths(
                self.node.location,
            )));
        }
        self.file
            .stats
            .record(self.access, self.start.elapsed(), self.node.location);

        let expected_rows = self.node.rows.end - self.node.rows.start;
        let n_rows = index_block
            .row_totals
            .get(&index_block.raw, index_block.row_totals.count - 1);
        if n_rows != expected_rows {
            return Err(CorruptionError::IndexBlockWrongNumberOfRows {
                location: self.node.location,
                n_rows,
                expected_rows,
            }
            .into());
        }
        Ok(index_block)
    }
}

impl<K> IndexBlock<K>
where
    K: DataTrait + ?Sized,
{
    pub(super) fn from_raw(
        raw: Arc<FBuf>,
        location: BlockLocation,
        first_row: u64,
    ) -> Result<Self, Error> {
        let header =
            IndexBlockHeader::read_le(&mut io::Cursor::new(raw.as_slice())).map_err(|e| {
                Error::Corruption(CorruptionError::Binrw {
                    location,
                    block_type: "index",
                    inner: e.to_string(),
                })
            })?;
        if header.n_children == 0 {
            return Err(CorruptionError::EmptyIndex(location).into());
        }

        let row_totals = VarintReader::new(
            &raw,
            header.row_total_varint,
            header.row_totals_offset as usize,
            header.n_children as usize,
        )?;
        for i in 1..header.n_children as usize {
            let prev = row_totals.get(&raw, i - 1);
            let next = row_totals.get(&raw, i);
            if prev >= next {
                return Err(CorruptionError::NonmonotonicIndex {
                    location,
                    prev,
                    next,
                }
                .into());
            }
        }

        Ok(Self {
            location,
            child_type: header.child_type,
            bounds: VarintReader::new(
                &raw,
                header.bound_map_varint,
                header.bound_map_offset as usize,
                header.n_children as usize * 2,
            )?,
            row_totals,
            child_offsets: VarintReader::new(
                &raw,
                header.child_offset_varint,
                header.child_offsets_offset as usize,
                header.n_children as usize,
            )?,
            child_sizes: VarintReader::new(
                &raw,
                header.child_size_varint,
                header.child_sizes_offset as usize,
                header.n_children as usize,
            )?,
            raw,
            first_row,
            _phantom: PhantomData,
        })
    }
    pub(super) fn from_raw_with_cache(
        raw: Arc<FBuf>,
        node: &TreeNode,
        cache: &BufferCache,
        file_id: FileId,
    ) -> Result<Arc<Self>, Error> {
        let block = Arc::new(Self::from_raw(raw, node.location, node.rows.start)?);
        cache.insert(file_id, node.location.offset, block.clone(), true);
        Ok(block)
    }
    fn from_cache_entry(
        cache_entry: Arc<dyn CacheEntry>,
        location: BlockLocation,
    ) -> Result<Arc<Self>, Error> {
        cache_entry
            .downcast()
            .ok_or(Error::Corruption(CorruptionError::BadBlockType(location)))
    }

    fn new_blocking(file: &ImmutableFileRef, node: &TreeNode) -> Result<Arc<Self>, Error> {
        match IndexBlockReader::new(file, node)? {
            Either::Left(index_block_reader) => {
                let entry = Self::from_raw_with_cache(
                    file.read_blocking(node.location)?,
                    &node,
                    &index_block_reader.cache,
                    file.file_handle.file_id(),
                )?;
                index_block_reader.complete(entry)
            }
            Either::Right(index_block) => Ok(index_block),
        }
    }
    async fn new_async(
        file: &ImmutableFileRef,
        context: &AsyncCacheContext,
        node: TreeNode,
    ) -> Result<Arc<Self>, Error> {
        match IndexBlockReader::new(file, &node)? {
            Either::Left(index_block_reader) => {
                let compression = file.compression;
                let entry = context
                    .read(
                        node.location,
                        move |raw| {
                            Ok(Arc::new(Self::from_raw(
                                decompress(compression, node.location, raw)?,
                                node.location,
                                node.rows.start,
                            )?))
                        },
                        true,
                    )
                    .await?;
                index_block_reader.complete(entry)
            }
            Either::Right(index_block) => Ok(index_block),
        }
    }

    /// Returns the range of rows covered by this index block.
    fn rows(&self) -> Range<u64> {
        self.first_row..self.first_row + self.row_totals.get(&self.raw, self.row_totals.count - 1)
    }

    fn get_child_location(&self, index: usize) -> Result<BlockLocation, Error> {
        let offset = self.child_offsets.get(&self.raw, index) << 9;
        let size = self.child_sizes.get(&self.raw, index) << 9;
        BlockLocation::new(offset, size as usize).map_err(|error: InvalidBlockLocation| {
            Error::Corruption(CorruptionError::InvalidChild {
                location: self.location,
                index,
                child_offset: error.offset,
                child_size: error.size,
            })
        })
    }

    fn get_child(&self, index: usize) -> Result<TreeNode, Error> {
        Ok(TreeNode {
            location: self.get_child_location(index)?,
            node_type: self.child_type,
            rows: self.get_rows(index),
        })
    }

    fn get_child_by_row(&self, row: u64) -> Result<TreeNode, Error> {
        self.get_child(self.find_row(row)?)
    }

    fn get_children_by_row<const N: usize>(
        &self,
        row: u64,
    ) -> Result<SmallVec<[TreeNode; N]>, Error> {
        let mut nodes = SmallVec::new();
        for index in self.find_row(row)?..self.n_children() {
            if nodes.len() == nodes.inline_size() {
                break;
            }
            nodes.push(self.get_child(index)?);
        }
        Ok(nodes)
    }

    fn get_rows(&self, index: usize) -> Range<u64> {
        let low = if index == 0 {
            0
        } else {
            self.row_totals.get(&self.raw, index - 1)
        };
        let high = self.row_totals.get(&self.raw, index);
        (self.first_row + low)..(self.first_row + high)
    }

    fn get_row_bound(&self, index: usize) -> u64 {
        if index == 0 {
            0
        } else if index % 2 == 1 {
            self.row_totals.get(&self.raw, index / 2) - 1
        } else {
            self.row_totals.get(&self.raw, index / 2 - 1)
        }
    }

    fn find_row(&self, row: u64) -> Result<usize, Error> {
        let mut indexes = 0..self.n_children();
        while !indexes.is_empty() {
            let mid = indexes.start.midpoint(indexes.end);
            let rows = self.get_rows(mid);
            if row < rows.start {
                indexes.end = mid;
            } else if row >= rows.end {
                indexes.start = mid + 1;
            } else {
                return Ok(mid);
            }
        }
        Err(CorruptionError::MissingRow(row).into())
    }

    unsafe fn get_bound(&self, index: usize, bound: &mut K) {
        let offset = self.bounds.get(&self.raw, index) as usize;
        bound.deserialize_from_bytes(&self.raw, offset)
    }

    unsafe fn find_exact<C>(
        &self,
        key_factory: &dyn Factory<K>,
        target_rows: &Range<u64>,
        compare: &C,
    ) -> Option<usize>
    where
        C: Fn(&K) -> Ordering,
    {
        let mut result = None;
        key_factory.with(&mut |bound| {
            let mut start = 0;
            let mut end = self.n_children();
            result = loop {
                if start >= end {
                    break None;
                }
                let mid = start.midpoint(end);
                let rows = self.get_rows(mid);

                /// Compares `a` to `b` and reports their relationship.
                fn compare_ranges(a: &Range<u64>, b: &Range<u64>) -> Case {
                    if a.end <= b.start {
                        Case::Before
                    } else if b.end <= a.start {
                        Case::After
                    } else if b.end <= a.end {
                        if a.start <= b.start {
                            Case::Contains
                        } else {
                            Case::OverlapEnd
                        }
                    } else if b.start <= a.start {
                        Case::Inside
                    } else {
                        Case::OverlapStart
                    }
                }

                /// The relationship between two ranges `a` and `b`.
                ///
                /// A visual representation of the possibilities:
                ///
                /// ```text
                ///                    [-------b-------]
                ///   [--before--]        [--inside--]      [--after--]
                ///              [---------contains---------]
                ///              [overlap-start]
                ///                           [-overlap-end-]
                /// ```
                enum Case {
                    /// `a` is before `b`, with no overlap.
                    Before,

                    /// `a` is after `b`, with no overlap.
                    After,

                    /// `a` contains all of `b` (and might stick out on either
                    /// side).  This includes the case where `a` and `b` are
                    /// equal.
                    Contains,

                    /// `a` is inside `b`.  (If `a` and `b` are equal, that is
                    /// [Self::Contains] instead.)
                    Inside,

                    /// `a` starts before `b` and overlaps its beginning (but
                    /// not all of it: that would be [Self::Contains]).
                    OverlapStart,

                    /// `a` starts within `b` and overlaps its end (but doesn't
                    /// contain all of `b`: that would be [Self::Contains]).
                    OverlapEnd,
                }

                let cmp = match compare_ranges(target_rows, &rows) {
                    Case::Before => Less,
                    Case::After => Greater,
                    Case::Inside => Equal,
                    Case::Contains => {
                        self.get_bound(mid * 2, bound);
                        match compare(bound) {
                            Greater => {
                                self.get_bound(mid * 2 + 1, bound);
                                match compare(bound) {
                                    Less => Equal,
                                    other => other,
                                }
                            }
                            other => other,
                        }
                    }
                    Case::OverlapStart => {
                        self.get_bound(mid * 2, bound);
                        match compare(bound) {
                            Greater => Equal,
                            other => other,
                        }
                    }
                    Case::OverlapEnd => {
                        self.get_bound(mid * 2 + 1, bound);
                        match compare(bound) {
                            Less => Equal,
                            other => other,
                        }
                    }
                };

                match cmp {
                    Less => end = mid,
                    Greater => start = mid + 1,
                    Equal => break Some(mid),
                }
            };
        });

        result
    }

    unsafe fn find_best_match<C>(
        &self,
        key_factory: &dyn Factory<K>,
        target_rows: &Range<u64>,
        compare: &C,
        bias: Ordering,
    ) -> Option<usize>
    where
        C: Fn(&K) -> Ordering,
    {
        let mut result: Option<usize> = None;

        key_factory.with(&mut |bound| {
            let mut start = 0;
            let mut end = self.n_children() * 2;
            result = None;
            while start < end {
                let mid = start.midpoint(end);
                let row = self.get_row_bound(mid) + self.first_row;
                let cmp = match range_compare(target_rows, row) {
                    Equal => {
                        self.get_bound(mid, bound);
                        let cmp = compare(bound);
                        if cmp == Equal {
                            result = Some(mid / 2);
                            return;
                        }
                        cmp
                    }
                    cmp => cmp,
                };
                if cmp == Less {
                    end = mid
                } else {
                    start = mid + 1
                };
                if bias == cmp {
                    result = Some(mid / 2);
                }
            }
        });

        result
    }

    unsafe fn find_next(
        &self,
        tmp_lower: &mut K,
        tmp_upper: &mut K,
        targets: &DynVec<K>,
        mut target_indexes: Range<usize>,
        start: &mut usize,
    ) -> Option<(usize, usize)> {
        let start_index = target_indexes.next().unwrap();
        let mut end = self.n_children();
        while *start < end {
            let mid = start.midpoint(end);
            self.get_bound(mid * 2, tmp_lower);
            if &targets[start_index] < tmp_lower {
                end = mid;
            } else {
                *start = mid + 1;
                self.get_bound(mid * 2 + 1, tmp_upper);
                if &targets[start_index] <= tmp_upper {
                    let n = 1 + target_indexes
                        .take_while(|i| &targets[*i] <= tmp_upper)
                        .count();
                    return Some((mid, n));
                }
            }
        }
        None
    }

    fn n_children(&self) -> usize {
        self.child_offsets.count
    }

    /// Returns the comparison of the largest bound key` using `compare`.
    unsafe fn compare_max<C>(&self, key_factory: &dyn Factory<K>, compare: &C) -> Ordering
    where
        C: Fn(&K) -> Ordering,
    {
        let mut ordering = Equal;
        key_factory.with(&mut |key| {
            self.get_bound(self.n_children() * 2 - 1, key);
            ordering = compare(key);
        });
        ordering
    }
}

impl<K> Debug for IndexBlock<K>
where
    K: DataTrait + ?Sized + Debug,
{
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        write!(
            f,
            "IndexBlock {{ first_row: {}, child_type: {:?}, children = {{",
            self.first_row, self.child_type
        )?;
        for i in 0..self.n_children() {
            if i > 0 {
                write!(f, ",")?;
            }
            write!(
                f,
                " [{i}] = {{ rows: {:?}, location: {:?} }}",
                self.get_rows(i),
                self.get_child_location(i),
            )?;
        }
        write!(f, " }}")
    }
}

impl CacheEntry for FileTrailer {
    fn cost(&self) -> usize {
        size_of::<FileTrailer>()
    }
}

impl FileTrailer {
    fn from_raw(raw: Arc<FBuf>, location: BlockLocation) -> Result<Self, Error> {
        Ok(
            Self::read_le(&mut io::Cursor::new(raw.as_slice())).map_err(|e| {
                Error::Corruption(CorruptionError::Binrw {
                    location,
                    block_type: "trailer",
                    inner: e.to_string(),
                })
            })?,
        )
    }
    fn new(
        cache: fn() -> Arc<BufferCache>,
        file_handle: &dyn FileReader,
        location: BlockLocation,
        stats: &AtomicCacheStats,
    ) -> Result<Arc<FileTrailer>, Error> {
        let start = Instant::now();
        let cache = cache();
        #[allow(clippy::borrow_deref_ref)]
        let (access, entry) = match cache.get(&*file_handle, location) {
            Some(entry) => {
                let entry = entry
                    .downcast()
                    .ok_or(Error::Corruption(CorruptionError::BadBlockType(location)))?;
                (CacheAccess::Hit, entry)
            }
            None => {
                let block = file_handle.read_block(location)?;
                let entry = Arc::new(Self::from_raw(block, location)?);
                cache.insert(file_handle.file_id(), location.offset, entry.clone(), false);
                (CacheAccess::Miss, entry)
            }
        };
        stats.record(access, start.elapsed(), location);
        Ok(entry)
    }
}

#[derive(Debug)]
struct Column {
    root: Option<TreeNode>,
    factories: AnyFactories,
    n_rows: u64,
}

impl FilterBlock {
    fn new(file_handle: &dyn FileReader, location: BlockLocation) -> Result<Self, Error> {
        let block = file_handle.read_block(location)?;
        Ok(
            Self::read_le(&mut io::Cursor::new(block.as_slice())).map_err(|e| {
                Error::Corruption(CorruptionError::Binrw {
                    location,
                    block_type: "filter",
                    inner: e.to_string(),
                })
            })?,
        )
    }
}

impl Column {
    fn new(factories: &AnyFactories, info: &FileTrailerColumn) -> Result<Self, Error> {
        let FileTrailerColumn {
            node_offset,
            node_size,
            node_type,
            n_rows,
        } = *info;
        let root = if n_rows != 0 {
            let location = match BlockLocation::new(node_offset, node_size as usize) {
                Ok(location) => location,
                Err(_) => {
                    return Err(Error::Corruption(CorruptionError::InvalidColumnRoot {
                        node_offset,
                        node_size,
                    }));
                }
            };
            Some(TreeNode {
                location,
                node_type,
                rows: 0..n_rows,
            })
        } else {
            None
        };
        Ok(Self {
            root,
            n_rows,
            factories: factories.clone(),
        })
    }
}

/// Encapsulates storage and a file handle.
struct ImmutableFileRef {
    path: StoragePath,
    cache: fn() -> Arc<BufferCache>,
    file_handle: Arc<dyn FileReader>,
    compression: Option<Compression>,
    stats: AtomicCacheStats,
}

impl Debug for ImmutableFileRef {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("ImmutableFileRef")
            .field("path", &self.path)
            .finish()
    }
}
impl Drop for ImmutableFileRef {
    fn drop(&mut self) {
        if Arc::strong_count(&self.file_handle) == 1 {
            self.evict();
        }
    }
}

impl ImmutableFileRef {
    fn new(
        cache: fn() -> Arc<BufferCache>,
        file_handle: Arc<dyn FileReader>,
        path: StoragePath,
        compression: Option<Compression>,
        stats: AtomicCacheStats,
    ) -> Self {
        Self {
            cache,
            path,
            file_handle,
            compression,
            stats,
        }
    }

    pub fn evict(&self) {
        (self.cache)().evict(&*self.file_handle);
    }

    pub fn read_blocking(&self, location: BlockLocation) -> Result<Arc<FBuf>, Error> {
        decompress(
            self.compression,
            location,
            self.file_handle.read_block(location)?,
        )
    }
}

fn decompress(
    compression: Option<Compression>,
    location: BlockLocation,
    raw: Arc<FBuf>,
) -> Result<Arc<FBuf>, Error> {
    let raw = if let Some(compression) = compression {
        let compressed_len = u32::from_le_bytes(raw[..4].try_into().unwrap()) as usize;
        let Some(compressed) = raw[4..].get(..compressed_len) else {
            return Err(CorruptionError::BadCompressedLen {
                location,
                compressed_len,
                max_compressed_len: raw.len() - 4,
            }
            .into());
        };
        match compression {
            Compression::Snappy => {
                let decompressed_len = decompress_len(compressed).map_err(|error| {
                    Error::Corruption(CorruptionError::Snappy { location, error })
                })?;
                let mut decompressed = FBuf::with_capacity(decompressed_len);
                decompressed.resize(decompressed_len, 0);
                match Decoder::new().decompress(compressed, decompressed.as_mut_slice()) {
                    Ok(n) if n == decompressed_len => {}
                    Ok(n) => {
                        return Err(CorruptionError::UnexpectedDecompressionLength {
                            location,
                            length: n,
                            expected_length: decompressed_len,
                        }
                        .into())
                    }
                    Err(error) => return Err(CorruptionError::Snappy { location, error }.into()),
                }
                Arc::new(decompressed)
            }
        }
    } else {
        raw
    };
    let computed_checksum = crc32c(&raw[4..]);
    let checksum = u32::from_le_bytes(raw[..4].try_into().unwrap());
    if checksum != computed_checksum {
        return Err(CorruptionError::InvalidChecksum {
            location,
            magic: raw[4..8].try_into().unwrap(),
            checksum,
            computed_checksum,
        }
        .into());
    }
    Ok(raw)
}

/// Layer file column specification.
///
/// A column specification must take the form `K0, A0, N0`, where `(K0, A0)` is
/// the first column's key and auxiliary data types.  If there is only one
/// column, `N0` is `()`; otherwise, it is `(K1, A1, N1)`, where `(K1, A1)` is
/// the second column's key and auxiliary data types.  If there are only two
/// columns, `N1` is `()`, otherwise it is `(K2, A2, N2)`; and so on.  Thus:
///
/// * For one column, `T` is `(K0, A0, ())`.
///
/// * For two columns, `T` is `(K0, A0, (K1, A1, ()))`.
///
/// * For three columns, `T` is `(K0, A0, (K1, A1, (K2, A2, ())))`.
pub trait ColumnSpec {
    /// Returns the number of columns in this `ColumnSpec`.
    fn n_columns() -> usize;
}

impl ColumnSpec for () {
    fn n_columns() -> usize {
        0
    }
}

impl<K, A, N> ColumnSpec for (&'static K, &'static A, N)
where
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
    N: ColumnSpec,
{
    fn n_columns() -> usize {
        1 + N::n_columns()
    }
}

/// Layer file reader.
///
/// `T` in `Reader<T>` must be a [`ColumnSpec`] that specifies the key and
/// auxiliary data types for all of the columns in the file to be read.
///
/// Use [Reader::rows] to read data using blocking I/O and [Reader::rows_async]
/// for an async API.
#[derive(Debug)]
pub struct Reader<T> {
    file: ImmutableFileRef,
    bloom_filter: BloomFilter,
    columns: Vec<Column>,

    /// `fn() -> T` is `Send` and `Sync` regardless of `T`.  See
    /// <https://doc.rust-lang.org/nomicon/phantom-data.html>.
    _phantom: PhantomData<fn() -> T>,
}

impl<T> Reader<T>
where
    T: ColumnSpec,
{
    /// Creates and returns a new `Reader` for `file`.
    pub(crate) fn new(
        factories: &[&AnyFactories],
        path: StoragePath,
        cache: fn() -> Arc<BufferCache>,
        file_handle: Arc<dyn FileReader>,
        bloom_filter: Option<BloomFilter>,
    ) -> Result<Self, Error> {
        let file_size = file_handle.get_size()?;
        if file_size < 512 || (file_size % 512) != 0 {
            return Err(CorruptionError::InvalidFileSize(file_size).into());
        }

        let stats = AtomicCacheStats::default();
        let file_trailer = FileTrailer::new(
            cache,
            &*file_handle,
            BlockLocation::new(file_size - 512, 512).unwrap(),
            &stats,
        )?;
        if file_trailer.version != VERSION_NUMBER {
            return Err(CorruptionError::InvalidVersion {
                version: file_trailer.version,
                expected_version: VERSION_NUMBER,
            }
            .into());
        }

        assert_eq!(factories.len(), file_trailer.columns.len());

        let columns: Vec<_> = file_trailer
            .columns
            .iter()
            .zip(factories.iter())
            .map(|(info, factories)| Column::new(factories, info))
            .collect::<Result<_, _>>()?;
        if columns.is_empty() {
            return Err(CorruptionError::NoColumns.into());
        }
        if columns.len() != T::n_columns() {
            return Err(Error::WrongNumberOfColumns {
                actual: columns.len(),
                expected: T::n_columns(),
            });
        }
        for i in 1..columns.len() {
            let prev_n_rows = columns[i - 1].n_rows;
            let this_n_rows = columns[i].n_rows;
            if this_n_rows < prev_n_rows {
                return Err(CorruptionError::DecreasingRowCount {
                    column: i,
                    prev_n_rows,
                    this_n_rows,
                }
                .into());
            }
        }

        let bloom_filter = match bloom_filter {
            Some(bloom_filter) => bloom_filter,
            None => FilterBlock::new(
                &*file_handle,
                BlockLocation::new(
                    file_trailer.filter_offset,
                    file_trailer.filter_size as usize,
                )
                .map_err(|error: InvalidBlockLocation| {
                    Error::Corruption(CorruptionError::InvalidFilterLocation(error))
                })?,
            )?
            .into(),
        };

        Ok(Self {
            file: ImmutableFileRef::new(cache, file_handle, path, file_trailer.compression, stats),
            columns,
            bloom_filter,
            _phantom: PhantomData,
        })
    }

    /// Marks the file of the reader as being part of a checkpoint.
    pub fn mark_for_checkpoint(&self) {
        self.file.file_handle.mark_for_checkpoint();
    }

    /// Instantiates a reader given an existing path.
    pub fn open(
        factories: &[&AnyFactories],
        cache: fn() -> Arc<BufferCache>,
        storage_backend: &dyn StorageBackend,
        path: &StoragePath,
    ) -> Result<Self, Error> {
        Self::new(
            factories,
            path.clone(),
            cache,
            storage_backend.open(path)?,
            None,
        )
    }

    /// The number of columns in the layer file.
    ///
    /// This is a fixed value for any given `Reader`.
    pub fn n_columns(&self) -> usize {
        T::n_columns()
    }

    /// The number of rows in the given `column`.
    ///
    /// For column 0, this is the number of rows that may be visited with
    /// [`rows`](Self::rows).  In other columns, it is the number of rows that
    /// may be visited in total by calling `next_column()` on each of the rows
    /// in the previous column.
    pub fn n_rows(&self, column: usize) -> u64 {
        self.columns[column].n_rows
    }

    /// Returns the storage path for the underlying object.
    pub fn path(&self) -> StoragePath {
        self.file.path.clone()
    }

    /// Returns the size of the underlying file in bytes.
    pub fn byte_size(&self) -> Result<u64, Error> {
        Ok(self.file.file_handle.get_size()?)
    }

    /// Evict this file from the cache.
    #[cfg(test)]
    pub fn evict(&self) {
        self.file.evict();
    }

    /// Returns the cache statistics for this file.  The statistics are specific
    /// to this file's cache behavior for reads.
    pub fn cache_stats(&self) -> CacheStats {
        self.file.stats.read()
    }

    /// Returns the `FileReader` embedded in this `Reader`.
    pub fn file_handle(&self) -> &dyn FileReader {
        &*self.file.file_handle
    }

    /// Returns a context that can be used for performing overlapped I/O
    /// operations on this reader.
    pub fn new_async_context(&self) -> AsyncCacheContext {
        AsyncCacheContext::new((self.file.cache)().clone(), &*self.file.file_handle)
    }
}

impl<K, A, N> Reader<(&'static K, &'static A, N)>
where
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
    (&'static K, &'static A, N): ColumnSpec,
{
    /// Asks the bloom filter of the reader if we have the key.
    pub fn maybe_contains_key(&self, key: &K) -> bool {
        self.bloom_filter
            .contains(&key.default_hash().to_le_bytes())
    }

    /// Returns a [`RowGroup`] for all of the rows in column 0.
    pub fn rows(&self) -> RowGroup<K, A, N, (&'static K, &'static A, N)> {
        RowGroup::new(self, 0, 0..self.columns[0].n_rows)
    }

    /// Returns a [`BulkRows`] for column 0.
    pub fn bulk_rows(&self) -> Result<BulkRows<K, A, N, (&'static K, &'static A, N)>, Error> {
        BulkRows::new(self, 0)
    }

    pub fn multifetch<'a, 'b>(
        &'a self,
        keys: &'b DynVec<K>,
    ) -> Result<Multifetch0<'a, 'b, K, A, N, (&'static K, &'static A, N)>, Error> {
        Multifetch0::new(self, keys)
    }

    /// Returns an [AsyncRowGroup] for all of the rows in column 0.
    ///
    /// Use [Reader::new_async_context] to create `context`.
    pub fn rows_async<'a>(
        &'a self,
        context: &'a AsyncCacheContext,
    ) -> AsyncRowGroup<'a, K, A, N, (&'static K, &'static A, N)> {
        AsyncRowGroup {
            row_group: self.rows(),
            context,
        }
    }
}

/// A sorted, indexed group of unique rows in a [`Reader`].
///
/// Column 0 in a layer file has a single [`RowGroup`] that includes all of the
/// rows in column 0.  This row group, obtained with [`Reader::rows`], is empty
/// if the layer file is empty.
///
/// Row groups for other columns are obtained by first obtaining a [`Cursor`]
/// for a row in column 0 and calling [`Cursor::next_column`] to get its row
/// group in column 1, and then repeating as many times as necessary to get to
/// the desired column.
pub struct RowGroup<'a, K, A, N, T>
where
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
{
    reader: &'a Reader<T>,
    factories: Factories<K, A>,
    column: usize,
    rows: Range<u64>,
    _phantom: PhantomData<fn(&K, &A, N)>,
}

impl<K, A, N, T> Clone for RowGroup<'_, K, A, N, T>
where
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
{
    fn clone(&self) -> Self {
        Self {
            reader: self.reader,
            factories: self.factories.clone(),
            column: self.column,
            rows: self.rows.clone(),
            _phantom: PhantomData,
        }
    }
}

impl<K, A, N, T> Debug for RowGroup<'_, K, A, N, T>
where
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
{
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        write!(f, "RowGroup(column={}, rows={:?})", self.column, self.rows)
    }
}

impl<'a, K, A, N, T> RowGroup<'a, K, A, N, T>
where
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
{
    fn new(reader: &'a Reader<T>, column: usize, rows: Range<u64>) -> Self {
        Self {
            reader,
            factories: reader.columns[column].factories.factories(),
            column,
            rows,
            _phantom: PhantomData,
        }
    }

    fn root_node(&self) -> Option<TreeNode> {
        self.reader.columns[self.column].root.clone()
    }

    fn cursor(&self, position: Position<K, A>) -> Cursor<'a, K, A, N, T> {
        Cursor {
            row_group: self.clone(),
            position,
        }
    }

    /// Returns `true` if the row group contains no rows.
    ///
    /// The row group for column 0 is empty if and only if the layer file is
    /// empty.  A row group obtained from [`Cursor::next_column`] is never
    /// empty.
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Returns the number of rows in the row group.
    pub fn len(&self) -> u64 {
        self.rows.end - self.rows.start
    }

    /// Returns a cursor for just before the row group.
    pub fn before(&self) -> Cursor<'a, K, A, N, T> {
        self.cursor(Position::Before)
    }

    /// Return a cursor for just after the row group.
    pub fn after(&self) -> Cursor<'a, K, A, N, T> {
        self.cursor(Position::After { hint: None })
    }

    /// Return a cursor for the first row in the row group, or just after the
    /// row group if it is empty.
    pub fn first(&self) -> Result<Cursor<'a, K, A, N, T>, Error> {
        let position = if self.is_empty() {
            Position::After { hint: None }
        } else {
            Position::for_row_blocking(self, self.rows.start)?
        };
        Ok(self.cursor(position))
    }

    /// Return a cursor for the first row in the row group, or just after the
    /// row group if it is empty, using `hint` as an internal starting point for
    /// searching the B-tree. For best performance, use a `hint` near the first
    /// row in the row group (but the result will be correct regardless of
    /// `hint`).
    pub fn first_with_hint(
        &self,
        hint: &Cursor<'a, K, A, N, T>,
    ) -> Result<Cursor<'a, K, A, N, T>, Error>
    where
        T: ColumnSpec,
    {
        let position = if self.is_empty() {
            Position::After { hint: None }
        } else {
            Position::for_row_from_hint_blocking(self, &hint.position, self.rows.start)?
        };
        Ok(self.cursor(position))
    }

    /// Return a cursor for the last row in the row group, or just after the
    /// row group if it is empty.
    pub fn last(&self) -> Result<Cursor<'a, K, A, N, T>, Error> {
        let position = if self.is_empty() {
            Position::After { hint: None }
        } else {
            Position::for_row_blocking(self, self.rows.end - 1)?
        };
        Ok(self.cursor(position))
    }

    /// If `row` is less than the number of rows in the row group, returns a
    /// cursor for that row; otherwise, returns a cursor for just after the row
    /// group.
    pub fn nth(&self, row: u64) -> Result<Cursor<'a, K, A, N, T>, Error> {
        let position = if row < self.len() {
            Position::for_row_blocking(self, self.rows.start + row)?
        } else {
            Position::After { hint: None }
        };
        Ok(self.cursor(position))
    }

    /// Returns a row group for a subset of the rows in this one.
    pub fn subset<B>(&self, range: B) -> Self
    where
        B: RangeBounds<u64>,
    {
        let start = match range.start_bound() {
            Bound::Included(&index) => index,
            Bound::Excluded(&index) => index + 1,
            Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            Bound::Included(&index) => index + 1,
            Bound::Excluded(&index) => index,
            Bound::Unbounded => self.len(),
        };
        let subset = start..end;

        let start = self.rows.start + subset.start;
        let end = start + (subset.end - subset.start);
        Self {
            rows: start..end,
            factories: self.factories.clone(),
            ..*self
        }
    }
}

/// Trait for equality comparisons that might fail due to an I/O error.
pub trait FallibleEq {
    /// Compares `self` to `other` and returns whether they are equal, with
    /// the possibility of failure due to an I/O error.
    fn equals(&self, other: &Self) -> Result<bool, Error>;
}

impl<K, A, N> FallibleEq for Reader<(&'static K, &'static A, N)>
where
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
    (&'static K, &'static A, N): ColumnSpec,
    for<'a> RowGroup<'a, K, A, N, (&'static K, &'static A, N)>: FallibleEq,
{
    fn equals(&self, other: &Self) -> Result<bool, Error> {
        self.rows().equals(&other.rows())
    }
}

impl<'a, K, A, T> FallibleEq for RowGroup<'a, K, A, (), T>
where
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
    T: ColumnSpec,
{
    fn equals(&self, other: &Self) -> Result<bool, Error> {
        if self.len() != other.len() {
            return Ok(false);
        }
        let mut sc: Cursor<'a, _, _, _, _> = self.clone().first()?;
        let mut oc: Cursor<'a, _, _, _, _> = other.clone().first()?;

        while sc.has_value() {
            if unsafe { sc.archived_item() != oc.archived_item() } {
                return Ok(false);
            }
            sc.move_next()?;
            oc.move_next()?;
        }
        Ok(true)
    }
}

impl<'a, K, A, NK, NA, NN, T> FallibleEq for RowGroup<'a, K, A, (&'static NK, &'static NA, NN), T>
where
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
    NK: DataTrait + ?Sized,
    NA: DataTrait + ?Sized,
    T: ColumnSpec,
    RowGroup<'a, NK, NA, NN, T>: FallibleEq,
{
    fn equals(&self, other: &Self) -> Result<bool, Error> {
        if self.len() != other.len() {
            return Ok(false);
        }
        let mut sc = self.clone().first()?;
        let mut oc = other.clone().first()?;
        while sc.has_value() {
            if unsafe { sc.archived_item() != oc.archived_item() } {
                return Ok(false);
            }
            if !sc.next_column()?.equals(&oc.next_column()?)? {
                return Ok(false);
            }
            sc.move_next()?;
            oc.move_next()?;
        }
        Ok(true)
    }
}

/// A cursor for a layer file.
///
/// A cursor traverses a [`RowGroup`].  It can be positioned on a particular row
/// or before or after the row group.  (If the row group is empty, then the
/// cursor can only be before or after the row group.)
pub struct Cursor<'a, K, A, N, T>
where
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
{
    row_group: RowGroup<'a, K, A, N, T>,
    position: Position<K, A>,
}

impl<K, A, N, T> Clone for Cursor<'_, K, A, N, T>
where
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
{
    fn clone(&self) -> Self {
        Self {
            row_group: self.row_group.clone(),
            position: self.position.clone(),
        }
    }
}

impl<K, A, N, T> Debug for Cursor<'_, K, A, N, T>
where
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
{
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        write!(f, "Cursor({:?}, {:?})", self.row_group, self.position)
    }
}

impl<'a, K, A, N, T> Cursor<'a, K, A, N, T>
where
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
    T: ColumnSpec,
{
    fn rows(&self) -> &Range<u64> {
        &self.row_group.rows
    }

    /// Moves to the next row in the row group.  If the cursor was previously
    /// before the row group, it moves to the first row; if it was on the last
    /// row, it moves after the row group.
    pub fn move_next(&mut self) -> Result<(), Error> {
        self.position
            .move_to(&self.row_group, self.position.row().next(self.rows()))
    }

    /// Moves to the previous row in the row group.  If the cursor was
    /// previously after the row group, it moves to the last row; if it was
    /// on the first row, it moves before the row group.
    pub fn move_prev(&mut self) -> Result<(), Error> {
        self.position
            .move_to(&self.row_group, self.position.row().prev(self.rows()))
    }

    /// Moves to the first row in the row group.  If the row group is empty,
    /// this has no effect.
    pub fn move_first(&mut self) -> Result<(), Error> {
        self.position
            .move_to(&self.row_group, Row::first(&self.row_group.rows))
    }

    /// Moves to the last row in the row group.  If the row group is empty,
    /// this has no effect.
    pub fn move_last(&mut self) -> Result<(), Error> {
        self.position
            .move_to(&self.row_group, Row::last(self.rows()))
    }

    /// Moves to row `row`.  If `row >= self.len()`, moves after the row group.
    pub fn move_to_row(&mut self, row: u64) -> Result<(), Error> {
        self.position
            .move_to(&self.row_group, Row::nth(self.rows(), row))
    }

    /// Returns the key in the current row, or `None` if the cursor is before or
    /// after the row group.
    ///
    /// # Safety
    ///
    /// Unsafe because `rkyv` deserialization is unsafe.
    pub unsafe fn key(&self, key: &'a mut K) -> Option<&'a mut K> {
        self.position.key(&self.row_group.factories, key)
    }

    /// Returns the auxiliary data in the current row, or `None` if the cursor
    /// is before or after the row group.
    ///
    /// # Safety
    ///
    /// Unsafe because `rkyv` deserialization is unsafe.
    pub unsafe fn aux<'b>(&self, aux: &'b mut A) -> Option<&'b mut A> {
        self.position.aux(&self.row_group.factories, aux)
    }

    /// Returns the key and auxiliary data in the current row, or `None` if the
    /// cursor is before or after the row group.
    ///
    /// # Safety
    ///
    /// Unsafe because `rkyv` deserialization is unsafe.
    pub unsafe fn item<'b>(&self, item: (&'b mut K, &'b mut A)) -> Option<(&'b mut K, &'b mut A)> {
        self.position.item(&self.row_group.factories, item)
    }

    /// Returns archived representation of the key and auxiliary data in the
    /// current row, or `None` if the cursor is before or after the row
    /// group.
    ///
    /// # Safety
    ///
    /// Unsafe because `rkyv` deserialization is unsafe.
    pub unsafe fn archived_item(&self) -> Option<&dyn ArchivedItem<'_, K, A>> {
        self.position.archived_item(&self.row_group.factories)
    }

    /// Returns `true` if the cursor is on a row.
    pub fn has_value(&self) -> bool {
        self.position.has_value()
    }

    /// Returns the number of rows in the cursor's row group.
    pub fn len(&self) -> u64 {
        self.row_group.len()
    }

    /// Returns true if this cursor's row group has no rows.
    pub fn is_empty(&self) -> bool {
        self.row_group.is_empty()
    }

    /// Returns the row number of the current row, as an absolute number
    /// relative to the top of the column rather than the top of the row group.
    /// If the cursor is before the row group or on the first row, returns the
    /// row number of the first row in the row group; if the cursor is after the
    /// row group, returns the row number of the row just after the row group.
    pub fn absolute_position(&self) -> u64 {
        self.position.absolute_position(&self.row_group)
    }

    /// Returns the number of times [`move_next`](Self::move_next) may be called
    /// before the cursor is after the row group.
    pub fn remaining_rows(&self) -> u64 {
        self.position.remaining_rows(&self.row_group)
    }

    /// Moves the cursor forward past rows for which `predicate` returns false,
    /// where `predicate` is a function such that if it is true for a given key,
    /// it is also true for all larger keys.
    ///
    /// This function does not move the cursor if `predicate` is true for the
    /// current row or a previous row.
    ///
    /// # Safety
    ///
    /// Unsafe because `rkyv` deserialization is unsafe.
    pub unsafe fn seek_forward_until<P>(&mut self, predicate: P) -> Result<(), Error>
    where
        P: Fn(&K) -> bool + Clone,
    {
        self.advance_to_first_ge(&|key| {
            if predicate(key) {
                Less
            } else {
                Greater
            }
        })
    }

    /// Moves the cursor forward past rows whose keys are less than `target`.
    /// This function does not move the cursor if the current row's key is
    /// greater than or equal to `target`.
    ///
    /// # Safety
    ///
    /// Unsafe because `rkyv` deserialization is unsafe.
    pub unsafe fn advance_to_value_or_larger(&mut self, target: &K) -> Result<(), Error> {
        self.advance_to_first_ge(&|key| target.cmp(key))
    }

    /// Moves the cursor to the row whose key is exactly `target`.  This
    /// function does not move the cursor if no key is exactly `target`.
    ///
    /// # Safety
    ///
    /// Unsafe because `rkyv` deserialization is unsafe.
    pub unsafe fn seek_exact(&mut self, target: &K) -> Result<bool, Error> {
        match Position::find_exact_blocking::<N, T, _>(&self.row_group, &|key| target.cmp(key))? {
            Some(position) => {
                self.position = position;
                Ok(true)
            }
            None => Ok(false),
        }
    }

    /// Moves the cursor forward past rows for which `compare` returns [`Less`],
    /// where `compare` is a function such that if it returns [`Equal`] or
    /// [`Greater`] for a given key, it returns [`Greater`] for all larger keys.
    ///
    /// This function does not move the cursor if `compare` returns [`Equal`] or
    /// [`Greater`] for the current row or a previous row.
    ///
    /// # Error handling
    ///
    /// If this returns an error, then the cursor's position might be lost. If
    /// so, then its position is advanced past the end of the row group.
    ///
    /// # Safety
    ///
    /// Unsafe because `rkyv` deserialization is unsafe.
    pub unsafe fn advance_to_first_ge<C>(&mut self, compare: &C) -> Result<(), Error>
    where
        C: Fn(&K) -> Ordering,
    {
        self.position
            .advance_to_first_ge_blocking(&self.row_group, compare)
    }

    /// Moves the cursor backward past rows for which `predicate` returns false,
    /// where `predicate` is a function such that if it is true for a given key,
    /// it is also true for all lesser keys.
    ///
    /// This function does not move the cursor if `predicate` is true for the
    /// current row or a previous row.
    ///
    /// # Safety
    ///
    /// Unsafe because `rkyv` deserialization is unsafe.
    pub unsafe fn seek_backward_until<P>(&mut self, predicate: P) -> Result<(), Error>
    where
        P: Fn(&K) -> bool + Clone,
    {
        self.rewind_to_last_le(&|key| {
            if !predicate(key) {
                Less
            } else {
                Greater
            }
        })
    }

    /// Moves the cursor backward past rows whose keys are greater than
    /// `target`.  This function does not move the cursor if the current row's
    /// key is less than or equal to `target`.
    ///
    /// # Safety
    ///
    /// Unsafe because `rkyv` deserialization is unsafe.
    pub unsafe fn rewind_to_value_or_smaller(&mut self, target: &K) -> Result<(), Error>
    where
        K: Ord,
    {
        self.rewind_to_last_le(&|key| target.cmp(key))
    }

    /// Moves the cursor backward past rows for which `compare` returns
    /// [`Greater`], where `compare` is a function such that if it returns
    /// [`Equal`] or [`Less`] for a given key, it returns [`Less`] for all
    /// lesser keys.
    ///
    /// This function does not move the cursor if `compare` returns [`Equal`] or
    /// [`Less`] for the current row or a previous row.
    ///
    /// # Safety
    ///
    /// Unsafe because `rkyv` deserialization is unsafe.
    pub unsafe fn rewind_to_last_le<C>(&mut self, compare: &C) -> Result<(), Error>
    where
        C: Fn(&K) -> Ordering,
    {
        let position = Position::best_match_blocking::<N, T, _>(&self.row_group, compare, Greater)?;
        if position < self.position {
            self.position = position;
        }
        Ok(())
    }
}

impl<'a, K, A, NK, NA, NN, T> Cursor<'a, K, A, (&'static NK, &'static NA, NN), T>
where
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
    NK: DataTrait + ?Sized,
    NA: DataTrait + ?Sized,
    T: ColumnSpec,
{
    /// Obtains the row group in the next column associated with the current
    /// row.  If the cursor is on a row, the returned row group will contain at
    /// least one row.  If the cursor is before or after the row group, the
    /// returned row group will be empty.
    ///
    /// This method does not do I/O, but it can report [Error::Corruption].
    pub fn next_column<'b>(&'b self) -> Result<RowGroup<'a, NK, NA, NN, T>, Error> {
        Ok(RowGroup::new(
            self.row_group.reader,
            self.row_group.column + 1,
            self.position.row_group()?,
        ))
    }
}

/// A path from the root of a column to a data block.
///
/// # Naming convention
///
/// In this API, functions that can block on I/O have names that end in
/// `_blocking`. Thus, an `async` function should not call a `_blocking`
/// function.
struct Path<K: DataTrait + ?Sized, A: DataTrait + ?Sized> {
    row: u64,
    indexes: Vec<Arc<IndexBlock<K>>>,
    data: Arc<DataBlock<K, A>>,
}

impl<K: DataTrait + ?Sized, A: DataTrait + ?Sized> PartialEq for Path<K, A> {
    fn eq(&self, other: &Self) -> bool {
        self.row == other.row
    }
}

impl<K: DataTrait + ?Sized, A: DataTrait + ?Sized> Eq for Path<K, A> {}

impl<K: DataTrait + ?Sized, A: DataTrait + ?Sized> PartialOrd for Path<K, A> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<K: DataTrait + ?Sized, A: DataTrait + ?Sized> Ord for Path<K, A> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.row.cmp(&other.row)
    }
}

impl<K: DataTrait + ?Sized, A: DataTrait + ?Sized> Clone for Path<K, A> {
    fn clone(&self) -> Self {
        Self {
            row: self.row,
            indexes: self.indexes.clone(),
            data: self.data.clone(),
        }
    }
}

fn push_index_block<K>(
    indexes: &mut Vec<Arc<IndexBlock<K>>>,
    index_block: Arc<IndexBlock<K>>,
) -> Result<(), Error>
where
    K: DataTrait + ?Sized,
{
    const MAX_DEPTH: usize = 64;
    if indexes.len() > MAX_DEPTH {
        // A depth of 64 (very deep) with a branching factor of 2 (very
        // small) would allow for over `2**64` items.  A deeper file is a
        // bug or a memory exhaustion attack.
        return Err(CorruptionError::TooDeep {
            depth: indexes.len(),
            max_depth: MAX_DEPTH,
        }
        .into());
    }

    indexes.push(index_block);
    Ok(())
}

impl<K, A> Path<K, A>
where
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
{
    fn for_row_blocking<N, T>(
        row_group: &RowGroup<'_, K, A, N, T>,
        row: u64,
    ) -> Result<Self, Error> {
        Self::for_row_from_ancestors_blocking::<_, 4>(
            row_group.reader,
            Vec::new(),
            smallvec![row_group.root_node().unwrap()],
            row,
        )
    }
    async fn for_row_async<N, T>(
        row_group: &AsyncRowGroup<'_, K, A, N, T>,
        row: u64,
    ) -> Result<Self, Error> {
        Self::for_row_from_ancestor_async(
            &row_group.row_group.reader.file,
            row_group.context,
            Vec::new(),
            row_group.row_group.root_node().unwrap(),
            row,
        )
        .await
    }
    fn for_row_from_ancestors_blocking<T, const N: usize>(
        reader: &Reader<T>,
        mut indexes: Vec<Arc<IndexBlock<K>>>,
        nodes: SmallVec<[TreeNode; N]>,
        row: u64,
    ) -> Result<Self, Error> {
        match ThreadType::current() {
            ThreadType::Background => {
                let mut block = TreeNode::read_blocking_multiple(nodes, &reader.file)?;
                loop {
                    let next = block.lookup_row_and_successors::<N>(row)?;
                    match block {
                        TreeBlock::Data(data) => {
                            return Ok(Self { row, indexes, data });
                        }
                        TreeBlock::Index(index) => {
                            push_index_block(&mut indexes, index)?;
                        }
                    };
                    block = TreeNode::read_blocking_multiple(next, &reader.file)?;
                }
            }
            ThreadType::Foreground => {
                let mut node = nodes.into_iter().next().unwrap();
                loop {
                    let block = node.read_blocking(&reader.file)?;
                    let next = block.lookup_row(row)?;
                    match block {
                        TreeBlock::Data(data) => {
                            return Ok(Self { row, indexes, data });
                        }
                        TreeBlock::Index(index) => {
                            push_index_block(&mut indexes, index)?;
                        }
                    };
                    node = next.unwrap();
                }
            }
        }
    }
    async fn for_row_from_ancestor_async(
        file: &ImmutableFileRef,
        context: &AsyncCacheContext,
        mut indexes: Vec<Arc<IndexBlock<K>>>,
        mut node: TreeNode,
        row: u64,
    ) -> Result<Self, Error> {
        loop {
            let block = node.read_async(file, context).await?;
            let next = block.lookup_row(row)?;
            match block {
                TreeBlock::Data(data) => {
                    return Ok(Self { row, indexes, data });
                }
                TreeBlock::Index(index) => indexes.push(index),
            };
            node = next.unwrap();
        }
    }
    fn find_ancestor(&self, row: u64) -> Result<(TreeNode, Vec<Arc<IndexBlock<K>>>), Error> {
        for (idx, index_block) in self.indexes.iter().enumerate().rev() {
            match index_block.get_child_by_row(row) {
                Ok(node) => return Ok((node, self.indexes[0..=idx].to_vec())),
                Err(Error::Corruption(CorruptionError::MissingRow(_))) => {
                    // Ignore this because: we're moving upward looking until we
                    // find `row`, and we just haven't found it yet.
                }
                Err(error) => return Err(error),
            }
        }
        Err(CorruptionError::MissingRow(row).into())
    }
    fn find_ancestors<const N: usize>(
        &self,
        row: u64,
    ) -> Result<(SmallVec<[TreeNode; N]>, Vec<Arc<IndexBlock<K>>>), Error> {
        for (idx, index_block) in self.indexes.iter().enumerate().rev() {
            match index_block.get_children_by_row(row) {
                Ok(nodes) => return Ok((nodes, self.indexes[0..=idx].to_vec())),
                Err(Error::Corruption(CorruptionError::MissingRow(_))) => {
                    // Ignore this because: we're moving upward looking until we
                    // find `row`, and we just haven't found it yet.
                }
                Err(error) => return Err(error),
            }
        }
        Err(CorruptionError::MissingRow(row).into())
    }
    fn for_row_from_hint_blocking<N, T>(
        row_group: &RowGroup<'_, K, A, N, T>,
        hint: Option<&Self>,
        row: u64,
    ) -> Result<Self, Error> {
        let Some(hint) = hint else {
            return Self::for_row_blocking(row_group, row);
        };
        if hint.data.rows().contains(&row) {
            return Ok(Self {
                row,
                ..hint.clone()
            });
        }
        let (node, indexes) = hint.find_ancestor(row)?;
        Self::for_row_from_ancestors_blocking::<_, 4>(
            row_group.reader,
            indexes,
            smallvec![node],
            row,
        )
    }
    async fn for_row_from_hint_async<N, T>(
        row_group: &AsyncRowGroup<'_, K, A, N, T>,
        hint: Option<&Self>,
        row: u64,
    ) -> Result<Self, Error> {
        let Some(hint) = hint else {
            return Self::for_row_async(row_group, row).await;
        };
        if hint.data.rows().contains(&row) {
            return Ok(Self {
                row,
                ..hint.clone()
            });
        }
        let (node, indexes) = hint.find_ancestor(row)?;
        Self::for_row_from_ancestor_async(
            &row_group.row_group.reader.file,
            &row_group.context,
            indexes,
            node,
            row,
        )
        .await
    }
    unsafe fn key(&self, factories: &Factories<K, A>, key: &mut K) {
        self.data.key_for_row(factories, self.row, key)
    }
    unsafe fn aux(&self, factories: &Factories<K, A>, aux: &mut A) {
        self.data.aux_for_row(factories, self.row, aux)
    }
    unsafe fn item(&self, factories: &Factories<K, A>, item: (&mut K, &mut A)) {
        self.data.item_for_row(factories, self.row, item)
    }
    unsafe fn archived_item(&self, factories: &Factories<K, A>) -> &dyn ArchivedItem<K, A> {
        self.data.archived_item_for_row(factories, self.row)
    }

    fn row_group(&self) -> Result<Range<u64>, Error> {
        self.data.row_group_for_row(self.row)
    }
    fn move_to_row_blocking<T>(&mut self, reader: &Reader<T>, row: u64) -> Result<(), Error> {
        if self.data.rows().contains(&row) {
            self.row = row;
        } else {
            let (ancestors, indexes) = self.find_ancestors::<4>(row)?;
            *self = Self::for_row_from_ancestors_blocking(reader, indexes, ancestors, row)?;
        }
        Ok(())
    }
    async fn move_to_row_async(
        &mut self,
        file: &ImmutableFileRef,
        context: &AsyncCacheContext,
        row: u64,
    ) -> Result<(), Error> {
        if self.data.rows().contains(&row) {
            self.row = row;
        } else {
            let (ancestor, indexes) = self.find_ancestor(row)?;
            *self =
                Self::for_row_from_ancestor_async(file, context, indexes, ancestor, row).await?;
        }
        Ok(())
    }
    unsafe fn best_match_blocking<N, T, C>(
        row_group: &RowGroup<'_, K, A, N, T>,
        compare: &C,
        bias: Ordering,
    ) -> Result<Option<Self>, Error>
    where
        C: Fn(&K) -> Ordering,
    {
        let mut indexes = Vec::new();
        let Some(mut node) = row_group.root_node() else {
            return Ok(None);
        };
        loop {
            match node.read_blocking(&row_group.reader.file)? {
                TreeBlock::Index(index_block) => {
                    let Some(child_idx) = index_block.find_best_match(
                        row_group.factories.key_factory,
                        &row_group.rows,
                        compare,
                        bias,
                    ) else {
                        return Ok(None);
                    };
                    node = index_block.get_child(child_idx)?;
                    push_index_block(&mut indexes, index_block)?;
                }
                TreeBlock::Data(data_block) => {
                    return Ok(data_block
                        .find_best_match(&row_group.factories, &row_group.rows, compare, bias)
                        .map(|child_idx| Self {
                            row: data_block.first_row + child_idx as u64,
                            indexes,
                            data: data_block,
                        }));
                }
            }
        }
    }

    unsafe fn find_exact_blocking<N, T, C>(
        row_group: &RowGroup<'_, K, A, N, T>,
        compare: &C,
    ) -> Result<Option<Self>, Error>
    where
        T: ColumnSpec,
        C: Fn(&K) -> Ordering,
    {
        let mut indexes = Vec::new();
        let Some(mut node) = row_group.reader.columns[row_group.column].root.clone() else {
            return Ok(None);
        };
        loop {
            match node.read_blocking(&row_group.reader.file)? {
                TreeBlock::Index(index_block) => {
                    let Some(child_idx) = index_block.find_exact(
                        row_group.factories.key_factory,
                        &row_group.rows,
                        compare,
                    ) else {
                        return Ok(None);
                    };
                    node = index_block.get_child(child_idx)?;
                    indexes.push(index_block);
                }
                TreeBlock::Data(data_block) => {
                    return Ok(data_block
                        .find_exact(&row_group.factories, &row_group.rows, compare)
                        .map(|child_idx| Self {
                            row: data_block.first_row + child_idx as u64,
                            indexes,
                            data: data_block,
                        }));
                }
            }
        }
    }

    async unsafe fn find_exact_async<N, T, C>(
        row_group: &AsyncRowGroup<'_, K, A, N, T>,
        compare: &C,
    ) -> Result<Option<Self>, Error>
    where
        T: ColumnSpec,
        C: Fn(&K) -> Ordering,
    {
        let mut indexes = Vec::new();
        let Some(mut node) = row_group.row_group.reader.columns[row_group.row_group.column]
            .root
            .clone()
        else {
            return Ok(None);
        };
        loop {
            match node
                .read_async(&row_group.row_group.reader.file, row_group.context)
                .await?
            {
                TreeBlock::Index(index_block) => {
                    let Some(child_idx) = index_block.find_exact(
                        row_group.row_group.factories.key_factory,
                        &row_group.row_group.rows,
                        compare,
                    ) else {
                        return Ok(None);
                    };
                    node = index_block.get_child(child_idx)?;
                    indexes.push(index_block);
                }
                TreeBlock::Data(data_block) => {
                    return Ok(data_block
                        .find_exact(
                            &row_group.row_group.factories,
                            &row_group.row_group.rows,
                            compare,
                        )
                        .map(|child_idx| Self {
                            row: data_block.first_row + child_idx as u64,
                            indexes,
                            data: data_block,
                        }));
                }
            }
        }
    }

    /// This implements an equivalent of the following snippet, but it performs
    /// much better because it searches from the current path, reusing the data
    /// block and index blocks already in the path, instead of starting from the
    /// root node.
    ///
    /// ````text
    /// match Self::best_match(row_group, compare, Less)? {
    ///     Some(path) => {
    ///         *self = path;
    ///         return Ok(true);
    ///     }
    ///     None => return Ok(false),
    /// }
    /// ```
    ///
    /// If this returns `Ok(false)` or `Err(_)`, then the resulting `Path` can
    /// violate the invariant that `self.data` is not a direct child of the last
    /// element in `self.indexes`. The caller should not use this `Path` again.
    ///
    /// The same optimization would apply to backward seeks, but they haven't
    /// been important in practice yet.
    unsafe fn advance_to_first_ge_blocking<N, T, C>(
        &mut self,
        row_group: &RowGroup<'_, K, A, N, T>,
        compare: &C,
    ) -> Result<bool, Error>
    where
        C: Fn(&K) -> Ordering,
    {
        let rows = self.row..row_group.rows.end;

        // Check the current position first. We might already be done.
        let mut ordering = Equal;
        row_group.factories.key_factory.with(&mut |key| {
            self.key(&row_group.factories, key);
            ordering = compare(key);
        });
        if ordering != Greater {
            return Ok(true);
        }

        // If the last item in `rows` in the current data block is greater than
        // or equal to the target, then the position must be in the current data
        // block.
        if self.data.compare_row(
            &row_group.factories,
            min(self.data.rows().end, rows.end) - 1,
            compare,
        ) != Greater
        {
            let child_idx = self
                .data
                .find_best_match(&row_group.factories, &rows, compare, Less)
                .unwrap();
            self.row = self.data.first_row + child_idx as u64;
            return Ok(true);
        }

        while let Some(index_block) = self.indexes.pop() {
            // We need to go up another level if `rows.end` is beyond the end of
            // `index_block` and the greatest value under `index_block` is less
            // than the target.
            if rows.end > index_block.rows().end
                && index_block.compare_max(row_group.factories.key_factory, compare) == Greater
            {
                continue;
            }

            // Otherwise, our target (if any) must be below `index_block`.
            let Some(child_idx) = index_block.find_best_match(
                row_group.factories.key_factory,
                &row_group.rows,
                compare,
                Less,
            ) else {
                // `rows.end` is inside `index_block` but the largest key is
                // less than the target.
                return Ok(false);
            };
            let mut node = index_block.get_child(child_idx)?;
            push_index_block(&mut self.indexes, index_block)?;

            loop {
                match node.read_blocking::<K, A>(&row_group.reader.file)? {
                    TreeBlock::Index(index_block) => {
                        let Some(child_idx) = index_block.find_best_match(
                            row_group.factories.key_factory,
                            &row_group.rows,
                            compare,
                            Less,
                        ) else {
                            return Ok(false);
                        };
                        node = index_block.get_child(child_idx)?;
                        push_index_block(&mut self.indexes, index_block)?;
                    }
                    TreeBlock::Data(data_block) => {
                        let Some(child_idx) = data_block.find_best_match(
                            &row_group.factories,
                            &row_group.rows,
                            compare,
                            Less,
                        ) else {
                            return Ok(false);
                        };
                        self.row = child_idx as u64 + data_block.first_row;
                        self.data = data_block;
                        return Ok(true);
                    }
                }
            }
        }

        // Every value in `rows` is less than the target.
        Ok(false)
    }
}

impl<K, A> Debug for Path<K, A>
where
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
{
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        write!(f, "Path {{ row: {}, indexes:", self.row)?;
        for index in &self.indexes {
            let n = index.n_children();
            match index.find_row(self.row) {
                Ok(i) => {
                    let min_row = index.get_row_bound(i * 2);
                    let max_row = index.get_row_bound(i * 2 + 1);
                    write!(f, "\n[child {i} of {n}: rows {min_row}..={max_row}]",)?;
                }
                Err(_) => {
                    // This should not be possible because it indicates an
                    // invariant violation.  Possibly we should panic.
                    write!(f, " [unknown child of {n}]")?
                }
            }
        }
        write!(
            f,
            ", data: [row {} of {}] }}",
            self.row - self.data.first_row,
            self.data.n_values()
        )
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Position<K, A>
where
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
{
    /// Before a row group.
    Before,

    /// Within a row group.
    Row(Path<K, A>),

    /// After a row group.
    ///
    /// `hint` is optionally some position in this column. It is useful for
    /// optimizing finding another (presumably nearby) position in the same
    /// column. This optimizes the common case of iterating through one column
    /// (e.g. a key in an indexed wset) and visiting all of the corresponding
    /// values in the next column (e.g. the key's values in the indexed wset),
    /// using [RowGroup::first_with_hint] to visit the first value of each key
    /// after the first.
    ///
    /// We could add a hint to [Position::Before] but reverse iteration is
    /// uncommon so it probably wouldn't help with much.
    After { hint: Option<Path<K, A>> },
}

impl<K, A> Clone for Position<K, A>
where
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
{
    fn clone(&self) -> Self {
        match self {
            Position::Before => Position::Before,
            Position::Row(path) => Position::Row(path.clone()),
            Position::After { hint } => Position::After { hint: hint.clone() },
        }
    }
}

#[derive(Copy, Clone)]
enum Row {
    Before,
    At(u64),
    After,
}

impl Row {
    fn first(rows: &Range<u64>) -> Self {
        if rows.is_empty() {
            Self::After
        } else {
            Self::At(rows.start)
        }
    }
    fn last(rows: &Range<u64>) -> Self {
        if rows.is_empty() {
            Self::Before
        } else {
            Self::At(rows.end - 1)
        }
    }
    fn next(self, rows: &Range<u64>) -> Self {
        match self {
            Row::Before => Self::first(rows),
            Row::At(row) if row + 1 < rows.end => Row::At(row + 1),
            _ => Row::After,
        }
    }
    fn prev(self, rows: &Range<u64>) -> Self {
        match self {
            Row::After => Self::last(rows),
            Row::At(row) if row > rows.start => Row::At(row - 1),
            _ => Row::Before,
        }
    }

    fn nth(rows: &Range<u64>, row: u64) -> Self {
        if row < rows.end - rows.start {
            Self::At(rows.start + row)
        } else {
            Self::After
        }
    }
}

impl<K, A> Position<K, A>
where
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
{
    fn for_row_blocking<N, T>(
        row_group: &RowGroup<'_, K, A, N, T>,
        row: u64,
    ) -> Result<Self, Error> {
        Ok(Self::Row(Path::for_row_blocking(row_group, row)?))
    }
    async fn for_row_async<N, T>(
        row_group: &AsyncRowGroup<'_, K, A, N, T>,
        row: u64,
    ) -> Result<Self, Error> {
        Ok(Self::Row(Path::for_row_async(row_group, row).await?))
    }
    fn for_row_from_hint_blocking<N, T>(
        row_group: &RowGroup<'_, K, A, N, T>,
        hint: &Self,
        row: u64,
    ) -> Result<Self, Error>
    where
        T: ColumnSpec,
    {
        Ok(Self::Row(Path::for_row_from_hint_blocking(
            row_group,
            hint.hint(),
            row,
        )?))
    }
    async fn for_row_from_hint_async<N, T>(
        row_group: &AsyncRowGroup<'_, K, A, N, T>,
        hint: &Self,
        row: u64,
    ) -> Result<Self, Error> {
        Ok(Self::Row(
            Path::for_row_from_hint_async(row_group, hint.hint(), row).await?,
        ))
    }
    fn row(&self) -> Row {
        match self {
            Position::Before => Row::Before,
            Position::Row(path) => Row::At(path.row),
            Position::After { .. } => Row::After,
        }
    }
    fn move_to<N, T>(
        &mut self,
        row_group: &RowGroup<'_, K, A, N, T>,
        row: Row,
    ) -> Result<(), Error> {
        match row {
            Row::Before => *self = Self::Before,
            Row::After => self.move_after(),
            Row::At(row) => match self {
                Position::Row(path) => path.move_to_row_blocking(row_group.reader, row)?,
                _ => {
                    *self = Self::Row(Path::for_row_from_hint_blocking(
                        row_group,
                        self.hint(),
                        row,
                    )?)
                }
            },
        }
        Ok(())
    }
    fn move_after(&mut self) {
        *self = Position::After {
            hint: self.take_hint(),
        };
    }
    /// Replaces `self` by an arbitrary value and returns its prevous [Path]
    /// (whether the current row or a hint).
    fn take_hint(&mut self) -> Option<Path<K, A>> {
        match replace(self, Position::Before) {
            Position::Before => None,
            Position::Row(hint) => Some(hint),
            Position::After { hint } => hint,
        }
    }
    /// Returns the current row or a hint for one.
    fn hint(&self) -> Option<&Path<K, A>> {
        match self {
            Position::Before => None,
            Position::Row(path) => Some(path),
            Position::After { hint } => hint.as_ref(),
        }
    }
    async fn move_to_async<N, T>(
        &mut self,
        row_group: &AsyncRowGroup<'_, K, A, N, T>,
        row: Row,
    ) -> Result<(), Error> {
        match row {
            Row::Before => *self = Self::Before,
            Row::After => self.move_after(),
            Row::At(row) => match self {
                Position::Before => *self = Self::Row(Path::for_row_async(row_group, row).await?),
                Position::After { hint } => {
                    *self = Self::Row(
                        Path::for_row_from_hint_async(row_group, hint.as_ref(), row).await?,
                    )
                }
                Position::Row(path) => {
                    path.move_to_row_async(
                        &row_group.row_group.reader.file,
                        &row_group.context,
                        row,
                    )
                    .await?
                }
            },
        }
        Ok(())
    }

    fn path(&self) -> Option<&Path<K, A>> {
        match self {
            Position::Before => None,
            Position::Row(path) => Some(path),
            Position::After { .. } => None,
        }
    }
    pub unsafe fn key<'k>(&self, factories: &Factories<K, A>, key: &'k mut K) -> Option<&'k mut K> {
        self.path().map(|path| {
            path.key(factories, key);
            key
        })
    }
    pub unsafe fn aux<'a>(&self, factories: &Factories<K, A>, aux: &'a mut A) -> Option<&'a mut A> {
        self.path().map(|path| {
            path.aux(factories, aux);
            aux
        })
    }
    pub unsafe fn item<'a>(
        &self,
        factories: &Factories<K, A>,
        item: (&'a mut K, &'a mut A),
    ) -> Option<(&'a mut K, &'a mut A)> {
        self.path().map(|path| {
            path.item(factories, (item.0, item.1));
            item
        })
    }
    pub unsafe fn archived_item(
        &self,
        factories: &Factories<K, A>,
    ) -> Option<&dyn ArchivedItem<'_, K, A>> {
        self.path().map(|path| path.archived_item(factories))
    }

    pub fn row_group(&self) -> Result<Range<u64>, Error> {
        match self.path() {
            Some(path) => path.row_group(),
            None => Ok(0..0),
        }
    }
    fn has_value(&self) -> bool {
        self.path().is_some()
    }
    unsafe fn best_match_blocking<N, T, C>(
        row_group: &RowGroup<'_, K, A, N, T>,
        compare: &C,
        bias: Ordering,
    ) -> Result<Self, Error>
    where
        C: Fn(&K) -> Ordering,
    {
        match Path::best_match_blocking(row_group, compare, bias)? {
            Some(path) => Ok(Position::Row(path)),
            None => Ok(if bias == Less {
                Position::After { hint: None }
            } else {
                Position::Before
            }),
        }
    }
    unsafe fn find_exact_blocking<N, T, C>(
        row_group: &RowGroup<'_, K, A, N, T>,
        compare: &C,
    ) -> Result<Option<Self>, Error>
    where
        T: ColumnSpec,
        C: Fn(&K) -> Ordering,
    {
        Ok(Path::find_exact_blocking(row_group, compare)?.map(|path| Position::Row(path)))
    }
    async unsafe fn find_exact_async<N, T, C>(
        row_group: &AsyncRowGroup<'_, K, A, N, T>,
        compare: &C,
    ) -> Result<Option<Self>, Error>
    where
        T: ColumnSpec,
        C: Fn(&K) -> Ordering,
    {
        Ok(Path::find_exact_async(row_group, compare)
            .await?
            .map(|path| Position::Row(path)))
    }
    fn absolute_position<N, T>(&self, row_group: &RowGroup<K, A, N, T>) -> u64 {
        match self {
            Position::Before => row_group.rows.start,
            Position::Row(path) => path.row,
            Position::After { .. } => row_group.rows.end,
        }
    }
    fn remaining_rows<N, T>(&self, row_group: &RowGroup<K, A, N, T>) -> u64 {
        match self {
            Position::Before => row_group.len(),
            Position::Row(path) => row_group.rows.end - path.row,
            Position::After { .. } => 0,
        }
    }

    /// If this returns an I/O error, then the position might be lost (and set
    /// to `Position::After`).
    unsafe fn advance_to_first_ge_blocking<N, T, C>(
        &mut self,
        row_group: &RowGroup<'_, K, A, N, T>,
        compare: &C,
    ) -> Result<(), Error>
    where
        C: Fn(&K) -> Ordering,
    {
        match self {
            Position::Before => {
                *self = Self::best_match_blocking::<N, T, _>(row_group, compare, Less)?;
            }
            Position::After { .. } => (),
            Position::Row(path) => {
                match path.advance_to_first_ge_blocking(row_group, compare) {
                    Ok(false) => {
                        // Discard `path`, which might now violate its internal
                        // invariants (so don't even try to use it as a hint).
                        *self = Position::After { hint: None };
                    }
                    Ok(true) => (),
                    Err(error) => {
                        // Discard `path`, which might now violate its internal
                        // invariants (so don't even try to use it as a hint).
                        *self = Position::After { hint: None };
                        return Err(error);
                    }
                }
            }
        }
        Ok(())
    }
}

/// A [RowGroup] for use with Rust `async` and overlapping I/O.
pub struct AsyncRowGroup<'a, K, A, N, T>
where
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
{
    row_group: RowGroup<'a, K, A, N, T>,
    context: &'a AsyncCacheContext,
}

impl<K, A, N, T> Clone for AsyncRowGroup<'_, K, A, N, T>
where
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
{
    fn clone(&self) -> Self {
        Self {
            row_group: self.row_group.clone(),
            context: self.context,
        }
    }
}
impl<'a, K, A, N, T> AsyncRowGroup<'a, K, A, N, T>
where
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
{
    fn cursor(&self, position: Position<K, A>) -> AsyncCursor<'a, K, A, N, T> {
        AsyncCursor {
            row_group: self.clone(),
            position,
        }
    }

    /// Returns `true` if the row group contains no rows.
    ///
    /// The row group for column 0 is empty if and only if the layer file is
    /// empty.  A row group obtained from [`Cursor::next_column`] is never
    /// empty.
    pub fn is_empty(&self) -> bool {
        self.row_group.is_empty()
    }

    /// Returns the number of rows in the row group.
    pub fn len(&self) -> u64 {
        self.row_group.len()
    }

    /// Return a cursor for the first row in the row group, or just after the
    /// row group if it is empty.
    pub async fn first(&self) -> Result<AsyncCursor<'a, K, A, N, T>, Error> {
        let position = if self.is_empty() {
            Position::After { hint: None }
        } else {
            Position::for_row_async(self, self.row_group.rows.start).await?
        };
        Ok(self.cursor(position))
    }

    /// Return a cursor for the first row in the row group, or just after the
    /// row group if it is empty, using `hint` as an internal starting point for
    /// searching the B-tree. For best performance, use a `hint` near the first
    /// row in the row group (but the result will be correct regardless of
    /// `hint`).
    pub async fn first_with_hint(
        &self,
        hint: &AsyncCursor<'a, K, A, N, T>,
    ) -> Result<AsyncCursor<'a, K, A, N, T>, Error> {
        let position = if self.is_empty() {
            Position::After { hint: None }
        } else {
            Position::for_row_from_hint_async(self, &hint.position, self.row_group.rows.start)
                .await?
        };
        Ok(self.cursor(position))
    }

    /// Return a cursor for the last row in the row group, or just after the
    /// row group if it is empty.
    pub async fn last(&self) -> Result<AsyncCursor<'a, K, A, N, T>, Error> {
        let position = if self.is_empty() {
            Position::After { hint: None }
        } else {
            Position::for_row_async(self, self.row_group.rows.end - 1).await?
        };
        Ok(self.cursor(position))
    }

    /// If `row` is less than the number of rows in the row group, returns a
    /// cursor for that row; otherwise, returns a cursor for just after the row
    /// group.
    pub async fn nth(&self, row: u64) -> Result<AsyncCursor<'a, K, A, N, T>, Error> {
        let position = if row < self.len() {
            Position::for_row_async(self, self.row_group.rows.start + row).await?
        } else {
            Position::After { hint: None }
        };
        Ok(self.cursor(position))
    }

    /// If `target` exists in the row group, returns a cursor for it; otherwise,
    /// returns `Ok(None)`.
    pub async unsafe fn find_exact(
        &self,
        target: &K,
    ) -> Result<Option<AsyncCursor<'a, K, A, N, T>>, Error>
    where
        T: ColumnSpec,
    {
        let mut cursor = self.before();
        match cursor.seek_exact(target).await? {
            true => Ok(Some(cursor)),
            false => Ok(None),
        }
    }

    /// Returns a cursor for just before the row group.
    pub fn before(&self) -> AsyncCursor<'a, K, A, N, T> {
        self.cursor(Position::Before)
    }

    /// Return a cursor for just after the row group.
    pub fn after(&self) -> AsyncCursor<'a, K, A, N, T> {
        self.cursor(Position::After { hint: None })
    }

    /// Returns a row group for a subset of the rows in this one.
    pub fn subset<B>(&self, range: B) -> Self
    where
        B: RangeBounds<u64>,
    {
        Self {
            row_group: self.row_group.subset(range),
            context: self.context,
        }
    }
}

/// A [Cursor] for use with Rust `async` and overlapping I/O.
pub struct AsyncCursor<'a, K, A, N, T>
where
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
{
    row_group: AsyncRowGroup<'a, K, A, N, T>,
    position: Position<K, A>,
}

impl<'a, K, A, N, T> AsyncCursor<'a, K, A, N, T>
where
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
{
    /// Returns `true` if the cursor is on a row.
    pub fn has_value(&self) -> bool {
        self.position.has_value()
    }

    /// Returns the number of rows in the cursor's row group.
    pub fn len(&self) -> u64 {
        self.row_group.len()
    }

    /// Returns true if this cursor's row group has no rows.
    pub fn is_empty(&self) -> bool {
        self.row_group.is_empty()
    }

    fn rows(&self) -> &Range<u64> {
        &self.row_group.row_group.rows
    }

    /// Moves to the next row in the row group.  If the cursor was previously
    /// before the row group, it moves to the first row; if it was on the last
    /// row, it moves after the row group.
    pub async fn move_next(&mut self) -> Result<(), Error> {
        self.position
            .move_to_async(&self.row_group, self.position.row().next(self.rows()))
            .await
    }

    /// Moves to the previous row in the row group.  If the cursor was
    /// previously after the row group, it moves to the last row; if it was
    /// on the first row, it moves before the row group.
    pub async fn move_prev(&mut self) -> Result<(), Error> {
        self.position
            .move_to_async(&self.row_group, self.position.row().prev(self.rows()))
            .await
    }

    /// Moves to the first row in the row group.  If the row group is empty,
    /// this has no effect.
    pub async fn move_first(&mut self) -> Result<(), Error> {
        self.position
            .move_to_async(&self.row_group, Row::first(self.rows()))
            .await
    }

    /// Moves just before the row group.
    pub fn move_before(&mut self) {
        self.position = Position::Before;
    }

    /// Moves just after the row group.
    pub fn move_after(&mut self) {
        self.position.move_after();
    }

    /// Moves to the last row in the row group.  If the row group is empty,
    /// this has no effect.
    pub async fn move_last(&mut self) -> Result<(), Error> {
        self.position
            .move_to_async(&self.row_group, Row::last(self.rows()))
            .await
    }

    /// Moves to row `row`.  If `row >= self.len()`, moves after the row group.
    pub async fn move_to_row(&mut self, row: u64) -> Result<(), Error> {
        self.position
            .move_to_async(&self.row_group, Row::nth(self.rows(), row))
            .await
    }

    /// Returns the row number of the current row, as an absolute number
    /// relative to the top of the column rather than the top of the row group.
    /// If the cursor is before the row group or on the first row, returns the
    /// row number of the first row in the row group; if the cursor is after the
    /// row group, returns the row number of the row just after the row group.
    pub fn absolute_position(&self) -> u64 {
        self.position.absolute_position(&self.row_group.row_group)
    }

    /// Returns the number of times [`move_next`](Self::move_next) may be called
    /// before the cursor is after the row group.
    pub fn remaining_rows(&self) -> u64 {
        self.position.remaining_rows(&self.row_group.row_group)
    }

    /// Returns the key in the current row, or `None` if the cursor is before or
    /// after the row group.
    ///
    /// # Safety
    ///
    /// Unsafe because `rkyv` deserialization is unsafe.
    pub unsafe fn key(&self, key: &'a mut K) -> Option<&'a mut K> {
        self.position.key(&self.row_group.row_group.factories, key)
    }

    /// Returns the auxiliary data in the current row, or `None` if the cursor
    /// is before or after the row group.
    ///
    /// # Safety
    ///
    /// Unsafe because `rkyv` deserialization is unsafe.
    pub unsafe fn aux<'b>(&self, aux: &'b mut A) -> Option<&'b mut A> {
        self.position.aux(&self.row_group.row_group.factories, aux)
    }

    /// Returns the key and auxiliary data in the current row, or `None` if the
    /// cursor is before or after the row group.
    ///
    /// # Safety
    ///
    /// Unsafe because `rkyv` deserialization is unsafe.
    pub unsafe fn item<'b>(&self, item: (&'b mut K, &'b mut A)) -> Option<(&'b mut K, &'b mut A)> {
        self.position
            .item(&self.row_group.row_group.factories, item)
    }

    /// Returns archived representation of the key and auxiliary data in the
    /// current row, or `None` if the cursor is before or after the row
    /// group.
    ///
    /// # Safety
    ///
    /// Unsafe because `rkyv` deserialization is unsafe.
    pub unsafe fn archived_item(&self) -> Option<&dyn ArchivedItem<'_, K, A>> {
        self.position
            .archived_item(&self.row_group.row_group.factories)
    }

    /// Moves the cursor to the row whose key is exactly `target`.  This
    /// function does not move the cursor if no key is exactly `target`.
    ///
    /// # Safety
    ///
    /// Unsafe because `rkyv` deserialization is unsafe.
    pub async unsafe fn seek_exact(&mut self, target: &K) -> Result<bool, Error>
    where
        T: ColumnSpec,
    {
        match Position::find_exact_async::<N, T, _>(&self.row_group, &|key| target.cmp(key)).await?
        {
            Some(position) => {
                self.position = position;
                Ok(true)
            }
            None => Ok(false),
        }
    }
}

impl<'a, K, A, NK, NA, NN, T> AsyncCursor<'a, K, A, (&'static NK, &'static NA, NN), T>
where
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
    NK: DataTrait + ?Sized,
    NA: DataTrait + ?Sized,
    T: ColumnSpec,
{
    /// Obtains the row group in the next column associated with the current
    /// row.  If the cursor is on a row, the returned row group will contain at
    /// least one row.  If the cursor is before or after the row group, the
    /// returned row group will be empty.
    ///
    /// This method does not do I/O, but it can report [Error::Corruption].
    pub async fn next_column<'b>(&'b self) -> Result<AsyncRowGroup<'a, NK, NA, NN, T>, Error> {
        Ok(AsyncRowGroup {
            row_group: RowGroup::new(
                self.row_group.row_group.reader,
                self.row_group.row_group.column + 1,
                self.position.row_group()?,
            ),
            context: self.row_group.context,
        })
    }
}

struct BulkRead {
    node: TreeNode,
    level: usize,
}

struct BulkReadResults {
    reads: Vec<BulkRead>,
    results: Vec<Result<Arc<FBuf>, StorageError>>,
}

/// Reads all of the data in a column in order.
///
/// `BulkRows` provides non-blocking access to all of the data in a [Reader]
/// column.  It does all of the I/O asynchronously with heavy readahead.
pub struct BulkRows<'a, K, A, N, T>
where
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
{
    reader: &'a Reader<T>,
    cache: Arc<BufferCache>,
    factories: Factories<K, A>,
    column: usize,
    row: u64,
    n_rows: u64,

    receiver: Receiver<BulkReadResults>,
    sender: Sender<BulkReadResults>,

    /// If nonempty, then:
    /// - `data_blocks[0]` contains `row`.
    /// - `data_blocks[1..]` are subsequent blocks.
    data_blocks: VecDeque<Arc<DataBlock<K, A>>>,

    /// First row in the next block to be added to `data_blocks`.  If
    /// `data_blocks` is nonempty, then this is
    /// `data_blocks.last().unwrap().first_row`.
    next_data: u64,

    /// Blocks that have been received out of order.  They will be moved to
    /// `blocks` when `next_data` catches up to their starting row.
    out_of_order_data: BTreeMap<u64, Arc<DataBlock<K, A>>>,

    data_pending: usize,

    indexes: Vec<IndexLevel<K>>,
    _phantom: PhantomData<fn(&K, &A, N)>,
}

impl<'a, K, A, N, T> Debug for BulkRows<'a, K, A, N, T>
where
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
    T: ColumnSpec,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(
            f,
            "BulkRows {{ row: {}, n_rows: {}, n_readable: {} }}",
            self.row,
            self.n_rows,
            self.n_readable()
        )
    }
}

impl<'a, K, A, NK, NA, NN, T> BulkRows<'a, K, A, (&'static NK, &'static NA, NN), T>
where
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
    NK: DataTrait + ?Sized,
    NA: DataTrait + ?Sized,
    T: ColumnSpec,
{
    /// Returns a [BulkRows] for the next column.
    pub fn next_column<'b>(&'b self) -> Result<BulkRows<'a, NK, NA, NN, T>, Error> {
        BulkRows::new(&self.reader, self.column + 1)
    }
}

#[allow(missing_docs)]
impl<'a, K, A, N, T> BulkRows<'a, K, A, N, T>
where
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
    T: ColumnSpec,
{
    fn new(reader: &'a Reader<T>, column: usize) -> Result<Self, Error> {
        let (sender, receiver) = channel();
        let mut this = Self {
            reader,
            cache: (reader.file.cache)(),
            factories: reader.columns[column].factories.factories(),
            column,
            row: 0,
            sender,
            receiver,
            n_rows: reader.columns[column].n_rows,
            indexes: Vec::new(),
            data_blocks: VecDeque::new(),
            next_data: 0,
            out_of_order_data: BTreeMap::new(),
            data_pending: 0,
            _phantom: PhantomData,
        };
        if let Some(node) = &reader.columns[column].root {
            let reads = this.start_block_read(&node, 0)?.into_iter().collect();
            this.work_(reads)?;
        }
        Ok(this)
    }

    fn start_index_read(
        &mut self,
        node: &TreeNode,
        level: usize,
    ) -> Result<Option<BulkRead>, Error> {
        if level >= self.indexes.len() {
            self.indexes.push(IndexLevel::new());
        }
        self.indexes[level].pending += 1;
        if let Some(cache_entry) = self
            .cache
            .get(&*self.reader.file.file_handle, node.location)
        {
            self.indexes[level].received(IndexBlock::from_cache_entry(cache_entry, node.location)?);
            Ok(None)
        } else {
            Ok(Some(BulkRead {
                node: node.clone(),
                level,
            }))
        }
    }

    fn start_data_read(&mut self, node: &TreeNode) -> Result<Option<BulkRead>, Error> {
        self.data_pending += 1;
        if let Some(cache_entry) = self
            .cache
            .get(&*self.reader.file.file_handle, node.location)
        {
            self.received_data(DataBlock::from_cache_entry(cache_entry, node.location)?);
            Ok(None)
        } else {
            Ok(Some(BulkRead {
                node: node.clone(),
                level: 0,
            }))
        }
    }

    fn start_block_read(
        &mut self,
        node: &TreeNode,
        level: usize,
    ) -> Result<Option<BulkRead>, Error> {
        match node.node_type {
            NodeType::Data => self.start_data_read(node),
            NodeType::Index => self.start_index_read(node, level),
        }
    }

    fn process_read_results(&mut self, read_results: BulkReadResults) -> Result<(), Error> {
        for (BulkRead { node, level }, result) in read_results
            .reads
            .into_iter()
            .zip(read_results.results.into_iter())
        {
            let raw = decompress(self.reader.file.compression, node.location, result?)?;
            let file_id = self.reader.file.file_handle.file_id();
            let tree_block = TreeBlock::from_raw_with_cache(raw, &node, &self.cache, file_id)?;
            match tree_block {
                TreeBlock::Data(data_block) => self.received_data(data_block),
                TreeBlock::Index(index_block) => self.indexes[level].received(index_block),
            }
        }
        Ok(())
    }

    /// Initiates and continues background work for reading data in this column.
    /// This must be called periodically to keep data flowing.  It limits the
    /// amount of data buffered beyond the current read point.
    pub fn work(&mut self) -> Result<(), Error> {
        self.work_(Vec::new())
    }

    fn work_(&mut self, mut reads: Vec<BulkRead>) -> Result<(), Error> {
        // First, catch up on all completed reads.
        while let Ok(read_results) = self.receiver.try_recv() {
            self.process_read_results(read_results)?;
        }

        // Then schedule more reads.
        let mut level = 0;
        while level < self.indexes.len() {
            while let Some(node) = self.indexes[level].child()? {
                if self.is_level_full(node.node_type, level + 1) {
                    break;
                }
                self.indexes[level].next_child();
                if let Some(read) = self.start_block_read(&node, level + 1)? {
                    reads.push(read);
                }
            }
            level += 1;
        }

        if !reads.is_empty() {
            self.reader.file.file_handle.read_async(
                reads.iter().map(|read| read.node.location).collect(),
                {
                    let sender = self.sender.clone();
                    Box::new(move |results| {
                        let _ = sender.send(BulkReadResults { reads, results });
                    })
                },
            );
        }

        Ok(())
    }

    fn is_level_full(&self, node_type: NodeType, level: usize) -> bool {
        match node_type {
            NodeType::Data => {
                self.data_pending + self.data_blocks.len() + self.out_of_order_data.len() >= 100
            }
            NodeType::Index => self
                .indexes
                .get(level)
                .is_some_and(|child| child.is_full(level)),
        }
    }

    /// Adds `block` to the collection of data blocks.
    fn received_data(&mut self, block: Arc<DataBlock<K, A>>) {
        self.data_pending -= 1;
        if block.first_row == self.next_data {
            self.next_data = block.rows().end;
            self.data_blocks.push_back(block);
            while let Some(entry) = self.out_of_order_data.first_entry() {
                if *entry.key() != self.next_data {
                    break;
                }
                let block = entry.remove();
                self.next_data = block.rows().end;
                self.data_blocks.push_back(block);
            }
        } else if block.first_row > self.next_data {
            self.out_of_order_data.insert(block.first_row, block);
        } else {
            // File corruption or (more likely) a bug.
            todo!()
        }
    }

    pub fn n_readable(&self) -> usize {
        self.data_blocks
            .back()
            .map_or(0, |last| last.rows().end - self.row) as usize
    }

    pub fn at_eof(&self) -> bool {
        self.row >= self.n_rows
    }

    pub fn is_readable(&self) -> bool {
        !self.data_blocks.is_empty()
    }

    pub fn wait(&mut self) -> Result<(), Error> {
        if self.at_eof() {
            return Ok(());
        }

        while !self.is_readable() {
            // Process received blocks and schedule block reads.  The latter is
            // particularly important on the first loop iteration, since the
            // caller might have read all of the rows without ever calling
            // `work` to schedule more block reads.
            self.work()?;
            if self.is_readable() {
                break;
            }

            // Nothing is readable, and we're not at the end, so there must be
            // pending reads.  Wait until one completes, and process it.
            debug_assert!(self.pending());
            self.process_read_results(self.receiver.recv().unwrap())?;
        }
        Ok(())
    }

    /// Returns whether we're waiting on any block reads.
    #[allow(dead_code)]
    fn pending(&self) -> bool {
        self.data_pending > 0 || self.indexes.iter().any(|level| level.pending > 0)
    }

    /// Returns the key in the current row, or `None` if we're at EOF or this
    /// row isn't readable yet.
    ///
    /// # Safety
    ///
    /// Unsafe because `rkyv` deserialization is unsafe.
    pub unsafe fn key<'b>(&self, key: &'b mut K) -> Option<&'b mut K> {
        self.data_blocks.front().map(|block| {
            block.key_for_row(&self.factories, self.row, key);
            key
        })
    }

    /// Returns the auxiliary data in the current row, or `None` if we're at EOF
    /// or this row isn't readable yet.
    ///
    /// # Safety
    ///
    /// Unsafe because `rkyv` deserialization is unsafe.
    pub unsafe fn aux<'b>(&self, aux: &'b mut A) -> Option<&'b mut A> {
        self.data_blocks.front().map(|block| {
            block.aux_for_row(&self.factories, self.row, aux);
            aux
        })
    }

    pub unsafe fn item<'b>(&self, item: (&'b mut K, &'b mut A)) -> Option<(&'b mut K, &'b mut A)> {
        self.data_blocks.front().map(|block| {
            block.item_for_row(&self.factories, self.row, (item.0, item.1));
            item
        })
    }

    pub fn row_group(&self) -> Result<Option<Range<u64>>, Error> {
        self.data_blocks
            .front()
            .map(|block| block.row_group_for_row(self.row))
            .transpose()
    }

    pub fn row(&self) -> u64 {
        self.row
    }

    pub fn n_rows(&self) -> u64 {
        self.n_rows
    }

    pub fn step(&mut self) {
        debug_assert!(self.data_blocks[0].rows().contains(&self.row));
        self.row += 1;
        if self.row >= self.data_blocks[0].rows().end {
            self.data_blocks.pop_front();
        }
    }

    pub fn step_to(&mut self, target_row: u64) -> bool {
        debug_assert!(target_row >= self.row);
        debug_assert!(target_row <= self.n_rows);
        while target_row > self.row {
            let Some(end) = self.data_blocks.front().map(|block| block.rows().end) else {
                return false;
            };
            if target_row >= end {
                self.row = end;
                self.data_blocks.pop_front();
            } else {
                self.row = target_row;
            }
        }
        true
    }
}

struct IndexLevel<K>
where
    K: DataTrait + ?Sized,
{
    /// Number of outstanding block reads pending completion for this level.
    pending: usize,

    /// Sequential blocks whose children need to be loaded.
    blocks: VecDeque<Arc<IndexBlock<K>>>,

    index: usize,

    /// First row in the next block to be added to `blocks`.  If `blocks` is
    /// nonempty, then this is `blocks.last().unwrap().first_row`.
    next: u64,

    /// Blocks that have been received out of order.  They will be moved to
    /// `blocks` when `next` catches up to their starting row.
    out_of_order: BTreeMap<u64, Arc<IndexBlock<K>>>,
}

impl<K> IndexLevel<K>
where
    K: DataTrait + ?Sized,
{
    fn new() -> Self {
        Self {
            pending: 0,
            blocks: VecDeque::new(),
            index: 0,
            next: 0,
            out_of_order: BTreeMap::new(),
        }
    }

    /// Adds `block` to the collection of blocks in this level.
    fn received(&mut self, block: Arc<IndexBlock<K>>) {
        debug_assert!(self.pending > 0);
        self.pending -= 1;

        if block.first_row == self.next {
            self.next = block.rows().end;
            self.blocks.push_back(block);
            while let Some(entry) = self.out_of_order.first_entry() {
                if *entry.key() != self.next {
                    break;
                }
                let block = entry.remove();
                self.next = block.rows().end;
                self.blocks.push_back(block);
            }
        } else if block.first_row > self.next {
            self.out_of_order.insert(block.first_row, block);
        } else {
            // File corruption or (more likely) a bug.
            todo!()
        }
    }

    /// Returns the next [TreeNode] to read in the level below this one, or
    /// `None` if we've exhausted this level or there are none to read yet.
    /// (Use [eof](Self::eof) to distinguish the meanings of `None`.)
    fn child(&self) -> Result<Option<TreeNode>, Error> {
        self.blocks
            .front()
            .map(|block| block.get_child(self.index))
            .transpose()
    }

    fn next_child(&mut self) {
        let block = self.blocks.front().unwrap();
        self.index += 1;
        if self.index >= block.n_children() {
            self.blocks.pop_front();
            self.index = 0;
        }
    }

    fn is_full(&self, level: usize) -> bool {
        self.pending + self.blocks.len() + self.out_of_order.len() >= 1 << level
    }
}

pub struct Multifetch0<'a, 'b, K, A, N, T>
where
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
{
    reader: &'a Reader<T>,
    keys: &'b DynVec<K>,
    cache: Arc<BufferCache>,
    factories: Factories<K, A>,

    receiver: Receiver<MultifetchReadResults>,
    sender: Sender<MultifetchReadResults>,

    tmp_key: Box<K>,
    tmp_key2: Box<K>,
    output: Box<DynVec<K>>,
    row_groups: Vec<Range<u64>>,

    pending: usize,
    _phantom: PhantomData<fn(&N)>,
}

impl<'a, 'b, K, A, N, T> Multifetch0<'a, 'b, K, A, N, T>
where
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
    T: ColumnSpec,
{
    fn new(reader: &'a Reader<T>, keys: &'b DynVec<K>) -> Result<Self, Error> {
        let (sender, receiver) = channel();
        let factories = reader.columns[0].factories.factories();
        let output = factories.keys_factory.default_box();
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
            output,
            row_groups: Vec::new(),
            pending: 0,
            _phantom: PhantomData,
        };
        if !keys.is_empty() {
            if let Some(node) = &reader.columns[0].root {
                let mut reads = Vec::new();
                this.try_read(MultifetchRead::new(0..keys.len(), node.clone()), &mut reads)?;
                this.start_reads(reads);
            }
        }
        Ok(this)
    }

    pub fn is_done(&self) -> bool {
        self.pending == 0
    }

    fn finish(&mut self) {
        debug_assert!(self.is_done());
        self.output.sort_unstable();
        self.row_groups.sort_unstable_by_key(|rows| rows.start);
    }

    pub fn results(mut self) -> (Box<DynVec<K>>, Vec<Range<u64>>) {
        self.finish();
        (self.output, self.row_groups)
    }

    pub fn wait(&mut self) -> Result<(), Error> {
        if !self.is_done() {
            let mut reads = Vec::new();
            self.process_results(self.receiver.recv().unwrap(), &mut reads)?;
            self.run_(reads)?;
        }
        Ok(())
    }

    pub fn run(&mut self) -> Result<(), Error> {
        self.run_(Vec::new())
    }

    fn run_(&mut self, mut reads: Vec<MultifetchRead>) -> Result<(), Error> {
        while let Ok(results) = self.receiver.try_recv() {
            self.process_results(results, &mut reads)?;
        }
        self.start_reads(reads);
        Ok(())
    }

    fn process_results(
        &mut self,
        results: MultifetchReadResults,
        reads: &mut Vec<MultifetchRead>,
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

    fn start_reads(&mut self, reads: Vec<MultifetchRead>) {
        if !reads.is_empty() {
            self.reader.file.file_handle.read_async(
                reads.iter().map(|read| read.node.location).collect(),
                {
                    let sender = self.sender.clone();
                    Box::new(move |results| {
                        let _ = sender.send(MultifetchReadResults { reads, results });
                    })
                },
            );
            self.pending += 1;
        }
    }

    fn try_read(
        &mut self,
        read: MultifetchRead,
        reads: &mut Vec<MultifetchRead>,
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
        reads: &mut Vec<MultifetchRead>,
    ) -> Result<(), Error> {
        match tree_block {
            TreeBlock::Data(data_block) => {
                let mut start = 0;
                for i in key_range.clone() {
                    let key = &self.keys[i];
                    if let Some(child_index) = unsafe {
                        data_block.find_next(&self.factories, &mut self.tmp_key, key, &mut start)
                    } {
                        self.output.push_val(&mut self.tmp_key);
                        if data_block.row_groups.is_some() {
                            self.row_groups.push(data_block.row_group(child_index)?);
                        }
                    }
                    if start >= data_block.n_values() {
                        break;
                    }
                }
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
                            MultifetchRead::new(i..i + n_keys, index_block.get_child(child_index)?);
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
struct MultifetchRead {
    keys: Range<usize>,
    node: TreeNode,
}

impl MultifetchRead {
    fn new(keys: Range<usize>, node: TreeNode) -> Self {
        Self { keys, node }
    }
}

struct MultifetchReadResults {
    reads: Vec<MultifetchRead>,
    results: Vec<Result<Arc<FBuf>, StorageError>>,
}

#[derive(Debug)]
struct Multifetch1Read {
    keys: Rows,
    node: TreeNode,
}

impl Multifetch1Read {
    fn new(keys: Rows, node: TreeNode) -> Self {
        Self { keys, node }
    }
}

struct Multifetch1ReadResults {
    reads: Vec<Multifetch1Read>,
    results: Vec<Result<Arc<FBuf>, StorageError>>,
}

impl<'a, 'b, K, A, NK, NA, NN, T> Multifetch0<'a, 'b, K, A, (&'static NK, &'static NA, NN), T>
where
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
    NK: DataTrait + ?Sized,
    NA: DataTrait + ?Sized,
    T: ColumnSpec,
{
    pub fn next_column(self) -> Result<Multifetch1<'a, K, NK, NA, T>, Error> {
        Multifetch1::new(self)
    }
}

pub struct Multifetch1<'a, K0, K1, A1, T>
where
    K0: DataTrait + ?Sized,
    K1: DataTrait + ?Sized,
    A1: DataTrait + ?Sized,
{
    reader: &'a Reader<T>,
    cache: Arc<BufferCache>,
    factories: Factories<K1, A1>,

    receiver: Receiver<Multifetch1ReadResults>,
    sender: Sender<Multifetch1ReadResults>,

    keys: Box<DynVec<K0>>,
    offs: Vec<usize>,
    vals: Box<DynVec<K1>>,
    diffs: Box<DynVec<A1>>,

    rows: Vec<Range<u64>>,

    /// Next row to append to `vals` and `diffs`.
    next_row: u64,

    out_of_order: BTreeMap<u64, (Rows, Arc<DataBlock<K1, A1>>)>,

    pending: usize,
}

impl<'a, K0, K1, A1, T> Multifetch1<'a, K0, K1, A1, T>
where
    K0: DataTrait + ?Sized,
    K1: DataTrait + ?Sized,
    A1: DataTrait + ?Sized,
    T: ColumnSpec,
{
    fn new<'b, A0, N>(
        mut source: Multifetch0<'a, 'b, K0, A0, (&'static K1, &'static A1, N), T>,
    ) -> Result<Self, Error>
    where
        A0: DataTrait + ?Sized,
    {
        source.finish();

        let factories = source.reader.columns[1].factories.factories();

        let offs = {
            let mut offs = Vec::with_capacity(source.row_groups.len() + 1);
            offs.push(0);
            for row_group in &source.row_groups {
                offs.push(offs.last().unwrap() + (row_group.end - row_group.start) as usize);
            }
            offs
        };

        // Combine contiguous row groups.
        //
        // This could be done in-place with a little extra work.
        let rows = source
            .row_groups
            .into_iter()
            .coalesce(|x, y| {
                if x.end == y.start {
                    Ok(x.start..y.end)
                } else {
                    Err((x, y))
                }
            })
            .collect::<Vec<_>>();

        let (sender, receiver) = channel();
        let mut this = Self {
            reader: source.reader,
            cache: source.cache,
            keys: source.output,
            offs,
            vals: factories.keys_factory.default_box(),
            diffs: factories.auxes_factory.default_box(),
            next_row: rows[0].start,
            rows,
            factories,
            receiver,
            sender,
            out_of_order: BTreeMap::new(),
            pending: 0,
        };
        if !this.rows.is_empty() {
            if let Some(node) = &source.reader.columns[1].root {
                let mut reads = Vec::new();
                this.try_read(
                    Multifetch1Read::new(Rows::new(&this.rows), node.clone()),
                    &mut reads,
                )?;
                this.start_reads(reads);
            }
        }
        Ok(this)
    }

    fn start_reads(&mut self, reads: Vec<Multifetch1Read>) {
        if !reads.is_empty() {
            self.reader.file.file_handle.read_async(
                reads.iter().map(|read| read.node.location).collect(),
                {
                    let sender = self.sender.clone();
                    Box::new(move |results| {
                        let _ = sender.send(Multifetch1ReadResults { reads, results });
                    })
                },
            );
            self.pending += 1;
        }
    }
    fn try_read(
        &mut self,
        read: Multifetch1Read,
        reads: &mut Vec<Multifetch1Read>,
    ) -> Result<(), Error> {
        if let Some(tree_block) =
            TreeBlock::from_cache(&read.node, &self.cache, &*self.reader.file.file_handle).unwrap()
        {
            self.process_read(read.keys, tree_block, reads)?;
        } else {
            reads.push(read);
        }
        Ok(())
    }

    pub fn is_done(&self) -> bool {
        self.pending == 0
    }

    /*
    pub fn results(mut self) -> (Box<DynVec<K>>, Vec<Range<u64>>) {
        debug_assert!(self.is_done());
        todo!()
    }*/

    pub fn wait(&mut self) -> Result<(), Error> {
        if !self.is_done() {
            let mut reads = Vec::new();
            self.process_results(self.receiver.recv().unwrap(), &mut reads)?;
            self.run_(reads)?;
        }
        Ok(())
    }

    pub fn run(&mut self) -> Result<(), Error> {
        self.run_(Vec::new())
    }

    fn run_(&mut self, mut reads: Vec<Multifetch1Read>) -> Result<(), Error> {
        while let Ok(results) = self.receiver.try_recv() {
            self.process_results(results, &mut reads)?;
        }
        self.start_reads(reads);
        Ok(())
    }

    fn process_results(
        &mut self,
        results: Multifetch1ReadResults,
        reads: &mut Vec<Multifetch1Read>,
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
            self.process_read(read.keys, tree_block, reads)?;
        }
        Ok(())
    }

    fn process_data_block(&mut self, rows: Rows, data_block: Arc<DataBlock<K1, A1>>) {
        for row in rows.iter(&self.rows) {
            self.vals
                .push_with(&mut |val| unsafe { data_block.key_for_row(&self.factories, row, val) });
            self.diffs.push_with(&mut |diff| unsafe {
                data_block.aux_for_row(&self.factories, row, diff)
            });
        }
        self.next_row = Rows::next(&self.rows, data_block.rows().end);
    }

    fn process_read(
        &mut self,
        mut rows: Rows,
        tree_block: TreeBlock<K1, A1>,
        reads: &mut Vec<Multifetch1Read>,
    ) -> Result<(), Error> {
        match tree_block {
            TreeBlock::Data(data_block) => {
                let first_row = rows.first(&self.rows).unwrap();
                if first_row == self.next_row {
                    self.process_data_block(rows, data_block);
                    while let Some(first_entry) = self.out_of_order.first_entry() {
                        if &self.next_row != first_entry.key() {
                            break;
                        }
                        let (rows, data_block) = first_entry.remove();
                        self.process_data_block(rows, data_block);
                    }
                } else {
                    self.out_of_order.insert(first_row, (rows, data_block));
                }
            }
            TreeBlock::Index(index_block) => {
                while let Some(first_row) = rows.first(&self.rows) {
                    let child_idx = index_block.find_row(first_row)?;
                    let child_rows;
                    (child_rows, rows) =
                        rows.split(&self.rows, index_block.get_rows(child_idx).end);
                    let read = Multifetch1Read::new(child_rows, index_block.get_child(child_idx)?);
                    self.try_read(read, reads)?;
                }
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default)]
struct Rows {
    before: Option<Range<u64>>,
    middle: Range<usize>,
    after: Option<Range<u64>>,
}

impl Rows {
    pub fn empty() -> Self {
        Self::default()
    }

    #[allow(dead_code)]
    fn check_invariants(&self, _rows: &[Range<u64>]) {
        #[cfg(debug_assertions)]
        {
            if let Some(before) = &self.before {
                assert!(!before.is_empty());
                if !self.middle.is_empty() {
                    assert!(before.end < _rows[self.middle.start].start);
                }
                if let Some(after) = &self.after {
                    assert!(before.end < after.start);
                }
            }
            if let Some(after) = &self.after {
                assert!(!after.is_empty());
                if !self.middle.is_empty() {
                    assert!(_rows[self.middle.end - 1].end < after.start);
                }
            }
        }
    }

    pub fn new(rows: &[Range<u64>]) -> Self {
        #[cfg(debug_assertions)]
        {
            for range in rows {
                assert!(!range.is_empty());
            }
            for i in 1..rows.len() {
                assert!(rows[i - 1].end < rows[i].start);
            }
        }

        let this = Self {
            before: None,
            middle: 0..rows.len(),
            after: None,
        };
        this.check_invariants(rows);
        this
    }

    pub fn is_empty(&self) -> bool {
        self.before.is_none() && self.middle.is_empty() && self.after.is_none()
    }

    /// Equivalent to `self.iter(rows).next()`.
    pub fn first(&self, rows: &[Range<u64>]) -> Option<u64> {
        if let Some(before) = &self.before {
            Some(before.start)
        } else if !self.middle.is_empty() {
            Some(rows[self.middle.start].start)
        } else if let Some(after) = &self.after {
            Some(after.start)
        } else {
            None
        }
    }

    /// Returns the smallest row within the ranges in `rows` that is greater
    /// than or equal to `row`, or `row` if `row` is greater than all of the
    /// rows in `rows`.
    pub fn next(rows: &[Range<u64>], row: u64) -> u64 {
        match rows.binary_search_by_key(&row, |range| range.start) {
            Ok(_) => row,
            Err(0) if rows.is_empty() => row,
            Err(0) => rows[0].start,
            Err(index) if row < rows[index - 1].end => row,
            Err(index) if index < rows.len() => rows[index].start,
            _ => row,
        }
    }

    pub fn iter<'a>(&self, rows: &'a [Range<u64>]) -> RowsIter<'a> {
        RowsIter::new(self, rows)
    }

    /// Splits this set of rows into two at `row`.  Returns the rows before
    /// `row` and the rest as new `Rows`.
    pub fn split(self, rows: &[Range<u64>], row: u64) -> (Rows, Rows) {
        fn split_range(range: &Range<u64>, row: u64) -> (Option<Range<u64>>, Option<Range<u64>>) {
            if row == range.start {
                (None, Some(range.clone()))
            } else if row == range.end {
                (Some(range.clone()), None)
            } else {
                debug_assert!(range.contains(&row));
                (Some(range.start..row), Some(row..range.end))
            }
        }

        if let Some(before) = &self.before {
            if row < before.start {
                return (Self::empty(), self);
            } else if row <= before.end {
                let split = split_range(before, row);
                return (
                    Self {
                        before: split.0,
                        ..Self::empty()
                    },
                    Self {
                        before: split.1,
                        ..self
                    },
                );
            }
        }

        if let Some(after) = &self.after {
            if row >= after.end {
                return (self, Self::empty());
            } else if row >= after.start {
                let split = split_range(after, row);
                return (
                    Self {
                        after: split.0,
                        ..self
                    },
                    Self {
                        after: split.1,
                        ..Self::empty()
                    },
                );
            }
        }

        match rows.binary_search_by_key(&row, |range| range.start) {
            Ok(index) => (
                Self {
                    before: self.before,
                    middle: self.middle.start..index,
                    after: None,
                },
                Self {
                    before: None,
                    middle: index..self.middle.end,
                    after: self.after,
                },
            ),
            Err(0) => (
                Self {
                    before: self.before,
                    middle: 0..0,
                    after: None,
                },
                Self {
                    before: None,
                    ..self
                },
            ),
            Err(index) if row >= rows[index - 1].end => (
                Self {
                    before: self.before,
                    middle: self.middle.start..index,
                    after: None,
                },
                Self {
                    before: None,
                    middle: index..self.middle.end,
                    after: self.after,
                },
            ),
            Err(index) => (
                Self {
                    before: self.before,
                    middle: self.middle.start..index - 1,
                    after: Some(rows[index - 1].start..row),
                },
                Self {
                    before: Some(row..rows[index - 1].end),
                    middle: index..self.middle.end,
                    after: self.after,
                },
            ),
        }
    }
}

struct RowsIter<'a> {
    rows: Rows,
    range: Range<u64>,
    ranges: &'a [Range<u64>],
}

impl<'a> Iterator for RowsIter<'a> {
    type Item = u64;

    fn next(&mut self) -> Option<Self::Item> {
        if self.range.is_empty() {
            if let Some(before) = self.rows.before.take() {
                self.range = before;
            } else if !self.rows.middle.is_empty() {
                self.range = self.ranges[self.rows.middle.start].clone();
                self.rows.middle.start += 1;
            } else if let Some(after) = self.rows.after.take() {
                self.range = after;
            } else {
                return None;
            }
        }

        debug_assert!(!self.range.is_empty());
        let row = self.range.start;
        self.range.start += 1;
        Some(row)
    }
}

impl<'a> RowsIter<'a> {
    fn new(rows: &Rows, ranges: &'a [Range<u64>]) -> Self {
        Self {
            rows: rows.clone(),
            range: 0..0,
            ranges,
        }
    }
}

fn intersect<T>(a: &Range<T>, b: &Range<T>) -> Option<Range<T>>
where
    T: Copy + Ord + Default,
{
    if a.contains(&b.start) {
        Some(b.start..min(a.end, b.end))
    } else if b.contains(&a.start) {
        Some(a.start..min(a.end, b.end))
    } else {
        None
    }
}

#[cfg(test)]
mod test {
    use std::ops::Range;

    use itertools::Itertools;

    use crate::storage::file::reader::{intersect, Rows};

    #[test]
    fn intersection() {
        let a = 5..10;
        assert_eq!(intersect(&a, &(3..7)), Some(5..7));
        assert_eq!(intersect(&a, &(7..12)), Some(7..10));
        assert_eq!(intersect(&a, &(0..3)), None);
        assert_eq!(intersect(&a, &(13..15)), None);
        assert_eq!(intersect(&a, &(6..8)), Some(6..8));
        assert_eq!(intersect(&a, &(3..12)), Some(5..10));
    }

    fn check_rows(rows: &Rows, ranges: &[Range<u64>], mut expected: u32) {
        rows.check_invariants(ranges);
        let mut actual = 0;
        for row in rows.iter(&ranges) {
            assert!((0..32).contains(&row));
            actual |= 1 << row;
        }
        assert_eq!(actual, expected);
    }

    #[test]
    fn rows() {
        for pattern in 0..4096u32 {
            let ranges = (0..12)
                .map(|index| (pattern & (1 << index)) != 0)
                .enumerate()
                .dedup_by_with_count(|a, b| a.1 == b.1)
                .filter_map(|(count, (offset, value))| {
                    value.then(|| offset as u64..offset as u64 + count as u64)
                })
                .collect::<Vec<_>>();

            let rows = Rows::new(&ranges);
            check_rows(&rows, &ranges, pattern);
            assert_eq!(
                rows.first(&ranges),
                (pattern != 0).then(|| pattern.trailing_zeros() as u64)
            );
            assert_eq!(rows.is_empty(), pattern == 0);

            check_rows(&rows, &ranges, pattern);

            for i in 0..=12 {
                let (a, b) = rows.clone().split(&ranges, i);
                check_rows(&a, &ranges, ((1 << i) - 1) & pattern);
                check_rows(&b, &ranges, !((1 << i) - 1) & pattern);
            }

            for i in 0..=12 {
                let remaining = !((1 << i) - 1) & pattern;
                let next = if remaining != 0 {
                    remaining.trailing_zeros()
                } else {
                    i
                };
                assert_eq!(Rows::next(&ranges, i as u64), next as u64);
            }
        }
    }
}
