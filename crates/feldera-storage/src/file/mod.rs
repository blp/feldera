//! Layer file.
//!
//! A layer file stores `n > 0` columns of data, each of which has a key type
//! `K[i]` and an auxiliary data type `A[i]`.  Each column is arranged into
//! groups of rows, where column 0 forms a single group and each row in column
//! `i` is associated with a group of one or more rows in column `i + 1` (for
//! `i + 1 < n`).  A group contains sorted, unique values. A group cursor for
//! column `i` can move forward and backward by rows, seek forward and backward
//! by the key type `K[i]` or using a predicate based on `K[i]`, and (when `i +
//! 1 < n`) move to the row group in column `i + 1` associated with the cursor's
//! row.
//!
//! Thus, ignoring the auxiliary data in each column, a 1-column layer file is
//! analogous to `BTreeSet<K[0]>`, a 2-column layer file is analogous to
//! `BTreeMap<K[0], BTreeSet<K[1]>>`, and a 3-column layer file is analogous to
//! `BTreeMap<K[0], BTreeMap<K[1], BTreeSet<K[2]>>>`.
//!
//! For DBSP, it is likely that only 1-, 2, and 3-column layer files matter, and
//! maybe not even 3-column.
//!
//! Layer files are written once in their entirety and immutable thereafter.
//! Therefore, there are APIs for reading and writing layer files, but no API
//! for modifying them.
//!
//! Layer files use [`rkyv`] for serialization and deserialization.
//!
//! The "layer file" name comes from the `ColumnLayer` and `OrderedLayer` data
//! structures used in DBSP and inherited from Differential Dataflow.
//!
//! # Goals
//!
//! Layer files aim to balance read and write performance.  That is, neither
//! should be sacrificed to benefit the other.
//!
//! Row groups should implement indexing efficiently for `O(lg n)` seek by data
//! value and for sequential reads.  It should be possible to disable indexing
//! by data value for workloads that don't require it.[^0]
//!
//! Layer files should support approximate set membership query in `~O(1)`
//! time.[^0]
//!
//! Layer files should support 1 TB data size.
//!
//! Layer files should include data checksums to detect accidental corruption.
//!
//! [^0]: Not yet implemented.
//!
//! # Design
//!
//! Layer files are stored as on-disk trees, one tree per column, with data
//! blocks as leaf nodes and index blocks as interior nodes.  Each tree's
//! branching factor is the number of values per data block and the number of
//! index entries per index block.  Block sizes and the branching factor can be
//! set as [parameters](`writer::Parameters`) at write time.
//!
//! Layer files support variable-length data in all columns.  The layer file
//! writer automatically detects fixed-length data and store it slightly more
//! efficiently.
//!
//! Layer files index and compare data using [`Ord`] and [`Eq`], unlike many
//! data storage libraries that compare data lexicographically as byte arrays.
//! This convenience does prevent layer files from usefully storing only a
//! prefix of large data items plus a pointer to their full content.  In turn,
//! that means that, while layer files don't limit the size of data items, they
//! are always stored in full in index and data blocks, limiting performance for
//! large data.  This could be ameliorated if the layer file's clients were
//! permitted to provide a way to summarize data for comparisons.  The need for
//! this improvement is not yet clear, so it is not yet implemented.

#![warn(missing_docs)]

use binrw::{binrw, BinRead, BinResult, BinWrite, Error as BinError};
use num::FromPrimitive;
use num_derive::FromPrimitive;
use rkyv::{
    ser::serializers::AllocSerializer, with::Inline, AlignedVec, Archive, Archived, Deserialize,
    Infallible, Serialize,
};

pub mod reader;
pub mod writer;

/// Increment this on each incompatible change.
const VERSION_NUMBER: u32 = 1;

#[binrw]
#[derive(Debug)]
struct FileHeader {
    checksum: u32,

    #[brw(magic(b"LFFH"))]
    version: u32,

    n_columns: u32,
}

#[binrw]
#[derive(Debug)]
struct FileTrailer {
    checksum: u32,

    #[brw(magic(b"LFFT"))]
    version: u32,

    #[bw(calc(columns.len() as u32))]
    n_columns: u32,

    #[br(count = n_columns)]
    columns: Vec<FileTrailerColumn>,
}

#[binrw]
#[derive(Debug, Copy, Clone)]
struct FileTrailerColumn {
    index_offset: u64,
    index_size: u32,
    n_rows: u64,
}

#[derive(Copy, Clone, PartialEq, Eq, Debug)]
#[binrw]
#[brw(repr(u8))]
enum NodeType {
    Data = 0,
    Index = 1,
}

trait FixedLen {
    const LEN: usize;
}

#[binrw]
struct IndexBlockHeader {
    checksum: u32,
    #[brw(magic(b"LFIB"))]
    bound_map_offset: u32,
    row_totals_offset: u32,
    child_pointers_offset: u32,
    n_children: u16,
    child_type: NodeType,
    bound_map_varint: Varint,
    row_total_varint: Varint,
    #[brw(align_after = 16)]
    child_pointer_varint: Varint,
}

impl FixedLen for IndexBlockHeader {
    const LEN: usize = 32;
}

#[binrw]
struct DataBlockHeader {
    checksum: u32,
    #[brw(magic(b"LFDB"))]
    n_values: u32,
    value_map_ofs: u32,
    row_groups_ofs: u32,
    #[bw(write_with = Varint::write_opt)]
    #[br(parse_with = Varint::parse_opt)]
    value_map_varint: Option<Varint>,
    #[bw(write_with = Varint::write_opt)]
    #[br(parse_with = Varint::parse_opt)]
    #[brw(align_after = 16)]
    row_group_varint: Option<Varint>,
}

impl FixedLen for DataBlockHeader {
    const LEN: usize = 32;
}

#[derive(Copy, Clone, PartialEq, Eq, FromPrimitive)]
#[binrw]
#[brw(repr(u8))]
enum Varint {
    B8 = 1,
    B16 = 2,
    B24 = 3,
    B32 = 4,
    B48 = 6,
    B64 = 8,
}
impl Varint {
    fn from_max_value(max_value: u64) -> Varint {
        #[allow(clippy::unusual_byte_groupings, clippy::match_overlapping_arm)]
        match max_value {
            ..=0xff => Varint::B8,
            ..=0xffff => Varint::B16,
            ..=0xffff_ff => Varint::B24,
            ..=0xffff_ffff => Varint::B32,
            ..=0xffff_ffff_ffff => Varint::B48,
            _ => Varint::B64,
        }
    }
    fn from_len(len: usize) -> Varint {
        Self::from_max_value(len as u64 - 1)
    }
    fn alignment(&self) -> usize {
        match self {
            Self::B24 => 1,
            Self::B48 => 2,
            _ => *self as usize,
        }
    }
    fn align(&self, offset: usize) -> usize {
        next_multiple_of_pow2(offset, self.alignment())
    }
    fn len(&self) -> usize {
        *self as usize
    }
    fn put(&self, dst: &mut AlignedVec, value: u64) {
        #[allow(clippy::unnecessary_cast)]
        match *self {
            Self::B8 => dst.push(value as u8),
            Self::B16 => dst.extend_from_slice(&(value as u16).to_le_bytes()),
            Self::B24 => dst.extend_from_slice(&(value as u32).to_le_bytes()[..3]),
            Self::B32 => dst.extend_from_slice(&(value as u32).to_le_bytes()),
            Self::B48 => dst.extend_from_slice(&(value as u64).to_le_bytes()[..6]),
            Self::B64 => dst.extend_from_slice(&(value as u64).to_le_bytes()),
        }
    }
    fn get(&self, src: &AlignedVec, offset: usize) -> u64 {
        let mut raw = [0u8; 8];
        raw[..self.len()].copy_from_slice(&src[offset..offset + self.len()]);
        u64::from_le_bytes(raw)
    }
    #[binrw::parser(reader, endian)]
    fn parse_opt() -> BinResult<Option<Varint>> {
        let byte: u8 = <_>::read_options(reader, endian, ())?;
        match byte {
            0 => Ok(None),
            _ => match FromPrimitive::from_u8(byte) {
                Some(varint) => Ok(Some(varint)),
                None => Err(BinError::NoVariantMatch {
                    pos: reader.stream_position()? - 1,
                }),
            },
        }
    }
    #[binrw::writer(writer, endian)]
    fn write_opt(value: &Option<Varint>) -> BinResult<()> {
        value
            .map_or(0, |varint| varint as u8)
            .write_options(writer, endian, ())
    }
}

// Rounds up `offset` to the next multiple of `alignment`, which must be a power
// of 2.  This is equivalent to `offset.next_multiple(alignment)` except for the
// assumption about `alignment` being a power of 2, which allows it to be faster
// and smaller in the case where the compiler can't see the power-of-2 property.
fn next_multiple_of_pow2(offset: usize, alignment: usize) -> usize {
    let mask = alignment - 1;
    (offset + mask) & !mask
}

#[derive(Copy, Clone, Debug)]
struct InvalidBlockLocation {
    offset: u64,
    size: usize,
}

/// A block in a layer file.
///
/// Used for error reporting.
#[derive(Copy, Clone, Debug)]
struct BlockLocation {
    /// Byte offset, a multiple of 4096.
    offset: u64,

    /// Size in bytes, a power of 2 between 4096 and `2**31`.
    size: usize,
}

impl BlockLocation {
    fn new(offset: u64, size: usize) -> Result<Self, InvalidBlockLocation> {
        if (offset & 0xfff) != 0 || !(4096..=1 << 31).contains(&size) || !size.is_power_of_two() {
            Err(InvalidBlockLocation { offset, size })
        } else {
            Ok(Self { offset, size })
        }
    }
}

impl TryFrom<u64> for BlockLocation {
    type Error = InvalidBlockLocation;

    fn try_from(source: u64) -> Result<Self, Self::Error> {
        Self::new((source & !0x1f) << 7, 1 << (source & 0x1f))
    }
}

impl From<BlockLocation> for u64 {
    fn from(source: BlockLocation) -> Self {
        let shift = source.size.trailing_zeros() as u64;
        (source.offset >> 7) | shift
    }
}

/// Trait for data that can be serialized and deserialized with [`rkyv`].
pub trait Rkyv: Archive + for<'a> Serialize<Serializer<'a>> + Deserializable {}
impl<T> Rkyv for T where T: Archive + for<'a> Serialize<Serializer<'a>> + Deserializable {}

/// Trait for data that can be deserialized with [`rkyv`].
pub trait Deserializable: Archive<Archived = Self::ArchivedDeser> + Sized {
    /// Deserialized type.
    type ArchivedDeser: Deserialize<Self, Deserializer>;
}
impl<T: Archive> Deserializable for T
where
    Archived<T>: Deserialize<T, Deserializer>,
{
    type ArchivedDeser = Archived<T>;
}

/// The particular [`rkyv::ser::Serializer`] that we use.
pub type Serializer<'a> = AllocSerializer<1024>;

/// The particular [`rkyv`] deserializer that we use.
pub type Deserializer = Infallible;

#[derive(Archive, Serialize)]
struct Item<'a, K, A>(#[with(Inline)] &'a K, #[with(Inline)] &'a A)
where
    K: Rkyv,
    A: Rkyv;

#[cfg(test)]
mod test {
    use std::fs::File;

    use super::{
        reader::Reader,
        writer::{Parameters, Writer2},
    };

    #[test]
    fn test() {
        let mut layer_file =
            Writer2::new(File::create("file.layer").unwrap(), Parameters::default()).unwrap();
        let end = 1000_i32;
        let range = (0..end).step_by(2);
        println!("start");
        let c2range = (0..14_i32).step_by(2);
        let a1 = 0x1111_u64;
        let a2 = 0x2222_u64;
        for i in range.clone() {
            for j in c2range.clone() {
                layer_file.write1((&j, &a2)).unwrap();
            }
            layer_file.write0((&i, &a1)).unwrap();
        }
        println!("written");
        layer_file.close().unwrap();

        let reader =
            Reader::<(i32, u64, (i32, u64, ()))>::new(File::open("file.layer").unwrap()).unwrap();
        for i in range.clone() {
            if i % (end / 16) == 0 {
                println!("{i}");
            }
            let mut cursor = reader.rows().first().unwrap();
            unsafe { cursor.advance_to_value_or_larger(&i) }.unwrap();
            assert_eq!(unsafe { cursor.item() }, Some((i, a1)));

            let mut cursor = reader.rows().first().unwrap();
            unsafe { cursor.advance_to_value_or_larger(&(i - 1)) }.unwrap();
            assert_eq!(unsafe { cursor.item() }, Some((i, a1)));

            let mut cursor = reader.rows().first().unwrap();
            unsafe { cursor.seek_forward_until(|key| key >= &i) }.unwrap();
            assert_eq!(unsafe { cursor.item() }, Some((i, a1)));

            let mut cursor = reader.rows().first().unwrap();
            unsafe { cursor.seek_forward_until(|key| key >= &(i - 1)) }.unwrap();
            assert_eq!(unsafe { cursor.item() }, Some((i, a1)));

            let mut cursor = reader.rows().last().unwrap();
            unsafe { cursor.rewind_to_value_or_smaller(&i) }.unwrap();
            assert_eq!(unsafe { cursor.item() }, Some((i, a1)));

            let mut cursor = reader.rows().last().unwrap();
            unsafe { cursor.rewind_to_value_or_smaller(&(i + 1)) }.unwrap();
            assert_eq!(unsafe { cursor.item() }, Some((i, a1)));

            let mut cursor = reader.rows().last().unwrap();
            unsafe { cursor.seek_backward_until(|key| key <= &i) }.unwrap();
            assert_eq!(unsafe { cursor.item() }, Some((i, a1)));

            let mut cursor = reader.rows().last().unwrap();
            unsafe { cursor.seek_backward_until(|key| key <= &(i + 1)) }.unwrap();
            assert_eq!(unsafe { cursor.item() }, Some((i, a1)));

            for j in c2range.clone() {
                let mut c2cursor = cursor.next_column().unwrap().first().unwrap();
                unsafe { c2cursor.advance_to_value_or_larger(&j) }.unwrap();
                assert_eq!(unsafe { c2cursor.item() }, Some((j, a2)));

                let mut c2cursor = cursor.next_column().unwrap().first().unwrap();
                unsafe { c2cursor.advance_to_value_or_larger(&(j - 1)) }.unwrap();
                assert_eq!(unsafe { c2cursor.item() }, Some((j, a2)));

                let mut c2cursor = cursor.next_column().unwrap().first().unwrap();
                unsafe { c2cursor.seek_forward_until(|key| key >= &j) }.unwrap();
                assert_eq!(unsafe { c2cursor.item() }, Some((j, a2)));

                let mut c2cursor = cursor.next_column().unwrap().first().unwrap();
                unsafe { c2cursor.seek_forward_until(|key| key >= &(j - 1)) }.unwrap();
                assert_eq!(unsafe { c2cursor.item() }, Some((j, a2)));

                let mut c2cursor = cursor.next_column().unwrap().last().unwrap();
                unsafe { c2cursor.rewind_to_value_or_smaller(&j) }.unwrap();
                assert_eq!(unsafe { c2cursor.item() }, Some((j, a2)));

                let mut c2cursor = cursor.next_column().unwrap().last().unwrap();
                unsafe { c2cursor.rewind_to_value_or_smaller(&(j + 1)) }.unwrap();
                assert_eq!(unsafe { c2cursor.item() }, Some((j, a2)));

                let mut c2cursor = cursor.next_column().unwrap().last().unwrap();
                unsafe { c2cursor.seek_backward_until(|key| key <= &j) }.unwrap();
                assert_eq!(unsafe { c2cursor.item() }, Some((j, a2)));

                let mut c2cursor = cursor.next_column().unwrap().last().unwrap();
                unsafe { c2cursor.seek_backward_until(|key| key <= &(j + 1)) }.unwrap();
                assert_eq!(unsafe { c2cursor.item() }, Some((j, a2)));
            }
        }

        let mut cursor = reader.rows().first().unwrap();
        unsafe { cursor.advance_to_value_or_larger(&end) }.unwrap();
        assert_eq!(unsafe { cursor.item() }, None);

        let mut cursor = reader.rows().first().unwrap();
        unsafe { cursor.seek_forward_until(|key| key >= &end) }.unwrap();
        assert_eq!(unsafe { cursor.item() }, None);

        let mut cursor = reader.rows().last().unwrap();
        unsafe { cursor.rewind_to_value_or_smaller(&-1) }.unwrap();
        assert_eq!(unsafe { cursor.item() }, None);

        let mut cursor = reader.rows().last().unwrap();
        unsafe { cursor.seek_backward_until(|key| key <= &-1) }.unwrap();
        assert_eq!(unsafe { cursor.item() }, None);
    }
}
