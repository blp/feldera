use crate::{
    algebra::{AddAssignByRef, AddByRef, HasZero, MonoidValue, NegByRef},
    time::AntichainRef,
    trace::{
        layers::{
            column_leaf::{
                ColumnLeafConsumer, ColumnLeafCursor, ColumnLeafValues, OrderedColumnLeaf,
                OrderedColumnLeafBuilder,
            },
            Builder as TrieBuilder, Cursor as TrieCursor, MergeBuilder, Trie, TupleBuilder,
        },
        ord::merge_batcher::MergeBatcher,
        Batch, BatchReader, Builder, Consumer, Cursor, Merger, ValueConsumer,
    },
    NumEntries,
};
use size_of::SizeOf;
use std::{
    cmp::max,
    fmt::{self, Debug, Display},
    ops::{Add, AddAssign, Neg},
    rc::Rc,
};

/// An immutable collection of `(key, weight)` pairs without timing information.
#[derive(Debug, Clone, Eq, PartialEq, SizeOf)]
pub struct OrdZSet<K, R> {
    /// Where all the dataz is.
    pub layer: OrderedColumnLeaf<K, R>,
}

impl<K, R> Display for OrdZSet<K, R>
where
    K: Ord + Clone + Display,
    R: Eq + HasZero + AddAssign + AddAssignByRef + Clone + Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "layer:\n{}",
            textwrap::indent(&self.layer.to_string(), "    ")
        )
    }
}

impl<K, R> From<OrderedColumnLeaf<K, R>> for OrdZSet<K, R> {
    fn from(layer: OrderedColumnLeaf<K, R>) -> Self {
        Self { layer }
    }
}

impl<K, R> From<OrderedColumnLeaf<K, R>> for Rc<OrdZSet<K, R>> {
    fn from(layer: OrderedColumnLeaf<K, R>) -> Self {
        Rc::new(From::from(layer))
    }
}

impl<K, R> NumEntries for OrdZSet<K, R>
where
    K: Ord + Clone,
    R: Eq + HasZero + AddAssign + AddAssignByRef + Clone,
{
    const CONST_NUM_ENTRIES: Option<usize> = <OrderedColumnLeaf<K, R>>::CONST_NUM_ENTRIES;

    fn num_entries_shallow(&self) -> usize {
        self.layer.num_entries_shallow()
    }

    fn num_entries_deep(&self) -> usize {
        self.layer.num_entries_deep()
    }
}

impl<K, R> Default for OrdZSet<K, R> {
    fn default() -> Self {
        Self {
            layer: OrderedColumnLeaf::empty(),
        }
    }
}

impl<K, R> NegByRef for OrdZSet<K, R>
where
    K: Ord + Clone,
    R: MonoidValue + NegByRef,
{
    fn neg_by_ref(&self) -> Self {
        Self {
            layer: self.layer.neg_by_ref(),
        }
    }
}

impl<K, R> Neg for OrdZSet<K, R>
where
    K: Ord + Clone,
    R: MonoidValue + Neg<Output = R>,
{
    type Output = Self;

    fn neg(self) -> Self {
        Self {
            layer: self.layer.neg(),
        }
    }
}

// TODO: by-value merge
impl<K, R> Add<Self> for OrdZSet<K, R>
where
    K: Ord + Clone + 'static,
    R: MonoidValue,
{
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        Self {
            layer: self.layer.add(rhs.layer),
        }
    }
}

impl<K, R> AddAssign<Self> for OrdZSet<K, R>
where
    K: Ord + Clone + 'static,
    R: MonoidValue,
{
    fn add_assign(&mut self, rhs: Self) {
        self.layer.add_assign(rhs.layer);
    }
}

impl<K, R> AddAssignByRef for OrdZSet<K, R>
where
    K: Ord + Clone + 'static,
    R: MonoidValue,
{
    fn add_assign_by_ref(&mut self, rhs: &Self) {
        self.layer.add_assign_by_ref(&rhs.layer);
    }
}

impl<K, R> AddByRef for OrdZSet<K, R>
where
    K: Ord + Clone + 'static,
    R: MonoidValue,
{
    fn add_by_ref(&self, rhs: &Self) -> Self {
        Self {
            layer: self.layer.add_by_ref(&rhs.layer),
        }
    }
}

impl<K, R> BatchReader for OrdZSet<K, R>
where
    K: Ord + Clone + 'static,
    R: MonoidValue,
{
    type Key = K;
    type Val = ();
    type Time = ();
    type R = R;
    type Cursor<'s> = OrdZSetCursor<'s, K, R>;
    type Consumer = OrdZSetConsumer<K, R>;

    #[inline]
    fn cursor(&self) -> Self::Cursor<'_> {
        OrdZSetCursor {
            valid: true,
            cursor: self.layer.cursor(),
        }
    }

    #[inline]
    fn consumer(self) -> Self::Consumer {
        OrdZSetConsumer {
            consumer: ColumnLeafConsumer::from(self.layer),
        }
    }

    #[inline]
    fn key_count(&self) -> usize {
        self.layer.keys()
    }

    #[inline]
    fn len(&self) -> usize {
        self.layer.tuples()
    }

    #[inline]
    fn lower(&self) -> AntichainRef<'_, ()> {
        AntichainRef::new(&[()])
    }

    #[inline]
    fn upper(&self) -> AntichainRef<'_, ()> {
        AntichainRef::empty()
    }
}

impl<K, R> Batch for OrdZSet<K, R>
where
    K: Ord + Clone + SizeOf + 'static,
    R: MonoidValue + SizeOf,
{
    type Item = K;
    type Batcher = MergeBatcher<K, (), R, Self>;
    type Builder = OrdZSetBuilder<K, R>;
    type Merger = OrdZSetMerger<K, R>;

    fn item_from(key: K, _val: ()) -> Self::Item {
        key
    }

    fn from_keys(time: Self::Time, keys: Vec<(Self::Key, Self::R)>) -> Self {
        Self::from_tuples(time, keys)
    }

    fn begin_merge(&self, other: &Self) -> Self::Merger {
        OrdZSetMerger::new_merger(self, other)
    }

    fn recede_to(&mut self, _frontier: &()) {}

    fn empty(_time: Self::Time) -> Self {
        Self {
            layer: OrderedColumnLeaf::empty(),
        }
    }
}

/// State for an in-progress merge.
#[derive(SizeOf)]
pub struct OrdZSetMerger<K, R>
where
    K: Ord + Clone + 'static,
    R: MonoidValue,
{
    // result that we are currently assembling.
    result: <OrderedColumnLeaf<K, R> as Trie>::MergeBuilder,
}

impl<K, R> Merger<K, (), (), R, OrdZSet<K, R>> for OrdZSetMerger<K, R>
where
    Self: SizeOf,
    K: Ord + Clone + SizeOf + 'static,
    R: MonoidValue + SizeOf,
{
    fn new_merger(batch1: &OrdZSet<K, R>, batch2: &OrdZSet<K, R>) -> Self {
        Self {
            result:
                <<OrderedColumnLeaf<K, R> as Trie>::MergeBuilder as MergeBuilder>::with_capacity(
                    &batch1.layer,
                    &batch2.layer,
                ),
        }
    }

    fn done(self) -> OrdZSet<K, R> {
        OrdZSet {
            layer: self.result.done(),
        }
    }

    fn work(&mut self, source1: &OrdZSet<K, R>, source2: &OrdZSet<K, R>, fuel: &mut isize) {
        *fuel -= self
            .result
            .push_merge(source1.layer.cursor(), source2.layer.cursor()) as isize;
        *fuel = max(*fuel, 1);
    }
}

/// A cursor for navigating a single layer.
#[derive(Debug, SizeOf)]
pub struct OrdZSetCursor<'s, K, R>
where
    K: Ord + Clone,
    R: MonoidValue,
{
    valid: bool,
    cursor: ColumnLeafCursor<'s, K, R>,
}

impl<'s, K, R> Cursor<'s, K, (), (), R> for OrdZSetCursor<'s, K, R>
where
    K: Ord + Clone,
    R: MonoidValue,
{
    #[inline]
    fn key(&self) -> &K {
        self.cursor.current_key()
    }

    #[inline]
    fn val(&self) -> &() {
        &()
    }

    #[inline]
    fn map_times<L: FnMut(&(), &R)>(&mut self, mut logic: L) {
        if self.cursor.valid() {
            logic(&(), self.cursor.current_diff());
        }
    }

    #[inline]
    fn map_times_through<L: FnMut(&(), &R)>(&mut self, logic: L, _upper: &()) {
        self.map_times(logic)
    }

    #[inline]
    fn weight(&mut self) -> R {
        debug_assert!(&self.cursor.valid());
        self.cursor.current_diff().clone()
    }

    #[inline]
    fn key_valid(&self) -> bool {
        self.cursor.valid()
    }

    #[inline]
    fn val_valid(&self) -> bool {
        self.valid
    }

    #[inline]
    fn step_key(&mut self) {
        self.cursor.step();
        self.valid = true;
    }

    #[inline]
    fn seek_key(&mut self, key: &K) {
        self.cursor.seek_key(key);
        self.valid = true;
    }

    #[inline]
    fn last_key(&mut self) -> Option<&K> {
        self.cursor.last_key().map(|(k, _)| k)
    }

    #[inline]
    fn step_val(&mut self) {
        self.valid = false;
    }

    #[inline]
    fn seek_val(&mut self, _val: &()) {}

    #[inline]
    fn rewind_keys(&mut self) {
        self.cursor.rewind();
        self.valid = true;
    }

    #[inline]
    fn rewind_vals(&mut self) {
        self.valid = true;
    }
}

/// A builder for creating layers from unsorted update tuples.
#[derive(SizeOf)]
pub struct OrdZSetBuilder<K, R>
where
    K: Ord,
    R: MonoidValue,
{
    builder: OrderedColumnLeafBuilder<K, R>,
}

impl<K, R> Builder<K, (), R, OrdZSet<K, R>> for OrdZSetBuilder<K, R>
where
    Self: SizeOf,
    K: Ord + Clone + SizeOf + 'static,
    R: MonoidValue + SizeOf,
{
    #[inline]
    fn new_builder(_time: ()) -> Self {
        Self {
            builder: OrderedColumnLeafBuilder::new(),
        }
    }

    #[inline]
    fn with_capacity(_time: (), capacity: usize) -> Self {
        Self {
            builder: <OrderedColumnLeafBuilder<K, R> as TupleBuilder>::with_capacity(capacity),
        }
    }

    #[inline]
    fn reserve(&mut self, additional: usize) {
        self.builder.reserve(additional);
    }

    #[inline]
    fn push(&mut self, (key, diff): (K, R)) {
        self.builder.push_tuple((key, diff));
    }

    #[inline(never)]
    fn done(self) -> OrdZSet<K, R> {
        OrdZSet {
            layer: self.builder.done(),
        }
    }
}

#[derive(Debug, SizeOf)]
pub struct OrdZSetConsumer<K, R> {
    consumer: ColumnLeafConsumer<K, R>,
}

impl<K, R> Consumer<K, (), R> for OrdZSetConsumer<K, R> {
    type ValueConsumer<'a> = OrdZSetValueConsumer<'a, K, R>
    where
        Self: 'a;

    #[inline]
    fn key_valid(&self) -> bool {
        self.consumer.key_valid()
    }

    #[inline]
    fn next_key(&mut self) -> (K, Self::ValueConsumer<'_>) {
        let (key, values) = self.consumer.next_key();
        (key, OrdZSetValueConsumer { values })
    }

    #[inline]
    fn seek_key(&mut self, key: &K)
    where
        K: Ord,
    {
        self.consumer.seek_key(key);
    }
}

#[derive(Debug)]
pub struct OrdZSetValueConsumer<'a, K, R> {
    values: ColumnLeafValues<'a, K, R>,
}

impl<'a, K, R> ValueConsumer<'a, (), R> for OrdZSetValueConsumer<'a, K, R> {
    #[inline]
    fn value_valid(&self) -> bool {
        self.values.value_valid()
    }

    #[inline]
    fn next_value(&mut self) -> ((), R) {
        self.values.next_value()
    }
}
