use std::cmp::Ordering;

use crate::{
    algebra::Lattice,
    dynamic::{DynDataTyped, DynWeightedPairs, WeightTrait},
    time::Timestamp,
    trace::{
        cursor::{Pending, PushCursor},
        spine_async::index_set::IndexSet,
        Batch, BatchFactories, BatchReaderFactories, Builder, Filter, Weight,
    },
};

pub struct PushMerger<C, B>
where
    C: PushCursor<B::Key, B::Val, B::Time, B::R>,
    B: Batch,
{
    cursors: Vec<C>,
    key_filter: Option<Filter<B::Key>>,
    value_filter: Option<Filter<B::Val>>,
    any_values: bool,
    tmp_weight: Box<B::R>,
    time_diffs: Option<Box<DynWeightedPairs<DynDataTyped<B::Time>, B::R>>>,
}

impl<C, B> PushMerger<C, B>
where
    C: PushCursor<B::Key, B::Val, B::Time, B::R>,
    B: Batch,
{
    /// Creates a new merger for `cursors`.
    pub fn new(
        factories: &B::Factories,
        cursors: Vec<C>,
        key_filter: Option<Filter<B::Key>>,
        value_filter: Option<Filter<B::Val>>,
    ) -> Self {
        assert!(cursors.len() <= 64);
        Self {
            cursors,
            key_filter,
            value_filter,
            any_values: false,
            tmp_weight: factories.weight_factory().default_box(),
            time_diffs: factories.time_diffs_factory().map(|f| f.default_box()),
        }
    }

    fn is_done(&self) -> bool {
        self.cursors.iter().all(|cursor| cursor.key() == Ok(None))
    }

    fn is_ready(&self) -> bool {
        self.cursors.iter().all(|cursor| cursor.key().is_ok())
    }

    fn work(&mut self, builder: &mut B::Builder, frontier: &B::Time) {
        let _ = self.work_(builder, frontier);
    }

    fn work_(&mut self, builder: &mut B::Builder, frontier: &B::Time) -> Result<(), Pending> {
        // We can drop all the cursors whose keys are at EOI.  If that
        // eliminates all of them, we're all done.  If any keys are pending,
        // then we can't do any work.
        assert!(self.cursors.len() <= 64);
        let mut remaining_cursors = IndexSet::empty();
        for (index, cursor) in self.cursors.iter_mut().enumerate() {
            skip_filtered_keys(cursor, &self.key_filter, &self.value_filter)?;
            if cursor.key()?.is_some() {
                remaining_cursors.add(index);
            }
        }
        if remaining_cursors.is_empty() {
            return Ok(());
        }

        let advance_func = |t: &mut DynDataTyped<B::Time>| t.join_assign(frontier);

        let time_map_func = if frontier == &B::Time::minimum() {
            None
        } else {
            Some(&advance_func as &dyn Fn(&mut DynDataTyped<B::Time>))
        };

        // As long as there are multiple cursors...
        while remaining_cursors.is_long() {
            // Find the indexes of the cursors with minimum keys, among the
            // remaining cursors.
            let orig_min_keys = find_min_indexes(
                remaining_cursors
                    .into_iter()
                    .map(|index| (index, self.cursors[index].key().unwrap())),
            );

            // As long as there is more than one cursor with minimum keys...
            let mut min_keys = orig_min_keys;
            while min_keys.is_long() {
                // ...Find the indexes of the cursors with minimum values, among
                // those with minimum keys, and copy their time-diff pairs and
                // value into the output.
                let min_vals = find_min_indexes(
                    min_keys
                        .into_iter()
                        .map(|index| (index, self.cursors[index].val().unwrap())),
                );
                self.any_values =
                    self.copy_times(builder, time_map_func, min_vals) || self.any_values;

                // Then go on to the next value in each cursor, dropping the keys
                // for which we've exhausted the values.
                for index in min_vals {
                    self.cursors[index].step_val();
                    skip_filtered_values(&mut self.cursors[index], &self.value_filter)?;
                    if self.cursors[index].val()?.is_none() {
                        min_keys.remove(index);
                    }
                }
            }

            // If there's exactly one cursor left with minimum key, copy its
            // values into the output.
            if let Some(index) = min_keys.first() {
                loop {
                    self.any_values =
                        self.copy_times(builder, time_map_func, min_keys) || self.any_values;
                    self.cursors[index].step_val();
                    skip_filtered_values(&mut self.cursors[index], &self.value_filter)?;
                    if self.cursors[index].val()?.is_none() {
                        break;
                    }
                }
            }

            // If we wrote any values for these minimum keys, write the key.
            if self.any_values {
                let index = orig_min_keys.first().unwrap();
                builder.push_key(self.cursors[index].key().unwrap().unwrap());
                self.any_values = false;
            }

            // Advance each minimum-key cursor, dropping the cursors for which
            // we've exhausted the data.
            for index in orig_min_keys {
                self.cursors[index].step_key();
                skip_filtered_keys(
                    &mut self.cursors[index],
                    &self.key_filter,
                    &self.value_filter,
                )?;
                if self.cursors[index].key()?.is_none() {
                    remaining_cursors.remove(index);
                }
            }
        }

        // If there is a cursor left (there's either one or none), copy it
        // directly to the output.
        if let Some(index) = remaining_cursors.first() {
            loop {
                loop {
                    self.any_values = self.copy_times(builder, time_map_func, remaining_cursors)
                        || self.any_values;
                    self.cursors[index].step_val();
                    skip_filtered_values(&mut self.cursors[index], &self.value_filter)?;
                    if self.cursors[index].val()?.is_none() {
                        break;
                    }
                }
                debug_assert!(time_map_func.is_some() || self.any_values, "This assertion should fail only if B::Cursor is a spine or a CursorList, but we shouldn't be merging those");
                if self.any_values {
                    self.any_values = false;
                    builder.push_key(self.cursors[index].key().unwrap().unwrap());
                }
                self.cursors[index].step_key();
                skip_filtered_keys(
                    &mut self.cursors[index],
                    &self.key_filter,
                    &self.value_filter,
                )?;
                if self.cursors[index].key()?.is_none() {
                    break;
                }
            }
        }
        Ok(())
    }

    fn copy_times(
        &mut self,
        builder: &mut B::Builder,
        map_func: Option<&dyn Fn(&mut DynDataTyped<B::Time>)>,
        indexes: IndexSet,
    ) -> bool {
        // If this is a timed batch, we must consolidate the (time, weight) array; otherwise we
        // simply compute the total weight of the current value.
        if let Some(time_diffs) = &mut self.time_diffs {
            if let Some(map_func) = map_func {
                time_diffs.clear();
                for i in indexes {
                    self.cursors[i].map_times(&mut |time, w| {
                        let mut time: B::Time = time.clone();
                        map_func(&mut time);

                        time_diffs.push_refs((&time, w));
                    });
                }
                time_diffs.consolidate();
                if time_diffs.is_empty() {
                    return false;
                }
                for (time, diff) in time_diffs.dyn_iter().map(|td| td.split()) {
                    builder.push_time_diff(time, diff);
                }
            } else if indexes.is_long() {
                time_diffs.clear();
                for i in indexes {
                    self.cursors[i].map_times(&mut |time, w| {
                        time_diffs.push_refs((time, w));
                    });
                }
                time_diffs.consolidate();
                if time_diffs.is_empty() {
                    return false;
                }
                for (time, diff) in time_diffs.dyn_iter().map(|td| td.split()) {
                    builder.push_time_diff(time, diff);
                }
            } else {
                debug_assert_eq!(indexes.len(), 1);
                for i in indexes {
                    self.cursors[i].map_times(&mut |time, w| {
                        builder.push_time_diff(time, w);
                    });
                }
            }
        } else {
            self.tmp_weight.set_zero();
            for i in indexes {
                self.cursors[i].map_times(&mut |_time, weight| {
                    self.tmp_weight.add_assign(weight);
                });
            }
            if self.tmp_weight.is_zero() {
                return false;
            }
            builder.push_time_diff_mut(&mut B::Time::default(), &mut self.tmp_weight);
        }

        let index = indexes.first().unwrap();
        builder.push_val(self.cursors[index].val().unwrap().unwrap());
        true
    }
}

fn find_min_indexes<Item>(mut iterator: impl Iterator<Item = (usize, Item)>) -> IndexSet
where
    Item: Ord,
{
    let (min_index, mut min_value) = iterator.next().unwrap();
    let mut min_indexes = IndexSet::for_index(min_index);

    for (index, value) in iterator {
        match value.cmp(&min_value) {
            Ordering::Less => {
                min_value = value;
                min_indexes = IndexSet::for_index(index);
            }
            Ordering::Equal => {
                min_indexes.add(index);
            }
            Ordering::Greater => (),
        }
    }
    min_indexes
}

fn skip_filtered_keys<C, K, V, T, R>(
    cursor: &mut C,
    key_filter: &Option<Filter<K>>,
    value_filter: &Option<Filter<V>>,
) -> Result<(), Pending>
where
    C: PushCursor<K, V, T, R>,
    K: ?Sized,
    V: ?Sized,
    R: ?Sized,
{
    if let Some(key_filter) = key_filter {
        while cursor
            .key()?
            .is_some_and(|value| Filter::include(key_filter, value))
        {
            cursor.step_key();
        }
    }
    Ok(())
}

fn skip_filtered_values<C, K, V, T, R>(
    cursor: &mut C,
    value_filter: &Option<Filter<V>>,
) -> Result<bool, Pending>
where
    C: PushCursor<K, V, T, R>,
    K: ?Sized,
    V: ?Sized,
    R: ?Sized,
{
    if value_filter.is_some() {
        while let Some(value) = cursor.val()? {
            if Filter::include(value_filter, value) {
                return Ok(true);
            }
            cursor.step_val();
        }
        return Ok(false);
    } else {
        Ok(true)
    }
}
