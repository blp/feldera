//! Traits and types for navigating order sequences of
//! `(key, val, time, diff)` tuples.
//!
//! The `Cursor` trait contains several methods for efficiently navigating
//! ordered collections of tuples of the form `(key, val, time, diff)`.  The
//! tuples are ordered by key, then by value within each key.  Ordering by time
//! is not guaranteed, in particular [`CursorList`](`cursor_list::CursorList`)
//! cursors can contain out-of-order and duplicate timestamps.
//!
//! The cursor is different from an iterator both because it allows navigation
//! on multiple levels (key and val), but also because it supports efficient
//! seeking (via the `seek_key` and `seek_val` methods).

pub mod cursor_empty;
pub mod cursor_group;
pub mod cursor_list;
pub mod cursor_pair;

#[derive(Debug, PartialEq, Eq)]
enum Direction {
    Forward,
    Backward,
}

pub use cursor_empty::CursorEmpty;
pub use cursor_group::CursorGroup;
pub use cursor_list::CursorList;
pub use cursor_pair::CursorPair;

/// A cursor for navigating ordered `(key, val, time, diff)` tuples.
pub trait Cursor<K, V, T, R> {
    /// Indicates if the current key is valid.
    ///
    /// A value of `false` indicates that the cursor has exhausted all keys.
    fn key_valid(&self) -> bool;

    /// Indicates if the current value is valid.
    ///
    /// A value of `false` indicates that the cursor has exhausted all values
    /// for this key.
    fn val_valid(&self) -> bool;

    /// A reference to the current key. Panics if invalid.
    fn key(&self) -> &K;

    /// A reference to the current value. Panics if invalid.
    fn val(&self) -> &V;

    /// Returns a reference to the current key, if valid.
    fn get_key(&self) -> Option<&K> {
        if self.key_valid() {
            Some(self.key())
        } else {
            None
        }
    }

    /// Returns a reference to the current value, if valid.
    fn get_val(&self) -> Option<&V> {
        if self.val_valid() {
            Some(self.val())
        } else {
            None
        }
    }

    /// Applies `logic` to each pair of time and difference. Intended for
    /// mutation of the closure's scope.
    fn map_times<L>(&mut self, mut logic: L)
    where
        L: FnMut(&T, &R),
    {
        self.fold_times((), |(), time, diff| logic(time, diff));
    }

    fn fold_times<F, U>(&mut self, init: U, fold: F) -> U
    where
        F: FnMut(U, &T, &R) -> U;

    /// Applies `logic` to each pair of time and difference, restricted
    /// to times `t <= upper`.
    fn map_times_through<L>(&mut self, upper: &T, mut logic: L)
    where
        L: FnMut(&T, &R),
    {
        self.fold_times_through(upper, (), |(), time, diff| logic(time, diff));
    }

    fn fold_times_through<F, U>(&mut self, upper: &T, init: U, fold: F) -> U
    where
        F: FnMut(U, &T, &R) -> U;

    /// Returns the weight associated with the current key/value pair.
    ///
    /// This method is only defined for cursors with unit timestamp type
    /// (`T=()`), which contain exactly one weight per key/value pair.  It
    /// is more convenient (and potentially more efficient) than using
    /// [`Self::map_times`] to iterate over a single value.
    ///
    /// If the current key and value are not valid, behavior is unspecified
    fn weight(&mut self) -> R
    where
        T: PartialEq<()>;

    /// Apply a function to all values associated with the current key.
    fn map_values<L: FnMut(&V, &R)>(&mut self, mut logic: L)
    where
        T: PartialEq<()>,
    {
        while self.val_valid() {
            let weight = self.weight();
            let val = self.val();
            logic(val, &weight);
            self.step_val();
        }
    }

    /// Advances the cursor to the next key.
    fn step_key(&mut self);

    /// Moves the cursor to the previous key.
    fn step_key_reverse(&mut self);

    /// Advances the cursor to the specified key.
    fn seek_key(&mut self, key: &K);

    /// Move the cursor to the first key that satisfies `predicate`.
    /// Assumes that `predicate` remains true once it turns true.
    fn seek_key_with<P>(&mut self, predicate: P)
    where
        P: Fn(&K) -> bool + Clone;

    /// Move the cursor back to the first key that satisfies `predicate`.
    /// Assumes that `predicate` remains true once it turns true.
    fn seek_key_with_reverse<P>(&mut self, predicate: P)
    where
        P: Fn(&K) -> bool + Clone;

    /// Moves the cursor back to the specified key.
    fn seek_key_reverse(&mut self, key: &K);

    /// Advances the cursor to the next value.
    fn step_val(&mut self);

    /// Moves the cursor to the previous value.
    fn step_val_reverse(&mut self);

    /// Advances the cursor to the specified value.
    fn seek_val(&mut self, val: &V);

    /// Moves the cursor back to the specified value.
    fn seek_val_reverse(&mut self, val: &V);

    /// Move the cursor to the first value (for the current key) that satisfies
    /// `predicate`.  Assumes that `predicate` remains true once it turns true.
    fn seek_val_with<P>(&mut self, predicate: P)
    where
        P: Fn(&V) -> bool + Clone;

    /// Move the cursor back to the largest value (for the current key) that
    /// satisfies `predicate`.  Assumes that `predicate` remains true once
    /// it turns true.
    fn seek_val_with_reverse<P>(&mut self, predicate: P)
    where
        P: Fn(&V) -> bool + Clone;

    /// Rewinds the cursor to the first key.
    fn rewind_keys(&mut self);

    /// Moves the cursor to the last key.
    fn fast_forward_keys(&mut self);

    /// Rewinds the cursor to the first value for current key.
    fn rewind_vals(&mut self);

    /// Move the cursor to the last value for the current key.
    fn fast_forward_vals(&mut self);
}

/// A cursor for taking ownership of ordered `(K, V, R, T)` tuples
pub trait Consumer<K, V, R, T> {
    /// The consumer for the values and diffs associated with a particular key
    type ValueConsumer<'a>: ValueConsumer<'a, V, R, T>
    where
        Self: 'a;

    /// Returns `true` if the current key is valid
    fn key_valid(&self) -> bool;

    /// Returns a reference to the current key
    fn peek_key(&self) -> &K;

    /// Takes ownership of the current key and gets the consumer for its
    /// associated values
    fn next_key(&mut self) -> (K, Self::ValueConsumer<'_>);

    /// Advances the cursor to the specified value
    fn seek_key(&mut self, key: &K)
    where
        K: Ord;
}

/// A cursor for taking ownership of the values and diffs associated with a
/// given key
pub trait ValueConsumer<'a, V, R, T> {
    /// Returns `true` if the current value is valid
    fn value_valid(&self) -> bool;

    /// Takes ownership of the current value & diff pair
    // TODO: Maybe this should yield another consumer for `(R, T)` pairs
    fn next_value(&mut self) -> (V, R, T);

    /// Provides the number of remaining values
    fn remaining_values(&self) -> usize;

    // TODO: Seek value method?
}

/// Debugging and testing utilities for Cursor.
pub trait CursorDebug<K: Clone, V: Clone, T: Clone, R: Clone>: Cursor<K, V, T, R> {
    /// Rewinds the cursor and outputs its contents to a Vec
    #[allow(clippy::type_complexity)]
    fn to_vec(&mut self) -> Vec<((K, V), Vec<(T, R)>)> {
        let mut out = Vec::new();
        self.rewind_keys();
        self.rewind_vals();
        while self.key_valid() {
            while self.val_valid() {
                let mut kv_out = Vec::new();
                self.map_times(|ts, r| kv_out.push((ts.clone(), r.clone())));
                out.push(((self.key().clone(), self.val().clone()), kv_out));
                self.step_val();
            }
            self.step_key();
        }
        out
    }

    /// Returns values with time, weights for a given cursor.
    ///
    /// Starts wherever the current cursor is pointing to and walks to the end
    /// of the values for the current key.
    ///
    /// Should only be called with `key_valid() == true`.
    ///
    /// # Panics
    /// - Panics (in debug mode) if the key is not valid.
    fn val_to_vec(&mut self) -> Vec<(V, Vec<(T, R)>)> {
        debug_assert!(self.key_valid());
        let mut vs = Vec::new();
        while self.val_valid() {
            let mut weights = Vec::new();
            self.map_times(|ts, r| {
                weights.push((ts.clone(), r.clone()));
            });

            vs.push((self.val().clone(), weights));
            self.step_val();
        }

        vs
    }
}

impl<C, K: Clone, V: Clone, T: Clone, R: Clone> CursorDebug<K, V, T, R> for C where
    C: Cursor<K, V, T, R>
{
}
