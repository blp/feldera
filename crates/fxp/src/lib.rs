mod fixed128;
use std::ops::Add;

pub use fixed128::Fixed128;
mod fixed64;
pub use fixed64::Fixed64;
use num_traits::CheckedAdd;

/// Flexibly sized fixed-point decimal with fixed precision and scale.
///
/// `Fixed<P, S>`, where `P` in `1..=38` is the "precision" and `S` in `0..=P`
/// is the "scale", represents a signed decimal number in which `S - P` digits
/// precede the decimal point and `S` digits follow it.  The table below shows
/// the maximum values for a few combinations of `P` and `S`.  For each type,
/// the minimum value is the negation of the maximum:
///
/// |          Type |                                              Maximum Value |
/// |:--------------|-----------------------------------------------------------:|
/// | `Fixed<5,5>`  | `                                                 0.99999` |
/// | `Fixed<5,4>`  | `                                                 9.9999 ` |
/// | `Fixed<5,3>`  | `                                                99.999  ` |
/// | `Fixed<5,2>`  | `                                               999.99   ` |
/// | `Fixed<5,1>`  | `                                             9,999.9    ` |
/// | `Fixed<5,0>`  | `                                            99,999      ` |
/// | `Fixed<38,0>` | `99,999,999,999,999,999,999,999,999,999,999,999,999      ` |
/// | `Fixed<38,5>` | `       999,999,999,999,999,999,999,999,999,999,999.99999` |
///
/// # Implementation
///
/// This type is implemented in terms of [Fixed128] and [Fixed64]:
///
/// - For `1 ≤ S ≤ 18`, it internally uses 64-bit [Fixed64].
///
/// - For `19 ≤ S ≤ 38`, it internally uses 128-bit [Fixed128].
#[derive(Copy, Clone)]
pub struct Fixed<const P: usize, const S: usize>(<() as FixedImpl<P, S>>::T)
where
    (): FixedImpl<P, S>;

pub trait FixedImpl<const P: usize, const S: usize> {
    type T: Copy + Clone + Add<Output = Self::T> + CheckedAdd;
}

impl<const P: usize, const S: usize> Add for Fixed<P, S>
where
    (): FixedImpl<P, S>,
{
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        Self(self.0 + rhs.0)
    }
}

impl<const P: usize, const S: usize> Add for &Fixed<P, S>
where
    (): FixedImpl<P, S>,
{
    type Output = Fixed<P, S>;

    fn add(self, rhs: Self) -> Self::Output {
        Fixed(self.0 + rhs.0)
    }
}

impl<const P: usize, const S: usize> CheckedAdd for Fixed<P, S>
where
    (): FixedImpl<P, S>,
{
    fn checked_add(&self, rhs: &Self) -> Option<Self> {
        self.0.checked_add(&rhs.0).map(Self)
    }
}

macro_rules! fixed_impl {
    ($digits:expr, $type:ident) => {
        impl<const S: usize> FixedImpl<$digits, S> for () {
            type T = $type<$digits, S>;
        }
    };
}

// Up to 9 digits could be 32 bits, if we implemented Fixed32.
fixed_impl!(1, Fixed64);
fixed_impl!(2, Fixed64);
fixed_impl!(3, Fixed64);
fixed_impl!(4, Fixed64);
fixed_impl!(5, Fixed64);
fixed_impl!(6, Fixed64);
fixed_impl!(7, Fixed64);
fixed_impl!(8, Fixed64);
fixed_impl!(9, Fixed64);

// `10..=18` digits requires at least 64 bits.
fixed_impl!(10, Fixed64);
fixed_impl!(11, Fixed64);
fixed_impl!(12, Fixed64);
fixed_impl!(13, Fixed64);
fixed_impl!(14, Fixed64);
fixed_impl!(15, Fixed64);
fixed_impl!(16, Fixed64);
fixed_impl!(17, Fixed64);
fixed_impl!(18, Fixed64);

// `19..=38` digits requires at least 128 bits.
fixed_impl!(19, Fixed128);
fixed_impl!(20, Fixed128);
fixed_impl!(21, Fixed128);
fixed_impl!(22, Fixed128);
fixed_impl!(23, Fixed128);
fixed_impl!(24, Fixed128);
fixed_impl!(25, Fixed128);
fixed_impl!(26, Fixed128);
fixed_impl!(27, Fixed128);
fixed_impl!(28, Fixed128);
fixed_impl!(29, Fixed128);
fixed_impl!(30, Fixed128);
fixed_impl!(31, Fixed128);
fixed_impl!(32, Fixed128);
fixed_impl!(33, Fixed128);
fixed_impl!(34, Fixed128);
fixed_impl!(35, Fixed128);
fixed_impl!(36, Fixed128);
fixed_impl!(37, Fixed128);
fixed_impl!(38, Fixed128);
