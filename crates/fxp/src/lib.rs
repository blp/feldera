mod fixed128;
use std::{
    cmp::Ordering,
    fmt::{Debug, Display},
    hash::Hash,
    io::Write,
    ops::Add,
};

pub use fixed128::Fixed128;
mod fixed64;
pub use fixed64::Fixed64;
use num_traits::{CheckedAdd, PrimInt, Signed};
use smallvec::{Array, SmallVec};

/// Adaptively sized fixed-point decimal with fixed precision and scale.
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
/// - For `1 ≤ S ≤ 18`, it uses 64-bit [Fixed64] internally.
///
/// - For `19 ≤ S ≤ 38`, it uses 128-bit [Fixed128] internally.
#[derive(Copy, Clone, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "size_of", derive(size_of::SizeOf))]
pub struct Fixed<const P: usize, const S: usize>(<() as FixedImpl<P, S>>::T)
where
    (): FixedImpl<P, S>;

pub trait FixedImpl<const P: usize, const S: usize> {
    type T: Copy
        + Clone
        + Debug
        + Default
        + Add<Output = Self::T>
        + CheckedAdd
        + FixedConversions<P, S>
        + PartialEq
        + Eq
        + PartialOrd
        + Ord
        + Hash
        + Display;
}

impl<const P: usize, const S: usize> Debug for Fixed<P, S>
where
    (): FixedImpl<P, S>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(&self.0, f)
    }
}

impl<const P: usize, const S: usize> Display for Fixed<P, S>
where
    (): FixedImpl<P, S>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.0, f)
    }
}

pub trait FixedConversions<const P: usize, const S: usize> {
    /// True if a value of this type fits in [Fixed64].
    const FITS_IN_64_BITS: bool;

    /// Returns this value as `Fixed64<P, S>`.
    ///
    /// # Panic
    ///
    /// Panics if `FITS_IN_64_BITS` is false.
    fn to_fixed64(&self) -> Fixed64<P, S>;

    /// Returns `x` converted to this type.
    fn from_fixed64(x: Fixed64<P, S>) -> Self;

    /// Returns this value as `Fixed128<P, S>`.
    fn to_fixed128(&self) -> Fixed128<P, S>;

    /// Returns `x` converted to this type.
    ///
    /// # Panic
    ///
    /// Panics if this type can't hold `P` digits of precision.
    fn from_fixed128(x: Fixed128<P, S>) -> Self;
}

trait CheckedAddGeneric<Rhs, Output> {
    fn checked_add_generic(self, rhs: Rhs) -> Option<Output>;
}

impl<
        const P0: usize,
        const S0: usize,
        const P1: usize,
        const S1: usize,
        const P2: usize,
        const S2: usize,
    > CheckedAddGeneric<Fixed<P1, S1>, Fixed<P2, S2>> for Fixed<P0, S0>
where
    (): FixedImpl<P0, S0>,
    (): FixedImpl<P1, S1>,
    (): FixedImpl<P2, S2>,
{
    fn checked_add_generic(self, rhs: Fixed<P1, S1>) -> Option<Fixed<P2, S2>> {
        if <() as FixedImpl<P0, S0>>::T::FITS_IN_64_BITS
            && <() as FixedImpl<P1, S1>>::T::FITS_IN_64_BITS
            && <() as FixedImpl<P2, S2>>::T::FITS_IN_64_BITS
        {
            self.0
                .to_fixed64()
                .checked_add_generic(rhs.0.to_fixed64())
                .map(|sum| Fixed(<() as FixedImpl<P2, S2>>::T::from_fixed64(sum)))
        } else {
            self.0
                .to_fixed128()
                .checked_add_generic(rhs.0.to_fixed128())
                .map(|sum| Fixed(<() as FixedImpl<P2, S2>>::T::from_fixed128(sum)))
        }
    }
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

#[derive(Copy, Clone, Debug)]
pub struct OutOfRange;

/// Returns `floor(x / y)`.  This is copied out of `i64::div_floor` in the
/// standard library, which is not yet stable.
fn div_floor<T>(x: T, y: T) -> T
where
    T: PrimInt + Signed,
{
    let d = x / y;
    let r = x % y;

    // If the remainder is non-zero, we need to subtract one if the
    // signs of lhs and rhs differ, as this means we rounded upwards
    // instead of downwards. We do this branchlessly by creating a mask
    // which is all-ones iff the signs differ, and 0 otherwise. Then by
    // adding this mask (which corresponds to the signed value -1), we
    // get our correction.
    let bits = size_of::<T>() * 8;
    let correction = (x ^ y) >> (bits - 1);
    if !r.is_zero() {
        d + correction
    } else {
        d
    }
}

/// Returns `ceil(x / y)`.  This is copied out of `i64::div_ceil` in the
/// standard library, which is not yet stable.
fn div_ceil<T>(x: T, y: T) -> T
where
    T: PrimInt + Signed,
{
    let d = x / y;
    let r = x % y;

    // When remainder is non-zero we have a.div_ceil(b) == 1 + a.div_floor(b),
    // so we can re-use the algorithm from div_floor, just adding 1.
    let bits = size_of::<T>() * 8;
    let correction = T::one() + ((x ^ y) >> (bits - 1));
    if !r.is_zero() {
        d + correction
    } else {
        d
    }
}

fn debug_generic<T>(value: T, scale: T, f: &mut std::fmt::Formatter) -> std::fmt::Result
where
    T: PrimInt + Signed + Display,
{
    if scale.is_one() {
        write!(f, "{}", value)
    } else if (value % scale).is_zero() {
        write!(f, "{}", value / scale)
    } else {
        write!(
            f,
            "{}{}.{}",
            if value.is_negative() { "-" } else { "" },
            value.abs() / scale,
            (value.abs() % scale).abs()
        )
    }
}

fn display_generic<T>(value: T, scale: usize, f: &mut std::fmt::Formatter) -> std::fmt::Result
where
    T: PrimInt + Signed + Display,
{
    let mut buf = SmallVec::<[u8; 64]>::new();
    write!(&mut buf, "{:01$}", value.abs(), scale + 1).unwrap();
    debug_assert!(buf.len() > scale);
    let decimals = if let Some(precision) = f.precision() {
        match precision.cmp(&scale) {
            Ordering::Less => {
                let new_len = buf.len() - (scale - precision);
                let mut discard = buf[new_len..].iter();
                enum Rounding {
                    Up,
                    Down,
                    Even,
                }
                impl Rounding {
                    fn round<A>(&self, s: &mut SmallVec<A>)
                    where
                        A: Array<Item = u8>,
                    {
                        let round_up = match self {
                            Rounding::Down => false,
                            Rounding::Up => true,
                            Rounding::Even => s.last().unwrap() % 2 == 1,
                        };
                        if round_up {
                            let mut nines = 0;
                            let c = loop {
                                match s.pop() {
                                    Some(b'9') => nines += 1,
                                    Some(c) => break c,
                                    None => break b'0',
                                }
                            };
                            s.push(c + 1);
                            for _ in 0..nines {
                                s.push(b'0');
                            }
                        }
                    }
                }
                let rounding = match discard.next().unwrap() {
                    b'0'..=b'4' => Rounding::Down,
                    b'5' => loop {
                        match discard.next() {
                            Some(b'0') => (),
                            Some(_) => break Rounding::Up,
                            None => break Rounding::Even,
                        }
                    },
                    b'6'..=b'9' => Rounding::Up,
                    _ => unreachable!(),
                };
                buf.truncate(new_len);
                rounding.round(&mut buf);
            }
            Ordering::Equal => (),
            Ordering::Greater => {
                for _ in scale..precision {
                    buf.push(b'0');
                }
            }
        }
        precision
    } else {
        let mut decimals = scale;
        while decimals > 0 && buf.ends_with(b"0") {
            buf.pop();
            decimals -= 1;
        }
        decimals
    };
    if decimals > 0 {
        buf.insert(buf.len() - decimals, b'.');
    }

    // SAFETY: `buf` contains only ASCII characters.
    f.pad_integral(!value.is_negative(), "", unsafe {
        str::from_utf8_unchecked(&buf)
    })
}
