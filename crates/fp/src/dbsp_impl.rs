use dbsp::algebra::{MulByRef, OptionWeightType};

use crate::{Fixed, FixedInteger};

impl<const P: usize, const S: usize> OptionWeightType for Fixed<P, S> {}

impl<const P: usize, const S: usize> MulByRef for Fixed<P, S> {
    type Output = Self;

    fn mul_by_ref(&self, other: &Self) -> Self::Output {
        *self * *other
    }
}

impl<const P: usize, const S: usize> MulByRef<isize> for Fixed<P, S> {
    type Output = Self;

    fn mul_by_ref(&self, other: &isize) -> Self::Output {
        self.checked_mul_generic(FixedInteger::for_isize(*other))
            .unwrap()
    }
}

impl<const P: usize, const S: usize> MulByRef<i64> for Fixed<P, S> {
    type Output = Self;

    fn mul_by_ref(&self, other: &i64) -> Self::Output {
        self.checked_mul_generic(FixedInteger::for_i64(*other))
            .unwrap()
    }
}

impl<const P: usize, const S: usize> MulByRef<i32> for Fixed<P, S> {
    type Output = Self;

    fn mul_by_ref(&self, other: &i32) -> Self::Output {
        self.checked_mul_generic(FixedInteger::for_i32(*other))
            .unwrap()
    }
}
