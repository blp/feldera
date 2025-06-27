use dbsp::algebra::{MulByRef, OptionWeightType};
use feldera_types::serde_with_context::{
    serde_config::DecimalFormat, DeserializeWithContext, SerializeWithContext, SqlSerdeConfig,
};
use serde::{Deserializer, Serialize, Serializer};

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

impl<const P: usize, const S: usize> SerializeWithContext<SqlSerdeConfig> for Fixed<P, S> {
    fn serialize_with_context<Ser>(
        &self,
        serializer: Ser,
        context: &SqlSerdeConfig,
    ) -> Result<Ser::Ok, Ser::Error>
    where
        Ser: Serializer,
    {
        match context.decimal_format {
            DecimalFormat::Numeric => Serialize::serialize(&self, serializer),
            DecimalFormat::String => {
                // serde_arrow doesn't support scientific notation.
                serializer.serialize_str(&self.value.to_standard_notation_string())
            }
        }
    }
}

impl<'de, C, const P: usize, const S: usize> DeserializeWithContext<'de, C> for Fixed<P, S> {
    #[inline(never)]
    fn deserialize_with_context<D>(deserializer: D, _context: &'de C) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        serde::Deserialize::deserialize(deserializer)
    }
}
