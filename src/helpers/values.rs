// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use greptime_proto::v1::{Decimal128, IntervalMonthDayNano};

macro_rules! define_value_fn {
    ($fn_name:ident, $arg_type:ty, $inner_type:ident) => {
        #[inline]
        pub fn $fn_name(v: $arg_type) -> crate::api::v1::Value {
            crate::api::v1::Value {
                value_data: Some(crate::api::v1::value::ValueData::$inner_type(v)),
            }
        }
    };
    ($fn_name:ident, $arg_type:ty, $inner_type:ident, $convert_type:ty) => {
        #[inline]
        pub fn $fn_name(v: $arg_type) -> crate::api::v1::Value {
            crate::api::v1::Value {
                value_data: Some(crate::api::v1::value::ValueData::$inner_type(
                    v as $convert_type,
                )),
            }
        }
    };
}

pub fn none_value() -> crate::api::v1::Value {
    crate::api::v1::Value { value_data: None }
}

define_value_fn!(i8_value, i8, I8Value, i32);
define_value_fn!(i16_value, i16, I16Value, i32);
define_value_fn!(i32_value, i32, I32Value);
define_value_fn!(i64_value, i64, I64Value);

define_value_fn!(u8_value, u8, U8Value, u32);
define_value_fn!(u16_value, u16, U16Value, u32);
define_value_fn!(u32_value, u32, U32Value);
define_value_fn!(u64_value, u64, U64Value);

define_value_fn!(f32_value, f32, F32Value);
define_value_fn!(f64_value, f64, F64Value);

define_value_fn!(bool_value, bool, BoolValue);

define_value_fn!(string_value, String, StringValue);
define_value_fn!(binary_value, Vec<u8>, BinaryValue);

define_value_fn!(date_value, i32, DateValue);
define_value_fn!(datetime_value, i64, DatetimeValue);
define_value_fn!(timestamp_second_value, i64, TimestampSecondValue);
define_value_fn!(timestamp_millisecond_value, i64, TimestampMillisecondValue);
define_value_fn!(timestamp_microsecond_value, i64, TimestampMicrosecondValue);
define_value_fn!(timestamp_nanosecond_value, i64, TimestampNanosecondValue);
define_value_fn!(time_second_value, i64, TimeSecondValue);
define_value_fn!(time_millisecond_value, i64, TimeMillisecondValue);
define_value_fn!(time_microsecond_value, i64, TimeMicrosecondValue);
define_value_fn!(time_nanosecond_value, i64, TimeNanosecondValue);
define_value_fn!(interval_year_month_value, i32, IntervalYearMonthValue);
define_value_fn!(interval_day_time_value, i64, IntervalDayTimeValue);

#[inline]
pub fn interval_month_day_nano_value(
    months: i32,
    days: i32,
    nanoseconds: i64,
) -> crate::api::v1::Value {
    crate::api::v1::Value {
        value_data: Some(crate::api::v1::value::ValueData::IntervalMonthDayNanoValue(
            IntervalMonthDayNano {
                months,
                days,
                nanoseconds,
            },
        )),
    }
}

#[inline]
pub fn decimal128_value(v: i128) -> crate::api::v1::Value {
    crate::api::v1::Value {
        value_data: Some(crate::api::v1::value::ValueData::Decimal128Value(
            Decimal128 {
                hi: (v >> 64) as i64,
                lo: v as i64,
            },
        )),
    }
}
