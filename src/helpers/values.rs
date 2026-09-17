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
use json_value::Value;
use snafu::{ensure, OptionExt};

use crate::api::v1::{json_object, json_value, value::ValueData, JsonList, JsonObject, JsonValue};
use crate::error::InvalidJson2Snafu;

/// Parses a JSON object for a field declared with [`super::schema::json2_field`].
///
/// JSON `null` becomes SQL NULL, just like [`none_value`]. Nested arrays and scalars
/// are supported, but a non-null top-level value must be an object.
/// Invalid JSON or an unsupported top-level value returns an error before sending.
pub fn json2_value(json: &str) -> crate::Result<crate::api::v1::Value> {
    let value: serde_json::Value = serde_json::from_str(json).map_err(|error| {
        InvalidJson2Snafu {
            reason: error.to_string(),
        }
        .build()
    })?;
    ensure!(
        value.is_object() || value.is_null(),
        InvalidJson2Snafu {
            reason: "expected a JSON object or null",
        }
    );
    if value.is_null() {
        return Ok(none_value());
    }
    Ok(crate::api::v1::Value {
        value_data: Some(ValueData::JsonValue(encode_json(value)?)),
    })
}

fn encode_json(value: serde_json::Value) -> crate::Result<JsonValue> {
    Ok(JsonValue {
        value: match value {
            serde_json::Value::Null => None,
            serde_json::Value::Bool(v) => Some(Value::Boolean(v)),
            serde_json::Value::Number(v) => Some(if let Some(v) = v.as_u64() {
                Value::Uint(v)
            } else if let Some(v) = v.as_i64() {
                Value::Int(v)
            } else {
                Value::Float(v.as_f64().context(InvalidJson2Snafu {
                    reason: "JSON number cannot be represented as f64",
                })?)
            }),
            serde_json::Value::String(v) => Some(Value::Str(v)),
            serde_json::Value::Array(items) => Some(Value::Array(JsonList {
                items: items
                    .into_iter()
                    .map(encode_json)
                    .collect::<crate::Result<_>>()?,
            })),
            serde_json::Value::Object(entries) => Some(Value::Object(JsonObject {
                entries: entries
                    .into_iter()
                    .map(|(key, value)| {
                        Ok(json_object::Entry {
                            key,
                            value: Some(encode_json(value)?),
                        })
                    })
                    .collect::<crate::Result<_>>()?,
            })),
        },
    })
}

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::v1::{
        column_data_type_extension::TypeExt, ColumnDataType, Row, Rows, SemanticType,
    };
    use crate::helpers::schema::json2_field;
    use prost::Message;

    #[test]
    fn json2_row_encoding() -> Result<(), Box<dyn std::error::Error>> {
        use json_value::Value::*;

        let value = json2_value(
            r#"{"items":[true,-9223372036854775808,18446744073709551615,1.5,"你好",null,{},[],{"ok":false}]}"#,
        )?;
        let expected = JsonValue {
            value: Some(Object(JsonObject {
                entries: vec![json_object::Entry {
                    key: "items".into(),
                    value: Some(JsonValue {
                        value: Some(Array(JsonList {
                            items: vec![
                                Some(Boolean(true)),
                                Some(Int(i64::MIN)),
                                Some(Uint(u64::MAX)),
                                Some(Float(1.5)),
                                Some(Str("你好".into())),
                                None,
                                Some(Object(JsonObject { entries: vec![] })),
                                Some(Array(JsonList { items: vec![] })),
                                Some(Object(JsonObject {
                                    entries: vec![json_object::Entry {
                                        key: "ok".into(),
                                        value: Some(JsonValue {
                                            value: Some(Boolean(false)),
                                        }),
                                    }],
                                })),
                            ]
                            .into_iter()
                            .map(|value| JsonValue { value })
                            .collect(),
                        })),
                    }),
                }],
            })),
        };
        assert_eq!(value.value_data, Some(ValueData::JsonValue(expected)));
        let schema = json2_field("j");
        assert_eq!(schema.column_name, "j");
        assert_eq!(schema.datatype(), ColumnDataType::Json);
        assert_eq!(schema.semantic_type(), SemanticType::Field);
        let Some(TypeExt::JsonNativeType(extension)) = schema
            .datatype_extension
            .as_ref()
            .and_then(|ext| ext.type_ext.as_ref())
        else {
            return Err("JSON2 type marker not found".into());
        };
        assert_eq!(extension.datatype(), ColumnDataType::Json);
        assert!(extension.datatype_extension.is_none());
        let options = &schema
            .options
            .as_ref()
            .ok_or("missing JSON2 options")?
            .options;
        assert_eq!(
            options.get("ARROW:extension:name").map(String::as_str),
            Some("greptime.json2")
        );
        let metadata: serde_json::Value = serde_json::from_str(
            options
                .get("ARROW:extension:metadata")
                .ok_or("missing JSON2 metadata")?,
        )?;
        assert_eq!(
            metadata,
            serde_json::json!({
                "json_settings": {"type_hints": [], "max_auto_expanded_paths": 100},
                "layout_version": 2,
            })
        );

        let rows = Rows {
            schema: vec![schema],
            rows: vec![
                Row {
                    values: vec![value],
                },
                Row {
                    values: vec![json2_value("{}")?],
                },
                Row {
                    values: vec![json2_value("null")?],
                },
            ],
        };
        assert_eq!(Rows::decode(rows.encode_to_vec().as_slice())?, rows);
        assert_eq!(json2_value("null")?, none_value());
        assert_eq!(
            json2_value("{}")?.value_data,
            Some(ValueData::JsonValue(JsonValue {
                value: Some(Object(JsonObject { entries: vec![] })),
            }))
        );
        for invalid in [
            "",
            "{",
            "{\"a\":1} trailing",
            "[]",
            "1",
            "true",
            "\"text\"",
            "{\"a\":1e400}",
        ] {
            assert!(matches!(
                json2_value(invalid),
                Err(error @ crate::Error::InvalidJson2 { .. }) if !error.is_retriable()
            ));
        }
        Ok(())
    }
}
