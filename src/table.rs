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

//! Table schema and data structures for GreptimeDB bulk insert operations

use derive_builder::Builder;

use crate::api::v1::{ColumnDataType, SemanticType};

/// Represents a time-series data table with schema and data
#[derive(Debug, Clone, Builder)]
#[builder(setter(into))]
pub struct Table {
    /// Table name
    pub name: String,
    /// Table columns
    #[builder(default)]
    pub columns: Vec<Column>,
    /// Table data rows
    #[builder(default)]
    pub rows: Vec<Row>,
}

impl Table {
    /// Create a new table builder
    pub fn builder() -> TableBuilder {
        TableBuilder::default()
    }

    /// Create a new table with pre-allocated capacity for columns and rows
    pub fn with_capacity(column_capacity: usize, row_capacity: usize) -> Self {
        Self {
            name: String::new(),
            columns: Vec::with_capacity(column_capacity),
            rows: Vec::with_capacity(row_capacity),
        }
    }

    /// Add a tag column (for indexing and grouping)
    pub fn add_tag<T: Into<String>>(mut self, name: T, data_type: ColumnDataType) -> Self {
        self.columns.push(Column {
            name: name.into(),
            data_type,
            semantic_type: SemanticType::Tag,
        });
        self
    }

    /// Add a timestamp column (timeline for time series)
    pub fn add_timestamp<T: Into<String>>(mut self, name: T, data_type: ColumnDataType) -> Self {
        self.columns.push(Column {
            name: name.into(),
            data_type,
            semantic_type: SemanticType::Timestamp,
        });
        self
    }

    /// Add a field column (measurement values)
    pub fn add_field<T: Into<String>>(mut self, name: T, data_type: ColumnDataType) -> Self {
        self.columns.push(Column {
            name: name.into(),
            data_type,
            semantic_type: SemanticType::Field,
        });
        self
    }

    /// Add a data row
    pub fn add_row(mut self, row: Row) -> Self {
        self.rows.push(row);
        self
    }

    /// Add multiple rows
    pub fn add_rows(mut self, rows: Vec<Row>) -> Self {
        self.rows.extend(rows);
        self
    }
}

/// Table column definition
#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub data_type: ColumnDataType,
    pub semantic_type: SemanticType,
}

/// Represents a data row with type-safe value access
#[derive(Debug, Clone, Default)]
pub struct Row {
    values: Vec<Value>,
}

impl Row {
    /// Create a new empty row
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a new row with pre-allocated capacity
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            values: Vec::with_capacity(capacity),
        }
    }

    /// Create a row directly from values (more efficient than chaining add_value calls)
    pub fn from_values(values: Vec<Value>) -> Self {
        Self { values }
    }

    /// Add a value to the row
    pub fn add_value(mut self, value: Value) -> Self {
        self.values.push(value);
        self
    }

    /// Add multiple values to the row
    pub fn add_values(mut self, values: Vec<Value>) -> Self {
        self.values.extend(values);
        self
    }

    /// Add multiple values from an iterator
    pub fn add_values_iter(mut self, values: impl IntoIterator<Item = Value>) -> Self {
        self.values.extend(values);
        self
    }

    /// Get boolean value at index
    pub fn get_bool(&self, index: usize) -> Option<bool> {
        self.values.get(index)?.as_bool()
    }

    /// Get i8 value at index
    pub fn get_i8(&self, index: usize) -> Option<i8> {
        self.values.get(index)?.as_i8()
    }

    /// Get i16 value at index
    pub fn get_i16(&self, index: usize) -> Option<i16> {
        self.values.get(index)?.as_i16()
    }

    /// Get i32 value at index
    pub fn get_i32(&self, index: usize) -> Option<i32> {
        self.values.get(index)?.as_i32()
    }

    /// Get i64 value at index
    pub fn get_i64(&self, index: usize) -> Option<i64> {
        self.values.get(index)?.as_i64()
    }

    /// Get u8 value at index
    pub fn get_u8(&self, index: usize) -> Option<u8> {
        self.values.get(index)?.as_u8()
    }

    /// Get u16 value at index
    pub fn get_u16(&self, index: usize) -> Option<u16> {
        self.values.get(index)?.as_u16()
    }

    /// Get u32 value at index
    pub fn get_u32(&self, index: usize) -> Option<u32> {
        self.values.get(index)?.as_u32()
    }

    /// Get u64 value at index
    pub fn get_u64(&self, index: usize) -> Option<u64> {
        self.values.get(index)?.as_u64()
    }

    /// Get f32 value at index
    pub fn get_f32(&self, index: usize) -> Option<f32> {
        self.values.get(index)?.as_f32()
    }

    /// Get f64 value at index
    pub fn get_f64(&self, index: usize) -> Option<f64> {
        self.values.get(index)?.as_f64()
    }

    /// Get binary value at index
    pub fn get_binary(&self, index: usize) -> Option<Vec<u8>> {
        self.values.get(index)?.as_binary()
    }

    /// Get string value at index
    pub fn get_string(&self, index: usize) -> Option<String> {
        self.values.get(index)?.as_string()
    }

    /// Get date value at index
    pub fn get_date(&self, index: usize) -> Option<i32> {
        self.values.get(index)?.as_date()
    }

    /// Get datetime value at index
    pub fn get_datetime(&self, index: usize) -> Option<i64> {
        self.values.get(index)?.as_datetime()
    }

    /// Get timestamp value at index (generic, supports all timestamp types)
    pub fn get_timestamp(&self, index: usize) -> Option<i64> {
        self.values.get(index)?.as_timestamp()
    }

    /// Get JSON value at index
    pub fn get_json(&self, index: usize) -> Option<String> {
        self.values.get(index)?.as_json()
    }

    /// Get decimal128 value at index
    pub fn get_decimal128(&self, index: usize) -> Option<Vec<u8>> {
        self.values.get(index)?.as_decimal128()
    }
}

/// Type-safe value wrapper for all GreptimeDB data types
#[derive(Debug, Clone)]
pub enum Value {
    // Boolean
    Boolean(bool),

    // Integer types
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Uint8(u8),
    Uint16(u16),
    Uint32(u32),
    Uint64(u64),

    // Float types
    Float32(f32),
    Float64(f64),

    // String and Binary types
    Binary(Vec<u8>),
    String(String),

    // Date and Time types
    Date(i32),     // Days since Unix epoch
    Datetime(i64), // Milliseconds since Unix epoch

    // Timestamp types
    Timestamp(i64), // Generic timestamp (for backward compatibility)
    TimestampSecond(i64),
    TimestampMillisecond(i64),
    TimestampMicrosecond(i64),
    TimestampNanosecond(i64),

    // Time types (time of day without date)
    TimeSecond(i32),
    TimeMillisecond(i32),
    TimeMicrosecond(i64),
    TimeNanosecond(i64),

    // Decimal type (stored as binary for precision)
    Decimal128(Vec<u8>),

    // JSON type (stored as string)
    Json(String),

    // Null value
    Null,
}

impl Value {
    // Boolean accessors
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Boolean(v) => Some(*v),
            _ => None,
        }
    }

    // Integer accessors
    pub fn as_i8(&self) -> Option<i8> {
        match self {
            Value::Int8(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_i16(&self) -> Option<i16> {
        match self {
            Value::Int16(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_i32(&self) -> Option<i32> {
        match self {
            Value::Int32(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Value::Int64(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_u8(&self) -> Option<u8> {
        match self {
            Value::Uint8(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_u16(&self) -> Option<u16> {
        match self {
            Value::Uint16(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_u32(&self) -> Option<u32> {
        match self {
            Value::Uint32(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_u64(&self) -> Option<u64> {
        match self {
            Value::Uint64(v) => Some(*v),
            _ => None,
        }
    }

    // Float accessors
    pub fn as_f32(&self) -> Option<f32> {
        match self {
            Value::Float32(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::Float64(v) => Some(*v),
            _ => None,
        }
    }

    // String and Binary accessors
    pub fn as_binary(&self) -> Option<Vec<u8>> {
        match self {
            Value::Binary(v) => Some(v.clone()),
            _ => None,
        }
    }

    /// Get the binary value as a slice (zero-copy)
    pub fn as_binary_ref(&self) -> Option<&[u8]> {
        match self {
            Value::Binary(v) => Some(v),
            _ => None,
        }
    }

    pub fn as_string(&self) -> Option<String> {
        match self {
            Value::String(v) => Some(v.clone()),
            _ => None,
        }
    }

    /// Get the string value as a str slice (zero-copy)
    pub fn as_string_ref(&self) -> Option<&str> {
        match self {
            Value::String(v) => Some(v),
            _ => None,
        }
    }

    // Date and Time accessors
    pub fn as_date(&self) -> Option<i32> {
        match self {
            Value::Date(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_datetime(&self) -> Option<i64> {
        match self {
            Value::Datetime(v) => Some(*v),
            _ => None,
        }
    }

    // Timestamp accessors
    pub fn as_timestamp(&self) -> Option<i64> {
        match self {
            Value::Timestamp(v) => Some(*v),
            Value::TimestampSecond(v) => Some(*v),
            Value::TimestampMillisecond(v) => Some(*v),
            Value::TimestampMicrosecond(v) => Some(*v),
            Value::TimestampNanosecond(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_timestamp_second(&self) -> Option<i64> {
        match self {
            Value::TimestampSecond(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_timestamp_millisecond(&self) -> Option<i64> {
        match self {
            Value::TimestampMillisecond(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_timestamp_microsecond(&self) -> Option<i64> {
        match self {
            Value::TimestampMicrosecond(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_timestamp_nanosecond(&self) -> Option<i64> {
        match self {
            Value::TimestampNanosecond(v) => Some(*v),
            _ => None,
        }
    }

    // Time accessors
    pub fn as_time_second(&self) -> Option<i32> {
        match self {
            Value::TimeSecond(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_time_millisecond(&self) -> Option<i32> {
        match self {
            Value::TimeMillisecond(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_time_microsecond(&self) -> Option<i64> {
        match self {
            Value::TimeMicrosecond(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_time_nanosecond(&self) -> Option<i64> {
        match self {
            Value::TimeNanosecond(v) => Some(*v),
            _ => None,
        }
    }

    // Decimal accessor
    pub fn as_decimal128(&self) -> Option<Vec<u8>> {
        match self {
            Value::Decimal128(v) => Some(v.clone()),
            _ => None,
        }
    }

    // JSON accessor
    pub fn as_json(&self) -> Option<String> {
        match self {
            Value::Json(v) => Some(v.clone()),
            _ => None,
        }
    }
}

// Convenient constructors for values
impl From<bool> for Value {
    fn from(v: bool) -> Self {
        Value::Boolean(v)
    }
}

// Integer types
impl From<i8> for Value {
    fn from(v: i8) -> Self {
        Value::Int8(v)
    }
}

impl From<i16> for Value {
    fn from(v: i16) -> Self {
        Value::Int16(v)
    }
}

impl From<i32> for Value {
    fn from(v: i32) -> Self {
        Value::Int32(v)
    }
}

impl From<i64> for Value {
    fn from(v: i64) -> Self {
        Value::Int64(v)
    }
}

impl From<u8> for Value {
    fn from(v: u8) -> Self {
        Value::Uint8(v)
    }
}

impl From<u16> for Value {
    fn from(v: u16) -> Self {
        Value::Uint16(v)
    }
}

impl From<u32> for Value {
    fn from(v: u32) -> Self {
        Value::Uint32(v)
    }
}

impl From<u64> for Value {
    fn from(v: u64) -> Self {
        Value::Uint64(v)
    }
}

// Float types
impl From<f32> for Value {
    fn from(v: f32) -> Self {
        Value::Float32(v)
    }
}

impl From<f64> for Value {
    fn from(v: f64) -> Self {
        Value::Float64(v)
    }
}

// Binary type
impl From<Vec<u8>> for Value {
    fn from(v: Vec<u8>) -> Self {
        Value::Binary(v)
    }
}

impl From<String> for Value {
    fn from(v: String) -> Self {
        Value::String(v)
    }
}

impl From<&str> for Value {
    fn from(v: &str) -> Self {
        Value::String(v.to_string())
    }
}
