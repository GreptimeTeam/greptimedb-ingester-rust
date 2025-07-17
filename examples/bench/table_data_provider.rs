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

//! Table Data Provider trait and utilities for benchmarking
//!
//! This module provides the core TableDataProvider trait that defines the interface
//! for generating benchmark data, following the Java implementation pattern.

use greptimedb_ingester::{
    api::v1::{ColumnSchema, Row as ApiRow},
    table::{Row, TableSchema},
    Result,
};

/// Base trait for all data providers
pub trait DataProvider {
    /// Initialize the data provider
    fn init(&mut self) -> Result<()> {
        Ok(())
    }

    /// Get the total number of rows this provider will generate
    fn row_count(&self) -> usize;

    /// Cleanup resources
    fn close(&mut self) -> Result<()> {
        Ok(())
    }
}

/// Trait for providers that generate `Row` data for the bulk API
pub trait TableDataProvider: DataProvider {
    /// Get the table schema for bulk insertion
    fn table_schema(&self) -> TableSchema;

    /// Get an iterator over the `Row` objects
    fn rows(&mut self) -> Box<dyn Iterator<Item = Row> + '_>;
}

/// Trait for providers that generate `ApiRow` data for the regular API
pub trait ApiDataProvider: DataProvider {
    /// Get the table name
    fn table_name(&self) -> &str;

    /// Get the column schema for the regular API
    fn api_schema(&self) -> Vec<ColumnSchema>;

    /// Get an iterator over the `ApiRow` objects
    fn api_rows(&mut self) -> Box<dyn Iterator<Item = ApiRow> + '_>;
}
