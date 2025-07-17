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

//! LogTableDataProvider implementation
//!
//! Generates synthetic log data following the Java LogTableDataProvider pattern,
//! with 22 columns including timestamps, log entries, and hierarchical identifiers.

use crate::bench::log_text_helper::LogTextHelper;

use super::benchmark_runner::BenchmarkConfig;
use super::table_data_provider::{ApiDataProvider, DataProvider, TableDataProvider};
use greptimedb_ingester::api::v1::{
    ColumnDataType as ApiColumnDataType, ColumnSchema, Row as ApiRow, SemanticType,
};
use greptimedb_ingester::helpers::values::*;
use greptimedb_ingester::{ColumnDataType, Row, TableSchema, Value};
use rand::RngCore;
use std::time::{SystemTime, UNIX_EPOCH};

/// LogTableDataProvider that generates synthetic log data
/// Following the Java implementation with 22 columns
pub struct LogTableDataProvider {
    table_name: String,
    row_count: usize,
    current_row: usize,
    base_time: i64,
    // Pre-generated value pools for ultra-fast generation
    host_ids: Vec<String>,
    host_names: Vec<String>,
    service_ids: Vec<String>,
    service_names: Vec<String>,
    container_ids: Vec<String>,
    container_names: Vec<String>,
    pod_ids: Vec<String>,
    pod_names: Vec<String>,
    cluster_ids: Vec<String>,
    cluster_names: Vec<String>,
    trace_ids: Vec<String>,
    span_ids: Vec<String>,
    user_ids: Vec<String>,
    session_ids: Vec<String>,
    request_ids: Vec<String>,
    log_uids: Vec<String>,
    // Pre-generated log messages and levels (batch)
    log_entries: Vec<(String, String)>,
}

impl LogTableDataProvider {
    /// Create a new LogTableDataProvider with pre-generated value pools
    pub fn new(table_name: &str, config: &BenchmarkConfig) -> Self {
        let base_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        let mut temp_rng = rand::rng();
        let log_helper = LogTextHelper::new();

        // Pre-generate large pools of values for ultra-fast random access
        let pool_size = 10000.min(config.table_row_count * 2);

        println!("Pre-generating {pool_size} values for ultra-fast data generation...");
        let start = std::time::Instant::now();

        // Generate ID pools
        let host_ids: Vec<String> = (0..pool_size)
            .map(|i| format!("host-{}", temp_rng.next_u64() % 100000 + i as u64))
            .collect();
        let host_names: Vec<String> = (0..pool_size)
            .map(Self::generate_name_suffix_simple)
            .collect();
        let service_ids: Vec<String> = (0..pool_size)
            .map(|i| format!("service-{}", temp_rng.next_u64() % 100000 + i as u64))
            .collect();
        let service_names: Vec<String> = (0..pool_size)
            .map(|i| Self::generate_name_suffix_simple(i + 1000))
            .collect();
        let container_ids: Vec<String> = (0..pool_size)
            .map(|i| format!("container-{}", temp_rng.next_u64() % 100000 + i as u64))
            .collect();
        let container_names: Vec<String> = (0..pool_size)
            .map(|i| Self::generate_name_suffix_simple(i + 2000))
            .collect();
        let pod_ids: Vec<String> = (0..pool_size)
            .map(|i| format!("pod-{}", temp_rng.next_u64() % 100000 + i as u64))
            .collect();
        let pod_names: Vec<String> = (0..pool_size)
            .map(|i| Self::generate_name_suffix_simple(i + 3000))
            .collect();
        let cluster_ids: Vec<String> = (0..pool_size)
            .map(|i| format!("cluster-{}", temp_rng.next_u64() % 100000 + i as u64))
            .collect();
        let cluster_names: Vec<String> = (0..pool_size)
            .map(|i| Self::generate_name_suffix_simple(i + 4000))
            .collect();

        // Generate trace/span/user pools
        let trace_ids: Vec<String> = (0..pool_size)
            .map(|_| format!("trace_{}", temp_rng.next_u64()))
            .collect();
        let span_ids: Vec<String> = (0..pool_size)
            .map(|_| format!("span_{}", temp_rng.next_u64()))
            .collect();
        let user_ids: Vec<String> = (0..pool_size)
            .map(|_| format!("user_{}", (temp_rng.next_u32() % 9999) + 1))
            .collect();
        let session_ids: Vec<String> = (0..pool_size)
            .map(|_| format!("session_{}", temp_rng.next_u64()))
            .collect();
        let request_ids: Vec<String> = (0..pool_size)
            .map(|_| format!("req_{}", temp_rng.next_u64()))
            .collect();

        // Generate log UIDs pool
        let log_uids: Vec<String> = (0..pool_size)
            .map(|i| format!("log_{}_{}", base_time + i as i64, i))
            .collect();

        // Pre-generate log entries in batches for better performance
        let mut log_entries = Vec::with_capacity(pool_size);
        for _ in 0..pool_size {
            log_entries.push(log_helper.generate_text_with_len(1500));
        }

        let elapsed = start.elapsed();
        println!("Pre-generation completed in {}ms", elapsed.as_millis());

        Self {
            table_name: table_name.to_string(),
            row_count: config.table_row_count,
            current_row: 0,
            base_time,
            host_ids,
            host_names,
            service_ids,
            service_names,
            container_ids,
            container_names,
            pod_ids,
            pod_names,
            cluster_ids,
            cluster_names,
            trace_ids,
            span_ids,
            user_ids,
            session_ids,
            request_ids,
            log_uids,
            log_entries,
        }
    }

    /// Generate optimized name suffix using lookup table (deterministic version)
    fn generate_name_suffix_simple(seed: usize) -> String {
        // Use deterministic generation for better performance
        const SUFFIXES: &[&str] = &[
            "alpha",
            "beta",
            "gamma",
            "delta",
            "epsilon",
            "zeta",
            "eta",
            "theta",
            "iota",
            "kappa",
            "lambda",
            "mu",
            "nu",
            "xi",
            "omicron",
            "pi",
            "rho",
            "sigma",
            "tau",
            "upsilon",
            "phi",
            "chi",
            "psi",
            "omega",
            "prime",
            "secondary",
            "tertiary",
            "main",
            "backup",
            "standby",
            "primary",
            "replica",
            "master",
            "worker",
            "node",
            "edge",
        ];

        let idx = seed % SUFFIXES.len();
        format!("{}{}", SUFFIXES[idx], seed % 1000)
    }

    /// Generate a single log row using ultra-fast pre-generated pools
    fn generate_row(&mut self) -> Option<Row> {
        if self.current_row >= self.row_count {
            return None;
        }

        // Ultra-fast index calculation using bit operations for better performance
        let pool_len = self.host_ids.len();
        let base_idx = self.current_row % pool_len;

        // Use current row + small random offset for deterministic yet varied data
        let random_offset = (self.current_row * 7 + 13) % pool_len; // Simple pseudo-random
        let timestamp =
            self.base_time + self.current_row as i64 + (random_offset as i64 % 2000) - 1000;

        // Use pre-generated values with minimal cloning by accessing directly
        let log_uid = &self.log_uids[base_idx];

        // Get pre-generated log entry with circular access
        let log_entry_idx = self.current_row % self.log_entries.len();
        let (log_level, log_message) = &self.log_entries[log_entry_idx];

        // Use offset indices for variety without modulo operations
        let idx1 = base_idx;
        let idx2 = (base_idx + 1) % pool_len;
        let idx3 = (base_idx + 2) % pool_len;
        let idx4 = (base_idx + 3) % pool_len;
        let idx5 = (base_idx + 4) % pool_len;

        let response_time_ms = ((base_idx % 999) + 1) as i64;

        self.current_row += 1;

        // Directly create values using references to avoid unnecessary clones
        Some(Row::new().add_values(vec![
            Value::TimestampMillisecond(timestamp),
            Value::String(log_uid.clone()),
            Value::String(log_message.clone()),
            Value::String(log_level.clone()),
            Value::String(self.host_ids[idx1].clone()),
            Value::String(self.host_names[idx1].clone()),
            Value::String(self.service_ids[idx2].clone()),
            Value::String(self.service_names[idx2].clone()),
            Value::String(self.container_ids[idx3].clone()),
            Value::String(self.container_names[idx3].clone()),
            Value::String(self.pod_ids[idx4].clone()),
            Value::String(self.pod_names[idx4].clone()),
            Value::String(self.cluster_ids[idx5].clone()),
            Value::String(self.cluster_names[idx5].clone()),
            Value::String(self.trace_ids[idx1].clone()),
            Value::String(self.span_ids[idx2].clone()),
            Value::String(self.user_ids[idx3].clone()),
            Value::String(self.session_ids[idx4].clone()),
            Value::String(self.request_ids[idx5].clone()),
            Value::Int64(response_time_ms),
            Value::String("application".to_string()),
            Value::String("v1.0.0".to_string()),
        ]))
    }

    /// Generate a single api::v1::Row for regular API
    fn generate_api_row(&mut self) -> Option<ApiRow> {
        if self.current_row >= self.row_count {
            return None;
        }

        // Use the same deterministic generation logic as bulk API
        let pool_len = self.host_ids.len();
        let base_idx = self.current_row % pool_len;

        // Use current row + small random offset for deterministic yet varied data
        let random_offset = (self.current_row * 7 + 13) % pool_len; // Simple pseudo-random
        let timestamp =
            self.base_time + self.current_row as i64 + (random_offset as i64 % 2000) - 1000;

        // Use pre-generated values with minimal cloning by accessing directly
        let log_uid = &self.log_uids[base_idx];

        // Get pre-generated log entry with circular access
        let log_entry_idx = self.current_row % self.log_entries.len();
        let (log_level, log_message) = &self.log_entries[log_entry_idx];

        // Use offset indices for variety without modulo operations
        let idx1 = base_idx;
        let idx2 = (base_idx + 1) % pool_len;
        let idx3 = (base_idx + 2) % pool_len;
        let idx4 = (base_idx + 3) % pool_len;
        let idx5 = (base_idx + 4) % pool_len;

        let response_time_ms = ((base_idx % 999) + 1) as i64;

        // Create api::v1::Row with values in the same order as schema
        let api_row = ApiRow {
            values: vec![
                timestamp_millisecond_value(timestamp),
                string_value(log_uid.clone()),
                string_value(log_message.clone()),
                string_value(log_level.clone()),
                string_value(self.host_ids[idx1].clone()),
                string_value(self.host_names[idx1].clone()),
                string_value(self.service_ids[idx2].clone()),
                string_value(self.service_names[idx2].clone()),
                string_value(self.container_ids[idx3].clone()),
                string_value(self.container_names[idx3].clone()),
                string_value(self.pod_ids[idx4].clone()),
                string_value(self.pod_names[idx4].clone()),
                string_value(self.cluster_ids[idx5].clone()),
                string_value(self.cluster_names[idx5].clone()),
                string_value(self.trace_ids[idx1].clone()),
                string_value(self.span_ids[idx2].clone()),
                string_value(self.user_ids[idx3].clone()),
                string_value(self.session_ids[idx4].clone()),
                string_value(self.request_ids[idx5].clone()),
                i64_value(response_time_ms),
                string_value("application".to_string()),
                string_value("v1.0.0".to_string()),
            ],
        };

        self.current_row += 1;
        Some(api_row)
    }
}

impl DataProvider for LogTableDataProvider {
    fn row_count(&self) -> usize {
        self.row_count
    }
}

impl TableDataProvider for LogTableDataProvider {
    fn table_schema(&self) -> TableSchema {
        TableSchema::builder()
            .name(&self.table_name)
            .build()
            .unwrap()
            .add_timestamp("ts", ColumnDataType::TimestampMillisecond)
            .add_field("log_uid", ColumnDataType::String)
            .add_field("log_message", ColumnDataType::String)
            .add_field("log_level", ColumnDataType::String)
            .add_field("host_id", ColumnDataType::String)
            .add_field("host_name", ColumnDataType::String)
            .add_field("service_id", ColumnDataType::String)
            .add_field("service_name", ColumnDataType::String)
            .add_field("container_id", ColumnDataType::String)
            .add_field("container_name", ColumnDataType::String)
            .add_field("pod_id", ColumnDataType::String)
            .add_field("pod_name", ColumnDataType::String)
            .add_field("cluster_id", ColumnDataType::String)
            .add_field("cluster_name", ColumnDataType::String)
            .add_field("trace_id", ColumnDataType::String)
            .add_field("span_id", ColumnDataType::String)
            .add_field("user_id", ColumnDataType::String)
            .add_field("session_id", ColumnDataType::String)
            .add_field("request_id", ColumnDataType::String)
            .add_field("response_time_ms", ColumnDataType::Int64)
            .add_field("log_source", ColumnDataType::String)
            .add_field("version", ColumnDataType::String)
    }

    fn rows(&mut self) -> Box<dyn Iterator<Item = Row> + '_> {
        Box::new(LogRowIterator { provider: self })
    }
}

impl ApiDataProvider for LogTableDataProvider {
    fn table_name(&self) -> &str {
        &self.table_name
    }

    fn api_schema(&self) -> Vec<ColumnSchema> {
        vec![
            ColumnSchema {
                column_name: "ts".to_string(),
                datatype: ApiColumnDataType::TimestampMillisecond as i32,
                semantic_type: SemanticType::Timestamp as i32,
                ..Default::default()
            },
            ColumnSchema {
                column_name: "log_uid".to_string(),
                datatype: ApiColumnDataType::String as i32,
                semantic_type: SemanticType::Field as i32,
                ..Default::default()
            },
            ColumnSchema {
                column_name: "log_message".to_string(),
                datatype: ApiColumnDataType::String as i32,
                semantic_type: SemanticType::Field as i32,
                ..Default::default()
            },
            ColumnSchema {
                column_name: "log_level".to_string(),
                datatype: ApiColumnDataType::String as i32,
                semantic_type: SemanticType::Field as i32,
                ..Default::default()
            },
            ColumnSchema {
                column_name: "host_id".to_string(),
                datatype: ApiColumnDataType::String as i32,
                semantic_type: SemanticType::Field as i32,
                ..Default::default()
            },
            ColumnSchema {
                column_name: "host_name".to_string(),
                datatype: ApiColumnDataType::String as i32,
                semantic_type: SemanticType::Field as i32,
                datatype_extension: None,
                options: None,
            },
            ColumnSchema {
                column_name: "service_id".to_string(),
                datatype: ApiColumnDataType::String as i32,
                semantic_type: SemanticType::Field as i32,
                ..Default::default()
            },
            ColumnSchema {
                column_name: "service_name".to_string(),
                datatype: ApiColumnDataType::String as i32,
                semantic_type: SemanticType::Field as i32,
                ..Default::default()
            },
            ColumnSchema {
                column_name: "container_id".to_string(),
                datatype: ApiColumnDataType::String as i32,
                semantic_type: SemanticType::Field as i32,
                ..Default::default()
            },
            ColumnSchema {
                column_name: "container_name".to_string(),
                datatype: ApiColumnDataType::String as i32,
                semantic_type: SemanticType::Field as i32,
                ..Default::default()
            },
            ColumnSchema {
                column_name: "pod_id".to_string(),
                datatype: ApiColumnDataType::String as i32,
                semantic_type: SemanticType::Field as i32,
                ..Default::default()
            },
            ColumnSchema {
                column_name: "pod_name".to_string(),
                datatype: ApiColumnDataType::String as i32,
                semantic_type: SemanticType::Field as i32,
                ..Default::default()
            },
            ColumnSchema {
                column_name: "cluster_id".to_string(),
                datatype: ApiColumnDataType::String as i32,
                semantic_type: SemanticType::Field as i32,
                ..Default::default()
            },
            ColumnSchema {
                column_name: "cluster_name".to_string(),
                datatype: ApiColumnDataType::String as i32,
                semantic_type: SemanticType::Field as i32,
                ..Default::default()
            },
            ColumnSchema {
                column_name: "trace_id".to_string(),
                datatype: ApiColumnDataType::String as i32,
                semantic_type: SemanticType::Field as i32,
                ..Default::default()
            },
            ColumnSchema {
                column_name: "span_id".to_string(),
                datatype: ApiColumnDataType::String as i32,
                semantic_type: SemanticType::Field as i32,
                ..Default::default()
            },
            ColumnSchema {
                column_name: "user_id".to_string(),
                datatype: ApiColumnDataType::String as i32,
                semantic_type: SemanticType::Field as i32,
                ..Default::default()
            },
            ColumnSchema {
                column_name: "session_id".to_string(),
                datatype: ApiColumnDataType::String as i32,
                semantic_type: SemanticType::Field as i32,
                ..Default::default()
            },
            ColumnSchema {
                column_name: "request_id".to_string(),
                datatype: ApiColumnDataType::String as i32,
                semantic_type: SemanticType::Field as i32,
                ..Default::default()
            },
            ColumnSchema {
                column_name: "response_time_ms".to_string(),
                datatype: ApiColumnDataType::Int64 as i32,
                semantic_type: SemanticType::Field as i32,
                ..Default::default()
            },
            ColumnSchema {
                column_name: "log_source".to_string(),
                datatype: ApiColumnDataType::String as i32,
                semantic_type: SemanticType::Field as i32,
                ..Default::default()
            },
            ColumnSchema {
                column_name: "version".to_string(),
                datatype: ApiColumnDataType::String as i32,
                semantic_type: SemanticType::Field as i32,
                ..Default::default()
            },
        ]
    }

    fn api_rows(&mut self) -> Box<dyn Iterator<Item = ApiRow> + '_> {
        Box::new(ApiRowIterator {
            provider: self,
            current_row: 0,
        })
    }
}

/// Iterator for api::v1::Row (Regular API)
pub struct ApiRowIterator<'a> {
    provider: &'a mut LogTableDataProvider,
    current_row: usize,
}

impl<'a> Iterator for ApiRowIterator<'a> {
    type Item = ApiRow;

    fn next(&mut self) -> Option<Self::Item> {
        // Save the current state and restore provider's state
        let saved_current = self.provider.current_row;
        self.provider.current_row = self.current_row;

        let result = self.provider.generate_api_row();

        // Update our iterator state and restore provider state
        self.current_row = self.provider.current_row;
        self.provider.current_row = saved_current;

        result
    }
}

/// Iterator for LogTableDataProvider rows
struct LogRowIterator<'a> {
    provider: &'a mut LogTableDataProvider,
}

impl<'a> Iterator for LogRowIterator<'a> {
    type Item = Row;

    fn next(&mut self) -> Option<Self::Item> {
        self.provider.generate_row()
    }
}
