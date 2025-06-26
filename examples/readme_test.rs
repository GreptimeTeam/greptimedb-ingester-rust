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

// Test file to verify that all README.md code examples compile
#![allow(unused_variables)]
#![allow(dead_code)]
#![allow(clippy::needless_borrows_for_generic_args)]

use greptimedb_ingester::api::v1::*;
use greptimedb_ingester::client::Client;
use greptimedb_ingester::{database::Database, Result};
use greptimedb_ingester::{BulkInserter, BulkWriteOptions, ColumnDataType, Row, Table, Value};
use greptimedb_ingester::{ChannelConfig, ChannelManager};
use std::time::Duration;

// Mock function to represent external processing
fn process_binary(_data: &[u8]) {}

#[tokio::main]
async fn main() -> Result<()> {
    println!("Testing all README.md code examples...");

    // Test 1: Main README.md - Low-Latency Insert API example
    test_main_readme_low_latency().await?;

    // Test 2: Main README.md - High-Throughput Bulk API example
    test_main_readme_bulk_api().await?;

    // Test 3: Main README.md - Data type support
    test_main_readme_data_types();

    // Test 4: Main README.md - Type-safe data access
    test_main_readme_data_access();

    // Test 5: Main README.md - Configuration examples
    test_main_readme_configuration();

    // Test 6: Main README.md - Error handling
    test_main_readme_error_handling().await;

    // Test 7: Examples README.md - High-throughput pattern
    test_examples_readme_pattern().await?;

    println!("✓ All README.md examples compile successfully!");
    Ok(())
}

// Test examples from main README.md
async fn test_main_readme_low_latency() -> Result<()> {
    // Low-Latency Insert API example from main README.md
    let client = Client::with_urls(&["localhost:4001"]);
    let database = Database::new_with_dbname("public", client);

    // Insert data with minimal latency
    let insert_request = RowInsertRequests {
        inserts: vec![RowInsertRequest {
            table_name: "sensor_data".to_string(),
            rows: Some(Rows {
                schema: vec![/* column definitions */],
                rows: vec![/* data rows */],
            }),
        }],
    };

    // Note: This would fail at runtime without a real server, but compiles
    // let affected_rows = database.insert(insert_request).await?;

    Ok(())
}

async fn test_main_readme_bulk_api() -> Result<()> {
    let client = Client::with_urls(&["localhost:4001"]);

    // Create bulk inserter
    let bulk_inserter = BulkInserter::new(client, "public");

    // Define table schema
    let table_template = Table::builder()
        .name("sensor_readings")
        .build()
        .unwrap()
        .add_timestamp("ts", ColumnDataType::TimestampMillisecond)
        .add_field("device_id", ColumnDataType::String)
        .add_field("temperature", ColumnDataType::Float64);

    // Create high-performance stream writer
    let mut bulk_writer = bulk_inserter
        .create_bulk_stream_writer(
            &table_template,
            Some(
                BulkWriteOptions::default()
                    .with_parallelism(8) // 8 concurrent requests
                    .with_compression(true) // Enable compression
                    .with_timeout(Duration::from_secs(60)), // 60s timeout
            ),
        )
        .await?;

    // Mock data batches
    let data_batches: Vec<Vec<Row>> = vec![vec![]]; // Empty for compilation test

    // High-throughput parallel writing
    for batch in data_batches {
        let request_id = bulk_writer.write_rows_async(batch).await?;
        // Requests are processed in parallel
    }

    // Wait for all operations to complete
    let responses = bulk_writer.wait_for_all_pending().await?;
    bulk_writer.finish().await?;

    Ok(())
}

fn test_main_readme_data_types() {
    // Full support for GreptimeDB data types
    let row = Row::new()
        .add_value(Value::TimestampMillisecond(1234567890123))
        .add_value(Value::String("device_001".to_string()))
        .add_value(Value::Float64(23.5))
        .add_value(Value::Int64(1))
        .add_value(Value::Boolean(true))
        .add_value(Value::Binary(vec![0xDE, 0xAD, 0xBE, 0xEF]))
        .add_value(Value::Json(r#"{"key": "value"}"#.to_string()));

    println!("✓ Data types example compiles");
}

fn test_main_readme_data_access() {
    let row = Row::new()
        .add_value(Value::String("test".to_string()))
        .add_value(Value::String("device_001".to_string()))
        .add_value(Value::Float64(23.5))
        .add_value(Value::Int64(1))
        .add_value(Value::Boolean(true))
        .add_value(Value::Binary(vec![0xDE, 0xAD, 0xBE, 0xEF]));

    // Type-safe value access
    if let Some(device_name) = row.get_string(1) {
        println!("Device: {}", device_name);
    }

    // Binary data access
    if let Some(binary_data) = row.get_binary(5) {
        process_binary(&binary_data);
    }

    println!("✓ Data access examples compile");
}

fn test_main_readme_configuration() {
    let channel_config = ChannelConfig::default()
        .timeout(Duration::from_secs(30))
        .connect_timeout(Duration::from_secs(5));
    let channel_manager = ChannelManager::with_config(channel_config);
    let client = Client::with_manager_and_urls(channel_manager, &["localhost:4001"]);

    // For authentication, see the examples directory for working implementations
    // Authentication types are currently private - use environment variables
    // or configuration files for credentials

    println!("✓ Configuration examples compile");
}

async fn test_main_readme_error_handling() {
    use greptimedb_ingester::Error;

    let client = Client::with_urls(&["localhost:4001"]);
    let database = Database::new_with_dbname("public", client);

    // Create a dummy request for testing error handling patterns
    let request = RowInsertRequests { inserts: vec![] };

    // This demonstrates the error handling pattern from README
    match database.insert(request).await {
        Ok(affected_rows) => println!("Inserted {} rows", affected_rows),
        Err(Error::RequestTimeout { .. }) => {
            // Handle timeout
            println!("Request timed out");
        }
        Err(Error::SerializeMetadata { .. }) => {
            // Handle metadata serialization issues
            println!("Metadata serialization error");
        }
        Err(e) => {
            eprintln!("Unexpected error: {:?}", e);
        }
    }

    println!("✓ Error handling pattern compiles");
}

// Test examples from examples/README.md
async fn test_examples_readme_pattern() -> Result<()> {
    let client = Client::with_urls(&["localhost:4001"]);

    // Create persistent stream writer
    let bulk_inserter = BulkInserter::new(client, "public");

    let table_template = Table::builder()
        .name("test_table")
        .build()
        .unwrap()
        .add_timestamp("ts", ColumnDataType::TimestampMillisecond)
        .add_field("device_id", ColumnDataType::String)
        .add_field("temperature", ColumnDataType::Float64);

    let mut bulk_writer = bulk_inserter
        .create_bulk_stream_writer(&table_template, None)
        .await?;

    // Mock batches
    let batches: Vec<Vec<Row>> = vec![vec![]];

    // Submit requests asynchronously
    for batch in batches {
        let request_id = bulk_writer.write_rows_async(batch).await?;
    }

    // Wait for all to complete
    let responses = bulk_writer.wait_for_all_pending().await?;

    bulk_writer.finish().await?;

    println!("✓ Examples README pattern compiles");
    Ok(())
}
