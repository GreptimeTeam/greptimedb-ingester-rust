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

// Integration tests for GreptimeDB Rust Ingester
// These tests require a running GreptimeDB instance

use greptimedb_ingester::api::v1::*;
use greptimedb_ingester::client::Client;
use greptimedb_ingester::helpers::schema::*;
use greptimedb_ingester::helpers::values::*;
use greptimedb_ingester::{
    database::Database, BulkInserter, BulkWriteOptions, ColumnDataType, Result, Rows as BulkRows,
    TableSchema,
};
use std::time::{SystemTime, UNIX_EPOCH};

// Test configuration
struct TestConfig {
    endpoint: String,
    database: String,
}

impl TestConfig {
    fn new() -> Self {
        Self {
            endpoint: std::env::var("GREPTIMEDB_TEST_ENDPOINT")
                .unwrap_or_else(|_| "localhost:4001".to_string()),
            database: std::env::var("GREPTIMEDB_TEST_DATABASE")
                .unwrap_or_else(|_| "test_db".to_string()),
        }
    }
}

// Test helper to create unique table names
fn unique_table_name(prefix: &str) -> String {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    format!("{prefix}_{timestamp}")
}

#[tokio::test]
async fn test_low_latency_insert_basic() -> Result<()> {
    let config = TestConfig::new();
    let client = Client::with_urls([&config.endpoint]);
    let database = Database::new_with_dbname(&config.database, client);

    let table_name = unique_table_name("low_latency_basic");

    // Create insert request with basic data types
    let schema = vec![
        tag("device_id", ColumnDataType::String),
        timestamp("ts", ColumnDataType::TimestampMillisecond),
        field("temperature", ColumnDataType::Float64),
        field("status", ColumnDataType::Boolean),
    ];

    let current_time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    let rows = vec![
        Row {
            values: vec![
                string_value("device_001".to_string()),
                timestamp_millisecond_value(current_time),
                f64_value(23.5),
                bool_value(true),
            ],
        },
        Row {
            values: vec![
                string_value("device_002".to_string()),
                timestamp_millisecond_value(current_time + 1000),
                f64_value(24.2),
                bool_value(false),
            ],
        },
    ];

    let insert_request = RowInsertRequests {
        inserts: vec![RowInsertRequest {
            table_name: table_name.clone(),
            rows: Some(Rows { schema, rows }),
        }],
    };

    // Execute insert and verify
    let affected_rows = database.insert(insert_request).await?;
    assert_eq!(affected_rows, 2);

    println!("✓ Low-latency insert test passed: {affected_rows} rows inserted");
    Ok(())
}

#[tokio::test]
async fn test_bulk_stream_writer_sequential() -> Result<()> {
    let config = TestConfig::new();
    let client = Client::with_urls([&config.endpoint]);

    let table_name = unique_table_name("bulk_sequential");

    // Important: Bulk API does not create tables automatically
    // We must create the table manually first - here we use insert API
    println!("Creating table '{table_name}' using insert API (auto table creation)...");
    let database = Database::new_with_dbname(&config.database, client.clone());

    let init_schema = vec![
        timestamp("ts", ColumnDataType::TimestampMillisecond),
        field("sensor_id", ColumnDataType::String),
        field("temperature", ColumnDataType::Float64),
        field("status", ColumnDataType::Int64),
    ];

    let current_time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    let init_row = Row {
        values: vec![
            timestamp_millisecond_value(current_time),
            string_value("init_sensor".to_string()),
            f64_value(20.0),
            i64_value(1),
        ],
    };

    let init_request = RowInsertRequests {
        inserts: vec![RowInsertRequest {
            table_name: table_name.clone(),
            rows: Some(Rows {
                schema: init_schema,
                rows: vec![init_row],
            }),
        }],
    };

    let init_result = database.insert(init_request).await?;
    println!("Table created with initial row: {init_result} rows inserted");

    // Now use bulk API (table already exists)
    let bulk_inserter = BulkInserter::new(client, &config.database);

    // Create table template (must match the schema above exactly)
    let table_template = TableSchema::builder()
        .name(&table_name)
        .build()
        .unwrap()
        .add_timestamp("ts", ColumnDataType::TimestampMillisecond)
        .add_field("sensor_id", ColumnDataType::String)
        .add_field("temperature", ColumnDataType::Float64)
        .add_field("status", ColumnDataType::Int64);

    let mut bulk_writer = bulk_inserter
        .create_bulk_stream_writer(
            &table_template,
            Some(BulkWriteOptions::default().with_parallelism(1)),
        )
        .await?;

    // Generate test data
    let batch_size = 100;
    let mut total_rows = 0;

    for batch_num in 0..3 {
        let rows = create_test_batch_bulk(&bulk_writer, batch_num, batch_size)?;
        let response = bulk_writer.write_rows(rows).await?;
        total_rows += response.affected_rows();

        println!(
            "Batch {} inserted: {} rows",
            batch_num,
            response.affected_rows()
        );
    }

    bulk_writer.finish().await?;

    assert_eq!(total_rows, batch_size * 3);
    println!("✓ Bulk sequential test passed: {total_rows} total rows (+ 1 initial row)");
    Ok(())
}

#[tokio::test]
async fn test_bulk_stream_writer_parallel() -> Result<()> {
    let config = TestConfig::new();
    let client = Client::with_urls([&config.endpoint]);

    let table_name = unique_table_name("bulk_parallel");

    // 🎯 Important: Bulk API requires existing tables - create manually first
    println!("🔧 Creating table '{table_name}' using insert API for parallel test...");
    let database = Database::new_with_dbname(&config.database, client.clone());

    let init_schema = vec![
        timestamp("ts", ColumnDataType::TimestampMillisecond),
        field("sensor_id", ColumnDataType::String),
        field("temperature", ColumnDataType::Float64),
        field("status", ColumnDataType::Int64),
    ];

    let current_time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    let init_request = RowInsertRequests {
        inserts: vec![RowInsertRequest {
            table_name: table_name.clone(),
            rows: Some(Rows {
                schema: init_schema,
                rows: vec![Row {
                    values: vec![
                        timestamp_millisecond_value(current_time),
                        string_value("parallel_init_sensor".to_string()),
                        f64_value(25.0),
                        i64_value(1),
                    ],
                }],
            }),
        }],
    };

    let init_result = database.insert(init_request).await?;
    println!("Parallel test table created: {init_result} rows inserted");

    // Now use bulk API for parallel operations
    let bulk_inserter = BulkInserter::new(client, &config.database);

    let table_template = TableSchema::builder()
        .name(&table_name)
        .build()
        .unwrap()
        .add_timestamp("ts", ColumnDataType::TimestampMillisecond)
        .add_field("sensor_id", ColumnDataType::String)
        .add_field("temperature", ColumnDataType::Float64)
        .add_field("status", ColumnDataType::Int64);

    let mut bulk_writer = bulk_inserter
        .create_bulk_stream_writer(
            &table_template,
            Some(BulkWriteOptions::default().with_parallelism(4)),
        )
        .await?;

    // Submit multiple batches asynchronously
    let batch_size = 50;
    let batch_count = 5;
    let mut request_ids = Vec::new();

    for batch_num in 0..batch_count {
        let rows = create_test_batch_bulk(&bulk_writer, batch_num, batch_size)?;
        let request_id = bulk_writer.write_rows_async(rows).await?;
        request_ids.push(request_id);
    }

    // Wait for all requests to complete
    let responses = bulk_writer.finish_with_responses().await?;

    let total_rows: usize = responses.iter().map(|r| r.affected_rows()).sum();
    assert_eq!(responses.len(), batch_count);
    assert_eq!(total_rows, batch_size * batch_count);

    println!(
        "✓ Bulk parallel test passed: {} batches, {} total rows (+ 1 initial row)",
        responses.len(),
        total_rows
    );
    Ok(())
}

#[tokio::test]
async fn test_data_types_comprehensive() -> Result<()> {
    let config = TestConfig::new();
    let client = Client::with_urls([&config.endpoint]);
    let database = Database::new_with_dbname(&config.database, client);

    let table_name = unique_table_name("data_types_test");

    // Test all supported data types
    let schema = vec![
        tag("category", ColumnDataType::String),
        timestamp("ts", ColumnDataType::TimestampMillisecond),
        field("int8_val", ColumnDataType::Int8),
        field("int16_val", ColumnDataType::Int16),
        field("int32_val", ColumnDataType::Int32),
        field("int64_val", ColumnDataType::Int64),
        field("uint8_val", ColumnDataType::Uint8),
        field("uint16_val", ColumnDataType::Uint16),
        field("uint32_val", ColumnDataType::Uint32),
        field("uint64_val", ColumnDataType::Uint64),
        field("float32_val", ColumnDataType::Float32),
        field("float64_val", ColumnDataType::Float64),
        field("bool_val", ColumnDataType::Boolean),
        field("binary_val", ColumnDataType::Binary),
        field("string_val", ColumnDataType::String),
        field("date_val", ColumnDataType::Date),
    ];

    let current_time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    let rows = vec![Row {
        values: vec![
            string_value("test_category".to_string()),
            timestamp_millisecond_value(current_time),
            i8_value(127),
            i16_value(32767),
            i32_value(2147483647),
            i64_value(9223372036854775807),
            u8_value(255),
            u16_value(65535),
            u32_value(4294967295),
            u64_value(18446744073709551615),
            f32_value(5.14159_f32),
            f64_value(3.718281828459045_f64),
            bool_value(true),
            binary_value(b"test_binary_data".to_vec()),
            string_value("test_string".to_string()),
            date_value(19358), // 2023-01-01
        ],
    }];

    let insert_request = RowInsertRequests {
        inserts: vec![RowInsertRequest {
            table_name,
            rows: Some(Rows { schema, rows }),
        }],
    };

    let affected_rows = database.insert(insert_request).await?;
    assert_eq!(affected_rows, 1);

    println!("✓ Data types test passed: {affected_rows} row with all data types");
    Ok(())
}

#[tokio::test]
async fn test_error_handling() -> Result<()> {
    let config = TestConfig::new();
    let client = Client::with_urls([&config.endpoint]);
    let database = Database::new_with_dbname(&config.database, client);

    // Test with invalid schema (mismatched data types)
    let schema = vec![
        timestamp("ts", ColumnDataType::TimestampMillisecond),
        field("temperature", ColumnDataType::Float64),
    ];

    let rows = vec![Row {
        values: vec![
            string_value("invalid_timestamp".to_string()), // Wrong type
            f64_value(23.5),
        ],
    }];

    let insert_request = RowInsertRequests {
        inserts: vec![RowInsertRequest {
            table_name: unique_table_name("error_test"),
            rows: Some(Rows { schema, rows }),
        }],
    };

    // This should fail
    let result = database.insert(insert_request).await;
    assert!(result.is_err());

    println!("✓ Error handling test passed: correctly rejected invalid data");
    Ok(())
}

// Helper function to create test data batches for bulk operations
fn create_test_batch_bulk(
    bulk_writer: &greptimedb_ingester::BulkStreamWriter,
    batch_id: usize,
    batch_size: usize,
) -> Result<BulkRows> {
    use greptimedb_ingester::{Row, Value};

    let current_time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    let mut rows = bulk_writer.alloc_rows_buffer(batch_size, 1024)?;

    for i in 0..batch_size {
        let global_idx = batch_id * batch_size + i;
        let sensor_id = format!("sensor_{:06}", global_idx % 100);
        let temperature = 20.0 + (global_idx as f64 * 0.1) % 30.0;
        let timestamp = current_time + (global_idx as i64 * 10);
        let status = if global_idx % 10 == 0 { 0 } else { 1 };

        let row = Row::new().add_values(vec![
            Value::TimestampMillisecond(timestamp),
            Value::String(sensor_id),
            Value::Float64(temperature),
            Value::Int64(status),
        ]);
        rows.add_row(row)?;
    }

    Ok(rows)
}
