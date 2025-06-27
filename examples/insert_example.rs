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

#![allow(clippy::print_stderr)]
#![allow(clippy::print_stdout)]

mod config_utils;
use config_utils::DbConfig;

use greptimedb_ingester::api::v1::*;
use greptimedb_ingester::client::Client;
use greptimedb_ingester::helpers::schema::*;
use greptimedb_ingester::helpers::values::*;
use greptimedb_ingester::{database::Database, Result};

/// Example of real-time sensor data insertion using low-level API
/// Best for: Low-latency requirements, small batches, real-time data streaming
pub async fn realtime_sensor_ingest() -> Result<()> {
    let config = DbConfig::from_env();
    let urls = vec![config.endpoint.clone()];

    println!("=== Real-time Insert Example ===");
    println!("Use case: Low-latency, small batch inserts for real-time applications");
    println!("When to use: IoT sensors, real-time monitoring, interactive applications\n");

    let grpc_client = Client::with_urls(&urls);
    let database = Database::new_with_dbname(&config.database, grpc_client);

    // Simulate real-time data arrival - small batches with immediate processing
    for batch_num in 1..=5 {
        println!("Processing real-time batch {batch_num}...");

        let sensor_schema = build_sensor_schema();
        let sensor_data = build_current_sensor_reading(batch_num);

        let insert_request = RowInsertRequests {
            inserts: vec![RowInsertRequest {
                table_name: "realtime_sensor_readings".to_owned(),
                rows: Some(Rows {
                    schema: sensor_schema,
                    rows: sensor_data,
                }),
            }],
        };

        let start_time = std::time::Instant::now();
        let affected_rows = database.insert(insert_request).await?;
        let latency = start_time.elapsed();

        println!(
            "  ✓ Inserted {} rows in {:?} (latency: {:.1}ms)",
            affected_rows,
            latency,
            latency.as_millis()
        );

        // Simulate real-time intervals
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }

    println!("\n✓ Real-time insertion completed successfully!");
    Ok(())
}

/// Use schema helpers to build column definitions for sensor table  
fn build_sensor_schema() -> Vec<ColumnSchema> {
    vec![
        // Tag columns - for indexing and grouping
        tag("device_id", ColumnDataType::String),
        tag("location", ColumnDataType::String),
        // Timestamp column - timeline for time series data
        timestamp("ts", ColumnDataType::TimestampMillisecond),
        // Field columns - actual measurement values
        field("temperature", ColumnDataType::Float64),
        field("humidity", ColumnDataType::Float64),
        field("pressure", ColumnDataType::Float64),
        field("battery_level", ColumnDataType::Int32),
        field("is_online", ColumnDataType::Boolean),
    ]
}

/// Generate current sensor readings (simulating real-time data)
fn build_current_sensor_reading(batch_num: usize) -> Vec<Row> {
    use std::time::{SystemTime, UNIX_EPOCH};

    let current_time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    // Generate 1-3 sensor readings per batch (small, real-time batches)
    let sensor_count = (batch_num % 3) + 1;
    let mut rows = Vec::new();

    for i in 0..sensor_count {
        let sensor_id = format!("sensor_{:03}", (batch_num * 10) + i);
        let location = match i % 3 {
            0 => "data_center_rack_1",
            1 => "data_center_rack_2",
            _ => "data_center_rack_3",
        };

        // Simulate realistic sensor readings with some variation
        let base_temp = 22.0 + (batch_num as f64 * 0.5);
        let temperature = base_temp + (i as f64 * 0.2);
        let humidity = 55.0 + ((batch_num + i) as f64 * 0.8) % 20.0;
        let pressure = 1013.0 + ((batch_num as f64) * 0.1);
        let battery = 100 - ((batch_num + i) % 50) as i32;
        let is_online = battery > 10;

        rows.push(Row {
            values: vec![
                string_value(sensor_id),
                string_value(location.to_string()),
                timestamp_millisecond_value(current_time),
                f64_value(temperature),
                f64_value(humidity),
                f64_value(pressure),
                i32_value(battery),
                bool_value(is_online),
            ],
        });
    }

    rows
}

/// Demonstrate various data types support
/// Shows how to handle different column types in low-latency scenarios
pub async fn data_types_demonstration() -> Result<()> {
    let config = DbConfig::from_env();
    let urls = vec![config.endpoint.clone()];
    let grpc_client = Client::with_urls(&urls);
    let database = Database::new_with_dbname(&config.database, grpc_client);

    println!("=== Data Types Example ===");
    println!("Demonstrating support for various GreptimeDB column types\n");

    // Build table with comprehensive data type coverage
    let mixed_schema = vec![
        tag("category", ColumnDataType::String),
        timestamp("event_time", ColumnDataType::TimestampNanosecond),
        field("int8_val", ColumnDataType::Int8),
        field("int16_val", ColumnDataType::Int16),
        field("int64_val", ColumnDataType::Int64),
        field("uint32_val", ColumnDataType::Uint32),
        field("float32_val", ColumnDataType::Float32),
        field("binary_data", ColumnDataType::Binary),
        field("date_val", ColumnDataType::Date),
        field("json_data", ColumnDataType::Json),
    ];

    use std::time::{SystemTime, UNIX_EPOCH};
    let current_time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as i64;

    let mixed_data = vec![
        Row {
            values: vec![
                string_value("user_event".to_string()),
                timestamp_nanosecond_value(current_time),
                i8_value(127),
                i16_value(32767),
                i64_value(1234567890123456789),
                u32_value(4294967295),
                f32_value(std::f32::consts::PI),
                binary_value(b"Hello, GreptimeDB!".to_vec()),
                date_value(19358), // 2023-01-01
                string_value(
                    r#"{"user_id": 12345, "action": "login", "ip": "192.168.1.100"}"#.to_string(),
                ),
            ],
        },
        Row {
            values: vec![
                string_value("system_metric".to_string()),
                timestamp_nanosecond_value(current_time + 1_000_000), // +1ms
                i8_value(-128),
                i16_value(-32768),
                i64_value(-9223372036854775808),
                u32_value(0),
                f32_value(std::f32::consts::E), // Euler's number
                binary_value(vec![0xDE, 0xAD, 0xBE, 0xEF]),
                date_value(19359), // 2023-01-02
                string_value(
                    r#"{"cpu_usage": 0.75, "memory_gb": 8.5, "disk_free_gb": 120.7}"#.to_string(),
                ),
            ],
        },
    ];

    let insert_request = RowInsertRequests {
        inserts: vec![RowInsertRequest {
            table_name: "data_types_demo".to_string(),
            rows: Some(Rows {
                schema: mixed_schema,
                rows: mixed_data,
            }),
        }],
    };

    let start_time = std::time::Instant::now();
    let affected_rows = database.insert(insert_request).await?;
    let latency = start_time.elapsed();

    println!("✓ Successfully inserted {affected_rows} rows with various data types");
    println!("  Insertion latency: {:.1}ms", latency.as_millis());

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    println!("Starting low-level ingestion examples...");

    realtime_sensor_ingest().await?;

    data_types_demonstration().await?;

    println!("All examples completed successfully!");
    Ok(())
}
