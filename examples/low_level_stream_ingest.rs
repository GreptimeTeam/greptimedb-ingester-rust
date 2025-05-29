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

use std::time::{SystemTime, UNIX_EPOCH};
use tokio::time::{sleep, Duration};

mod config_utils;
use config_utils::DbConfig;

use greptimedb_ingester::api::v1::*;
use greptimedb_ingester::helpers::schema::*;
use greptimedb_ingester::helpers::values::*;
use greptimedb_ingester::{ClientBuilder, Database, Result};

/// Demonstrates low-level streaming API for sensor data ingestion
/// This example shows how to continuously stream sensor readings to GreptimeDB
pub async fn low_level_streaming_sensor_ingest() -> Result<()> {
    let config = DbConfig::from_env();

    // 1. Create client connection with streaming configuration
    let grpc_client = ClientBuilder::default()
        .peers(vec![&config.endpoint])
        .compression(greptimedb_ingester::Compression::Gzip)
        .build();

    let database = Database::new_with_dbname(&config.database, grpc_client);

    let stream_inserter = database.streaming_inserter(2048, Some("ttl=1d"))?;

    println!("Starting streaming sensor data ingestion...");

    for batch_id in 1..=5 {
        let sensor_batch = create_sensor_batch(batch_id).await;

        if let Err(e) = stream_inserter.row_insert(sensor_batch).await {
            eprintln!("Error inserting batch {}: {}", batch_id, e);
        } else {
            println!(
                "Successfully queued sensor batch {} for insertion",
                batch_id
            );
        }

        sleep(Duration::from_millis(500)).await;
    }

    let total_rows = stream_inserter.finish().await?;
    println!("Streaming completed. Total rows inserted: {}", total_rows);

    Ok(())
}

/// Demonstrates concurrent streaming for multiple data types
/// This shows how to handle different types of time-series data simultaneously
pub async fn low_level_concurrent_streaming() -> Result<()> {
    let config = DbConfig::from_env();

    let grpc_client = ClientBuilder::default()
        .peers(vec![&config.endpoint])
        .build();

    let database = Database::new_with_dbname(&config.database, grpc_client);

    let metrics_stream = database.streaming_inserter(1024, Some("ttl=7d"))?;
    let events_stream = database.streaming_inserter(512, Some("ttl=1d"))?;

    println!("Starting concurrent streaming for metrics and events...");

    let metrics_handle = tokio::spawn(async move {
        for i in 1..=3 {
            let metrics_data = create_metrics_batch(i).await;
            if let Err(e) = metrics_stream.row_insert(metrics_data).await {
                eprintln!("Error inserting metrics batch {}: {}", i, e);
            }
            sleep(Duration::from_millis(300)).await;
        }
        metrics_stream.finish().await
    });

    let events_handle = tokio::spawn(async move {
        for i in 1..=3 {
            let events_data = create_events_batch(i).await;
            if let Err(e) = events_stream.row_insert(events_data).await {
                eprintln!("Error inserting events batch {}: {}", i, e);
            }
            sleep(Duration::from_millis(400)).await;
        }
        events_stream.finish().await
    });

    let (metrics_result, events_result) = tokio::join!(metrics_handle, events_handle);

    match metrics_result {
        Ok(Ok(rows)) => println!("Metrics stream completed. Rows inserted: {}", rows),
        Ok(Err(e)) => eprintln!("Metrics stream error: {}", e),
        Err(e) => eprintln!("Metrics task error: {}", e),
    }

    match events_result {
        Ok(Ok(rows)) => println!("Events stream completed. Rows inserted: {}", rows),
        Ok(Err(e)) => eprintln!("Events stream error: {}", e),
        Err(e) => eprintln!("Events task error: {}", e),
    }

    Ok(())
}

/// Demonstrates error handling and recovery in streaming scenarios
pub async fn low_level_streaming_with_error_handling() -> Result<()> {
    let config = DbConfig::from_env();

    let grpc_client = ClientBuilder::default()
        .peers(vec![&config.endpoint])
        .build();

    let database = Database::new_with_dbname(&config.database, grpc_client);

    let stream_inserter = database.streaming_inserter(128, None)?;

    println!("Starting streaming with error handling demonstration...");

    let mut successful_batches = 0;
    let mut failed_batches = 0;

    for batch_id in 1..=10 {
        let data_batch = create_variable_data_batch(batch_id).await;

        match stream_inserter.row_insert(data_batch).await {
            Ok(_) => {
                successful_batches += 1;
                println!("Batch {} queued successfully", batch_id);
            }
            Err(e) => {
                failed_batches += 1;
                eprintln!("Failed to queue batch {}: {}", batch_id, e);
            }
        }

        let delay = if batch_id % 3 == 0 { 100 } else { 50 };
        sleep(Duration::from_millis(delay)).await;
    }

    match stream_inserter.finish().await {
        Ok(total_rows) => {
            println!("Stream completed successfully!");
            println!("Successful batches: {}", successful_batches);
            println!("Failed batches: {}", failed_batches);
            println!("Total rows inserted: {}", total_rows);
        }
        Err(e) => {
            eprintln!("Error completing stream: {}", e);
        }
    }

    Ok(())
}

/// Creates a batch of sensor readings with current timestamp
async fn create_sensor_batch(batch_id: u32) -> RowInsertRequests {
    let current_time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    let rows = (1..=3)
        .map(|sensor_id| {
            let device_id = format!("sensor_{:03}", batch_id * 10 + sensor_id);
            let location = format!("zone_{}", batch_id % 4 + 1);

            Row {
                values: vec![
                    string_value(device_id),
                    string_value(location),
                    timestamp_millisecond_value(current_time + (sensor_id as i64 * 100)),
                    f64_value(20.0 + (batch_id as f64) + (sensor_id as f64 * 0.5)),
                    f64_value(50.0 + (batch_id as f64 * 2.0)),
                    f64_value(1013.0 + (batch_id as f64 * 0.1)),
                    i32_value(80 + (batch_id as i32 % 20)),
                    bool_value(batch_id % 2 == 0),
                ],
            }
        })
        .collect();

    RowInsertRequests {
        inserts: vec![RowInsertRequest {
            table_name: "streaming_sensors".to_owned(),
            rows: Some(Rows {
                schema: build_sensor_schema(),
                rows,
            }),
        }],
    }
}

/// Creates a batch of system metrics
async fn create_metrics_batch(batch_id: u32) -> RowInsertRequests {
    let current_time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    let rows = vec![
        Row {
            values: vec![
                string_value("cpu".to_string()),
                string_value(format!("server_{}", batch_id)),
                timestamp_millisecond_value(current_time),
                f32_value(50.0 + (batch_id as f32 * 5.0)),
                string_value("percentage".to_string()),
            ],
        },
        Row {
            values: vec![
                string_value("memory".to_string()),
                string_value(format!("server_{}", batch_id)),
                timestamp_millisecond_value(current_time + 1000),
                f32_value(70.0 + (batch_id as f32 * 3.0)),
                string_value("percentage".to_string()),
            ],
        },
    ];

    RowInsertRequests {
        inserts: vec![RowInsertRequest {
            table_name: "system_metrics".to_owned(),
            rows: Some(Rows {
                schema: build_metrics_schema(),
                rows,
            }),
        }],
    }
}

/// Creates a batch of application events
async fn create_events_batch(batch_id: u32) -> RowInsertRequests {
    let current_time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    let event_types = ["login", "logout", "error", "warning"];
    let event_type = event_types[batch_id as usize % event_types.len()];

    let rows = vec![Row {
        values: vec![
            string_value(format!("app_{}", batch_id)),
            string_value(event_type.to_string()),
            timestamp_millisecond_value(current_time),
            string_value(format!(
                "Event {} occurred in batch {}",
                event_type, batch_id
            )),
            i32_value(batch_id as i32),
        ],
    }];

    RowInsertRequests {
        inserts: vec![RowInsertRequest {
            table_name: "application_events".to_owned(),
            rows: Some(Rows {
                schema: build_events_schema(),
                rows,
            }),
        }],
    }
}

/// Creates variable data batches for error handling demonstration
async fn create_variable_data_batch(batch_id: u32) -> RowInsertRequests {
    let current_time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    // Create different sized batches to test various scenarios
    let batch_size = if batch_id % 5 == 0 { 10 } else { 3 };

    let rows = (1..=batch_size)
        .map(|i| Row {
            values: vec![
                string_value(format!("test_device_{}", batch_id * 100 + i)),
                timestamp_millisecond_value(current_time + (i as i64 * 10)),
                f64_value((batch_id * i) as f64 / 10.0),
                i32_value((batch_id + i) as i32),
                bool_value(i % 2 == 0),
            ],
        })
        .collect();

    RowInsertRequests {
        inserts: vec![RowInsertRequest {
            table_name: "test_streaming_data".to_owned(),
            rows: Some(Rows {
                schema: build_test_schema(),
                rows,
            }),
        }],
    }
}

/// Schema definition for sensor data
fn build_sensor_schema() -> Vec<ColumnSchema> {
    vec![
        tag("device_id", ColumnDataType::String),
        tag("location", ColumnDataType::String),
        timestamp("timestamp", ColumnDataType::TimestampMillisecond),
        field("temperature", ColumnDataType::Float64),
        field("humidity", ColumnDataType::Float64),
        field("pressure", ColumnDataType::Float64),
        field("battery_level", ColumnDataType::Int32),
        field("is_online", ColumnDataType::Boolean),
    ]
}

/// Schema definition for system metrics
fn build_metrics_schema() -> Vec<ColumnSchema> {
    vec![
        tag("metric_name", ColumnDataType::String),
        tag("host", ColumnDataType::String),
        timestamp("timestamp", ColumnDataType::TimestampMillisecond),
        field("value", ColumnDataType::Float32),
        field("unit", ColumnDataType::String),
    ]
}

/// Schema definition for application events
fn build_events_schema() -> Vec<ColumnSchema> {
    vec![
        tag("application", ColumnDataType::String),
        tag("event_type", ColumnDataType::String),
        timestamp("timestamp", ColumnDataType::TimestampMillisecond),
        field("message", ColumnDataType::String),
        field("event_id", ColumnDataType::Int32),
    ]
}

/// Schema definition for test data
fn build_test_schema() -> Vec<ColumnSchema> {
    vec![
        tag("device_id", ColumnDataType::String),
        timestamp("timestamp", ColumnDataType::TimestampMillisecond),
        field("value", ColumnDataType::Float64),
        field("count", ColumnDataType::Int32),
        field("flag", ColumnDataType::Boolean),
    ]
}

#[tokio::main]
async fn main() -> Result<()> {
    println!("Starting low-level streaming API examples...");

    if let Err(e) = low_level_streaming_sensor_ingest().await {
        eprintln!("Sensor streaming failed: {}", e);
    }

    sleep(Duration::from_secs(2)).await;

    if let Err(e) = low_level_concurrent_streaming().await {
        eprintln!("Concurrent streaming failed: {}", e);
    }

    sleep(Duration::from_secs(2)).await;

    if let Err(e) = low_level_streaming_with_error_handling().await {
        eprintln!("Error handling example failed: {}", e);
    }

    println!("All streaming examples completed!");

    Ok(())
}
