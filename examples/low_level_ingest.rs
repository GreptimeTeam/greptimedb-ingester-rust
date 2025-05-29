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
use greptimedb_ingester::helpers::schema::*;
use greptimedb_ingester::helpers::values::*;
use greptimedb_ingester::{ClientBuilder, Database, Result};

/// Example of sensor data insertion using low-level API  
pub async fn low_level_sensor_ingest() -> Result<()> {
    let config = DbConfig::from_env();

    let grpc_client = ClientBuilder::default()
        .peers(vec![&config.endpoint])
        .build();

    let database = Database::new_with_dbname(&config.database, grpc_client);

    let sensor_schema = build_sensor_schema();

    let sensor_data = build_sensor_data();

    let insert_request = RowInsertRequests {
        inserts: vec![RowInsertRequest {
            table_name: "sensor_readings".to_owned(),
            rows: Some(Rows {
                schema: sensor_schema,
                rows: sensor_data,
            }),
        }],
    };

    let affected_rows = database.row_insert(insert_request).await?;
    println!("Successfully inserted {} rows", affected_rows);

    Ok(())
}

/// Use schema helpers to build column definitions for sensor table  
fn build_sensor_schema() -> Vec<ColumnSchema> {
    vec![
        // Tag columns - for indexing and grouping
        tag("device_id", ColumnDataType::String),
        tag("location", ColumnDataType::String),
        // Timestamp column - timeline for time series data
        timestamp("timestamp", ColumnDataType::TimestampMillisecond),
        // Field columns - actual measurement values
        field("temperature", ColumnDataType::Float64),
        field("humidity", ColumnDataType::Float64),
        field("pressure", ColumnDataType::Float64),
        field("battery_level", ColumnDataType::Int32),
        field("is_online", ColumnDataType::Boolean),
    ]
}

/// Use value helpers to build sensor data rows  
fn build_sensor_data() -> Vec<Row> {
    let mut rows = Vec::new();

    rows.push(Row {
        values: vec![
            string_value("sensor_001".to_string()),
            string_value("building_a_floor_1".to_string()),
            timestamp_millisecond_value(1748500685000),
            f64_value(23.5),
            f64_value(65.2),
            f64_value(1013.25),
            i32_value(85),
            bool_value(true),
        ],
    });

    rows.push(Row {
        values: vec![
            string_value("sensor_002".to_string()),
            string_value("building_a_floor_2".to_string()),
            timestamp_millisecond_value(1748500685000),
            f64_value(24.1),
            f64_value(62.8),
            f64_value(1012.80),
            i32_value(78),
            bool_value(true),
        ],
    });

    rows.push(Row {
        values: vec![
            string_value("sensor_003".to_string()),
            string_value("building_b_floor_1".to_string()),
            timestamp_millisecond_value(1748500685000),
            f64_value(22.8),
            f64_value(68.5),
            f64_value(1014.10),
            i32_value(15),
            bool_value(false),
        ],
    });

    rows
}

/// Demonstrate batch insertion of different data types  
pub async fn low_level_mixed_data_ingest() -> Result<()> {
    let config = DbConfig::from_env();

    let grpc_client = ClientBuilder::default()
        .peers(vec![&config.endpoint])
        .build();
    let database = Database::new_with_dbname(&config.database, grpc_client);

    // Build table with various data types
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
    ];

    let mixed_data = vec![Row {
        values: vec![
            string_value("test_category".to_string()),
            timestamp_nanosecond_value(1748500685000000000),
            i8_value(127),
            i16_value(32767),
            i64_value(9223372036854775807),
            u32_value(4294967295),
            f32_value(std::f32::consts::PI),
            binary_value(vec![0x48, 0x65, 0x6c, 0x6c, 0x6f]),
            date_value(19358),
        ],
    }];

    let insert_request = RowInsertRequests {
        inserts: vec![RowInsertRequest {
            table_name: "mixed_data_table".to_string(),
            rows: Some(Rows {
                schema: mixed_schema,
                rows: mixed_data,
            }),
        }],
    };

    let affected_rows = database.row_insert(insert_request).await?;
    println!("Mixed data insert: {} rows affected", affected_rows);

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    println!("Starting low-level ingestion examples...");

    low_level_sensor_ingest().await?;

    low_level_mixed_data_ingest().await?;

    println!("All examples completed successfully!");
    Ok(())
}
