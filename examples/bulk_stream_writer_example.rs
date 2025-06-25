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

//! Example demonstrating the new BulkStreamWriter with persistent connection logic

mod config_utils;
use config_utils::DbConfig;

use std::time::{SystemTime, UNIX_EPOCH};

use greptimedb_ingester::client::Client;
use greptimedb_ingester::{
    BulkInserter, BulkWriteOptions, ColumnDataType, Result, Row, Table, Value,
};

fn create_test_rows() -> Vec<Row> {
    // Get current timestamp
    let current_time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    // Create test data - 1000 rows
    let mut rows = Vec::new();
    for i in 0..1000 {
        let device_id = format!("device_{}", i % 10); // 10 different devices
        let value = (i as f64) * 0.5 + 20.0; // temperature-like values
        let timestamp = current_time + (i as i64 * 1000); // 1 second intervals
        let status = i as i64; // mostly online, some offline

        let row = Row::new()
            .add_value(Value::Timestamp(timestamp))
            .add_value(device_id.into())
            .add_value(value.into())
            .add_value(status.into());

        rows.push(row);
    }

    rows
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = DbConfig::from_env();
    let urls = vec![config.endpoint.clone()];

    let grpc_client = Client::with_urls(&urls);
    let bulk_inserter = BulkInserter::new(grpc_client, &config.database);

    println!("Creating BulkStreamWriter with compression enabled...");

    // Create a table template to define schema
    let table_template = Table::builder()
        .name("sensor_data")
        .build()
        .unwrap()
        .add_timestamp("ts", ColumnDataType::TimestampMillisecond)
        .add_field("device_id", ColumnDataType::String)
        .add_field("avalue", ColumnDataType::Float64)
        .add_field("astatus", ColumnDataType::Int64);

    // Create BulkStreamWriter bound to the table schema
    let mut bulk_writer = bulk_inserter
        .create_bulk_stream_writer(
            &table_template,
            Some(
                BulkWriteOptions::default()
                    .with_compression(true)
                    .with_timeout_ms(60000),
            ),
        )
        .await?;

    println!("Writing test data using BulkStreamWriter...");

    // Write multiple batches of rows
    let mut total_rows = 0u64;
    for batch_num in 0..5 {
        let rows = create_test_rows();
        let rows_count = rows.len();

        println!("Writing batch {} with {} rows", batch_num + 1, rows_count);
        let rows_written = bulk_writer.write_rows(rows).await?;
        total_rows += rows_written;
    }

    println!("Finishing bulk write operation...");

    // Finish and close the connection
    bulk_writer.finish().await?;

    println!("Bulk write completed successfully!");
    println!("Total rows written: {}", total_rows);

    Ok(())
}
