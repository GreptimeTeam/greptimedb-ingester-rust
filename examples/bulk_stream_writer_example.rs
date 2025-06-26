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

//! Example demonstrating how to leverage BulkStreamWriter's internal parallelism

mod config_utils;
use config_utils::DbConfig;

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use greptimedb_ingester::client::Client;
use greptimedb_ingester::{
    BulkInserter, BulkWriteOptions, ColumnDataType, Result, Row, Table, Value,
};

fn create_test_rows(batch_id: usize, rows_per_batch: usize) -> Vec<Row> {
    // Get current timestamp
    let current_time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    let mut rows = Vec::new();
    for i in 0..rows_per_batch {
        let global_idx = batch_id * rows_per_batch + i;
        let device_id = format!("device_{}", global_idx % 50); // 50 different devices
        let value = (global_idx as f64) * 0.5 + 20.0; // temperature-like values
        let timestamp = current_time + (global_idx as i64 * 100); // 100ms intervals
        let status = global_idx as i64;

        let row = Row::new()
            .add_value(Value::Timestamp(timestamp))
            .add_value(device_id.into())
            .add_value(value.into())
            .add_value(status.into());

        rows.push(row);
    }

    rows
}

async fn run_sequential_writes() -> Result<Duration> {
    let config = DbConfig::from_env();
    let urls = vec![config.endpoint.clone()];

    let grpc_client = Client::with_urls(&urls);
    let bulk_inserter = BulkInserter::new(grpc_client, &config.database);

    // Create a table template to define schema
    let table_template = Table::builder()
        .name("sensor_data_sequential")
        .build()
        .unwrap()
        .add_timestamp("ts", ColumnDataType::TimestampMillisecond)
        .add_field("device_id", ColumnDataType::String)
        .add_field("avalue", ColumnDataType::Float64)
        .add_field("astatus", ColumnDataType::Int64);

    // Create BulkStreamWriter with low parallelism for sequential comparison
    let mut bulk_writer = bulk_inserter
        .create_bulk_stream_writer(
            &table_template,
            Some(
                BulkWriteOptions::default()
                    .with_compression(true)
                    .with_parallelism(1) // Low parallelism for sequential baseline
                    .with_timeout_ms(30000),
            ),
        )
        .await?;

    let start_time = Instant::now();
    let batch_count = 50; // Same number of batches for fair comparison
    let rows_per_batch = 500; // Same batch size
    let mut total_rows = 0usize;

    println!(
        "  Writing {} batches of {} rows each with parallelism=1",
        batch_count, rows_per_batch
    );

    // Sequential writes - one batch at a time
    for batch_num in 0..batch_count {
        let rows = create_test_rows(batch_num, rows_per_batch);
        let response = bulk_writer.write_rows(rows).await?;
        total_rows += response.affected_rows();

        // Show progress every 10 batches
        if (batch_num + 1) % 10 == 0 {
            println!("  Completed {} batches", batch_num + 1);
        }
    }

    // Finish and close the connection
    bulk_writer.finish().await?;

    let duration = start_time.elapsed();
    println!(
        "  Sequential: {} rows in {:?} ({:.2} rows/sec)",
        total_rows,
        duration,
        total_rows as f64 / duration.as_secs_f64()
    );

    Ok(duration)
}

async fn run_truly_parallel_writes() -> Result<Duration> {
    let config = DbConfig::from_env();
    let urls = vec![config.endpoint.clone()];

    let grpc_client = Client::with_urls(&urls);
    let bulk_inserter = BulkInserter::new(grpc_client, &config.database);

    // Create a table template to define schema
    let table_template = Table::builder()
        .name("sensor_data_parallel")
        .build()
        .unwrap()
        .add_timestamp("ts", ColumnDataType::TimestampMillisecond)
        .add_field("device_id", ColumnDataType::String)
        .add_field("avalue", ColumnDataType::Float64)
        .add_field("astatus", ColumnDataType::Int64);

    // Create BulkStreamWriter with high parallelism
    let mut bulk_writer = bulk_inserter
        .create_bulk_stream_writer(
            &table_template,
            Some(
                BulkWriteOptions::default()
                    .with_compression(true)
                    .with_parallelism(8) // Allow 8 concurrent in-flight requests
                    .with_timeout_ms(30000),
            ),
        )
        .await?;

    let start_time = Instant::now();
    let batch_count = 50;
    let rows_per_batch = 500;

    println!(
        "  Submitting {} batches asynchronously with parallelism=8",
        batch_count
    );

    // Phase 1: Submit all requests without waiting (true parallelism!)
    let mut request_ids = Vec::new();
    let submit_start = Instant::now();

    for batch_num in 0..batch_count {
        let rows = create_test_rows(batch_num, rows_per_batch);
        match bulk_writer.write_rows_async(rows).await {
            Ok(request_id) => {
                request_ids.push(request_id);
                if (batch_num + 1) % 10 == 0 {
                    println!("  Submitted {} batches", batch_num + 1);
                }
            }
            Err(e) => println!("  Submit error: {:?}", e),
        }
    }

    let submit_duration = submit_start.elapsed();
    println!(
        "  All {} requests submitted in {:?}",
        request_ids.len(),
        submit_duration
    );

    // Phase 2: Wait for all responses
    println!("  Waiting for all responses...");
    let responses = bulk_writer.wait_for_all_pending().await?;

    // Alternative: You could also wait for individual responses:
    // for request_id in request_ids {
    //     let response = bulk_writer.wait_for_response(request_id).await?;
    //     println!("Request {} completed with {} rows", request_id, response.affected_rows());
    // }

    let total_rows: usize = responses.iter().map(|r| r.affected_rows()).sum();

    // Finish and close the connection - finish_with_responses ensures we get ALL responses
    let all_final_responses = bulk_writer.finish_with_responses().await?;
    println!(
        "  Final cleanup collected {} additional responses",
        all_final_responses.len().saturating_sub(responses.len())
    );

    let duration = start_time.elapsed();
    println!(
        "  Truly parallel: {} rows in {:?} ({:.2} rows/sec)",
        total_rows,
        duration,
        total_rows as f64 / duration.as_secs_f64()
    );
    println!("  - Submit phase: {:?}", submit_duration);
    println!("  - Processing phase: {:?}", duration - submit_duration);

    Ok(duration)
}

#[tokio::main]
async fn main() -> Result<()> {
    println!("=== BulkStreamWriter: Internal Parallelism Demonstration ===\n");

    // Test sequential writes (one write_rows at a time)
    println!("Testing sequential writes...");
    let sequential_duration = match run_sequential_writes().await {
        Ok(duration) => Some(duration),
        Err(e) => {
            println!("Sequential write error: {:?}", e);
            None
        }
    };

    // Small delay between tests
    tokio::time::sleep(Duration::from_secs(2)).await;
    println!();

    // Test truly parallel writes (using async submission)
    println!("Testing truly parallel writes...");
    let concurrent_duration = match run_truly_parallel_writes().await {
        Ok(duration) => Some(duration),
        Err(e) => {
            println!("High-parallelism write error: {:?}", e);
            None
        }
    };

    println!();

    // Show performance comparison
    println!("=== Performance Summary ===");
    match (sequential_duration, concurrent_duration) {
        (Some(seq_duration), Some(conc_duration)) => {
            println!("Sequential writes (parallelism=1): {:?}", seq_duration);
            println!("Truly parallel writes (parallelism=8): {:?}", conc_duration);

            let speedup = seq_duration.as_secs_f64() / conc_duration.as_secs_f64();
            if speedup > 1.0 {
                println!("Truly parallel writes are {:.2}x faster", speedup);
            } else {
                println!("Sequential writes are {:.2}x faster", 1.0 / speedup);
            }
        }
        _ => {
            println!("Could not compare due to errors");
        }
    }

    println!("\nNew Parallel Writing API:");
    println!("• write_rows(): Synchronous - waits for each request to complete");
    println!("• write_rows_async(): Asynchronous - submits request and returns request_id");
    println!("• wait_for_response(request_id): Waits for a specific request to complete");
    println!("• wait_for_all_pending(): Waits for all submitted requests to complete");
    println!("• finish(): Closes connection and discards any remaining responses");
    println!("• finish_with_responses(): Closes connection and returns ALL responses");
    println!("• This enables true parallelism by overlapping request submission and processing");

    Ok(())
}
