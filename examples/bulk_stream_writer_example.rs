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

//! High-throughput bulk streaming example using BulkStreamWriter
//! Best for: High-volume data ingestion, batch processing, ETL scenarios
//! Demonstrates: Parallel request submission, async processing, performance optimization

mod config_utils;
use config_utils::DbConfig;

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use greptimedb_ingester::client::Client;
use greptimedb_ingester::{
    BulkInserter, BulkWriteOptions, ColumnDataType, Result, Row, Table, Value,
};

/// Generate realistic time-series data for bulk ingestion testing
/// Simulates IoT sensor readings with device IDs, temperatures, and status codes
fn create_test_rows(batch_id: usize, rows_per_batch: usize) -> Vec<Row> {
    let current_time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    let mut rows = Vec::with_capacity(rows_per_batch);
    for i in 0..rows_per_batch {
        let global_idx = batch_id * rows_per_batch + i;
        let timestamp = current_time + (global_idx as i64 * 50); // 50ms intervals
        let device_id = format!("sensor_{:06}", global_idx % 1000); // 1000 unique sensors
        let temperature = 18.0 + (global_idx as f64 * 0.03) % 25.0; // 18-43°C range
        let status = if global_idx % 100 == 0 { 0 } else { 1 }; // 1% error rate

        // Row values must match table_template column order exactly:
        // Index 0: ts (timestamp), Index 1: sensor_id, Index 2: temperature, Index 3: sensor_status
        rows.push(Row::new().add_values(vec![
            Value::Timestamp(timestamp), // Index 0: ts
            Value::String(device_id),    // Index 1: sensor_id
            Value::Float64(temperature), // Index 2: temperature
            Value::Int64(status),        // Index 3: sensor_status
        ]));
    }

    rows
}

/// Demonstrates traditional sequential bulk writing (baseline performance)
/// Each batch waits for completion before submitting the next batch
async fn run_sequential_writes() -> Result<Duration> {
    let config = DbConfig::from_env();
    let urls = vec![config.endpoint.clone()];
    let grpc_client = Client::with_urls(&urls);
    let bulk_inserter = BulkInserter::new(grpc_client, &config.database);

    // Define time-series table schema for sensor data
    // IMPORTANT: Row data must match the exact column order defined in table_template
    let table_template = Table::builder()
        .name("high_throughput_sequential")
        .build()
        .unwrap()
        .add_timestamp("ts", ColumnDataType::TimestampMillisecond) // Index 0
        .add_field("sensor_id", ColumnDataType::String) // Index 1
        .add_field("temperature", ColumnDataType::Float64) // Index 2
        .add_field("sensor_status", ColumnDataType::Int64); // Index 3

    let mut bulk_writer = bulk_inserter
        .create_bulk_stream_writer(
            &table_template,
            Some(
                BulkWriteOptions::default()
                    .with_compression(true)
                    .with_parallelism(1) // Single in-flight request
                    .with_timeout(Duration::from_secs(30)), // Using Duration for clearer API
            ),
        )
        .await?;

    let start_time = Instant::now();
    let batch_count = 100;
    let rows_per_batch = 1000;
    let mut total_rows = 0usize;

    println!(
        "  Sequential processing: {} batches × {} rows = {} total rows",
        batch_count,
        rows_per_batch,
        batch_count * rows_per_batch
    );

    for batch_num in 0..batch_count {
        let rows = create_test_rows(batch_num, rows_per_batch);
        let response = bulk_writer.write_rows(rows).await?;
        total_rows += response.affected_rows();

        if (batch_num + 1) % 20 == 0 {
            let elapsed = start_time.elapsed();
            let rate = total_rows as f64 / elapsed.as_secs_f64();
            println!(
                "  Progress: {}/{} batches ({:.0} rows/sec)",
                batch_num + 1,
                batch_count,
                rate
            );
        }
    }

    bulk_writer.finish().await?;
    let duration = start_time.elapsed();
    let throughput = total_rows as f64 / duration.as_secs_f64();

    println!(
        "  ✓ Sequential: {} rows in {:.2}s ({:.0} rows/sec)",
        total_rows,
        duration.as_secs_f64(),
        throughput
    );

    Ok(duration)
}

/// Demonstrates high-throughput parallel bulk writing
/// Multiple requests can be in-flight simultaneously, maximizing network utilization
async fn run_parallel_writes() -> Result<Duration> {
    let config = DbConfig::from_env();
    let urls = vec![config.endpoint.clone()];
    let grpc_client = Client::with_urls(&urls);
    let bulk_inserter = BulkInserter::new(grpc_client, &config.database);

    // IMPORTANT: Row data must match the exact column order defined in table_template
    let table_template = Table::builder()
        .name("high_throughput_parallel")
        .build()
        .unwrap()
        .add_timestamp("ts", ColumnDataType::TimestampMillisecond) // Index 0
        .add_field("sensor_id", ColumnDataType::String) // Index 1
        .add_field("temperature", ColumnDataType::Float64) // Index 2
        .add_field("sensor_status", ColumnDataType::Int64); // Index 3

    let mut bulk_writer = bulk_inserter
        .create_bulk_stream_writer(
            &table_template,
            Some(
                BulkWriteOptions::default()
                    .with_compression(true)
                    .with_parallelism(16) // High concurrency for maximum throughput
                    .with_timeout(Duration::from_secs(60)), // 60 second timeout
            ),
        )
        .await?;

    let start_time = Instant::now();
    let batch_count = 100;
    let rows_per_batch = 1000;

    println!(
        "  Parallel processing: {} batches × {} rows = {} total rows",
        batch_count,
        rows_per_batch,
        batch_count * rows_per_batch
    );
    println!("  Using parallelism=16 for maximum throughput");

    // Phase 1: Async submission - submit all batches without waiting
    let mut request_ids = Vec::with_capacity(batch_count);
    let submit_start = Instant::now();

    for batch_num in 0..batch_count {
        let rows = create_test_rows(batch_num, rows_per_batch);
        match bulk_writer.write_rows_async(rows).await {
            Ok(request_id) => {
                request_ids.push(request_id);
                if (batch_num + 1) % 25 == 0 {
                    println!("  Submitted: {}/{} batches", batch_num + 1, batch_count);
                }
            }
            Err(e) => eprintln!("  Submission error for batch {batch_num}: {e:?}"),
        }
    }

    let submit_duration = submit_start.elapsed();
    println!(
        "  ✓ All {} batches submitted in {:.3}s ({:.0} batches/sec)",
        request_ids.len(),
        submit_duration.as_secs_f64(),
        request_ids.len() as f64 / submit_duration.as_secs_f64()
    );

    // Phase 2: Wait for completion - collect all responses
    println!("  Waiting for parallel processing to complete...");
    let wait_start = Instant::now();
    let responses = bulk_writer.wait_for_all_pending().await?;
    let wait_duration = wait_start.elapsed();

    let total_rows: usize = responses.iter().map(|r| r.affected_rows()).sum();
    let success_count = responses.len();

    // Clean shutdown - ensure no responses are lost
    bulk_writer.finish().await?;

    let total_duration = start_time.elapsed();
    let throughput = total_rows as f64 / total_duration.as_secs_f64();
    let avg_latency = wait_duration.as_millis() as f64 / success_count as f64;

    println!(
        "  ✓ Parallel: {} rows in {:.2}s ({:.0} rows/sec)",
        total_rows,
        total_duration.as_secs_f64(),
        throughput
    );
    println!("    - Submission: {:.3}s", submit_duration.as_secs_f64());
    println!(
        "    - Processing: {:.3}s (avg {:.1}ms/batch)",
        wait_duration.as_secs_f64(),
        avg_latency
    );
    println!("    - Success rate: {success_count}/{batch_count} batches");

    Ok(total_duration)
}

#[tokio::main]
async fn main() -> Result<()> {
    println!("=== High-Throughput Bulk Stream Writer Example ===");
    println!("Use case: ETL, data migration, batch processing, log ingestion");
    println!("When to use: High-volume data, can tolerate higher latency for better throughput\n");

    // Baseline: traditional sequential approach
    println!("[1/2] Sequential Baseline (traditional approach)");
    let sequential_duration = match run_sequential_writes().await {
        Ok(duration) => Some(duration),
        Err(e) => {
            eprintln!("Sequential write error: {e:?}");
            None
        }
    };

    tokio::time::sleep(Duration::from_secs(3)).await;
    println!();

    // Optimized: parallel submission approach
    println!("[2/2] Parallel Optimization (async submission)");
    let parallel_duration = match run_parallel_writes().await {
        Ok(duration) => Some(duration),
        Err(e) => {
            eprintln!("Parallel write error: {e:?}");
            None
        }
    };

    println!();
    println!("=== Performance Comparison ===");

    match (sequential_duration, parallel_duration) {
        (Some(seq_dur), Some(par_dur)) => {
            let speedup = seq_dur.as_secs_f64() / par_dur.as_secs_f64();
            println!("Sequential approach: {:.2}s", seq_dur.as_secs_f64());
            println!("Parallel approach:   {:.2}s", par_dur.as_secs_f64());

            if speedup > 1.0 {
                println!("⚡ Speedup: {speedup:.1}x faster with parallel approach");
            } else {
                println!(
                    "⚠️  Sequential was {:.1}x faster (network may be bottleneck)",
                    1.0 / speedup
                );
            }

            let efficiency = (speedup - 1.0) / 15.0 * 100.0; // 16 parallel vs 1 = theoretical 16x
            println!(
                "📊 Parallel efficiency: {:.0}% of theoretical maximum",
                efficiency.max(0.0)
            );
        }
        _ => println!("❌ Could not complete performance comparison"),
    }

    println!();
    println!("=== BulkStreamWriter Parallel API ===");
    println!("🔄 write_rows():           Submit batch and wait for completion (traditional)");
    println!("⚡ write_rows_async():     Submit batch without waiting, returns request_id");
    println!("🔍 wait_for_response(id):  Wait for specific request by ID");
    println!("⏳ wait_for_all_pending(): Wait for all submitted requests");
    println!("🏁 finish():               Close connection, discard remaining responses");
    println!("📦 finish_with_responses(): Close connection, return ALL responses");

    println!();
    println!("=== Best Practices for High Throughput ===");
    println!("• Use parallelism=8-16 for network-bound workloads");
    println!("• Batch 500-2000 rows per request for optimal performance");
    println!("• Use write_rows_async() + wait_for_all_pending() for maximum throughput");
    println!("• Enable compression for better network utilization");
    println!("• Monitor memory usage when submitting many async requests");
    println!("• Consider backpressure control for very high-volume scenarios");

    Ok(())
}
