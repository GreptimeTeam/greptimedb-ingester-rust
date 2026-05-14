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

#[path = "util/mod.rs"]
mod util;
use util::DbConfig;

use clap::Parser;
use greptimedb_ingester::api::v1::Basic;
use greptimedb_ingester::client::Client;
use greptimedb_ingester::{
    BulkInserter, BulkStreamWriter, BulkWriteOptions, ColumnDataType, CompressionType, Result, Row,
    Rows, TableSchema, Value,
};
use std::error::Error as StdError;
use std::thread;
use std::time::{Duration, Instant};

use greptime_proto::v1::auth_header::AuthScheme;

type ExampleResult<T> = std::result::Result<T, Box<dyn StdError + Send + Sync>>;

#[derive(Clone, Copy, Debug)]
struct WriteConfig {
    batch_count: usize,
    rows_per_batch: usize,
}

/// Generate test data using the optimized schema-bound buffer API
/// This method provides the best performance by reusing the writer's cached schema
fn create_test_rows_optimized(
    writer: &BulkStreamWriter,
    batch_id: usize,
    rows_per_batch: usize,
) -> Result<Rows> {
    let start_time_micros = (batch_id * rows_per_batch) as i64;

    // Use the writer's optimized buffer allocation - this shares the Arc<Schema>
    let mut rows = writer.alloc_rows_buffer(rows_per_batch)?;

    for i in 0..rows_per_batch {
        let timestamp = start_time_micros + i as i64;
        let id = batch_id;
        let value = id as f64 * 2.0;
        let raw_value = id * 2;

        // Traditional approach: build row by index (fast but error-prone)
        let row = Row::new().add_values(vec![
            Value::TimestampMicrosecond(timestamp),
            Value::Uint32(id as u32),
            Value::Float64(value),
            Value::Uint64(raw_value as u64),
        ]);
        rows.add_row(row)?;
    }

    Ok(rows)
}

/// Demonstrates high-throughput parallel bulk writing
/// Multiple requests can be in-flight simultaneously, maximizing network utilization
async fn run_parallel_writes(
    region: usize,
    write_config: WriteConfig,
    config: DbConfig,
) -> Result<usize> {
    let urls = vec![config.endpoint.clone()];
    let grpc_client = Client::with_urls(&urls);
    let mut bulk_inserter = BulkInserter::new(grpc_client, &config.dbname);

    if let (Some(username), Some(password)) = (config.username.clone(), config.password.clone()) {
        bulk_inserter.set_auth(AuthScheme::Basic(Basic { username, password }));
    }

    // IMPORTANT: Row data must match the exact column order defined in table_template
    let table_template = TableSchema::builder()
        .name("t1")
        .build()
        .unwrap()
        .add_timestamp("ts", ColumnDataType::TimestampMicrosecond) // Index 0
        .add_tag("id", ColumnDataType::Uint32) // Index 1
        .add_field("value", ColumnDataType::Float64) // Index 2
        .add_field("raw_value", ColumnDataType::Uint64); // Index 3

    let mut bulk_writer = bulk_inserter
        .create_bulk_stream_writer(
            &table_template,
            Some(
                BulkWriteOptions::default()
                    .with_compression(CompressionType::Zstd)
                    .with_parallelism(16) // High concurrency for maximum throughput
                    .with_timeout(Duration::from_secs(60)), // 60 second timeout
            ),
        )
        .await?;

    let start_time = Instant::now();
    let batch_count = write_config.batch_count;
    let rows_per_batch = write_config.rows_per_batch;

    println!(
        "  Region {region}: {} batches x {} rows = {} total rows",
        batch_count,
        rows_per_batch,
        batch_count * rows_per_batch
    );
    println!("  Using parallelism=16 for maximum throughput");

    // Phase 1: Async submission - submit all batches without waiting
    let mut request_ids = Vec::with_capacity(batch_count);
    let submit_start = Instant::now();

    for batch_num in 0..batch_count {
        let batch_id = region * batch_count + batch_num;
        // Demonstrate different API approaches
        let rows = create_test_rows_optimized(&bulk_writer, batch_id, rows_per_batch)?;
        match bulk_writer.write_rows_async(rows).await {
            Ok(ids) => {
                request_ids.extend(ids);
                if (batch_num + 1) % 100 == 0 {
                    println!(
                        "  Region {region}: submitted {}/{} batches",
                        batch_num + 1,
                        batch_count
                    );
                }
            }
            Err(e) => eprintln!("  Region {region}: submission error for batch {batch_num}: {e:?}"),
        }
    }

    let submit_duration = submit_start.elapsed();
    let submit_throughput = if submit_duration.is_zero() {
        f64::INFINITY
    } else {
        request_ids.len() as f64 / submit_duration.as_secs_f64()
    };
    println!(
        "  Region {region}: SUCCESS all {} batches submitted in {:.3}s ({:.0} batches/sec)",
        request_ids.len(),
        submit_duration.as_secs_f64(),
        submit_throughput
    );

    // Phase 2: Wait for completion - collect all responses
    println!("  Region {region}: waiting for parallel processing to complete...");
    let wait_start = Instant::now();
    let responses = bulk_writer.wait_for_all_pending().await?;
    let wait_duration = wait_start.elapsed();

    let total_rows: usize = responses.iter().map(|r| r.affected_rows()).sum();
    let success_count = responses.len();

    // Clean shutdown - ensure no responses are lost
    bulk_writer.finish().await?;

    let total_duration = start_time.elapsed();
    let throughput = if total_duration.is_zero() {
        f64::INFINITY
    } else {
        total_rows as f64 / total_duration.as_secs_f64()
    };
    let avg_latency = if success_count == 0 {
        0.0
    } else {
        wait_duration.as_millis() as f64 / success_count as f64
    };

    println!(
        "  Region {region}: SUCCESS parallel write: {} rows in {:.2}s ({:.0} rows/sec)",
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

    Ok(total_rows)
}

#[derive(Parser, Debug)]
struct Args {
    #[arg(long, default_value_t = 4)]
    regions: usize,

    #[arg(long, default_value_t = 2000)]
    batch_count: usize,

    #[arg(long, default_value_t = 10_0000)]
    rows_per_batch: usize,
}

fn main() -> ExampleResult<()> {
    println!("=== High-Throughput Bulk Stream Writer Example ===");
    println!("Use case: ETL, data migration, batch processing, log ingestion");
    println!("When to use: High-volume data, can tolerate higher latency for better throughput");
    println!();

    let args = Args::parse();
    let config = DbConfig::from_env();
    config.display();
    println!();

    let write_config = WriteConfig {
        batch_count: args.batch_count,
        rows_per_batch: args.rows_per_batch,
    };

    let total_start = Instant::now();
    let region_writes: Vec<_> = (0..args.regions)
        .map(|region| {
            let config = config.clone();
            thread::spawn(move || -> ExampleResult<usize> {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()?;
                Ok(runtime.block_on(run_parallel_writes(region, write_config, config))?)
            })
        })
        .collect();

    let mut total_rows = 0usize;
    for thread in region_writes {
        let rows = thread
            .join()
            .map_err(|panic| format!("table write thread panicked: {panic:?}"))??;
        total_rows += rows;
    }

    let total_duration = total_start.elapsed();
    let total_throughput = if total_duration.is_zero() {
        f64::INFINITY
    } else {
        total_rows as f64 / total_duration.as_secs_f64()
    };

    println!();
    println!(
        "SUCCESS Total: {} rows in {:.2}s ({:.0} rows/sec)",
        total_rows,
        total_duration.as_secs_f64(),
        total_throughput
    );

    Ok(())
}
