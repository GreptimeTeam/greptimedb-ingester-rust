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

//! Bulk API Log Benchmark Example
//!
//! This benchmark demonstrates the LogTableDataProvider for generating synthetic log data
//! and measuring GreptimeDB bulk API ingestion performance.
//!
//! Note: The bulk API does not automatically create tables. You must manually create the table
//! before running this benchmark using the following schema:
//!
//! ```sql
//! CREATE TABLE IF NOT EXISTS `benchmark_logs` (
//!   `ts` TIMESTAMP(3) NOT NULL,
//!   `log_uid` STRING NULL,
//!   `log_message` STRING NULL,
//!   `log_level` STRING NULL,
//!   `host_id` STRING NULL,
//!   `host_name` STRING NULL,
//!   `service_id` STRING NULL,
//!   `service_name` STRING NULL,
//!   `container_id` STRING NULL,
//!   `container_name` STRING NULL,
//!   `pod_id` STRING NULL,
//!   `pod_name` STRING NULL,
//!   `cluster_id` STRING NULL,
//!   `cluster_name` STRING NULL,
//!   `trace_id` STRING NULL,
//!   `span_id` STRING NULL,
//!   `user_id` STRING NULL,
//!   `session_id` STRING NULL,
//!   `request_id` STRING NULL,
//!   `response_time_ms` BIGINT NULL,
//!   `log_source` STRING NULL,
//!   `version` STRING NULL,
//!   TIME INDEX (`ts`)
//! )
//! ENGINE=mito
//! WITH(
//!   append_mode = 'true',
//!   skip_wal = 'true'
//! );
//! ```
//!
//! Usage:
//!   cargo run --example bulk_api_log_benchmark --release
//!
//! Environment variables:
//!   GREPTIME_ENDPOINT - GreptimeDB endpoint (default: localhost:4001)
//!   GREPTIMEDB_DBNAME - Database name (default: public)  
//!   TABLE_ROW_COUNT   - Number of rows to generate (default: 1_000_000)
//!   BATCH_SIZE        - Batch size for ingestion (default: 64 * 1024)
//!   PARALLELISM       - Parallel requests (default: 8)
//!   COMPRESSION       - Enable compression (default: lz4)

mod bench;

use bench::benchmark_runner::BulkApiBenchmarkRunner;
use bench::{show_benchmark_results, BenchmarkConfig, LogTableDataProvider};

#[tokio::main]
async fn main() -> greptimedb_ingester::Result<()> {
    println!("=== GreptimeDB Bulk API Log Benchmark ===");
    println!("Synthetic log data generation and bulk API ingestion performance test\n");

    // Load configuration from environment
    let config = BenchmarkConfig::from_env();
    let runner = BulkApiBenchmarkRunner::new(config.clone());

    // Display system information
    runner.display_system_info();

    // Run the main benchmark
    println!("=== Running Bulk API Log Data Benchmark ===");

    // Create log table data provider
    let log_provider = LogTableDataProvider::new("benchmark_logs", &config);

    // Run benchmark
    let result = runner
        .run_benchmark(log_provider, "LogTableDataProvider")
        .await;

    // Display results
    result.display();

    // Show comprehensive results
    show_benchmark_results(&[result]);

    Ok(())
}
