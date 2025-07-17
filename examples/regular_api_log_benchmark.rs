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

//! Regular API Log Benchmark Example
//!
//! This benchmark demonstrates the regular Database::insert() API for log data insertion.
//! Run this independently from the bulk API benchmark to compare their performance.
//!
//! Usage:
//!   cargo run --example regular_api_log_benchmark --release
//!
//! Environment variables:
//!   GREPTIME_ENDPOINT - GreptimeDB endpoint (default: localhost:4001)
//!   GREPTIMEDB_DBNAME - Database name (default: public)  
//!   TABLE_ROW_COUNT   - Number of rows to generate (default: 1_000_000)
//!   BATCH_SIZE        - Batch size for ingestion (default: 64 * 1024)

mod bench;

use bench::benchmark_runner::RegularApiBenchmarkRunner;
use bench::{show_benchmark_results, BenchmarkConfig, LogTableDataProvider};

#[tokio::main]
async fn main() -> greptimedb_ingester::Result<()> {
    println!("=== GreptimeDB Regular API Log Benchmark ===");
    println!("Regular Database::insert() API performance test for log data\n");

    let config = BenchmarkConfig::from_env();

    let runner = RegularApiBenchmarkRunner::new(config.clone());

    // Display system information
    runner.display_system_info();

    // Run the main benchmark
    println!("=== Running Regular API Log Data Benchmark ===");

    // Create log table data provider
    let log_provider = LogTableDataProvider::new("benchmark_logs", &config);

    // Run benchmark
    let result = runner
        .run_regular_api_benchmark(log_provider, "Regular API")
        .await;

    // Display results
    result.display();

    // Show comprehensive results in comparable format
    show_benchmark_results(&[result]);

    Ok(())
}
