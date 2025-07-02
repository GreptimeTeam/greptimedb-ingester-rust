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

//! Log Benchmark Example
//!
//! This benchmark demonstrates the LogTableDataProvider for generating synthetic log data
//! and measuring GreptimeDB ingestion performance.
//!
//! Usage:
//!   cargo run --example log_benchmark
//!
//! Environment variables:
//!   GREPTIME_ENDPOINT - GreptimeDB endpoint (default: localhost:4001)
//!   GREPTIME_DATABASE - Database name (default: public)  
//!   table_row_count   - Number of rows to generate (default: 10000000)
//!   batch_size        - Batch size for ingestion (default: 1000)
//!   parallelism       - Parallel requests (default: 4)
//!   compression       - Enable compression (default: true)

mod bench;

use bench::{compare_benchmark_results, BenchmarkConfig, BenchmarkRunner, LogTableDataProvider};

#[tokio::main]
async fn main() -> greptimedb_ingester::Result<()> {
    println!("=== GreptimeDB Log Benchmark ===");
    println!("Synthetic log data generation and ingestion performance test\n");

    // Load configuration from environment
    let config = BenchmarkConfig::from_env();
    let runner = BenchmarkRunner::new(config.clone());

    // Display system information
    runner.display_system_info();

    // Run the main benchmark
    println!("=== Running Log Data Benchmark ===");

    // Create log table data provider
    let log_provider = LogTableDataProvider::new("benchmark_logs", &config);

    // Run benchmark
    let result = runner
        .run_benchmark(log_provider, "LogTableDataProvider")
        .await;

    // Display results
    result.display();

    // Show comprehensive results
    compare_benchmark_results(&[result]);

    Ok(())
}
