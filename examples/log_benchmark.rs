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

use bench::{
    compare_benchmark_results, BenchmarkConfig, BenchmarkRunner, LogTableDataProvider,
    LogTextHelper, TableDataProvider,
};

#[tokio::main]
async fn main() -> greptimedb_ingester::Result<()> {
    println!("=== GreptimeDB Log Benchmark ===");
    println!("Synthetic log data generation and ingestion performance test\n");

    // Load configuration from environment
    let config = BenchmarkConfig::from_env();
    let runner = BenchmarkRunner::new(config.clone());

    // Display system information
    runner.display_system_info();

    // Demonstrate LogTextHelper capabilities
    demonstrate_log_text_helper();

    // Demonstrate provider capabilities
    demonstrate_log_provider(&config);

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

/// Demonstrate LogTextHelper capabilities
fn demonstrate_log_text_helper() {
    println!("[Demo] LogTextHelper - Realistic Log Generation");

    // Show log level distribution
    println!("\nLog Level Distribution (based on 10,000 samples):");
    let distribution = LogTextHelper::generate_distribution_stats(10000);
    for (level, percentage) in &distribution {
        println!("  {level}: {percentage:.1}%");
    }

    // Show sample log entries for each level
    println!("\nSample Log Entries:");
    for level in ["INFO", "DEBUG", "WARN", "ERROR"] {
        println!("\n{level} Level Examples:");
        for i in 1..=3 {
            let message = LogTextHelper::generate_log_message(level);
            println!("  {i}: {message}");
        }
    }

    // Show automatic generation
    println!("\nAutomatic Generation (with level distribution):");
    for i in 1..=5 {
        let (level, message) = LogTextHelper::generate_log_entry();
        println!("  {i}: [{level}] {message}");
    }

    // Show 1500-character generation
    println!("\n1500-Character Log Generation:");
    for i in 1..=3 {
        let (level, message) = LogTextHelper::generate_text_with_len(1500);
        println!(
            "  {i}: [{level}] ({}chars) {}...",
            message.len(),
            &message[..message.len().min(100)]
        );
    }
    println!();
}

/// Demonstrate the LogTableDataProvider capabilities
fn demonstrate_log_provider(config: &BenchmarkConfig) {
    println!("[Demo] LogTableDataProvider Capabilities");

    // Create a small demo provider
    let demo_config = BenchmarkConfig {
        table_row_count: 3,
        ..config.clone()
    };

    let mut demo_provider = LogTableDataProvider::new("demo_logs", &demo_config);

    // Show table schema
    let table = demo_provider.table_schema();
    println!(
        "Table: {} with {} columns",
        table.name(),
        table.columns().len()
    );

    println!("Column names:");
    for (i, column) in table.columns().iter().enumerate() {
        println!("  {}: {} ({:?})", i + 1, column.name, column.data_type);
    }
    println!();

    // Show sample data
    println!("Sample generated rows:");
    for (i, row) in demo_provider.rows().enumerate() {
        println!("Row {}: 22 values", i + 1);

        // Show key fields
        if let Some(log_level) = row.get_string(3) {
            println!("  Log Level: {log_level}");
        }
        if let Some(log_message) = row.get_string(2) {
            println!("  Message: {log_message}");
        }
        if let Some(host_name) = row.get_string(5) {
            println!("  Host: {host_name}");
        }
        if let Some(trace_id) = row.get_string(14) {
            println!("  Trace ID: {trace_id}");
        }
        println!();
    }

    println!(
        "Provider configured for {} total rows",
        demo_provider.row_count()
    );
    println!();
}
