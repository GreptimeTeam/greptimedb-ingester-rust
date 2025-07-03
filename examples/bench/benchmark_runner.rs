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

//! Benchmark Runner
//!
//! Provides a framework for running benchmarks with different TableDataProvider implementations.
//! This module handles benchmark execution, configuration, and results.

use super::table_data_provider::TableDataProvider;
use greptimedb_ingester::{BulkInserter, BulkWriteOptions, CompressionType, Result};
use std::time::{Duration, Instant};

/// Configuration for benchmark runs
#[derive(Debug, Clone)]
pub struct BenchmarkConfig {
    pub endpoint: String,
    pub dbname: String,
    pub table_row_count: usize,
    pub batch_size: usize,
    pub parallelism: usize,
    pub compression: String,
}

impl Default for BenchmarkConfig {
    fn default() -> Self {
        Self {
            endpoint: "localhost:4001".to_string(),
            dbname: "public".to_string(),
            table_row_count: 1_000_000,
            batch_size: 64 * 1024,
            parallelism: 4,
            compression: "lz4".to_string(),
        }
    }
}

impl BenchmarkConfig {
    /// Create configuration from environment variables
    pub fn from_env() -> Self {
        Self {
            endpoint: std::env::var("GREPTIME_ENDPOINT")
                .unwrap_or_else(|_| "localhost:4001".to_string()),
            dbname: std::env::var("GREPTIMEDB_DBNAME").unwrap_or_else(|_| "public".to_string()),
            table_row_count: std::env::var("TABLE_ROW_COUNT")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(1_000_000),
            batch_size: std::env::var("BATCH_SIZE")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(64 * 1024),
            parallelism: std::env::var("PARALLELISM")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(8),
            compression: std::env::var("COMPRESSION").unwrap_or_else(|_| "lz4".to_string()),
        }
    }
}

/// Results from a benchmark run
#[derive(Debug, Clone)]
pub struct BenchmarkResult {
    pub provider_name: String,
    pub table_name: String,
    pub total_rows: usize,
    pub duration_ms: u64,
    pub rows_per_second: f64,
    pub success: bool,
    pub error_message: Option<String>,
}

impl BenchmarkResult {
    pub fn new(provider_name: &str, table_name: &str, total_rows: usize) -> Self {
        Self {
            provider_name: provider_name.to_string(),
            table_name: table_name.to_string(),
            total_rows,
            duration_ms: 0,
            rows_per_second: 0.0,
            success: false,
            error_message: None,
        }
    }

    pub fn success(mut self, duration_ms: u64) -> Self {
        self.duration_ms = duration_ms;
        self.rows_per_second = if duration_ms > 0 {
            (self.total_rows as f64) / (duration_ms as f64 / 1000.0)
        } else {
            0.0
        };
        self.success = true;
        self
    }

    pub fn error(mut self, error: String) -> Self {
        self.error_message = Some(error);
        self.success = false;
        self
    }

    /// Display formatted results
    pub fn display(&self) {
        println!("=== {} Benchmark Results ===", self.provider_name);
        println!("Table: {}", self.table_name);

        if self.success {
            println!("SUCCESS");
            println!("Total rows: {}", self.total_rows);
            println!("Duration: {}ms", self.duration_ms);
            println!("Throughput: {:.0} rows/sec", self.rows_per_second);
        } else {
            println!("FAILED");
            if let Some(ref error) = self.error_message {
                println!("Error: {error}");
            }
        }
        println!();
    }
}

/// Benchmark runner that can execute tests with any TableDataProvider
pub struct BenchmarkRunner {
    config: BenchmarkConfig,
}

impl BenchmarkRunner {
    /// Create a new benchmark runner with configuration
    pub fn new(config: BenchmarkConfig) -> Self {
        Self { config }
    }

    /// Run a benchmark with the given provider using the zero-cost Rows API
    pub async fn run_benchmark(
        &self,
        mut provider: super::LogTableDataProvider,
        provider_name: &str,
    ) -> BenchmarkResult {
        let table_schema = provider.table_schema();
        let table_name = table_schema.name().to_string();
        let total_rows = provider.row_count();

        let result = BenchmarkResult::new(provider_name, &table_name, total_rows);

        // Initialize provider
        if let Err(e) = provider.init() {
            return result.error(format!("Failed to initialize provider: {e:?}"));
        }

        // Create client and database
        let client = match self.create_client().await {
            Ok(client) => client,
            Err(e) => return result.error(format!("Failed to create client: {e:?}")),
        };

        let bulk_inserter = BulkInserter::new(client, &self.config.dbname);

        // Create bulk stream writer
        println!("Setting up bulk stream writer...");
        let mut bulk_writer = match bulk_inserter
            .create_bulk_stream_writer(&table_schema, Some(self.create_bulk_options()))
            .await
        {
            Ok(writer) => writer,
            Err(e) => return result.error(format!("Failed to create bulk writer: {e:?}")),
        };

        println!("Starting benchmark: {provider_name}");
        println!(
            "Table: {} ({} columns)",
            table_name,
            table_schema.columns().len()
        );
        println!("Target rows: {total_rows}");
        println!("Batch size: {}", self.config.batch_size);
        println!("Parallelism: {}", self.config.parallelism);
        println!();

        let start_time = Instant::now();
        let mut rows_written = 0;
        let mut batch_count = 0;

        let mut row_iter = provider.rows();

        loop {
            let mut break_out = false;
            let batch_size = self.config.batch_size;
            let mut rows_buf = bulk_writer.alloc_rows_buffer(batch_size, 1024).unwrap();
            for _ in 0..batch_size {
                if let Some(row) = row_iter.next() {
                    rows_buf.add_row(row).unwrap();
                } else {
                    break_out = true;
                    break;
                }
            }

            rows_written += rows_buf.len();
            batch_count += 1;

            let _request_id = bulk_writer.write_rows_async(rows_buf).await.unwrap();

            let elapsed = start_time.elapsed();
            let rate = rows_written as f64 / elapsed.as_secs_f64();
            println!("→ Batch {batch_count}: {rows_written} rows processed ({rate:.0} rows/sec)");

            if batch_count % 10 == 0 {
                let responses = bulk_writer.flush_completed_responses();
                if !responses.is_empty() {
                    let total_affected_rows: usize =
                        responses.iter().map(|r| r.affected_rows()).sum();
                    println!(
                        "Flushed {} responses (total {} affected rows)",
                        responses.len(),
                        total_affected_rows
                    );
                }
            }

            if break_out {
                break;
            }
        }

        // Finish writing
        println!("Finishing bulk writer and waiting for all responses...");
        if let Err(e) = bulk_writer.finish().await {
            return result.error(format!("Failed to finish bulk writer: {e:?}"));
        }
        println!("All bulk writes completed successfully");

        // Drop the iterator to release the mutable borrow
        drop(row_iter);

        // Cleanup provider
        println!("Cleaning up data provider...");
        if let Err(e) = provider.close() {
            return result.error(format!("Failed to close provider: {e:?}"));
        }

        let duration = start_time.elapsed();
        println!("Benchmark completed successfully!");
        println!("Final Results:");
        println!("  • Total rows: {rows_written}");
        println!("  • Total batches: {batch_count}");
        println!("  • Duration: {:.2}s", duration.as_secs_f64());
        println!(
            "  • Throughput: {:.0} rows/sec",
            rows_written as f64 / duration.as_secs_f64()
        );
        println!();

        result.success(duration.as_millis() as u64)
    }

    /// Create GreptimeDB client
    async fn create_client(&self) -> Result<greptimedb_ingester::client::Client> {
        let client =
            greptimedb_ingester::client::Client::with_urls(&[self.config.endpoint.clone()]);
        Ok(client)
    }

    /// Create bulk write options
    fn create_bulk_options(&self) -> BulkWriteOptions {
        let compression = match self.config.compression.to_lowercase().as_str() {
            "none" | "false" | "0" => CompressionType::None,
            "lz4" => CompressionType::Lz4,
            "zstd" => CompressionType::Zstd,
            _ => {
                println!(
                    "Warning: unknown compression type '{}', defaulting to lz4",
                    self.config.compression
                );
                CompressionType::Lz4
            }
        };

        BulkWriteOptions::default()
            .with_compression(compression)
            .with_parallelism(self.config.parallelism)
            .with_timeout(Duration::from_secs(60))
    }

    /// Display system information
    pub fn display_system_info(&self) {
        println!("=== Benchmark Configuration ===");
        println!("Endpoint: {}", self.config.endpoint);
        println!("Database: {}", self.config.dbname);
        println!("Max rows per provider: {}", self.config.table_row_count);
        println!("Batch size: {}", self.config.batch_size);
        println!("Parallelism: {}", self.config.parallelism);
        println!("Compression: {}", self.config.compression);

        if let Ok(hostname) = std::env::var("HOSTNAME") {
            println!("Hostname: {hostname}");
        }

        let cpu_count = num_cpus::get();
        println!("CPU cores: {cpu_count}");

        println!(
            "Build profile: {}",
            if cfg!(debug_assertions) {
                "debug"
            } else {
                "release"
            }
        );
        println!();
    }
}

/// Compare multiple benchmark results
pub fn compare_benchmark_results(results: &[BenchmarkResult]) {
    if results.is_empty() {
        return;
    }

    println!("=== Benchmark Comparison ===");

    let successful_results: Vec<_> = results.iter().filter(|r| r.success).collect();

    if successful_results.is_empty() {
        println!("No successful benchmarks to compare");
        return;
    }

    // Find the fastest provider
    let fastest = successful_results
        .iter()
        .max_by(|a, b| a.rows_per_second.partial_cmp(&b.rows_per_second).unwrap())
        .unwrap();

    println!(
        "Fastest provider: {} ({:.0} rows/sec)",
        fastest.provider_name, fastest.rows_per_second
    );
    println!();

    // Summary table
    println!(
        "{:<25} {:>12} {:>12} {:>15} {:>10}",
        "Provider", "Rows", "Duration(ms)", "Throughput", "Status"
    );
    println!("{:-<74}", "");

    for result in results {
        if result.success {
            println!(
                "{:<25} {:>12} {:>12} {:>10.0} r/s {:>10}",
                result.provider_name,
                result.total_rows,
                result.duration_ms,
                result.rows_per_second,
                "SUCCESS"
            );
        } else {
            println!(
                "{:<25} {:>12} {:>12} {:>15} {:>10}",
                result.provider_name, result.total_rows, "N/A", "N/A", "FAILED"
            );
        }
    }
    println!();

    // Relative performance
    if successful_results.len() > 1 {
        println!("Relative Performance:");
        for result in &successful_results {
            let relative_perf = result.rows_per_second / fastest.rows_per_second;
            if result.provider_name == fastest.provider_name {
                println!("[FASTEST] {}: Baseline (fastest)", result.provider_name);
            } else {
                println!(
                    "{}: {:.1}% of fastest",
                    result.provider_name,
                    relative_perf * 100.0
                );
            }
        }
    }
}
