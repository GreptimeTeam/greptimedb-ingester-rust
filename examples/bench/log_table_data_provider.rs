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

//! LogTableDataProvider implementation
//!
//! Generates synthetic log data following the Java LogTableDataProvider pattern,
//! with 22 columns including timestamps, log entries, and hierarchical identifiers.

use super::benchmark_runner::BenchmarkConfig;
use super::table_data_provider::TableDataProvider;
use greptimedb_ingester::bulk::AdaptiveAllocStats;
use greptimedb_ingester::{ColumnDataType, Row, Rows, TableSchema, Value};
use rand::RngCore;
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

/// LogTableDataProvider that generates synthetic log data
/// Following the Java implementation with 22 columns
pub struct LogTableDataProvider {
    table_name: String,
    row_count: usize,
    current_row: usize,
    // Performance optimization: reuse RNG and reduce allocations
    rng: Box<dyn RngCore>,
    base_time: i64,
}

impl LogTableDataProvider {
    /// Create a new LogTableDataProvider
    pub fn new(table_name: &str, config: &BenchmarkConfig) -> Self {
        let base_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        Self {
            table_name: table_name.to_string(),
            row_count: config.table_row_count,
            current_row: 0,
            rng: Box::new(rand::rng()),
            base_time,
        }
    }

    /// Generate data directly into Rows using the new zero-cost API
    /// This provides the best performance with user-friendly API
    pub fn generate_rows_batches(
        &mut self,
        alloc_stats: Arc<AdaptiveAllocStats>,
        batch_size: usize,
    ) -> Box<dyn Iterator<Item = greptimedb_ingester::Result<Rows>> + '_> {
        Box::new(RowsIterator {
            provider: self,
            alloc_stats,
            batch_size,
        })
    }

    /// Generate random ID with name pair (optimized version)
    /// Following the Java nextIdWithName pattern
    fn next_id_with_name_optimized(rng: &mut Box<dyn RngCore>, prefix: &str) -> (String, String) {
        let id = rng.next_u64();
        let name_suffix: String = (0..6)
            .map(|_| {
                let byte = (rng.next_u32() % 26) as u8 + b'a';
                byte as char
            })
            .collect();

        (format!("{prefix}-{id}"), format!("{prefix}-{name_suffix}"))
    }

    /// Generate optimized log text with 2k target length
    fn generate_optimized_log_text(&mut self) -> (String, String) {
        // Simple log level selection with same distribution
        let level_choice = self.rng.next_u32() % 100;
        let level = if level_choice < 84 {
            "INFO"
        } else if level_choice < 94 {
            "DEBUG"
        } else if level_choice < 99 {
            "WARN"
        } else {
            "ERROR"
        };

        // Generate base message with realistic content
        let mut message = match level {
            "INFO" => format!(
                "Request processed successfully for user_{} in {}ms. Database query executed successfully with {} rows affected. Cache hit ratio: {:.2}%. Memory usage: heap={}MB, non-heap={}MB. Thread pool status: active={}, queue_size={}. Network I/O: sent={}KB, received={}KB",
                (self.rng.next_u32() % 89999) + 10000,
                (self.rng.next_u32() % 4999) + 1,
                (self.rng.next_u32() % 9999) + 1,
                (self.rng.next_u32() % 10000) as f64 / 100.0,
                (self.rng.next_u32() % 2048) + 512,
                (self.rng.next_u32() % 512) + 128,
                (self.rng.next_u32() % 50) + 1,
                (self.rng.next_u32() % 1000) + 1,
                (self.rng.next_u32() % 9999) + 1,
                (self.rng.next_u32() % 9999) + 1
            ),
            "DEBUG" => format!(
                "Detailed system metrics collected. CPU usage: {:.2}%, Memory usage: {:.2}%, Disk I/O: read={}KB/s, write={}KB/s. Database connection pool: active={}, idle={}, max={}. Cache statistics: hits={}, misses={}, evictions={}. JVM garbage collection: young_gen={}ms, old_gen={}ms",
                (self.rng.next_u32() % 10000) as f64 / 100.0,
                (self.rng.next_u32() % 10000) as f64 / 100.0,
                (self.rng.next_u32() % 99999) + 1000,
                (self.rng.next_u32() % 99999) + 1000,
                (self.rng.next_u32() % 50) + 1,
                (self.rng.next_u32() % 100) + 10,
                (self.rng.next_u32() % 200) + 50,
                (self.rng.next_u32() % 999999) + 100000,
                (self.rng.next_u32() % 99999) + 1000,
                (self.rng.next_u32() % 9999) + 100,
                (self.rng.next_u32() % 500) + 50,
                (self.rng.next_u32() % 2000) + 100
            ),
            "WARN" => format!(
                "Performance degradation detected. High memory usage: {}% of heap space used. Slow queries detected: avg_time={}ms, max_time={}ms. Connection pool near exhaustion: {}/{} connections used. Rate limiting triggered for user_{}: {}/{} requests per minute. Disk usage warning: {}% full on partition /data. Cache miss ratio elevated: {:.2}% over last 15 minutes",
                (self.rng.next_u32() % 40) + 60,
                (self.rng.next_u32() % 4000) + 1000,
                (self.rng.next_u32() % 8000) + 2000,
                (self.rng.next_u32() % 95) + 80,
                (self.rng.next_u32() % 50) + 100,
                (self.rng.next_u32() % 89999) + 10000,
                (self.rng.next_u32() % 950) + 950,
                1000,
                (self.rng.next_u32() % 30) + 70,
                (self.rng.next_u32() % 5000) as f64 / 100.0
            ),
            "ERROR" => {
                let service_id = (self.rng.next_u32() % 999) + 1;
                format!(
                    "Critical system failure detected. Database connection failed after {} retry attempts. Transaction rollback required for {} pending operations. Service service_{} is completely unavailable. Last successful health check: {} minutes ago. Error details: connection_timeout={}ms, max_retries_exceeded=true, circuit_breaker_state=OPEN",
                    (self.rng.next_u32() % 10) + 3,
                    (self.rng.next_u32() % 999) + 100,
                    service_id,
                    (self.rng.next_u32() % 60) + 5,
                    (self.rng.next_u32() % 9000) + 1000
                )
            },
            _ => "Unknown log level".to_string(),
        };

        // Add context metadata to reach ~2k length
        let context_parts = [
            format!("correlation_id=req_{}", self.rng.next_u64()),
            format!("session_id=session_{}", self.rng.next_u64()),
            format!("trace_id=trace_{}", self.rng.next_u64()),
            format!("span_id=span_{}", self.rng.next_u64()),
            format!(
                "client_ip=192.168.{}.{}",
                (self.rng.next_u32() % 254) + 1,
                (self.rng.next_u32() % 254) + 1
            ),
            format!(
                "user_agent=HttpClient/{}.{}",
                (self.rng.next_u32() % 3) + 1,
                (self.rng.next_u32() % 10)
            ),
            format!("datacenter=dc{}", (self.rng.next_u32() % 5) + 1),
            format!(
                "region=us-{}",
                if self.rng.next_u32() % 2 == 0 {
                    "west"
                } else {
                    "east"
                }
            ),
            format!("instance_id=i-{:x}", self.rng.next_u64()),
            format!("request_id=req_{}", self.rng.next_u64()),
            format!("operation=operation_{}", (self.rng.next_u32() % 100) + 1),
            format!("component=component_{}", (self.rng.next_u32() % 50) + 1),
            format!("thread=thread-{}", (self.rng.next_u32() % 200) + 1),
            format!(
                "hostname=host-{}.example.com",
                (self.rng.next_u32() % 999) + 1
            ),
            format!(
                "environment={}",
                if self.rng.next_u32() % 3 == 0 {
                    "production"
                } else if self.rng.next_u32() % 2 == 0 {
                    "staging"
                } else {
                    "development"
                }
            ),
        ];

        message.push_str(" [");
        for (i, part) in context_parts.iter().enumerate() {
            if i > 0 {
                message.push_str(", ");
            }
            message.push_str(part);
        }
        message.push(']');

        // Add stack trace for ERROR logs to increase length
        if level == "ERROR" {
            let stack_frames = [
                "at com.example.service.DatabaseService.connect(DatabaseService.java:127)",
                "at com.example.service.DatabaseService.executeQuery(DatabaseService.java:89)",
                "at com.example.controller.ApiController.processRequest(ApiController.java:45)",
                "at com.example.filter.SecurityFilter.doFilter(SecurityFilter.java:83)",
                "at com.example.middleware.RequestMiddleware.process(RequestMiddleware.java:156)",
                "at org.springframework.web.filter.OncePerRequestFilter.doFilter(OncePerRequestFilter.java:119)",
                "at com.example.repository.DatabaseRepository.findById(DatabaseRepository.java:234)",
                "at com.example.service.DataService.processRequest(DataService.java:156)",
                "at com.example.util.CacheManager.get(CacheManager.java:78)",
                "at com.example.handler.RequestHandler.handle(RequestHandler.java:92)",
                "at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1136)",
                "at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:635)",
                "at java.base/java.lang.Thread.run(Thread.java:842)",
            ];

            message.push_str("\nStack trace:");
            for frame in &stack_frames[..(self.rng.next_u32() % 8 + 3) as usize] {
                message.push_str("\n    ");
                message.push_str(frame);
            }
        }

        // Fill remaining space to reach approximately 2k if needed
        while message.len() < 1300 {
            let uuid1 = self.rng.next_u64();
            let uuid2 = self.rng.next_u64();
            message.push_str(&format!(
                " Additional diagnostic info: timestamp={}, uuid=uuid-{:x}-{:x}, checksum={:x}, sequence={}, retry_count={}, elapsed_time={}ms",
                self.base_time + self.current_row as i64,
                uuid1,
                uuid2,
                self.rng.next_u64(),
                self.current_row,
                self.rng.next_u32() % 5,
                (self.rng.next_u32() % 9999) + 1
            ));
        }

        // Truncate if too long to keep around 2k
        if message.len() > 1600 {
            message.truncate(2000);
            message.push_str("...");
        }

        (level.to_string(), message)
    }

    /// Generate a single log row
    fn generate_row(&mut self) -> Option<Row> {
        if self.current_row >= self.row_count {
            return None;
        }

        // Use reused RNG and base time for better performance
        let timestamp =
            self.base_time + self.current_row as i64 + (self.rng.next_u32() as i64 % 2000) - 1000;

        // Generate log UID (unique identifier) - use current_row for uniqueness instead of UUID
        let log_uid = format!("log_{}_{}", timestamp, self.current_row);

        // Generate log message and level using optimized method
        let (log_level, log_message) = self.generate_optimized_log_text();

        // Generate hierarchical identifiers (following Java pattern)
        let (host_id, host_name) = Self::next_id_with_name_optimized(&mut self.rng, "host");
        let (service_id, service_name) =
            Self::next_id_with_name_optimized(&mut self.rng, "service");
        let (container_id, container_name) =
            Self::next_id_with_name_optimized(&mut self.rng, "container");
        let (pod_id, pod_name) = Self::next_id_with_name_optimized(&mut self.rng, "pod");
        let (cluster_id, cluster_name) =
            Self::next_id_with_name_optimized(&mut self.rng, "cluster");

        // Additional fields to match Java 22-column structure
        let trace_id = format!("trace_{}", self.rng.next_u64());
        let span_id = format!("span_{}", self.rng.next_u64());
        let user_id = format!("user_{}", (self.rng.next_u32() % 9999) + 1);
        let session_id = format!("session_{}", self.rng.next_u64());
        let request_id = format!("req_{}", self.rng.next_u64());
        let response_time_ms = ((self.rng.next_u32() % 999) + 1) as i64;

        self.current_row += 1;

        Some(Row::new().add_values(vec![
            Value::Timestamp(timestamp),              // ts
            Value::String(log_uid),                   // log_uid
            Value::String(log_message),               // log_message
            Value::String(log_level.to_string()),     // log_level
            Value::String(host_id),                   // host_id
            Value::String(host_name),                 // host_name
            Value::String(service_id),                // service_id
            Value::String(service_name),              // service_name
            Value::String(container_id),              // container_id
            Value::String(container_name),            // container_name
            Value::String(pod_id),                    // pod_id
            Value::String(pod_name),                  // pod_name
            Value::String(cluster_id),                // cluster_id
            Value::String(cluster_name),              // cluster_name
            Value::String(trace_id),                  // trace_id
            Value::String(span_id),                   // span_id
            Value::String(user_id),                   // user_id
            Value::String(session_id),                // session_id
            Value::String(request_id),                // request_id
            Value::Int64(response_time_ms),           // response_time_ms
            Value::String("application".to_string()), // log_source
            Value::String("v1.0.0".to_string()),      // version
        ]))
    }
}

impl TableDataProvider for LogTableDataProvider {
    fn table_schema(&self) -> TableSchema {
        TableSchema::builder()
            .name(&self.table_name)
            .build()
            .unwrap()
            .add_timestamp("ts", ColumnDataType::TimestampMillisecond)
            .add_field("log_uid", ColumnDataType::String)
            .add_field("log_message", ColumnDataType::String)
            .add_field("log_level", ColumnDataType::String)
            .add_field("host_id", ColumnDataType::String)
            .add_field("host_name", ColumnDataType::String)
            .add_field("service_id", ColumnDataType::String)
            .add_field("service_name", ColumnDataType::String)
            .add_field("container_id", ColumnDataType::String)
            .add_field("container_name", ColumnDataType::String)
            .add_field("pod_id", ColumnDataType::String)
            .add_field("pod_name", ColumnDataType::String)
            .add_field("cluster_id", ColumnDataType::String)
            .add_field("cluster_name", ColumnDataType::String)
            .add_field("trace_id", ColumnDataType::String)
            .add_field("span_id", ColumnDataType::String)
            .add_field("user_id", ColumnDataType::String)
            .add_field("session_id", ColumnDataType::String)
            .add_field("request_id", ColumnDataType::String)
            .add_field("response_time_ms", ColumnDataType::Int64)
            .add_field("log_source", ColumnDataType::String)
            .add_field("version", ColumnDataType::String)
    }

    fn rows(&mut self) -> Box<dyn Iterator<Item = Row> + '_> {
        Box::new(LogRowIterator { provider: self })
    }

    fn row_count(&self) -> usize {
        self.row_count
    }
}

/// Iterator for LogTableDataProvider rows
struct LogRowIterator<'a> {
    provider: &'a mut LogTableDataProvider,
}

impl<'a> Iterator for LogRowIterator<'a> {
    type Item = Row;

    fn next(&mut self) -> Option<Self::Item> {
        self.provider.generate_row()
    }
}

/// Iterator for generating Rows using the new zero-cost API
struct RowsIterator<'a> {
    provider: &'a mut LogTableDataProvider,
    alloc_stats: Arc<AdaptiveAllocStats>,
    batch_size: usize,
}

impl<'a> Iterator for RowsIterator<'a> {
    type Item = greptimedb_ingester::Result<Rows>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.provider.current_row >= self.provider.row_count {
            return None;
        }

        // Calculate actual batch size (might be smaller for the last batch)
        let remaining_rows = self.provider.row_count - self.provider.current_row;
        let actual_batch_size = remaining_rows.min(self.batch_size);

        // Create Rows for this batch
        let table_schema = self.provider.table_schema();
        let mut rows = match Rows::new(
            table_schema.columns(),
            actual_batch_size,
            1024,
            self.alloc_stats.clone(),
        ) {
            Ok(rows) => rows,
            Err(e) => return Some(Err(e)),
        };

        let now = Instant::now();
        for _ in 0..actual_batch_size {
            if let Some(row) = self.provider.generate_row() {
                if let Err(e) = rows.add_row(row) {
                    return Some(Err(e));
                }
            } else {
                break;
            }
        }
        let elapsed = now.elapsed();
        println!("generate_row took {}ms", elapsed.as_millis());
        Some(Ok(rows))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_schema() {
        let config = BenchmarkConfig::default();
        let provider = LogTableDataProvider::new("test_logs", &config);
        let table = provider.table_schema();

        assert_eq!(table.name(), "test_logs");
        assert_eq!(table.columns().len(), 22); // Should match Java version
    }

    #[test]
    fn test_next_id_with_name() {
        let mut rng = rand::rng();
        let (id, name) = LogTableDataProvider::next_id_with_name(&mut rng, "test");

        assert!(id.starts_with("test-"));
        assert!(name.starts_with("test-"));
        assert_ne!(id, name); // Should be different
    }
}
