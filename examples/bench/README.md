# GreptimeDB Benchmark Utilities

This module provides lightweight benchmark utilities and data providers for GreptimeDB performance testing, specifically designed to compare bulk API and regular API ingestion performance with realistic synthetic log data.

## LogTableDataProvider Data Model Analysis

### Overview

The LogTableDataProvider is a simple yet effective log data generator designed for GreptimeDB performance testing. It generates synthetic log data with 22 fields that simulate common distributed system logging scenarios.

### Table Schema

The generated table contains the following 22 fields:

```sql
CREATE TABLE IF NOT EXISTS `benchmark_logs` (
  `ts` TIMESTAMP(3) NOT NULL,
  `log_uid` STRING NULL,
  `log_message` STRING NULL,
  `log_level` STRING NULL,
  `host_id` STRING NULL,
  `host_name` STRING NULL,
  `service_id` STRING NULL,
  `service_name` STRING NULL,
  `container_id` STRING NULL,
  `container_name` STRING NULL,
  `pod_id` STRING NULL,
  `pod_name` STRING NULL,
  `cluster_id` STRING NULL,
  `cluster_name` STRING NULL,
  `trace_id` STRING NULL,
  `span_id` STRING NULL,
  `user_id` STRING NULL,
  `session_id` STRING NULL,
  `request_id` STRING NULL,
  `response_time_ms` BIGINT NULL,
  `log_source` STRING NULL,
  `version` STRING NULL,
  TIME INDEX (`ts`)
)
ENGINE=mito
WITH(
  append_mode = 'true',
  skip_wal = 'true'
);
```

## Field Generation Logic and Cardinality Analysis

### Key Feature: Large log_message Field

- **Target Length**: 1,500 characters (actual length varies between 1,350-1,650 characters)
- **Content Generation**: Template-based system generating different message types based on log level
- **Stack Traces**: ERROR level logs have 70% probability of including 3-8 lines of Java stack trace information
- **Placeholder Replacement**: Dynamic substitution of variables like user IDs, IP addresses, timestamps, etc.

### Field Cardinality Distribution

#### High Cardinality Fields (Near-Unique)
- **`trace_id`, `span_id`**: Generated using 64-bit random numbers
- **`session_id`, `request_id`**: Generated using 64-bit random numbers  
- **`log_uid`**: Based on timestamp + row index, ensuring uniqueness

#### Medium Cardinality Fields (~100K values)
- **`host_id/host_name`**: Format `host-{0-99999}` with Greek letter naming patterns
- **`service_id/service_name`**: Format `service-{0-99999}` with Greek letter naming patterns
- **`container_id/container_name`**: Format `container-{0-99999}` with Greek letter naming patterns
- **`pod_id/pod_name`**: Format `pod-{0-99999}` with Greek letter naming patterns
- **`cluster_id/cluster_name`**: Format `cluster-{0-99999}` with Greek letter naming patterns

#### Low Cardinality Fields
- **`user_id`**: Range 1-9999, simulating 9,999 users
- **`log_level`**: INFO (84%), DEBUG (10%), WARN (5%), ERROR (1%)
- **`response_time_ms`**: 1-999 milliseconds in cyclic distribution

#### Constant Fields
- **`log_source`**: Fixed value "application"
- **`version`**: Fixed value "v1.0.0"

### Timestamp Generation Pattern

- **Base Time**: Current system time
- **Distribution**: Base time ± 1 second random offset
- **Sequence**: Generally increasing with small random perturbations
- **Formula**: `base_time + current_row + (random_offset % 2000) - 1000`

### Performance Optimization Features

#### Pre-generated Value Pools
- **Pool Size**: Maximum 10,000 values or 2x row count (whichever is smaller)
- **Content**: All field values are pre-generated and stored in memory

#### Deterministic Randomization
- **Algorithm**: Uses `(current_row * 7 + 13) % pool_len` for offset generation
- **Benefits**: 
  - Avoids overhead of true random number generation
  - Ensures reproducible test results
  - Provides sufficient data variation

#### Memory Optimization
- **String Reuse**: Uses references instead of cloning to reduce memory allocation
- **Batch Generation**: Generates large amounts of data at once to reduce system calls
- **Pre-allocated Buffers**: Pre-allocates string capacity

## Data Distribution Characteristics

### Log Level Distribution (Realistic Production Patterns)
- **INFO**: 84% - Normal operational messages
- **DEBUG**: 10% - Detailed diagnostic information
- **WARN**: 5% - Warning conditions
- **ERROR**: 1% - Error conditions with stack traces

### Message Templates by Level

#### INFO Templates Examples
- "Request processed successfully for user_id={USER} in {TIME}ms"
- "Database query executed: SELECT * FROM {TABLE} WHERE id={ID} ({TIME}ms)"
- "User {USER} logged in from IP {IP}"

#### ERROR Templates Examples
- "Database connection failed: timeout after {TIME}ms"
- "Authentication failed for user={USER} from IP={IP}"
- "Service {SERVICE} is unavailable (status={STATUS})"

### Placeholder Substitution
- **{USER}**: user_10000-user_10999
- **{IP}**: 192.168.x.x format IP addresses
- **{TIME}**: Random time 1-4999 milliseconds
- **{SIZE}**: Random size 1KB-1MB
- **{COUNT}**: Random count 1-999

## Data Simulation Features

### Infrastructure Field Generation
The data model includes infrastructure-related fields for realistic log data simulation:
- **Host**, **Service**, **Container**, **Pod**, **Cluster** identifiers and names

### Distributed Tracing Simulation
- **Trace/Span IDs**: High cardinality values simulating distributed request flows
- **Request Correlation**: Links between user sessions, requests, and trace data

### Realistic Data Characteristics
- **Log Level Distribution**: Approximates typical production environments
- **Response Times**: 1-999ms range typical for web applications
- **Message Content**: Includes database queries, user operations, system monitoring
- **Error Handling**: ERROR logs include realistic stack traces

### Data Relationships
- **Temporal Correlation**: Timestamps generally increase, simulating real-time log streams
- **Trace Correlation**: trace_id and span_id simulate distributed system call chains

## Usage Examples

### Bulk API Benchmark
```bash
cargo run --example bulk_api_log_benchmark --release
```

### Regular API Benchmark
```bash
cargo run --example regular_api_log_benchmark --release
```

### Environment Variables
- `GREPTIME_ENDPOINT` - GreptimeDB endpoint (default: localhost:4001)
- `GREPTIMEDB_DBNAME` - Database name (default: public)
- `TABLE_ROW_COUNT` - Number of rows to generate (default: 2,000,000)
- `BATCH_SIZE` - Batch size for ingestion (default: 100,000)
- `PARALLELISM` - Parallel requests (default: 8)
- `COMPRESSION` - Enable compression (default: lz4)

## Benchmark Results

### Test Environment
- **CPU**: 10 cores
- **Build Profile**: Release (optimized)
- **Dataset**: 2,000,000 rows, 22 columns
- **Batch Size**: 100,000 rows per batch
- **Parallelism**: 8 concurrent streams (bulk API only)

### Performance Comparison

| API Type | Throughput | Duration | Average Latency | Performance Gain |
|----------|------------|----------|-----------------|------------------|
| **Bulk API** | **154,655 rows/sec** | **12.93s** | N/A (async) | **+44.7%** |
| Regular API | 106,872 rows/sec | 18.71s | 667.94ms | Baseline |

### Bulk API Results
```
% cargo run --example bulk_api_log_benchmark --release
   Compiling greptimedb-ingester v0.15.0 (/Users/jeremy/workspace/codes/greptimedb-ingester-rust)
    Finished `release` profile [optimized + debuginfo] target(s) in 2.45s
     Running `target/release/examples/bulk_api_log_benchmark`
=== GreptimeDB Bulk API Log Benchmark ===
Synthetic log data generation and bulk API ingestion performance test

=== Bulk API Benchmark Configuration ===
Endpoint: localhost:4001
Database: public
Max rows per provider: 2000000
Batch size: 100000
Parallelism: 8
Compression: lz4
CPU cores: 10
Build profile: release

=== Running Bulk API Log Data Benchmark ===
Pre-generating 10000 values for ultra-fast data generation...
Pre-generation completed in 169ms
Setting up bulk stream writer...
Starting bulk API benchmark: LogTableDataProvider
Table: benchmark_logs (22 columns)
Target rows: 2000000
Batch size: 100000
Parallelism: 8

→ Batch 1: 100000 rows processed (206174 rows/sec)
→ Batch 2: 200000 rows processed (208237 rows/sec)
→ Batch 3: 300000 rows processed (213118 rows/sec)
→ Batch 4: 400000 rows processed (213122 rows/sec)
→ Batch 5: 500000 rows processed (208223 rows/sec)
→ Batch 6: 600000 rows processed (206783 rows/sec)
→ Batch 7: 700000 rows processed (205819 rows/sec)
→ Batch 8: 800000 rows processed (204782 rows/sec)
→ Batch 9: 900000 rows processed (203174 rows/sec)
→ Batch 10: 1000000 rows processed (203273 rows/sec)
Flushed 5 responses (total 500000 affected rows)
→ Batch 11: 1100000 rows processed (203345 rows/sec)
→ Batch 12: 1200000 rows processed (203347 rows/sec)
→ Batch 13: 1300000 rows processed (203847 rows/sec)
→ Batch 14: 1400000 rows processed (203826 rows/sec)
→ Batch 15: 1500000 rows processed (203501 rows/sec)
→ Batch 16: 1600000 rows processed (203321 rows/sec)
→ Batch 17: 1700000 rows processed (203008 rows/sec)
→ Batch 18: 1800000 rows processed (203444 rows/sec)
→ Batch 19: 1900000 rows processed (203249 rows/sec)
→ Batch 20: 2000000 rows processed (202809 rows/sec)
Flushed 7 responses (total 700000 affected rows)
Finishing bulk writer and waiting for all responses...
All bulk writes completed successfully
Cleaning up data provider...
Bulk API benchmark completed successfully!
Final Results:
  • Total rows: 2000000
  • Total batches: 20
  • Duration: 12.93s
  • Throughput: 154651 rows/sec

=== LogTableDataProvider Benchmark Results ===
Table: benchmark_logs
SUCCESS
Total rows: 2000000
Duration: 12932ms
Throughput: 154655 rows/sec

=== Benchmark Comparison ===
Fastest provider: LogTableDataProvider (154655 rows/sec)

Provider                          Rows Duration(ms)      Throughput     Status
--------------------------------------------------------------------------
LogTableDataProvider           2000000        12932     154655 r/s    SUCCESS
```

### Regular API Results
```
% cargo run --example regular_api_log_benchmark --release
   Compiling greptimedb-ingester v0.15.0 (/Users/jeremy/workspace/codes/greptimedb-ingester-rust)
    Finished `release` profile [optimized + debuginfo] target(s) in 2.71s
     Running `target/release/examples/regular_api_log_benchmark`
=== GreptimeDB Regular API Log Benchmark ===
Regular Database::insert() API performance test for log data

=== Regular API Benchmark Configuration ===
Endpoint: localhost:4001
Database: public
Max rows per provider: 2000000
Batch size: 100000
Parallelism: 8
Compression: lz4
CPU cores: 10
Build profile: release

=== Running Regular API Log Data Benchmark ===
Pre-generating 10000 values for ultra-fast data generation...
Pre-generation completed in 146ms
Starting regular API benchmark: Regular API
Table: benchmark_logs (22 columns)
Target rows: 2000000
Batch size: 100000

→ Batch 1: 100000 rows processed, 100000 affected (126551 rows/sec, 632.74ms latency)
→ Batch 2: 100000 rows processed, 100000 affected (120220 rows/sec, 621.99ms latency)
→ Batch 3: 100000 rows processed, 100000 affected (117597 rows/sec, 627.52ms latency)
→ Batch 4: 100000 rows processed, 100000 affected (119874 rows/sec, 550.78ms latency)
→ Batch 5: 100000 rows processed, 100000 affected (120046 rows/sec, 598.94ms latency)
→ Batch 6: 100000 rows processed, 100000 affected (111263 rows/sec, 805.44ms latency)
→ Batch 7: 100000 rows processed, 100000 affected (110980 rows/sec, 633.58ms latency)
→ Batch 8: 100000 rows processed, 100000 affected (104324 rows/sec, 1066.02ms latency)
→ Batch 9: 100000 rows processed, 100000 affected (105995 rows/sec, 587.05ms latency)
→ Batch 10: 100000 rows processed, 100000 affected (105630 rows/sec, 761.72ms latency)
→ Batch 11: 100000 rows processed, 100000 affected (106755 rows/sec, 588.12ms latency)
→ Batch 12: 100000 rows processed, 100000 affected (108126 rows/sec, 560.52ms latency)
→ Batch 13: 100000 rows processed, 100000 affected (109697 rows/sec, 536.07ms latency)
→ Batch 14: 100000 rows processed, 100000 affected (106446 rows/sec, 865.18ms latency)
→ Batch 15: 100000 rows processed, 100000 affected (106613 rows/sec, 681.73ms latency)
→ Batch 16: 100000 rows processed, 100000 affected (106358 rows/sec, 738.93ms latency)
→ Batch 17: 100000 rows processed, 100000 affected (107184 rows/sec, 581.04ms latency)
→ Batch 18: 100000 rows processed, 100000 affected (106432 rows/sec, 702.45ms latency)
→ Batch 19: 100000 rows processed, 100000 affected (106657 rows/sec, 634.81ms latency)
→ Batch 20: 100000 rows processed, 100000 affected (107343 rows/sec, 584.20ms latency)
Regular API benchmark completed successfully!
Final Results:
  • Total rows: 2000000
  • Total batches: 20
  • Duration: 18.71s
  • Throughput: 106870 rows/sec
  • Average latency: 667.94ms

=== Regular API Benchmark Results ===
Table: benchmark_logs
SUCCESS
Total rows: 2000000
Duration: 18714ms
Throughput: 106872 rows/sec

=== Benchmark Comparison ===
Fastest provider: Regular API (106872 rows/sec)

Provider                          Rows Duration(ms)      Throughput     Status
--------------------------------------------------------------------------
Regular API                    2000000        18714     106872 r/s    SUCCESS
```
