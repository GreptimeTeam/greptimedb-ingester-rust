# GreptimeDB Ingester Examples

This directory contains comprehensive examples demonstrating two distinct ingestion approaches for different use cases.

## Examples Overview

### 🚀 `insert_example.rs` - Low-Latency Insert API
**Best for**: Real-time applications, IoT sensors, interactive systems

```bash
cargo run --example insert_example
```

**Features:**
- **Minimal latency**: sub-millisecond per operation
- **Real-time processing**: Immediate feedback and processing
- **Small batches**: Optimized for 200-1000 rows per request
- **Interactive patterns**: Perfect for user-facing applications

**Demonstrates:**
- Real-time sensor data ingestion with latency measurements
- Comprehensive data type support (JSON, binary, timestamps, etc.)
- Best practices for low-latency scenarios

### ⚡ `bulk_stream_writer_example.rs` - High-Throughput Bulk API
**Best for**: ETL operations, data migration, batch processing, log ingestion

```bash
cargo run --example bulk_stream_writer_example
```

**Features:**
- **High throughput**: 10k-100k+ rows/sec
- **Parallel processing**: Configurable concurrent requests
- **Efficient batching**: Optimized for 2k-100k+ rows per request
- **Performance optimization**: Compression, connection reuse

**Demonstrates:**
- Sequential vs parallel processing performance comparison
- Async submission patterns with `write_rows_async()`
- Optimal configuration for high-volume scenarios
- Performance metrics and best practices
- **Important**: Bulk API requires manual table creation (does not auto-create tables)
- Current limitation: bulk operations work only with field columns (tag support coming)

## Choosing the Right Example

| Scenario | Example | Why |
|----------|---------|-----|
| **IoT sensor data** | `insert_example.rs` | Real-time requirements, small batches |
| **Interactive dashboards** | `insert_example.rs` | User expects immediate feedback |
| **ETL pipelines** | `bulk_stream_writer_example.rs` | Process millions of records efficiently |
| **Log ingestion** | `bulk_stream_writer_example.rs` | High volume, can batch data |
| **Data migration** | `bulk_stream_writer_example.rs` | Transfer large datasets quickly |
| **Real-time alerts** | `insert_example.rs` | Immediate processing required |

## Configuration

Both examples use environment variables for configuration. Create a `.env` file or set:

```bash
export GREPTIMEDB_ENDPOINT="localhost:4001"
export GREPTIMEDB_DATABASE="public"
```

Alternatively, modify `db-connection.toml`:

```toml
[database]
endpoints = ["localhost:4001"]
dbname = "public"
```

## Quick Start

1. **For real-time applications**:
   ```bash
   cargo run --example insert_example
   ```

2. **For high-volume batch processing**:
   ```bash
   cargo run --example bulk_stream_writer_example
   ```

## Performance Characteristics

### Low-Latency Insert (`insert_example.rs`)
- **Latency**: sub-millisecond per operation
- **Throughput**: 200-1,000 ops/sec
- **Memory**: Low, constant
- **Network**: One request per operation

### High-Throughput Bulk (`bulk_stream_writer_example.rs`)
- **Latency**: 1-1000 milliseconds per batch
- **Throughput**: > 10k rows/sec
- **Memory**: Higher during batching
- **Network**: Parallel requests with compression

## Code Patterns

### Low-Latency Pattern
```rust
// Connect and insert immediately
let database = Database::new_with_dbname("public", client);
let affected_rows = database.insert(insert_request).await?;
println!("Inserted {} rows in {}ms", affected_rows, latency.as_millis());
```

### High-Throughput Pattern
```rust
// Create persistent stream writer
let mut bulk_writer = bulk_inserter
    .create_bulk_stream_writer(&table_template, None)
    .await?;

// Submit requests asynchronously
for batch in batches {
    let request_id = bulk_writer.write_rows_async(batch).await?;
}

// Wait for all to complete
let responses = bulk_writer.wait_for_all_pending().await?;

bulk_writer.finish().await?;
```

## Running Examples

### Commands
```bash
# Check compilation
cargo check --examples

# Run low-latency example
cargo run --example insert_example

# Run high-throughput example  
cargo run --example bulk_stream_writer_example

# Run both with logging
RUST_LOG=info cargo run --example insert_example
RUST_LOG=info cargo run --example bulk_stream_writer_example
```

## Example Output

### insert_example.rs
```
=== Real-time Insert Example ===
Use case: Low-latency, small batch inserts for real-time applications
Processing real-time batch 1...
  ✓ Inserted 2 rows in 12ms (latency: 12.0ms)
...
✓ Real-time insertion completed successfully!
```

### bulk_stream_writer_example.rs
```
=== High-Throughput Bulk Stream Writer Example ===
[1/2] Sequential Baseline (traditional approach)
  Sequential processing: 100 batches × 1000 rows = 100000 total rows
  ✓ Sequential: 100000 rows in 45.23s (2211 rows/sec)

[2/2] Parallel Optimization (async submission)
  ✓ All 100 batches submitted in 2.156s (46 batches/sec)
  ✓ Parallel: 100000 rows in 15.78s (6340 rows/sec)
⚡ Speedup: 2.9x faster with parallel approach
```

## Understanding the Output

Both examples provide detailed performance metrics to help you understand:
- **Latency**: Time per individual operation
- **Throughput**: Total operations per second
- **Success rates**: Proportion of successful operations
- **Memory usage**: Resource consumption patterns
- **Network efficiency**: Bandwidth utilization

Use these metrics to:
1. Validate performance requirements
2. Tune configuration parameters
3. Choose the right approach for your use case
4. Monitor production performance

## Important Notes for Bulk Operations

**Manual Table Creation Required**: Unlike the insert API which can automatically create tables, the bulk API requires tables to exist beforehand. In production, you should:

1. **Create tables manually using SQL DDL**:
   ```sql
   CREATE TABLE sensor_readings (
       ts TIMESTAMP TIME INDEX,
       sensor_id STRING,
       temperature DOUBLE,
       sensor_status BIGINT
   );
   ```

2. **Or use insert API first** (as shown in examples):
   ```rust
   // Insert one row to create the table
   database.insert(initial_request).await?;
   // Then use bulk API for high-throughput operations
   ```

## Column Types in Bulk vs Insert Operations

**Important Difference**: The two examples use different column types due to current limitations:

### Low-Latency Insert (`insert_example.rs`)
- Uses **tag columns** for primary key: `tag("device_id", ColumnDataType::String)`
- Uses **field columns** for measurements: `field("temperature", ColumnDataType::Float64)`
- Tag columns are part of the primary key and provide fast querying

### High-Throughput Bulk (`bulk_stream_writer_example.rs`)  
- Uses **field columns** for all data: `add_field("sensor_id", ColumnDataType::String)`
- Uses **field columns** for measurements: `add_field("temperature", ColumnDataType::Float64)`
- Currently, bulk operations only support field columns

> **Key Insight**: Bulk operations currently don't support tables with tag columns. This is a temporary limitation - future versions will support tag columns in bulk operations. Tag columns serve as part of the primary key in GreptimeDB.

## Next Steps

After running the examples:
1. Study the code patterns that match your use case
2. Adapt the schema definitions for your data
3. Adjust performance parameters (batch sizes, parallelism)
4. Implement error handling and retry logic for production
5. Monitor performance metrics in your application

For more details, see the main [README.md](../README.md) documentation.
