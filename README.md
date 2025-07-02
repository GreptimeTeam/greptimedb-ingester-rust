# GreptimeDB Rust Ingester

A high-performance Rust client for ingesting data into GreptimeDB, supporting both low-latency individual inserts and high-throughput bulk streaming operations.

## Features

- **Two Ingestion Approaches**: Choose between low-latency inserts and high-throughput bulk streaming
- **Parallel Processing**: Async request submission with configurable parallelism
- **Type Safety**: Comprehensive support for all GreptimeDB data types
- **Performance Optimized**: Memory-efficient operations with zero-copy access patterns
- **Production Ready**: Robust error handling, timeouts, and connection management

## Architecture Overview

The ingester provides two main APIs tailored for different use cases:

### 1. Low-Latency Insert API
**Best for**: Real-time applications, IoT sensors, interactive systems

```rust,no_run
use greptimedb_ingester::api::v1::*;
use greptimedb_ingester::client::Client;
use greptimedb_ingester::helpers::schema::*;
use greptimedb_ingester::helpers::values::*;
use greptimedb_ingester::{database::Database, Result, ColumnDataType};

#[tokio::main]
async fn main() -> Result<()> {
    // Connect to GreptimeDB
    let client = Client::with_urls(&["localhost:4001"]);
    let database = Database::new_with_dbname("public", client);
    
    // Define schema
    let schema = vec![
        tag("device_id", ColumnDataType::String),
        timestamp("ts", ColumnDataType::TimestampMillisecond),
        field("temperature", ColumnDataType::Float64),
    ];
    
    // Create data rows
    let rows = vec![Row {
        values: vec![
            string_value("device_001".to_string()),
            timestamp_millisecond_value(1234567890000),
            f64_value(23.5),
        ],
    }];
    
    // Insert data with minimal latency
    let insert_request = RowInsertRequests {
        inserts: vec![RowInsertRequest {
            table_name: "sensor_data".to_string(),
            rows: Some(Rows {
                schema,
                rows,
            }),
        }],
    };
    
    let affected_rows = database.insert(insert_request).await?;
    println!("Inserted {} rows", affected_rows);
    Ok(())
}
```

### 2. High-Throughput Bulk API
**Best for**: ETL operations, data migration, batch processing, log ingestion

```rust,no_run
use greptimedb_ingester::{BulkInserter, BulkWriteOptions, ColumnDataType, CompressionType, Row, TableSchema, Value};
use greptimedb_ingester::api::v1::*;
use greptimedb_ingester::helpers::schema::*;
use greptimedb_ingester::helpers::values::*;
use greptimedb_ingester::client::Client;
use greptimedb_ingester::database::Database;
use std::time::Duration;

#[tokio::main]
async fn main() -> greptimedb_ingester::Result<()> {
    let client = Client::with_urls(&["localhost:4001"]);
    let current_timestamp = || 1234567890000i64;
    struct SensorData { timestamp: i64, device_id: String, temperature: f64 }
    let sensor_data: Vec<SensorData> = vec![
        SensorData { timestamp: 1234567890000, device_id: "device_001".to_string(), temperature: 23.5 },
        SensorData { timestamp: 1234567890001, device_id: "device_002".to_string(), temperature: 24.0 },
    ];

    // Step 1: Create table manually (bulk API requires table to exist beforehand)
    // Option A: Use insert API to create table
    let database = Database::new_with_dbname("public", client.clone());
    let init_schema = vec![
        timestamp("ts", ColumnDataType::TimestampMillisecond),
        field("device_id", ColumnDataType::String),
        field("temperature", ColumnDataType::Float64),
    ];

    let init_request = RowInsertRequests {
        inserts: vec![RowInsertRequest {
            table_name: "sensor_readings".to_string(),
            rows: Some(Rows {
                schema: init_schema,
                rows: vec![greptimedb_ingester::api::v1::Row {
                    values: vec![
                        timestamp_millisecond_value(current_timestamp()),
                        string_value("init_device".to_string()),
                        f64_value(0.0),
                    ],
                }],
            }),
        }],
    };

    database.insert(init_request).await?; // Table is now created

    // Option B: Create table using SQL (if you have SQL access)
    // CREATE TABLE sensor_readings (
    //     ts TIMESTAMP TIME INDEX,
    //     device_id STRING,
    //     temperature DOUBLE
    // );

    // Step 2: Now use bulk API for high-throughput operations
    let bulk_inserter = BulkInserter::new(client, "public");

    // Define table schema (must match the insert API schema above)
    let table_template = TableSchema::builder()
        .name("sensor_readings")
        .build()
        .unwrap()
        .add_timestamp("ts", ColumnDataType::TimestampMillisecond)
        .add_field("device_id", ColumnDataType::String)
        .add_field("temperature", ColumnDataType::Float64);

    // Create high-performance stream writer
    let mut bulk_writer = bulk_inserter
        .create_bulk_stream_writer(
            &table_template,
            Some(BulkWriteOptions::default()
                .with_parallelism(8)            // 8 concurrent requests
                .with_compression(CompressionType::Zstd) // Enable Zstandard compression
                .with_timeout(Duration::from_secs(60))   // 60s timeout
            ),
        )
        .await?;

    // Method 1: Optimized API (recommended for production)
    let mut rows1 = bulk_writer.alloc_rows_buffer(10000, 1024)?;  // capacity: 10000, row_buffer_size: 1024
    for data in &sensor_data {
        let row = Row::new().add_values(vec![
            Value::Timestamp(data.timestamp),
            Value::String(data.device_id.clone()),
            Value::Float64(data.temperature),
        ]);
        rows1.add_row(row)?;
    }
    let request_id1 = bulk_writer.write_rows_async(rows1).await?;

    // Method 2: Schema-safe API
    let mut rows2 = bulk_writer.alloc_rows_buffer(10000, 1024)?;  // capacity: 10000, row_buffer_size: 1024
    for data in &sensor_data {
        let row = bulk_writer.new_row()
            .set("ts", Value::Timestamp(data.timestamp))?
            .set("device_id", Value::String(data.device_id.clone()))?
            .set("temperature", Value::Float64(data.temperature))?
            .build()?;
        rows2.add_row(row)?;
    }
    let request_id2 = bulk_writer.write_rows_async(rows2).await?;

    // Wait for all operations to complete
    let responses = bulk_writer.wait_for_all_pending().await?;
    bulk_writer.finish().await?;
    Ok(())
}
```

> **Important**: 
> 1. **Manual Table Creation Required**: Bulk API does **not** create tables automatically. You must create the table beforehand using either:
>    - Insert API (which supports auto table creation), or 
>    - SQL DDL statements (CREATE TABLE)
> 2. **Schema Matching**: The table template in bulk API must exactly match the existing table schema.
> 3. **Column Types**: For bulk operations, currently use `add_field()` instead of `add_tag()`. Tag columns are part of the primary key in GreptimeDB, but bulk operations don't yet support tables with tag columns. This limitation will be addressed in future versions.

## When to Choose Which API

| Scenario | API Choice | Why |
|----------|------------|-----|
| **IoT sensor data** | Low-Latency Insert | Real-time requirements, small batches |
| **Interactive dashboards** | Low-Latency Insert | User expects immediate feedback |
| **ETL pipelines** | Bulk Streaming | Process millions of records efficiently |
| **Log ingestion** | Bulk Streaming | High volume, can batch data |
| **Data migration** | Bulk Streaming | Transfer large datasets quickly |

## Examples

The repository includes comprehensive examples demonstrating both approaches:

### Low-Latency Examples

Run with: `cargo run --example insert_example`

- **Real-time sensor ingestion**: Simulates IoT devices sending data with latency measurements
- **Data type demonstration**: Shows support for all GreptimeDB column types
- **Interactive patterns**: Best practices for real-time applications

### High-Throughput Examples

Run with: `cargo run --example bulk_stream_writer_example`

- **Performance comparison**: Sequential vs parallel processing benchmarks
- **Async submission patterns**: Demonstrates `write_rows_async()` for maximum throughput
- **Best practices**: Optimal configuration for high-volume scenarios

## API Design & Optimization

### Schema-Bound Writer

Each `BulkStreamWriter` is bound to a specific table schema, providing both safety and performance benefits:

- **Schema Validation**: Automatic validation ensures data consistency
- **Zero-Cost Optimization**: Schema-bound buffers share `Arc<Schema>` for ultra-fast validation  
- **Type Safety**: Prevents common mistakes like field order errors
- **Dynamic Growth**: Arrow builders automatically expand capacity as needed

### Buffer Allocation

```rust,no_run
use greptimedb_ingester::{BulkStreamWriter, Rows, Column};

async fn example(bulk_writer: &BulkStreamWriter, column_schemas: &[Column]) -> greptimedb_ingester::Result<()> {
    // Recommended: Use writer-bound buffer allocation
    let mut rows = bulk_writer.alloc_rows_buffer(10000, 1024)?;  // capacity: 10000, row_buffer_size: 1024
    // Shares Arc<Schema> with writer for optimal performance
    // Automatic schema compatibility
    
    // Alternative: Direct allocation
    let mut rows = Rows::new(column_schemas, 10000, 1024)?;  // capacity: 10000, row_buffer_size: 1024
    // Requires schema conversion and validation overhead
    Ok(())
}
```

### Row Building APIs

**Fast API (production recommended):**
```rust,no_run
use greptimedb_ingester::{Row, Value};

fn create_row() -> Row {
    let ts = 1234567890i64;
    let device_id = "device001".to_string();
    let temperature = 25.0f64;
    
    Row::new().add_values(vec![
        Value::Timestamp(ts),
        Value::String(device_id),
        Value::Float64(temperature),
    ])
    // Fastest performance
    // Requires correct field order
}
```

**Safe API (development recommended):**
```rust,no_run
use greptimedb_ingester::{BulkStreamWriter, Value};

async fn example(bulk_writer: &BulkStreamWriter) -> greptimedb_ingester::Result<()> {
    let ts = 1234567890i64;
    let device_id = "device001".to_string();
    let temperature = 25.0f64;
    
    let row = bulk_writer.new_row()
        .set("timestamp", Value::Timestamp(ts))?
        .set("device_id", Value::String(device_id))?
        .set("temperature", Value::Float64(temperature))?
        .build()?;
    // O(1) field name lookup (HashMap-based)
    // Field name validation
    // Prevents field order mistakes
    // Compile-time safety
    Ok(())
}
```

## Performance Characteristics

### Low-Latency Insert API
- **Latency**: sub-millisecond per operation
- **Throughput**: 1k ~ 10k ops/sec
- **Memory**: Low, constant
- **Use case**: Real-time applications

### Bulk Streaming API
- **Latency**: 1-1000 milliseconds per batch
- **Throughput**: > 10k rows/sec
- **Memory**: Higher during batching
- **Use case**: High-volume processing

## Advanced Usage

### Parallel Bulk Operations

The bulk API supports true parallelism through async request submission:

```rust,no_run
use greptimedb_ingester::{BulkStreamWriter, Rows};

async fn example(bulk_writer: &mut BulkStreamWriter, batches: Vec<Rows>) -> greptimedb_ingester::Result<()> {
    // Submit multiple batches without waiting
    let mut request_ids = Vec::new();
    for batch in batches {
        let id = bulk_writer.write_rows_async(batch).await?;
        request_ids.push(id);
    }
    
    // Option 1: Wait for all pending requests
    let responses = bulk_writer.wait_for_all_pending().await?;
    
    // Option 2: Wait for specific requests
    for request_id in request_ids {
        let response = bulk_writer.wait_for_response(request_id).await?;
        println!("Request {} completed with {} rows", 
                 request_id, response.affected_rows());
    }
    Ok(())
}
```

### Data Type Support

Full support for GreptimeDB data types:

```rust,no_run
use greptimedb_ingester::{Value, ColumnDataType, Row};

fn create_data_row() -> Row {
    Row::new()
        .add_value(Value::TimestampMillisecond(1234567890123))
        .add_value(Value::String("device_001".to_string()))
        .add_value(Value::Float64(23.5))
        .add_value(Value::Int64(1))
        .add_value(Value::Boolean(true))
        .add_value(Value::Binary(vec![0xDE, 0xAD, 0xBE, 0xEF]))
        .add_value(Value::Json(r#"{"key": "value"}"#.to_string()))
}
```

### Type-Safe Data Access

Efficient data access patterns:

```rust,no_run
use greptimedb_ingester::Row;

fn process_row_data(row: &Row) {
    fn process_binary(_data: &[u8]) {
        // Process binary data
    }
    
    // Type-safe value access
    if let Some(device_name) = row.get_string(1) {
        println!("Device: {}", device_name);
    }
    
    // Binary data access
    if let Some(binary_data) = row.get_binary(5) {
        process_binary(&binary_data);
    }
}
```

## Best Practices

### For Low-Latency Applications
- Use small batch sizes (200-1000 rows)
- Monitor and optimize network round-trip times

### For High-Throughput Applications  
- **Create tables manually first** - bulk API requires existing tables
- Use parallelism=8-16 for network-bound workloads
- Batch 2000-100000 rows per request for optimal performance
- Enable compression to reduce network overhead
- Monitor memory usage when submitting many async requests
- Implement backpressure control for very high-volume scenarios

### General Recommendations
- Use appropriate data types to minimize serialization overhead
- Pre-allocate vectors with known capacity
- Reuse connections when possible
- Handle errors gracefully with retry logic
- Monitor performance metrics in production

## Configuration

Set up your GreptimeDB connection:

```rust,no_run
use greptimedb_ingester::{ChannelConfig, ChannelManager};
use greptimedb_ingester::client::Client;
use std::time::Duration;

fn setup_client() -> Client {
    let channel_config = ChannelConfig::default()
        .timeout(Duration::from_secs(30))
        .connect_timeout(Duration::from_secs(5));
    let channel_manager = ChannelManager::with_config(channel_config);
    Client::with_manager_and_urls(channel_manager, 
        &["localhost:4001"])
}
```

## Error Handling

The library provides comprehensive error types:

```rust,no_run
use greptimedb_ingester::{Result, Error};
use greptimedb_ingester::api::v1::RowInsertRequests;
use greptimedb_ingester::database::Database;

async fn handle_insert(database: &Database, request: RowInsertRequests) {
    match database.insert(request).await {
        Ok(affected_rows) => println!("Inserted {} rows", affected_rows),
        Err(Error::RequestTimeout { .. }) => {
            // Handle timeout
        },
        Err(Error::SerializeMetadata { .. }) => {
            // Handle metadata serialization issues
        },
        Err(e) => {
            eprintln!("Unexpected error: {:?}", e);
        }
    }
}
```

## API Reference

### Core Types
- `Client`: Connection management
- `Database`: Low-level insert operations  
- `BulkInserter`: High-level bulk operations
- `BulkStreamWriter`: Streaming bulk writer
- `Table`: Table schema definition
- `Row`: Data row representation
- `Value`: Type-safe value wrapper

### Key Methods

**Low-Latency API:**
- `database.insert(request)` - Insert with immediate response

**Bulk API:**
- `bulk_writer.write_rows(rows)` - Submit and wait for completion
- `bulk_writer.write_rows_async(rows)` - Submit without waiting  
- `bulk_writer.wait_for_response(id)` - Wait for specific request
- `bulk_writer.wait_for_all_pending()` - Wait for all pending requests
- `bulk_writer.finish()` - Clean shutdown
- `bulk_writer.finish_with_responses()` - Shutdown with response collection

## License

This library uses the Apache 2.0 license to strike a balance between open contributions and allowing you to use the software however you want.

## Links

- [GreptimeDB Documentation](https://docs.greptime.com/)
- [Examples Directory](./examples/)
- [API Documentation](https://docs.rs/greptimedb-ingester/)
