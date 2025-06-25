# High-Performance Bulk Insert API

This document introduces the **BulkStreamWriter** - the recommended high-performance API for bulk data insertion into GreptimeDB.

## Why BulkStreamWriter?

The BulkStreamWriter provides the most efficient approach for bulk data insertion with these key advantages:

✅ **Schema-bound design** - each writer is bound to a specific table schema  
✅ **Persistent connection** - eliminates connection overhead  
✅ **Configurable compression** - reduces network bandwidth  
✅ **Row-based streaming** - simple API for continuous data ingestion  
✅ **High throughput** - optimized for time-series data patterns  

## Core API Design

### 1. Schema Definition
Define your table schema once using the builder pattern:

```rust
use greptimedb_ingester::{ColumnDataType, SemanticType};
use greptimedb_ingester::bulk::*;

// Define schema template
let table_template = Table::builder()
    .name("sensor_readings")
    .build()?
    .add_tag("device_id", ColumnDataType::String)       // Indexing column
    .add_tag("location", ColumnDataType::String)        // Grouping column
    .add_timestamp("timestamp", ColumnDataType::TimestampMillisecond)  // Timeline
    .add_field("temperature", ColumnDataType::Float64)  // Measurement
    .add_field("humidity", ColumnDataType::Float64)     // Measurement
    .add_field("pressure", ColumnDataType::Float64);    // Measurement
```

### 2. Writer Creation
Create a BulkStreamWriter bound to your schema:

```rust
// Configure performance options
let options = BulkWriteOptions::default()
    .with_compression(true)      // Enable LZ4 compression
    .with_timeout_ms(60000);    // 60 second timeout

// Create schema-bound writer
let mut bulk_writer = inserter.create_bulk_stream_writer(
    &table_template,
    Some(options)
).await?;
```

### 3. Continuous Data Streaming
Stream row data continuously using the same writer:

```rust
// Stream multiple batches of data
for batch_id in 0..100 {
    let rows = create_sensor_rows(batch_id, 1000);  // Vec<Row>
    
    let rows_written = bulk_writer.write_rows(rows).await?;
    println!("Batch {}: {} rows written", batch_id, rows_written);
}

// Finish and close the connection
bulk_writer.finish().await?;
println!("Bulk write completed successfully");
```

## Data Type Examples

### Using Different Column Types

```rust
// Example showing all major data types
let comprehensive_table = Table::builder()
    .name("comprehensive_data")
    .build()?
    // Tags (indexing columns)
    .add_tag("device_id", ColumnDataType::String)
    .add_tag("region_id", ColumnDataType::Uint32)
    
    // Timestamp (timeline)
    .add_timestamp("timestamp", ColumnDataType::TimestampMillisecond)
    
    // Numeric fields
    .add_field("temperature", ColumnDataType::Float32)
    .add_field("pressure", ColumnDataType::Float64)
    .add_field("humidity", ColumnDataType::Int16)
    .add_field("battery_level", ColumnDataType::Uint8)
    .add_field("sensor_count", ColumnDataType::Int64)
    
    // Boolean fields
    .add_field("is_online", ColumnDataType::Boolean)
    
    // Binary and JSON fields  
    .add_field("config_blob", ColumnDataType::Binary)
    .add_field("metadata", ColumnDataType::Json)
    
    // Decimal field
    .add_field("precise_value", ColumnDataType::Decimal128);

// Create rows with mixed data types
let rows = vec![
    Row::new()
        .add_value("sensor_001".into())           // String tag
        .add_value(Value::Uint32(1))              // Uint32 tag
        .add_value(Value::Timestamp(1640995200000)) // Timestamp
        .add_value(Value::Float32(23.5))          // Float32 field
        .add_value(Value::Float64(1013.25))       // Float64 field
        .add_value(Value::Int16(65))              // Int16 field
        .add_value(Value::Uint8(85))              // Uint8 field
        .add_value(Value::Int64(42))              // Int64 field
        .add_value(Value::Boolean(true))          // Boolean field
        .add_value(Value::Binary(vec![0x01, 0x02, 0x03])) // Binary field
        .add_value(Value::Json(r#"{"status":"active"}"#.to_string())) // JSON field
        .add_value(Value::Decimal128(vec![0x12, 0x34, 0x56, 0x78])) // Decimal128 field
];
```

## Complete Example

### IoT Sensor Data Streaming

```rust
use greptimedb_ingester::{ColumnDataType, SemanticType};
use greptimedb_ingester::bulk::*;
use greptimedb_ingester::client::Client;

#[tokio::main]
async fn main() -> Result<()> {
    // 1. Setup client
    let client = Client::with_urls(&["http://localhost:4001"]);
    let inserter = BulkInserter::new(client, "iot_database");

    // 2. Define schema once
    let sensor_schema = Table::builder()
        .name("sensor_data")
        .build()?
        .add_tag("device_id", ColumnDataType::String)
        .add_tag("location", ColumnDataType::String)
        .add_timestamp("timestamp", ColumnDataType::TimestampMillisecond)
        .add_field("temperature", ColumnDataType::Float64)
        .add_field("humidity", ColumnDataType::Float64)
        .add_field("pressure", ColumnDataType::Float64);

    // 3. Create high-performance writer
    let mut bulk_writer = inserter.create_bulk_stream_writer(
        &sensor_schema,
        Some(BulkWriteOptions::default()
            .with_compression(true))
    ).await?;

    // 4. Stream sensor data continuously
    for hour in 0..24 {
        let rows = generate_hourly_sensor_data(hour);
        
        let count = bulk_writer.write_rows(rows).await?;
        println!("Hour {}: {} sensor readings ingested", hour, count);
    }

    // 5. Complete the stream
    bulk_writer.finish().await?;
    println!("Sensor data streaming completed!");

    Ok(())
}

fn generate_hourly_sensor_data(hour: usize) -> Vec<Row> {
    let mut rows = Vec::new();
    let base_time = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64 - (hour as i64 * 3600000);

    // Generate data for 10 devices, 360 readings per hour (every 10 seconds)
    for device in 0..10 {
        for reading in 0..360 {
            let timestamp = base_time + (reading as i64 * 10000); // 10 second intervals
            
            let row = Row::new()
                .add_value(format!("device_{:03}", device).into())
                .add_value(format!("zone_{}", device % 3).into())
                .add_value(Value::Timestamp(timestamp))
                .add_value((20.0 + (hour as f64) + (device as f64 * 0.5)).into())  // temperature
                .add_value((50.0 + (reading as f64 % 40.0)).into())               // humidity
                .add_value((1013.25 + (device as f64 * 0.1)).into());             // pressure
            
            rows.push(row);
        }
    }

    rows
}
```

### Metrics Data Streaming

```rust
// Application metrics schema
let metrics_schema = Table::builder()
    .name("app_metrics")
    .build()?
    .add_tag("service", ColumnDataType::String)
    .add_tag("instance", ColumnDataType::String)
    .add_timestamp("timestamp", ColumnDataType::TimestampNanosecond)
    .add_field("value", ColumnDataType::Float64)
    .add_field("count", ColumnDataType::Int64)
    .add_field("cpu_usage", ColumnDataType::Float64)
    .add_field("memory_usage", ColumnDataType::Float64)
    .add_field("request_count", ColumnDataType::Int64);

// Create metrics writer
let mut metrics_writer = inserter.create_bulk_stream_writer(
    &metrics_schema,
    Some(BulkWriteOptions::default()
        .with_compression(true))
).await?;

// Stream metrics every minute
loop {
    let metrics_rows = collect_application_metrics();
    
    if !metrics_rows.is_empty() {
        metrics_writer.write_rows(metrics_rows).await?;
    }
    
    tokio::time::sleep(Duration::from_secs(60)).await;
}
```

## Performance Configuration

### BulkWriteOptions
Configure the writer for optimal performance:

```rust
let options = BulkWriteOptions::default()
    .with_compression(true)        // Enable LZ4_FRAME compression (recommended)
    .with_timeout_ms(60000);      // Request timeout (60 seconds)
```

### Performance Guidelines

| Data Pattern | Compression | Use Case |
|-------------|-------------|----------|
| **High-frequency sensors** | ✅ Enabled | IoT, telemetry |
| **Application metrics** | ✅ Enabled | APM, monitoring |
| **Log events** | ✅ Enabled | Logging, events |
| **Small payloads** | ❌ Disabled | Low-latency scenarios |

## Schema Templates

Create schema templates for common use cases:

### Sensor Data Template
```rust
let sensor_table = Table::builder()
    .name("my_sensors")
    .build()?
    .add_tag("device_id", ColumnDataType::String)
    .add_tag("location", ColumnDataType::String)
    .add_timestamp("timestamp", ColumnDataType::TimestampMillisecond)
    .add_field("temperature", ColumnDataType::Float64)
    .add_field("humidity", ColumnDataType::Float64)
    .add_field("pressure", ColumnDataType::Float64)
    .add_field("voltage", ColumnDataType::Float64)     // Add custom fields
    .add_field("current", ColumnDataType::Float64);
```

### Metrics Data Template  
```rust
let metrics_table = Table::builder()
    .name("my_metrics")
    .build()?
    .add_tag("service", ColumnDataType::String)
    .add_tag("instance", ColumnDataType::String)
    .add_timestamp("timestamp", ColumnDataType::TimestampNanosecond)
    .add_field("value", ColumnDataType::Float64)
    .add_field("count", ColumnDataType::Int64)
    .add_field("error_rate", ColumnDataType::Float64)  // Add custom fields
    .add_field("latency_p99", ColumnDataType::Float64);
```

## Data Types and Semantic Types

### Supported Column Types

**Integer Types:**
- `ColumnDataType::Int8` - 8-bit signed integer
- `ColumnDataType::Int16` - 16-bit signed integer  
- `ColumnDataType::Int32` - 32-bit signed integer
- `ColumnDataType::Int64` - 64-bit signed integer (recommended for counters)
- `ColumnDataType::Uint8` - 8-bit unsigned integer
- `ColumnDataType::Uint16` - 16-bit unsigned integer
- `ColumnDataType::Uint32` - 32-bit unsigned integer
- `ColumnDataType::Uint64` - 64-bit unsigned integer

**Float Types:**
- `ColumnDataType::Float32` - 32-bit floating point
- `ColumnDataType::Float64` - 64-bit floating point (recommended for measurements)

**String and Binary Types:**
- `ColumnDataType::String` - UTF-8 text (for tags and labels)
- `ColumnDataType::Binary` - Binary data

**Date and Time Types:**
- `ColumnDataType::Date` - Date (days since Unix epoch)
- `ColumnDataType::Datetime` - Datetime with millisecond precision
- `ColumnDataType::TimestampSecond` - Timestamp with second precision
- `ColumnDataType::TimestampMillisecond` - Timestamp with millisecond precision
- `ColumnDataType::TimestampMicrosecond` - Timestamp with microsecond precision  
- `ColumnDataType::TimestampNanosecond` - Timestamp with nanosecond precision
- `ColumnDataType::TimeSecond` - Time of day with second precision
- `ColumnDataType::TimeMillisecond` - Time of day with millisecond precision
- `ColumnDataType::TimeMicrosecond` - Time of day with microsecond precision
- `ColumnDataType::TimeNanosecond` - Time of day with nanosecond precision

**Advanced Types:**
- `ColumnDataType::Boolean` - Boolean values (for flags)
- `ColumnDataType::Decimal128` - 128-bit decimal numbers (stored as binary)
- `ColumnDataType::Json` - JSON documents (stored as binary)

### Semantic Types
- **Tag**: Indexing and grouping columns (device_id, service_name, etc.)
- **Timestamp**: Timeline column (exactly one per table)
- **Field**: Measurement values (temperature, cpu_usage, etc.)

## Error Handling

```rust
match bulk_writer.write_rows(rows).await {
    Ok(count) => println!("Successfully wrote {} rows", count),
    Err(e) => {
        eprintln!("Write failed: {}", e);
        // Handle retry logic or fallback
    }
}
```

## Running the Example

```bash
# Check compilation
cargo check --example bulk_stream_writer_example

# Run the complete example
cargo run --example bulk_stream_writer_example

# Run with performance validation
cargo clippy --workspace --all-targets -- -D warnings
```

## Best Practices

1. **Define schema once** - Create table templates and reuse them
2. **Enable compression** - Reduces bandwidth for most workloads (enabled by default)
3. **Handle errors gracefully** - Implement retry logic for production use
4. **Monitor performance** - Track throughput and optimize row batch sizes in your application
5. **Reuse writers** - Create one BulkStreamWriter per table and reuse it for multiple data batches

The BulkStreamWriter is specifically designed for time-series data patterns where you have a consistent schema and need to continuously stream large volumes of data efficiently.