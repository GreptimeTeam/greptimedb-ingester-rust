## Examples Descriptions

### Recommended Examples

#### `bulk_stream_writer_example.rs` ⭐ **RECOMMENDED**
High-performance bulk insertion using the schema-bound BulkStreamWriter:
- **Schema-bound design**: Each writer binds to a specific table schema
- **Persistent connection**: Eliminates connection overhead for multiple batches
- **Configurable compression**: Reduces network bandwidth
- **Row-based API**: Simple `write_rows()` method for streaming data

**Usage pattern:**
```rust
// 1. Define schema once
let table_template = Table::builder()
    .name("sensor_data")
    .build()?
    .add_tag("device_id", ColumnType::String)
    .add_timestamp("timestamp", TimestampUnit::Millisecond)
    .add_field("value", ColumnType::Float64);

// 2. Create schema-bound writer
let mut bulk_writer = inserter
    .create_bulk_stream_writer(&table_template, options)
    .await?;

// 3. Stream rows continuously
for batch in data_batches {
    let rows = create_rows(batch);
    bulk_writer.write_rows(rows).await?;
}

// 4. Finish
bulk_writer.finish().await?;
```

### Legacy Examples (Not Recommended)

#### `bulk_insert_example.rs`
Low-level Arrow Flight example - **use BulkStreamWriter instead**

#### `insert_example.rs` 
Row-based insertion example - **use BulkStreamWriter for better performance**

## Getting Started

**For new projects, start with `bulk_stream_writer_example.rs`** - it demonstrates the most efficient and user-friendly approach for bulk data insertion.

Run the example:
```bash
cargo run --example bulk_stream_writer_example
```