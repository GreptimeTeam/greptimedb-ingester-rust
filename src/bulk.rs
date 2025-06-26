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

//! High-level bulk insert API for GreptimeDB
//!
//! This module provides a user-friendly API for bulk inserting data into GreptimeDB,
//! abstracting away the low-level Arrow Flight details.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::builder::BinaryBuilder;
use arrow_array::{Array, RecordBatch};
use arrow_flight::{FlightData, FlightDescriptor};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use futures::channel::mpsc;
use futures::{SinkExt, Stream, StreamExt};

use crate::api::v1::ColumnDataType;
use crate::client::Client;
use crate::database::Database;
use crate::flight::do_put::DoPutMetadata;
use crate::flight::{FlightEncoder, FlightMessage};
use crate::table::{Column, Row, Table};
use crate::{error, Result};
use snafu::{ensure, ResultExt};

// Macro to generate array conversion for simple types
macro_rules! build_primitive_array {
    ($rows:expr, $col_idx:expr, $getter:ident, $array_type:ty) => {{
        let values: Vec<Option<_>> = $rows.iter().map(|row| row.$getter($col_idx)).collect();
        Arc::new(<$array_type>::from(values)) as Arc<dyn Array>
    }};
}

/// High-level bulk inserter for GreptimeDB
#[derive(Clone)]
pub struct BulkInserter {
    database: Database,
}

impl BulkInserter {
    /// Create a new bulk inserter
    pub fn new(client: Client, database_name: &str) -> Self {
        Self {
            database: Database::new_with_dbname(database_name, client),
        }
    }

    /// Create a bulk stream writer from a table template
    ///
    /// This is a convenience method that extracts the schema from a table
    /// and creates a BulkStreamWriter bound to that schema.
    pub async fn create_bulk_stream_writer(
        &self,
        table: &Table,
        options: Option<BulkWriteOptions>,
    ) -> Result<BulkStreamWriter> {
        let options = options.unwrap_or_default();
        BulkStreamWriter::new(&self.database, &table.name, table.columns.clone(), options).await
    }
}

/// Configuration options for bulk write operations
#[derive(Debug, Clone)]
pub struct BulkWriteOptions {
    pub compression: bool,
    pub timeout_ms: u64,
    pub parallelism: usize,
}

impl Default for BulkWriteOptions {
    fn default() -> Self {
        Self {
            compression: true,
            timeout_ms: 30000,
            parallelism: 1,
        }
    }
}

impl BulkWriteOptions {
    /// Enable or disable compression
    pub fn with_compression(mut self, compression: bool) -> Self {
        self.compression = compression;
        self
    }

    /// Set timeout in milliseconds
    pub fn with_timeout_ms(mut self, timeout_ms: u64) -> Self {
        self.timeout_ms = timeout_ms;
        self
    }

    /// Set parallelism for concurrent requests
    pub fn with_parallelism(mut self, parallelism: usize) -> Self {
        self.parallelism = parallelism;
        self
    }
}

/// High-performance bulk stream writer that maintains a persistent connection
/// Each writer is bound to a specific table with fixed schema
pub struct BulkStreamWriter {
    sender: mpsc::Sender<FlightData>,
    response_stream: Pin<Box<dyn Stream<Item = Result<crate::flight::do_put::DoPutResponse>>>>,
    table_name: String,
    table_schema: Vec<Column>,
    // Cache the Arrow schema to avoid recreating it for each batch
    arrow_schema: Arc<Schema>,
    request_id_generator: i64,
    encoder: FlightEncoder,
    schema_sent: bool,
    // Parallel processing fields
    parallelism: usize,
    timeout_ms: u64,
    // Track pending requests: request_id -> (sent_time, completed)
    pending_requests: HashMap<i64, (Instant, bool)>,
}

impl BulkStreamWriter {
    /// Create a new bulk stream writer bound to a specific table schema
    pub async fn new(
        database: &Database,
        table_name: &str,
        table_schema: Vec<Column>,
        options: BulkWriteOptions,
    ) -> Result<Self> {
        // Create the encoder with compression settings
        let encoder = if options.compression {
            FlightEncoder::default()
        } else {
            FlightEncoder::with_compression_disabled()
        };

        // Pre-compute Arrow schema to avoid recreating it for each batch
        let fields: Result<Vec<Field>> = table_schema
            .iter()
            .map(|col| {
                column_data_type_to_arrow(col.data_type)
                    .map(|data_type| Field::new(&col.name, data_type, true))
            })
            .collect();
        let arrow_schema = Arc::new(Schema::new(fields?));

        // Create a channel for streaming FlightData
        let (sender, receiver) = mpsc::channel::<FlightData>(1000);

        // Convert receiver to a stream and start the do_put operation
        let flight_stream = receiver.boxed();
        let response_stream = database.do_put(flight_stream).await?;

        Ok(Self {
            sender,
            response_stream,
            table_name: table_name.to_string(),
            table_schema,
            arrow_schema,
            request_id_generator: 0,
            encoder,
            schema_sent: false,
            parallelism: options.parallelism,
            timeout_ms: options.timeout_ms,
            pending_requests: HashMap::new(),
        })
    }

    /// Write rows to the stream using the fixed table schema
    pub async fn write_rows(&mut self, rows: Vec<Row>) -> Result<u64> {
        if rows.is_empty() {
            return Ok(0);
        }

        let record_batch = self.rows_to_record_batch(&rows)?;
        let total_rows = record_batch.num_rows() as u64;
        let _response = self.write_record_batch_parallel(record_batch).await?;

        Ok(total_rows)
    }

    /// Write rows to the stream and return the server response
    pub async fn write_rows_with_response(
        &mut self,
        rows: Vec<Row>,
    ) -> Result<(u64, crate::flight::do_put::DoPutResponse)> {
        ensure!(!rows.is_empty(), error::EmptyRowsSnafu);

        let record_batch = self.rows_to_record_batch(&rows)?;
        let total_rows = record_batch.num_rows() as u64;
        let response = self.write_record_batch_parallel(record_batch).await?;

        Ok((total_rows, response))
    }

    /// Write a record batch to the stream with parallel processing
    async fn write_record_batch_parallel(
        &mut self,
        batch: RecordBatch,
    ) -> Result<crate::flight::do_put::DoPutResponse> {
        // Send schema first if not already sent
        if !self.schema_sent {
            let mut schema_data = self.encoder.encode(FlightMessage::Schema(batch.schema()));
            let metadata = DoPutMetadata::new(0);
            schema_data.app_metadata = serde_json::to_vec(&metadata)
                .context(error::SerializeMetadataSnafu)?
                .into();

            schema_data.flight_descriptor = Some(FlightDescriptor {
                r#type: arrow_flight::flight_descriptor::DescriptorType::Path as i32,
                path: vec![self.table_name.clone()],
                ..Default::default()
            });

            self.sender
                .send(schema_data)
                .await
                .map_err(|_| error::SendDataSnafu.build())?;

            if let Some(response) = self.response_stream.next().await {
                let _schema_response = response?;
            }

            self.schema_sent = true;
        }

        // Wait for available slot if we've reached parallelism limit
        while self.pending_requests.len() >= self.parallelism {
            self.process_pending_responses().await?;
        }

        // Send the request
        self.request_id_generator += 1;
        let request_id = self.request_id_generator;
        let message = FlightMessage::RecordBatch(batch);
        let mut data = self.encoder.encode(message);
        let metadata = DoPutMetadata::new(request_id);
        data.app_metadata = serde_json::to_vec(&metadata)
            .context(error::SerializeMetadataSnafu)?
            .into();

        self.sender
            .send(data)
            .await
            .map_err(|_| error::SendDataSnafu.build())?;

        // Track this request
        self.pending_requests
            .insert(request_id, (Instant::now(), false));

        // Wait for this specific request's response
        self.wait_for_response(request_id).await
    }

    /// Process pending responses and handle timeouts
    async fn process_pending_responses(&mut self) -> Result<()> {
        let timeout_duration = Duration::from_millis(self.timeout_ms);
        let now = Instant::now();

        // Check for timeouts
        let timed_out_requests: Vec<i64> = self
            .pending_requests
            .iter()
            .filter(|(_, (sent_time, completed))| {
                !completed && now.duration_since(*sent_time) > timeout_duration
            })
            .map(|(id, _)| *id)
            .collect();

        if !timed_out_requests.is_empty() {
            return error::RequestTimeoutSnafu {
                request_ids: timed_out_requests,
                timeout_ms: self.timeout_ms,
            }
            .fail();
        }

        // Process one response to make room for new requests
        if let Some(response) = self.response_stream.next().await {
            let response = response?;
            let request_id = response.request_id();
            if let Some((_, completed)) = self.pending_requests.get_mut(&request_id) {
                *completed = true;
            }
        }

        // Remove completed requests
        self.pending_requests
            .retain(|_, (_, completed)| !*completed);

        Ok(())
    }

    /// Wait for a specific request's response by request_id
    async fn wait_for_response(
        &mut self,
        target_request_id: i64,
    ) -> Result<crate::flight::do_put::DoPutResponse> {
        let timeout_duration = Duration::from_millis(self.timeout_ms);
        let start_time = Instant::now();

        loop {
            // Check timeout
            if start_time.elapsed() > timeout_duration {
                return error::RequestTimeoutSnafu {
                    request_ids: vec![target_request_id],
                    timeout_ms: self.timeout_ms,
                }
                .fail();
            }

            if let Some(response) = self.response_stream.next().await {
                let response = response?;
                let request_id = response.request_id();
                if request_id == target_request_id {
                    // Mark as completed and remove from pending
                    self.pending_requests.remove(&request_id);
                    return Ok(response);
                } else {
                    // Mark other request as completed
                    if let Some((_, completed)) = self.pending_requests.get_mut(&request_id) {
                        *completed = true;
                    }
                }
            } else {
                return error::StreamEndedSnafu.fail();
            }
        }
    }

    /// Convert rows to Arrow RecordBatch using cached schema
    fn rows_to_record_batch(&self, rows: &[Row]) -> Result<RecordBatch> {
        ensure!(!rows.is_empty(), error::EmptyTableSnafu);

        // Convert all rows to arrays
        let arrays = self.rows_to_arrays(rows)?;
        let batch = RecordBatch::try_new(Arc::clone(&self.arrow_schema), arrays)
            .context(error::CreateRecordBatchSnafu)?;

        Ok(batch)
    }

    /// Convert rows to Arrow arrays (optimized version without cloning schema)
    fn rows_to_arrays(&self, rows: &[Row]) -> Result<Vec<Arc<dyn Array>>> {
        // Pre-allocate with exact capacity
        let mut arrays = Vec::with_capacity(self.table_schema.len());

        for (col_idx, column) in self.table_schema.iter().enumerate() {
            let array = match &column.data_type {
                // Boolean type
                ColumnDataType::Boolean => {
                    build_primitive_array!(rows, col_idx, get_bool, arrow_array::BooleanArray)
                }

                // Integer types
                ColumnDataType::Int8 => {
                    build_primitive_array!(rows, col_idx, get_i8, arrow_array::Int8Array)
                }
                ColumnDataType::Int16 => {
                    build_primitive_array!(rows, col_idx, get_i16, arrow_array::Int16Array)
                }
                ColumnDataType::Int32 => {
                    build_primitive_array!(rows, col_idx, get_i32, arrow_array::Int32Array)
                }
                ColumnDataType::Int64 => {
                    build_primitive_array!(rows, col_idx, get_i64, arrow_array::Int64Array)
                }
                ColumnDataType::Uint8 => {
                    build_primitive_array!(rows, col_idx, get_u8, arrow_array::UInt8Array)
                }
                ColumnDataType::Uint16 => {
                    build_primitive_array!(rows, col_idx, get_u16, arrow_array::UInt16Array)
                }
                ColumnDataType::Uint32 => {
                    build_primitive_array!(rows, col_idx, get_u32, arrow_array::UInt32Array)
                }
                ColumnDataType::Uint64 => {
                    build_primitive_array!(rows, col_idx, get_u64, arrow_array::UInt64Array)
                }

                // Float types
                ColumnDataType::Float32 => {
                    build_primitive_array!(rows, col_idx, get_f32, arrow_array::Float32Array)
                }
                ColumnDataType::Float64 => {
                    build_primitive_array!(rows, col_idx, get_f64, arrow_array::Float64Array)
                }

                // String and Binary types
                ColumnDataType::Binary => {
                    // Convert binary data to Arrow Binary array using builder
                    let mut builder = BinaryBuilder::new();
                    for row in rows {
                        match row.get_binary(col_idx) {
                            Some(data) => builder.append_value(&data),
                            None => builder.append_null(),
                        }
                    }
                    Arc::new(builder.finish()) as Arc<dyn Array>
                }
                ColumnDataType::String => {
                    build_primitive_array!(rows, col_idx, get_string, arrow_array::StringArray)
                }

                // Date and Time types
                ColumnDataType::Date => {
                    build_primitive_array!(rows, col_idx, get_date, arrow_array::Date32Array)
                }
                ColumnDataType::Datetime => build_primitive_array!(
                    rows,
                    col_idx,
                    get_datetime,
                    arrow_array::TimestampMillisecondArray
                ),

                // Timestamp types
                ColumnDataType::TimestampSecond => build_primitive_array!(
                    rows,
                    col_idx,
                    get_timestamp,
                    arrow_array::TimestampSecondArray
                ),
                ColumnDataType::TimestampMillisecond => build_primitive_array!(
                    rows,
                    col_idx,
                    get_timestamp,
                    arrow_array::TimestampMillisecondArray
                ),
                ColumnDataType::TimestampMicrosecond => build_primitive_array!(
                    rows,
                    col_idx,
                    get_timestamp,
                    arrow_array::TimestampMicrosecondArray
                ),
                ColumnDataType::TimestampNanosecond => build_primitive_array!(
                    rows,
                    col_idx,
                    get_timestamp,
                    arrow_array::TimestampNanosecondArray
                ),

                // Time types
                ColumnDataType::TimeSecond => {
                    build_primitive_array!(rows, col_idx, get_i32, arrow_array::Time32SecondArray)
                }
                ColumnDataType::TimeMillisecond => build_primitive_array!(
                    rows,
                    col_idx,
                    get_i32,
                    arrow_array::Time32MillisecondArray
                ),
                ColumnDataType::TimeMicrosecond => build_primitive_array!(
                    rows,
                    col_idx,
                    get_i64,
                    arrow_array::Time64MicrosecondArray
                ),
                ColumnDataType::TimeNanosecond => build_primitive_array!(
                    rows,
                    col_idx,
                    get_i64,
                    arrow_array::Time64NanosecondArray
                ),

                // Decimal type (stored as binary)
                ColumnDataType::Decimal128 => {
                    let mut builder = BinaryBuilder::new();
                    for row in rows {
                        match row.get_decimal128(col_idx) {
                            Some(data) => builder.append_value(&data),
                            None => builder.append_null(),
                        }
                    }
                    Arc::new(builder.finish()) as Arc<dyn Array>
                }

                // JSON type (stored as binary per Java implementation)
                ColumnDataType::Json => {
                    let mut builder = BinaryBuilder::new();
                    for row in rows {
                        match row.get_json(col_idx) {
                            Some(json_str) => builder.append_value(json_str.as_bytes()),
                            None => builder.append_null(),
                        }
                    }
                    Arc::new(builder.finish()) as Arc<dyn Array>
                }

                // Unsupported types - these should not be used
                _ => {
                    return error::UnsupportedDataTypeSnafu {
                        data_type: format!("{:?}. Only basic types, timestamps, times, decimal128, and json are supported", column.data_type),
                    }.fail();
                }
            };
            arrays.push(array);
        }

        Ok(arrays)
    }

    /// Finish the bulk write operation and close the connection
    pub async fn finish(mut self) -> Result<()> {
        // Wait for all pending requests to complete
        while !self.pending_requests.is_empty() {
            self.process_pending_responses().await?;
        }

        // Close the sender to signal the end of the stream
        self.sender
            .close()
            .await
            .map_err(|_| error::CloseSenderSnafu.build())?;

        Ok(())
    }
}

impl Drop for BulkStreamWriter {
    fn drop(&mut self) {
        // Close the sender if it's still open to avoid hanging connections
        if !self.sender.is_closed() {
            let _ = futures::executor::block_on(self.sender.close());
        }
    }
}

// Helper function to convert ColumnDataType to Arrow DataType
// Based on GreptimeDB Java implementation - only supports actually implemented types
fn column_data_type_to_arrow(data_type: ColumnDataType) -> Result<DataType> {
    Ok(match data_type {
        // Integer types
        ColumnDataType::Int8 => DataType::Int8,
        ColumnDataType::Int16 => DataType::Int16,
        ColumnDataType::Int32 => DataType::Int32,
        ColumnDataType::Int64 => DataType::Int64,
        ColumnDataType::Uint8 => DataType::UInt8,
        ColumnDataType::Uint16 => DataType::UInt16,
        ColumnDataType::Uint32 => DataType::UInt32,
        ColumnDataType::Uint64 => DataType::UInt64,

        // Float types
        ColumnDataType::Float32 => DataType::Float32,
        ColumnDataType::Float64 => DataType::Float64,

        // Boolean type
        ColumnDataType::Boolean => DataType::Boolean,

        // String and Binary types
        ColumnDataType::Binary => DataType::Binary,
        ColumnDataType::String => DataType::Utf8,

        // Date type
        ColumnDataType::Date => DataType::Date32,

        // Timestamp types
        ColumnDataType::TimestampSecond => DataType::Timestamp(TimeUnit::Second, None),
        ColumnDataType::TimestampMillisecond => DataType::Timestamp(TimeUnit::Millisecond, None),
        // DateTime is an alias of TIMESTAMP_MICROSECOND per GreptimeDB docs
        ColumnDataType::Datetime => DataType::Timestamp(TimeUnit::Microsecond, None),
        ColumnDataType::TimestampMicrosecond => DataType::Timestamp(TimeUnit::Microsecond, None),
        ColumnDataType::TimestampNanosecond => DataType::Timestamp(TimeUnit::Nanosecond, None),

        // Time types (without date)
        ColumnDataType::TimeSecond => DataType::Time32(arrow_schema::TimeUnit::Second),
        ColumnDataType::TimeMillisecond => DataType::Time32(arrow_schema::TimeUnit::Millisecond),
        ColumnDataType::TimeMicrosecond => DataType::Time64(arrow_schema::TimeUnit::Microsecond),
        ColumnDataType::TimeNanosecond => DataType::Time64(arrow_schema::TimeUnit::Nanosecond),

        // Decimal type (precision and scale should be provided via extension, using defaults for now)
        ColumnDataType::Decimal128 => DataType::Decimal128(38, 10),

        // JSON type (represented as Binary per Java implementation)
        ColumnDataType::Json => DataType::Binary,

        // Unsupported types - these should not be used
        _ => {
            return error::UnsupportedDataTypeSnafu {
                data_type: format!("{:?}. Not supported", data_type),
            }
            .fail();
        }
    })
}

// Re-export the proto ColumnDataType for convenience
pub use crate::api::v1::ColumnDataType as ColumnType;
