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

use tokio::select;
use tokio::time::timeout;

use arrow_array::builder::BinaryBuilder;
use arrow_array::{Array, RecordBatch};
use arrow_flight::{FlightData, FlightDescriptor};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use futures::channel::mpsc;
use futures::{SinkExt, Stream, StreamExt};

use crate::api::v1::ColumnDataType;
use crate::client::Client;
use crate::database::Database;
use crate::flight::do_put::{DoPutMetadata, DoPutResponse};
use crate::flight::{FlightEncoder, FlightMessage};
use crate::table::{Column, Row, Table};
use crate::{error, Result};
use snafu::{ensure, ResultExt};

pub type RequestId = i64;

// Macro to generate array conversion for simple types
macro_rules! build_primitive_array {
    ($rows:expr, $col_idx:expr, $getter:ident, $array_type:ty) => {{
        let values: Vec<Option<_>> = $rows.iter().map(|row| row.$getter($col_idx)).collect();
        Arc::new(<$array_type>::from(values)) as Arc<dyn Array>
    }};
}

// Macro to generate binary array conversion with better capacity estimation
macro_rules! build_binary_array {
    ($rows:expr, $col_idx:expr, $getter:ident) => {{
        // Estimate better capacity based on data type
        let estimated_size = match stringify!($getter) {
            "get_decimal128" => $rows.len() * 16, // Decimal128 is typically 16 bytes
            "get_json" => $rows.len() * 128,      // JSON varies, use conservative estimate
            _ => $rows.len() * 64,                // General binary data
        };
        let mut builder = BinaryBuilder::with_capacity($rows.len(), estimated_size);
        for row in $rows {
            match row.$getter($col_idx) {
                Some(data) => builder.append_value(data),
                None => builder.append_null(),
            }
        }
        Arc::new(builder.finish()) as Arc<dyn Array>
    }};
}

// Macro for JSON array conversion (strings to bytes)
macro_rules! build_json_array {
    ($rows:expr, $col_idx:expr) => {{
        let mut builder = BinaryBuilder::with_capacity($rows.len(), $rows.len() * 128);
        for row in $rows {
            match row.get_json($col_idx) {
                Some(json_str) => builder.append_value(json_str.as_bytes()),
                None => builder.append_null(),
            }
        }
        Arc::new(builder.finish()) as Arc<dyn Array>
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
#[derive(Debug, Clone, Copy)]
pub struct BulkWriteOptions {
    pub compression: bool,
    pub timeout: Duration,
    pub parallelism: usize,
}

impl Default for BulkWriteOptions {
    fn default() -> Self {
        Self {
            compression: true,
            timeout: Duration::from_secs(60),
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

    /// Set timeout duration
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
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
    response_stream: Pin<Box<dyn Stream<Item = Result<DoPutResponse>>>>,
    table_name: String,
    table_schema: Vec<Column>,
    // Cache the Arrow schema to avoid recreating it for each batch
    arrow_schema: Arc<Schema>,
    next_request_id: RequestId,
    encoder: FlightEncoder,
    schema_sent: bool,
    // Parallel processing fields
    parallelism: usize,
    timeout: Duration,
    // Track pending requests: request_id -> sent_time
    pending_requests: HashMap<RequestId, Instant>,
    // Cache completed responses that were processed but not yet retrieved
    completed_responses: HashMap<RequestId, DoPutResponse>,
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
            FlightEncoder::without_compression()
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
            next_request_id: 0,
            encoder,
            schema_sent: false,
            parallelism: options.parallelism,
            timeout: options.timeout,
            pending_requests: HashMap::new(),
            completed_responses: HashMap::new(),
        })
    }

    /// Write rows to the stream using the fixed table schema
    pub async fn write_rows(&mut self, rows: Vec<Row>) -> Result<DoPutResponse> {
        // Use the async implementation and wait for the response
        let request_id = self.write_rows_async(rows).await?;
        self.wait_for_response(request_id).await
    }

    /// Submit rows for writing without waiting for response
    /// Returns a request_id that can be used to wait for the specific response
    pub async fn write_rows_async(&mut self, rows: Vec<Row>) -> Result<RequestId> {
        let record_batch = self.rows_to_record_batch(&rows)?;
        let request_id = self.submit_record_batch(record_batch).await?;

        Ok(request_id)
    }

    /// Wait for a specific request's response by request_id
    pub async fn wait_for_response(
        &mut self,
        target_request_id: RequestId,
    ) -> Result<DoPutResponse> {
        // Check if the response is already cached
        if let Some(response) = self.completed_responses.remove(&target_request_id) {
            return Ok(response);
        }

        let timeout_duration = self.timeout;
        let start_time = Instant::now();

        loop {
            let remaining_timeout = timeout_duration.saturating_sub(start_time.elapsed());
            // Check timeout
            if remaining_timeout.is_zero() {
                return error::RequestTimeoutSnafu {
                    request_ids: vec![target_request_id],
                    timeout: self.timeout,
                }
                .fail();
            }

            let next_result = timeout(remaining_timeout, self.response_stream.next()).await;
            let next_option = match next_result {
                Ok(option) => option,
                Err(_) => {
                    return error::RequestTimeoutSnafu {
                        request_ids: vec![target_request_id],
                        timeout: self.timeout,
                    }
                    .fail();
                }
            };
            if let Some(response) = next_option {
                let response = response?;
                let request_id = response.request_id();
                self.pending_requests.remove(&request_id);
                if request_id == target_request_id {
                    return Ok(response);
                } else {
                    self.completed_responses.insert(request_id, response);
                }
            } else {
                return error::StreamEndedSnafu.fail();
            }
        }
    }

    /// Wait for all pending requests to complete and return the responses
    pub async fn wait_for_all_pending(&mut self) -> Result<Vec<DoPutResponse>> {
        let mut responses =
            Vec::with_capacity(self.pending_requests.len() + self.completed_responses.len());

        // First, drain all cached responses that have corresponding pending requests
        let completed_responses = std::mem::take(&mut self.completed_responses);
        for (request_id, response) in completed_responses {
            // Always add response to results, and remove from pending if exists
            self.pending_requests.remove(&request_id);
            responses.push(response);
        }

        let timeout_duration = self.timeout;
        let start_time = Instant::now();

        // Then wait for remaining responses
        while !self.pending_requests.is_empty() {
            let remaining_timeout = timeout_duration.saturating_sub(start_time.elapsed());
            let timeout_sleep = tokio::time::sleep(remaining_timeout);

            select! {
                _ = timeout_sleep => {
                    let pending_ids: Vec<RequestId> = self.pending_requests.keys().cloned().collect();
                    return error::RequestTimeoutSnafu {
                        request_ids: pending_ids,
                        timeout: self.timeout,
                    }
                    .fail();
                }
                next_option = self.response_stream.next() => {
                    match next_option {
                        Some(response) => {
                            // Process the first response
                            self.handle_single_response(response?, &mut responses)?;

                            // Drain immediately available responses to avoid false timeouts
                            loop {
                                let drain_timeout = tokio::time::sleep(Duration::from_millis(5));
                                select! {
                                    _ = drain_timeout => break,
                                    next_option = self.response_stream.next() => {
                                        match next_option {
                                            Some(response) => {
                                                self.handle_single_response(response?, &mut responses)?;
                                            }
                                            None => return self.handle_stream_end(responses),
                                        }
                                    }
                                }
                            }
                        }
                        None => return self.handle_stream_end(responses),
                    }
                }
            }
        }

        Ok(responses)
    }

    /// Helper method to handle a single response
    fn handle_single_response(
        &mut self,
        response: DoPutResponse,
        responses: &mut Vec<DoPutResponse>,
    ) -> Result<()> {
        let request_id = response.request_id();
        self.pending_requests.remove(&request_id);
        responses.push(response);
        Ok(())
    }

    /// Helper method to cache a single response
    fn cache_response(&mut self, response: DoPutResponse) -> Result<()> {
        let request_id = response.request_id();
        self.pending_requests.remove(&request_id);
        self.completed_responses.insert(request_id, response);
        Ok(())
    }

    /// Helper method to handle stream end cases
    fn handle_stream_end(&self, responses: Vec<DoPutResponse>) -> Result<Vec<DoPutResponse>> {
        ensure!(self.pending_requests.is_empty(), error::StreamEndedSnafu);
        Ok(responses)
    }

    /// Submit a record batch without waiting for response
    /// Returns the request_id for later tracking
    async fn submit_record_batch(&mut self, batch: RecordBatch) -> Result<RequestId> {
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

            let response_result = timeout(self.timeout, self.response_stream.next()).await;
            match response_result {
                Ok(Some(response)) => {
                    let _schema_response = response?;
                }
                Ok(None) => {}
                Err(_) => {
                    return Err(error::RequestTimeoutSnafu {
                        request_ids: vec![],
                        timeout: self.timeout,
                    }
                    .build());
                }
            }

            self.schema_sent = true;
        }

        // Wait for available slot if we've reached parallelism limit
        while self.pending_requests.len() >= self.parallelism {
            self.process_pending_responses().await?;
        }

        // Send the request
        let request_id = self.next_request_id();
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

        // Track this request but don't wait for response
        self.pending_requests.insert(request_id, Instant::now());

        Ok(request_id)
    }

    /// Check for timed out requests
    fn check_timeouts(&self) -> Result<()> {
        let timeout_duration = self.timeout;
        let now = Instant::now();

        let timed_out_requests: Vec<RequestId> = self
            .pending_requests
            .iter()
            .filter_map(|(&request_id, &sent_time)| {
                if now.duration_since(sent_time) > timeout_duration {
                    Some(request_id)
                } else {
                    None
                }
            })
            .collect();

        if !timed_out_requests.is_empty() {
            return error::RequestTimeoutSnafu {
                request_ids: timed_out_requests,
                timeout: self.timeout,
            }
            .fail();
        }

        Ok(())
    }

    /// Process pending responses to make room for new requests
    async fn process_pending_responses(&mut self) -> Result<()> {
        // First check for any timed out requests
        self.check_timeouts()?;

        // Process responses to make room for new requests
        // First, wait for at least one response (blocking)
        let response_result = timeout(self.timeout, self.response_stream.next()).await;
        match response_result {
            Ok(Some(response)) => {
                let response = response?;
                self.cache_response(response)?;
            }
            Ok(None) => return Ok(()), // Stream ended
            Err(_) => {
                let pending_ids: Vec<RequestId> = self.pending_requests.keys().cloned().collect();
                return Err(error::RequestTimeoutSnafu {
                    request_ids: pending_ids,
                    timeout: self.timeout,
                }
                .build());
            }
        }

        // Then drain any additional responses quickly
        loop {
            let drain_timeout = tokio::time::sleep(Duration::from_millis(1));
            select! {
                _ = drain_timeout => break,
                next_option = self.response_stream.next() => {
                    match next_option {
                        Some(response) => {
                            let response = response?;
                            self.cache_response(response)?;
                        }
                        None => break, // Stream ended
                    }
                }
            }
        }

        Ok(())
    }

    /// Convert rows to Arrow RecordBatch using cached schema
    fn rows_to_record_batch(&self, rows: &[Row]) -> Result<RecordBatch> {
        ensure!(!rows.is_empty(), error::EmptyRowsSnafu);

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
                    build_binary_array!(rows, col_idx, get_binary)
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
                    build_binary_array!(rows, col_idx, get_decimal128)
                }

                // JSON type (stored as binary per Java implementation)
                ColumnDataType::Json => {
                    build_json_array!(rows, col_idx)
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
    pub async fn finish(self) -> Result<()> {
        let _responses = self.finish_with_responses().await?;
        // Discard responses since finish() doesn't return them
        Ok(())
    }

    /// Finish the bulk write operation and return all responses
    pub async fn finish_with_responses(mut self) -> Result<Vec<DoPutResponse>> {
        let mut all_responses = Vec::new();

        // First, collect any already cached responses
        let completed_responses = std::mem::take(&mut self.completed_responses);
        for (request_id, response) in completed_responses {
            // Remove from pending_requests if it exists, but collect the response regardless
            // This handles both normal cases and orphaned responses
            self.pending_requests.remove(&request_id);
            all_responses.push(response);
        }

        // Then wait for any remaining pending requests
        if !self.pending_requests.is_empty() {
            let remaining_responses = self.wait_for_all_pending().await?;
            all_responses.extend(remaining_responses);
        }

        // Close the sender to signal the end of the stream
        self.sender
            .close()
            .await
            .map_err(|_| error::CloseSenderSnafu.build())?;

        Ok(all_responses)
    }

    fn next_request_id(&mut self) -> RequestId {
        // Skip ID 0 as it's reserved for special cases
        self.next_request_id = self.next_request_id.wrapping_add(1);
        if self.next_request_id == 0 {
            self.next_request_id = 1;
        }
        self.next_request_id
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
                data_type: format!("{data_type:?}. Not supported"),
            }
            .fail();
        }
    })
}

// Re-export the proto ColumnDataType for convenience
pub use crate::api::v1::ColumnDataType as ColumnType;
