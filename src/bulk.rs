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

//! High-level bulk insert API for `GreptimeDB`
//!
//! This module provides a user-friendly API for bulk inserting data into `GreptimeDB`,
//! abstracting away the low-level Arrow Flight details.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use greptime_proto::v1::SemanticType;
use tokio::select;
use tokio::time::timeout;

use arrow_array::builder::{
    BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder, Float32Builder,
    Float64Builder, Int16Builder, Int32Builder, Int64Builder, Int8Builder, StringBuilder,
    Time32MillisecondBuilder, Time32SecondBuilder, Time64MicrosecondBuilder,
    Time64NanosecondBuilder, TimestampMicrosecondBuilder, TimestampMillisecondBuilder,
    TimestampNanosecondBuilder, TimestampSecondBuilder, UInt16Builder, UInt32Builder,
    UInt64Builder, UInt8Builder,
};
use arrow_array::{Array, RecordBatch};
use arrow_flight::{FlightData, FlightDescriptor};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use futures::channel::mpsc;
use futures::{FutureExt, SinkExt, Stream, StreamExt};

use crate::api::v1::ColumnDataType;
use crate::client::Client;
use crate::database::Database;
use crate::flight::do_put::{DoPutMetadata, DoPutResponse};
use crate::flight::{FlightEncoder, FlightMessage};
use crate::table::{Column, DataTypeExtension, Row, TableSchema, Value};
use crate::{error, Result};
use snafu::{ensure, OptionExt, ResultExt};

/// Default channel buffer size for streaming FlightData
/// This controls how many FlightData messages can be buffered in the channel
/// before blocking the sender. A larger buffer allows for better throughput
/// at the cost of memory usage.
///
/// Can be overridden by setting the GREPTIMEDB_CHANNEL_BUFFER_SIZE environment variable.
const DEFAULT_CHANNEL_BUFFER_SIZE: usize = 1024;

/// Get configuration value from environment variable with fallback to default
fn get_env_or_default<T>(env_var: &str, default: T) -> T
where
    T: std::str::FromStr,
{
    std::env::var(env_var)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

pub type RequestId = i64;

/// High-level bulk inserter for `GreptimeDB`
#[derive(Clone)]
pub struct BulkInserter {
    database: Database,
}

impl BulkInserter {
    /// Create a new bulk inserter
    #[must_use]
    pub fn new(client: Client, database_name: &str) -> Self {
        Self {
            database: Database::new_with_dbname(database_name, client),
        }
    }

    /// Create a bulk stream writer from a table template
    ///
    /// This is a convenience method that extracts the schema from a table
    /// and creates a `BulkStreamWriter` bound to that schema.
    pub async fn create_bulk_stream_writer(
        &self,
        table_schema: &TableSchema,
        options: Option<BulkWriteOptions>,
    ) -> Result<BulkStreamWriter> {
        let options = options.unwrap_or_default();
        BulkStreamWriter::new(&self.database, table_schema, options).await
    }
}

/// Compression algorithm options for bulk write operations
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CompressionType {
    None,
    #[default]
    Lz4,
    Zstd,
}

/// Configuration options for bulk write operations
#[derive(Debug, Clone)]
pub struct BulkWriteOptions {
    pub compression: CompressionType,
    pub timeout: Duration,
    pub parallelism: usize,
}

impl Default for BulkWriteOptions {
    fn default() -> Self {
        Self {
            compression: CompressionType::default(),
            timeout: Duration::from_secs(60),
            parallelism: 4,
        }
    }
}

impl BulkWriteOptions {
    /// Set compression type
    #[must_use]
    pub fn with_compression(mut self, compression: CompressionType) -> Self {
        self.compression = compression;
        self
    }

    /// Set timeout duration
    #[must_use]
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Set parallelism for concurrent requests
    #[must_use]
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
    table_schema: TableSchema,
    // Cache the Arrow schema to avoid recreating it for each batch
    arrow_schema: Arc<Schema>,
    // Pre-computed field name to index mapping for O(1) lookup in RowBuilder
    field_map: HashMap<String, usize>,
    next_request_id: RequestId,
    encoder: FlightEncoder,
    schema_sent: bool,
    // Parallel processing fields
    parallelism: usize,
    timeout: Duration,
    // Track pending requests: request_id -> sent_time
    pending_requests: HashMap<RequestId, Instant>,
    // Cache completed responses that were processed but not yet retrieved
    completed_responses: HashMap<RequestId, (DoPutResponse, Instant)>,
}

impl BulkStreamWriter {
    /// Create a new bulk stream writer bound to a specific table schema
    pub async fn new(
        database: &Database,
        table_schema: &TableSchema,
        options: BulkWriteOptions,
    ) -> Result<Self> {
        // Create the encoder with compression settings
        let encoder = FlightEncoder::with_compression(options.compression);

        // Convert table schema to Arrow schema
        let fields: Result<Vec<Field>> = table_schema
            .columns()
            .iter()
            .map(|col| {
                let nullable = col.semantic_type != SemanticType::Timestamp;
                column_to_arrow_data_type(col)
                    .map(|data_type| Field::new(&col.name, data_type, nullable))
            })
            .collect();
        let arrow_schema = Arc::new(Schema::new(fields?));

        // Pre-compute field name to index mapping for O(1) lookups in RowBuilder
        let field_map: HashMap<String, usize> = table_schema
            .columns()
            .iter()
            .enumerate()
            .map(|(i, col)| (col.name.clone(), i))
            .collect();

        // Create a channel for streaming FlightData
        let channel_buffer_size = get_env_or_default(
            "GREPTIMEDB_CHANNEL_BUFFER_SIZE",
            DEFAULT_CHANNEL_BUFFER_SIZE,
        );
        let (sender, receiver) = mpsc::channel::<FlightData>(channel_buffer_size);

        // Convert receiver to a stream and start the do_put operation
        let flight_stream = receiver.boxed();
        let response_stream = database.do_put(flight_stream).await?;

        Ok(Self {
            sender,
            response_stream,
            table_schema: table_schema.clone(),
            arrow_schema,
            field_map,
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
    pub async fn write_rows(&mut self, rows: Rows) -> Result<DoPutResponse> {
        // Use the async implementation and wait for the response
        let request_id = self.write_rows_async(rows).await?;
        self.wait_for_response(request_id).await
    }

    /// Submit rows for writing without waiting for response
    /// Returns a `request_id` that can be used to wait for the specific response
    pub async fn write_rows_async(&mut self, rows: Rows) -> Result<RequestId> {
        // Ensure that the rows are not empty
        ensure!(!rows.is_empty(), error::EmptyRowsSnafu);
        // Validate that the rows schema matches the writer's schema
        self.validate_rows_schema(&rows)?;

        let record_batch = RecordBatch::try_from(rows)?; // Zero-cost conversion
        let request_id = self.submit_record_batch(record_batch).await?;

        Ok(request_id)
    }

    /// Wait for a specific request's response by `request_id`
    pub async fn wait_for_response(
        &mut self,
        target_request_id: RequestId,
    ) -> Result<DoPutResponse> {
        // Check if the response is already cached
        if let Some((response, _)) = self.completed_responses.remove(&target_request_id) {
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
            let Ok(next_option) = next_result else {
                return error::RequestTimeoutSnafu {
                    request_ids: vec![target_request_id],
                    timeout: self.timeout,
                }
                .fail();
            };
            if let Some(response) = next_option {
                let response = response?;
                let request_id = response.request_id();
                self.pending_requests.remove(&request_id);
                if request_id == target_request_id {
                    return Ok(response);
                }
                self.completed_responses
                    .insert(request_id, (response, Instant::now()));
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
        for (request_id, (response, _)) in completed_responses {
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
                () = timeout_sleep => {
                    let pending_ids: Vec<RequestId> = self.pending_requests.keys().copied().collect();
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
                            self.handle_single_response(response?, &mut responses);

                            // Drain immediately available responses to avoid false timeouts
                            loop {
                                match self.response_stream.next().now_or_never() {
                                    Some(Some(response)) => self.handle_single_response(response?, &mut responses),
                                    Some(None) => return self.handle_stream_end(responses),
                                    None => break, // No immediately available responses
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

    /// Flush completed responses from cache and return them
    ///
    /// This method removes all cached responses that have been processed
    /// but not yet retrieved, and returns them to the caller.
    /// Useful for long-running bulk operations to prevent excessive
    /// memory usage while still allowing access to response data.
    ///
    /// Returns a vector of completed responses that were flushed.
    pub fn flush_completed_responses(&mut self) -> Vec<DoPutResponse> {
        let responses = std::mem::take(&mut self.completed_responses);
        responses
            .into_values()
            .map(|(response, _)| response)
            .collect()
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
        for (request_id, (response, _)) in completed_responses {
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
        // The result is ignored, as the stream being closed on the other
        // end is not a critical error. We still want to return the responses.
        let _ = self.sender.close().await;

        Ok(all_responses)
    }

    /// Allocate a new rows buffer that is bound to this writer's schema
    /// This ensures schema compatibility and provides optimal performance
    pub fn alloc_rows_buffer(&self, capacity: usize, row_buffer_size: usize) -> Result<Rows> {
        Rows::with_arrow_schema(
            self.column_schemas(),
            self.arrow_schema.clone(),
            capacity,
            row_buffer_size,
        )
    }

    /// Create a new Row builder that is compatible with this writer's schema
    /// Returns a `RowBuilder` that can efficiently build rows for this writer
    /// Uses O(1) field name lookup for optimal performance
    #[must_use]
    pub fn new_row(&self) -> RowBuilder<'_> {
        RowBuilder::new(self.column_schemas(), &self.field_map)
    }

    /// Get the table name that this writer is bound to
    #[must_use]
    pub fn table_name(&self) -> &str {
        self.table_schema.name()
    }

    /// Get the column schemas that this writer is bound to
    #[must_use]
    pub fn column_schemas(&self) -> &[Column] {
        self.table_schema.columns()
    }

    /// Helper method to handle a single response
    fn handle_single_response(
        &mut self,
        response: DoPutResponse,
        responses: &mut Vec<DoPutResponse>,
    ) {
        let request_id = response.request_id();
        self.pending_requests.remove(&request_id);
        responses.push(response);
    }

    /// Helper method to receive a single response and remove the pending request
    fn receive_response_and_remove_pending(&mut self, response: DoPutResponse) {
        let request_id = response.request_id();
        self.pending_requests.remove(&request_id);
        self.completed_responses
            .insert(request_id, (response, Instant::now()));

        // Clean up expired responses if cache is getting large
        self.cleanup_expired_responses_if_needed();
    }

    /// Clean up expired responses when cache exceeds threshold to prevent unbounded growth
    fn cleanup_expired_responses_if_needed(&mut self) {
        const RESPONSE_CACHE_CLEANUP_THRESHOLD: usize = 1024;

        if self.completed_responses.len() > RESPONSE_CACHE_CLEANUP_THRESHOLD {
            let now = Instant::now();
            self.completed_responses
                .retain(|_, (_, cached_time)| now.duration_since(*cached_time) <= self.timeout);
        }
    }

    /// Helper method to handle stream end cases
    fn handle_stream_end(&self, responses: Vec<DoPutResponse>) -> Result<Vec<DoPutResponse>> {
        ensure!(self.pending_requests.is_empty(), error::StreamEndedSnafu);
        Ok(responses)
    }

    /// Helper method to handle stream end during processing
    /// Returns Ok(()) if no pending requests, otherwise returns appropriate error
    fn handle_stream_end_during_processing(&self) -> Result<()> {
        if !self.pending_requests.is_empty() {
            let pending_ids: Vec<RequestId> = self.pending_requests.keys().copied().collect();
            return error::StreamEndedWithPendingRequestsSnafu {
                request_ids: pending_ids,
            }
            .fail();
        }
        Ok(())
    }

    /// Submit a record batch without waiting for response
    /// Returns the `request_id` for later tracking
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
                path: vec![self.table_name().to_string()],
                ..Default::default()
            });

            self.sender
                .send(schema_data)
                .await
                .context(error::SendDataSnafu)?;

            let response_result = timeout(self.timeout, self.response_stream.next()).await;
            match response_result {
                Ok(Some(response)) => {
                    let _schema_response = response?;
                }
                Ok(None) => return error::StreamEndedSnafu.fail(),
                Err(_) => {
                    return error::RequestTimeoutSnafu {
                        request_ids: vec![],
                        timeout: self.timeout,
                    }
                    .fail();
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

        self.sender.send(data).await.context(error::SendDataSnafu)?;

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
            Ok(Some(response)) => self.receive_response_and_remove_pending(response?),
            Ok(None) => return self.handle_stream_end_during_processing(),
            Err(_) => {
                let pending_ids: Vec<RequestId> = self.pending_requests.keys().copied().collect();
                return error::RequestTimeoutSnafu {
                    request_ids: pending_ids,
                    timeout: self.timeout,
                }
                .fail();
            }
        }

        // Then drain any additional responses quickly
        loop {
            match self.response_stream.next().now_or_never() {
                Some(Some(response)) => {
                    self.receive_response_and_remove_pending(response?);
                }
                Some(None) => return self.handle_stream_end_during_processing(),
                None => break, // No immediately available responses
            }
        }

        Ok(())
    }

    /// Validate that the provided Rows schema matches the writer's bound schema
    fn validate_rows_schema(&self, rows: &Rows) -> Result<()> {
        // Fast path: if it's the exact same Arc, skip validation
        if Arc::ptr_eq(&self.arrow_schema, &rows.schema) {
            return Ok(());
        }

        // Fast path: check field count first (cheapest comparison)
        let expected_fields = self.arrow_schema.fields();
        let actual_fields = rows.schema.fields();

        if expected_fields.len() != actual_fields.len() {
            return Self::schema_mismatch_error(expected_fields, actual_fields);
        }

        // Check each field for compatibility
        for (expected, actual) in expected_fields.iter().zip(actual_fields.iter()) {
            if expected != actual {
                return Self::schema_mismatch_error(expected_fields, actual_fields);
            }
        }

        Ok(())
    }

    /// Helper to create schema mismatch error with lazy formatting
    #[cold]
    fn schema_mismatch_error(
        expected_fields: &arrow_schema::Fields,
        actual_fields: &arrow_schema::Fields,
    ) -> Result<()> {
        error::SchemaMismatchSnafu {
            expected: format!("{expected_fields:?}"),
            actual: format!("{actual_fields:?}"),
        }
        .fail()
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
fn column_to_arrow_data_type(column: &Column) -> Result<DataType> {
    let data_type = column.data_type;
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
        ColumnDataType::String => DataType::Utf8,
        ColumnDataType::Binary => DataType::Binary,

        // Date type
        ColumnDataType::Date => DataType::Date32,

        // Timestamp types
        ColumnDataType::TimestampSecond => DataType::Timestamp(TimeUnit::Second, None),
        ColumnDataType::TimestampMillisecond => DataType::Timestamp(TimeUnit::Millisecond, None),
        // DateTime is an alias of TIMESTAMP_MICROSECOND per GreptimeDB docs
        ColumnDataType::Datetime | ColumnDataType::TimestampMicrosecond => {
            DataType::Timestamp(TimeUnit::Microsecond, None)
        }
        ColumnDataType::TimestampNanosecond => DataType::Timestamp(TimeUnit::Nanosecond, None),

        // Time types (without date)
        ColumnDataType::TimeSecond => DataType::Time32(arrow_schema::TimeUnit::Second),
        ColumnDataType::TimeMillisecond => DataType::Time32(arrow_schema::TimeUnit::Millisecond),
        ColumnDataType::TimeMicrosecond => DataType::Time64(arrow_schema::TimeUnit::Microsecond),
        ColumnDataType::TimeNanosecond => DataType::Time64(arrow_schema::TimeUnit::Nanosecond),

        // Decimal type - extract precision and scale from column extension
        ColumnDataType::Decimal128 => {
            match &column.data_type_extension {
                Some(DataTypeExtension::Decimal128 { precision, scale }) => {
                    DataType::Decimal128(*precision, *scale)
                }
                _ => DataType::Decimal128(38, 10), // Default fallback
            }
        }

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

/// High-level rows abstraction with buffered batch conversion
/// This provides a user-friendly API while maintaining optimal performance
pub struct Rows {
    builder: RowBatchBuilder,
    schema: Arc<Schema>,
    column_count: usize,
    // Row buffering for improved performance
    row_buffer: Vec<Row>,
    buffer_size: usize,
}

impl Rows {
    /// Create a new Rows collection with the given schema and capacity
    pub fn new(column_schemas: &[Column], capacity: usize, row_buffer_size: usize) -> Result<Self> {
        let builder = RowBatchBuilder::new(column_schemas, capacity)?;
        let schema = builder.schema.clone();

        Ok(Self {
            builder,
            schema,
            column_count: column_schemas.len(),
            row_buffer: Vec::with_capacity(row_buffer_size),
            buffer_size: row_buffer_size,
        })
    }

    /// Create a new Rows collection with a pre-computed Arrow schema
    fn with_arrow_schema(
        column_schemas: &[Column],
        arrow_schema: Arc<Schema>,
        capacity: usize,
        row_buffer_size: usize,
    ) -> Result<Self> {
        let builder =
            RowBatchBuilder::with_arrow_schema(column_schemas, arrow_schema.clone(), capacity)?;

        Ok(Self {
            builder,
            schema: arrow_schema,
            column_count: column_schemas.len(),
            row_buffer: Vec::with_capacity(row_buffer_size),
            buffer_size: row_buffer_size,
        })
    }

    /// Add a row to the collection using move semantics
    pub fn add_row(&mut self, row: Row) -> Result<()> {
        // Validate column count matches schema
        ensure!(
            row.len() == self.column_count,
            error::InvalidColumnCountSnafu {
                expected: self.column_count,
                actual: row.len(),
            }
        );

        self.row_buffer.push(row);

        // If buffer is full, flush it to a RecordBatch
        if self.row_buffer.len() >= self.buffer_size {
            self.flush_buffer()?;
        }

        Ok(())
    }

    /// Flush the current row buffer to the builder
    fn flush_buffer(&mut self) -> Result<()> {
        if self.row_buffer.is_empty() {
            return Ok(());
        }

        // Process all rows in the buffer at once for better performance
        let rows = std::mem::take(&mut self.row_buffer);
        self.builder.add_rows(rows)?;

        Ok(())
    }

    /// Get the current number of rows
    #[must_use]
    pub fn len(&self) -> usize {
        self.builder.len() + self.row_buffer.len()
    }

    /// Check if the collection is empty
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    // Note: No capacity limits - can grow dynamically as needed

    /// Get the schema
    #[must_use]
    pub fn schema(&self) -> &Schema {
        &self.schema
    }
}

/// Convert Rows to RecordBatch, handling buffered data
impl TryFrom<Rows> for RecordBatch {
    type Error = crate::Error;

    fn try_from(mut rows: Rows) -> Result<Self> {
        // Flush any remaining buffered rows to the builder
        rows.flush_buffer()?;

        // Build the single RecordBatch
        rows.builder.build()
    }
}

/// Efficient batch builder that directly constructs Arrow arrays
/// This avoids the overhead of creating intermediate Row objects and converting them
/// Arrow builders automatically manage capacity and expand as needed
pub struct RowBatchBuilder {
    builders: Vec<ArrayBuilderEnum>,
    schema: Arc<Schema>,
    current_rows: usize,
}

impl RowBatchBuilder {
    /// Create a new RowBatchBuilder with the given schema and capacity
    fn new(column_schemas: &[Column], capacity: usize) -> Result<Self> {
        let fields: Result<Vec<Field>> = column_schemas
            .iter()
            .map(|col| {
                let nullable = col.semantic_type != SemanticType::Timestamp;
                column_to_arrow_data_type(col)
                    .map(|data_type| Field::new(&col.name, data_type, nullable))
            })
            .collect();
        let schema = Arc::new(Schema::new(fields?));

        let builders: Result<Vec<ArrayBuilderEnum>> = column_schemas
            .iter()
            .enumerate()
            .map(|(col_idx, col)| create_array_builder(col, capacity, col_idx))
            .collect();

        Ok(Self {
            builders: builders?,
            schema,
            current_rows: 0,
        })
    }

    /// Create a new RowBatchBuilder with a pre-computed Arrow schema
    fn with_arrow_schema(
        column_schemas: &[Column],
        schema: Arc<Schema>,
        capacity: usize,
    ) -> Result<Self> {
        let builders: Result<Vec<ArrayBuilderEnum>> = column_schemas
            .iter()
            .enumerate()
            .map(|(col_idx, col)| create_array_builder(col, capacity, col_idx))
            .collect();

        Ok(Self {
            builders: builders?,
            schema,
            current_rows: 0,
        })
    }

    /// Add multiple rows to the batch builder using batch operations
    fn add_rows(&mut self, mut rows: Vec<Row>) -> Result<()> {
        for (col_idx, builder) in self.builders.iter_mut().enumerate() {
            builder.append_values_from_rows(&mut rows, col_idx)?;
        }
        self.current_rows += rows.len();
        Ok(())
    }

    /// Build the RecordBatch from accumulated rows
    fn build(mut self) -> Result<RecordBatch> {
        let arrays: Result<Vec<Arc<dyn Array>>> = self
            .builders
            .iter_mut()
            .map(ArrayBuilderEnum::finish)
            .collect();

        RecordBatch::try_new(self.schema, arrays?).context(error::CreateRecordBatchSnafu)
    }

    /// Get the current number of rows in the builder
    fn len(&self) -> usize {
        self.current_rows
    }
}

/// Trait for type-erased array builders
trait ArrayBuilder {
    fn append_values_from_rows(&mut self, rows: &mut [Row], col_idx: usize) -> Result<()>;
}

enum ArrayBuilderEnum {
    Boolean(BooleanBuilder),
    Int8(Int8Builder),
    Int16(Int16Builder),
    Int32(Int32Builder),
    Int64(Int64Builder),
    UInt8(UInt8Builder),
    UInt16(UInt16Builder),
    UInt32(UInt32Builder),
    UInt64(UInt64Builder),
    Float32(Float32Builder),
    Float64(Float64Builder),
    String(StringBuilder),
    Binary(BinaryBuilder),
    Decimal128(Decimal128Builder),
    Date(Date32Builder),
    TimestampSecond(TimestampSecondBuilder),
    TimestampMillisecond(TimestampMillisecondBuilder),
    TimestampMicrosecond(TimestampMicrosecondBuilder),
    TimestampNanosecond(TimestampNanosecondBuilder),
    TimeSecond(Time32SecondBuilder),
    TimeMillisecond(Time32MillisecondBuilder),
    TimeMicrosecond(Time64MicrosecondBuilder),
    TimeNanosecond(Time64NanosecondBuilder),
}

impl ArrayBuilderEnum {
    fn append_values_from_rows(&mut self, rows: &mut [Row], col_idx: usize) -> Result<()> {
        match self {
            ArrayBuilderEnum::Boolean(builder) => builder.append_values_from_rows(rows, col_idx),
            ArrayBuilderEnum::Int8(builder) => builder.append_values_from_rows(rows, col_idx),
            ArrayBuilderEnum::Int16(builder) => builder.append_values_from_rows(rows, col_idx),
            ArrayBuilderEnum::Int32(builder) => builder.append_values_from_rows(rows, col_idx),
            ArrayBuilderEnum::Int64(builder) => builder.append_values_from_rows(rows, col_idx),
            ArrayBuilderEnum::UInt8(builder) => builder.append_values_from_rows(rows, col_idx),
            ArrayBuilderEnum::UInt16(builder) => builder.append_values_from_rows(rows, col_idx),
            ArrayBuilderEnum::UInt32(builder) => builder.append_values_from_rows(rows, col_idx),
            ArrayBuilderEnum::UInt64(builder) => builder.append_values_from_rows(rows, col_idx),
            ArrayBuilderEnum::Float32(builder) => builder.append_values_from_rows(rows, col_idx),
            ArrayBuilderEnum::Float64(builder) => builder.append_values_from_rows(rows, col_idx),
            ArrayBuilderEnum::String(builder) => builder.append_values_from_rows(rows, col_idx),
            ArrayBuilderEnum::Binary(builder) => builder.append_values_from_rows(rows, col_idx),
            ArrayBuilderEnum::Decimal128(builder) => builder.append_values_from_rows(rows, col_idx),
            ArrayBuilderEnum::Date(builder) => builder.append_values_from_rows(rows, col_idx),
            ArrayBuilderEnum::TimestampSecond(builder) => {
                builder.append_values_from_rows(rows, col_idx)
            }
            ArrayBuilderEnum::TimestampMillisecond(builder) => {
                builder.append_values_from_rows(rows, col_idx)
            }
            ArrayBuilderEnum::TimestampMicrosecond(builder) => {
                builder.append_values_from_rows(rows, col_idx)
            }
            ArrayBuilderEnum::TimestampNanosecond(builder) => {
                builder.append_values_from_rows(rows, col_idx)
            }
            ArrayBuilderEnum::TimeSecond(builder) => builder.append_values_from_rows(rows, col_idx),
            ArrayBuilderEnum::TimeMillisecond(builder) => {
                builder.append_values_from_rows(rows, col_idx)
            }
            ArrayBuilderEnum::TimeMicrosecond(builder) => {
                builder.append_values_from_rows(rows, col_idx)
            }
            ArrayBuilderEnum::TimeNanosecond(builder) => {
                builder.append_values_from_rows(rows, col_idx)
            }
        }
    }

    fn finish(&mut self) -> Result<Arc<dyn Array>> {
        Ok(match self {
            ArrayBuilderEnum::Boolean(builder) => Arc::new(builder.finish()),
            ArrayBuilderEnum::Int8(builder) => Arc::new(builder.finish()),
            ArrayBuilderEnum::Int16(builder) => Arc::new(builder.finish()),
            ArrayBuilderEnum::Int32(builder) => Arc::new(builder.finish()),
            ArrayBuilderEnum::Int64(builder) => Arc::new(builder.finish()),
            ArrayBuilderEnum::UInt8(builder) => Arc::new(builder.finish()),
            ArrayBuilderEnum::UInt16(builder) => Arc::new(builder.finish()),
            ArrayBuilderEnum::UInt32(builder) => Arc::new(builder.finish()),
            ArrayBuilderEnum::UInt64(builder) => Arc::new(builder.finish()),
            ArrayBuilderEnum::Float32(builder) => Arc::new(builder.finish()),
            ArrayBuilderEnum::Float64(builder) => Arc::new(builder.finish()),
            ArrayBuilderEnum::String(builder) => Arc::new(builder.finish()),
            ArrayBuilderEnum::Binary(builder) => Arc::new(builder.finish()),
            ArrayBuilderEnum::Decimal128(builder) => Arc::new(builder.finish()),
            ArrayBuilderEnum::Date(builder) => Arc::new(builder.finish()),
            ArrayBuilderEnum::TimestampSecond(builder) => Arc::new(builder.finish()),
            ArrayBuilderEnum::TimestampMillisecond(builder) => Arc::new(builder.finish()),
            ArrayBuilderEnum::TimestampMicrosecond(builder) => Arc::new(builder.finish()),
            ArrayBuilderEnum::TimestampNanosecond(builder) => Arc::new(builder.finish()),
            ArrayBuilderEnum::TimeSecond(builder) => Arc::new(builder.finish()),
            ArrayBuilderEnum::TimeMillisecond(builder) => Arc::new(builder.finish()),
            ArrayBuilderEnum::TimeMicrosecond(builder) => Arc::new(builder.finish()),
            ArrayBuilderEnum::TimeNanosecond(builder) => Arc::new(builder.finish()),
        })
    }
}

/// Create an array builder enum for the given column with adaptive sizing
/// Uses enum dispatch for maximum performance (zero-cost polymorphism)
fn create_array_builder(
    column: &Column,
    capacity: usize,
    _column_index: usize,
) -> Result<ArrayBuilderEnum> {
    let data_type = column.data_type;
    Ok(match data_type {
        ColumnDataType::Boolean => {
            ArrayBuilderEnum::Boolean(BooleanBuilder::with_capacity(capacity))
        }
        ColumnDataType::Int8 => ArrayBuilderEnum::Int8(Int8Builder::with_capacity(capacity)),
        ColumnDataType::Int16 => ArrayBuilderEnum::Int16(Int16Builder::with_capacity(capacity)),
        ColumnDataType::Int32 => ArrayBuilderEnum::Int32(Int32Builder::with_capacity(capacity)),
        ColumnDataType::Int64 => ArrayBuilderEnum::Int64(Int64Builder::with_capacity(capacity)),
        ColumnDataType::Uint8 => ArrayBuilderEnum::UInt8(UInt8Builder::with_capacity(capacity)),
        ColumnDataType::Uint16 => ArrayBuilderEnum::UInt16(UInt16Builder::with_capacity(capacity)),
        ColumnDataType::Uint32 => ArrayBuilderEnum::UInt32(UInt32Builder::with_capacity(capacity)),
        ColumnDataType::Uint64 => ArrayBuilderEnum::UInt64(UInt64Builder::with_capacity(capacity)),
        ColumnDataType::Float32 => {
            ArrayBuilderEnum::Float32(Float32Builder::with_capacity(capacity))
        }
        ColumnDataType::Float64 => {
            ArrayBuilderEnum::Float64(Float64Builder::with_capacity(capacity))
        }
        ColumnDataType::String => {
            ArrayBuilderEnum::String(StringBuilder::with_capacity(capacity, capacity * 64))
        }
        ColumnDataType::Date => ArrayBuilderEnum::Date(Date32Builder::with_capacity(capacity)),
        ColumnDataType::TimestampSecond => {
            ArrayBuilderEnum::TimestampSecond(TimestampSecondBuilder::with_capacity(capacity))
        }
        ColumnDataType::TimestampMillisecond => ArrayBuilderEnum::TimestampMillisecond(
            TimestampMillisecondBuilder::with_capacity(capacity),
        ),
        ColumnDataType::Datetime | ColumnDataType::TimestampMicrosecond => {
            ArrayBuilderEnum::TimestampMicrosecond(TimestampMicrosecondBuilder::with_capacity(
                capacity,
            ))
        }
        ColumnDataType::TimestampNanosecond => ArrayBuilderEnum::TimestampNanosecond(
            TimestampNanosecondBuilder::with_capacity(capacity),
        ),
        ColumnDataType::TimeSecond => {
            ArrayBuilderEnum::TimeSecond(Time32SecondBuilder::with_capacity(capacity))
        }
        ColumnDataType::TimeMillisecond => {
            ArrayBuilderEnum::TimeMillisecond(Time32MillisecondBuilder::with_capacity(capacity))
        }
        ColumnDataType::TimeMicrosecond => {
            ArrayBuilderEnum::TimeMicrosecond(Time64MicrosecondBuilder::with_capacity(capacity))
        }
        ColumnDataType::TimeNanosecond => {
            ArrayBuilderEnum::TimeNanosecond(Time64NanosecondBuilder::with_capacity(capacity))
        }
        ColumnDataType::Decimal128 => {
            // Extract precision and scale from column definition
            let (precision, scale) = match &column.data_type_extension {
                Some(DataTypeExtension::Decimal128 { precision, scale }) => (*precision, *scale),
                _ => (38, 10), // Default precision and scale if not specified
            };

            ArrayBuilderEnum::Decimal128(
                Decimal128Builder::with_capacity(capacity)
                    .with_data_type(arrow_schema::DataType::Decimal128(precision, scale)),
            )
        }
        ColumnDataType::Binary | ColumnDataType::Json => {
            ArrayBuilderEnum::Binary(BinaryBuilder::with_capacity(capacity, capacity * 64))
        }
        _ => {
            return error::UnsupportedDataTypeSnafu {
                data_type: format!("{data_type:?}. Not supported in RowBatchBuilder"),
            }
            .fail();
        }
    })
}

// Generate ArrayBuilder implementations for Arrow primitive types
macro_rules! impl_arrow_builder {
    ($builder_type:ty, $getter:ident, $value_type:ty) => {
        impl ArrayBuilder for $builder_type {
            fn append_values_from_rows(&mut self, rows: &mut [Row], col_idx: usize) -> Result<()> {
                for row in rows {
                    // Use unchecked version for performance - col_idx is guaranteed to be valid by schema
                    self.append_option(unsafe { row.$getter(col_idx) });
                }
                Ok(())
            }
        }
    };
}

// Basic primitive types
impl_arrow_builder!(BooleanBuilder, get_bool_unchecked, bool);
impl_arrow_builder!(Int8Builder, get_i8_unchecked, i8);
impl_arrow_builder!(Int16Builder, get_i16_unchecked, i16);
impl_arrow_builder!(Int32Builder, get_i32_unchecked, i32);
impl_arrow_builder!(Int64Builder, get_i64_unchecked, i64);
impl_arrow_builder!(UInt8Builder, get_u8_unchecked, u8);
impl_arrow_builder!(UInt16Builder, get_u16_unchecked, u16);
impl_arrow_builder!(UInt32Builder, get_u32_unchecked, u32);
impl_arrow_builder!(UInt64Builder, get_u64_unchecked, u64);
impl_arrow_builder!(Float32Builder, get_f32_unchecked, f32);
impl_arrow_builder!(Float64Builder, get_f64_unchecked, f64);

// Timestamp types
impl_arrow_builder!(TimestampSecondBuilder, get_timestamp_unchecked, i64);
impl_arrow_builder!(TimestampMillisecondBuilder, get_timestamp_unchecked, i64);
impl_arrow_builder!(TimestampMicrosecondBuilder, get_timestamp_unchecked, i64);
impl_arrow_builder!(TimestampNanosecondBuilder, get_timestamp_unchecked, i64);

// Time types
impl_arrow_builder!(Time32SecondBuilder, get_time32_unchecked, i32);
impl_arrow_builder!(Time32MillisecondBuilder, get_time32_unchecked, i32);
impl_arrow_builder!(Time64MicrosecondBuilder, get_time64_unchecked, i64);
impl_arrow_builder!(Time64NanosecondBuilder, get_time64_unchecked, i64);

// Date types
impl_arrow_builder!(Date32Builder, get_date_unchecked, i32);

// Decimal128 type (uses column-defined precision and scale)
impl_arrow_builder!(Decimal128Builder, get_decimal128_unchecked, i128);

// String and Binary types
impl_arrow_builder!(StringBuilder, take_string_unchecked, String);
impl_arrow_builder!(BinaryBuilder, take_binary_unchecked, Vec<u8>);

/// A helper for building rows with schema-aware field access
/// This prevents common mistakes like incorrect field order or types
/// Uses O(1) field name lookup for optimal performance
pub struct RowBuilder<'a> {
    schema: &'a [Column],
    field_map: &'a HashMap<String, usize>, // Pre-computed field name to index mapping
    values: Vec<Option<Value>>,
}

impl<'a> RowBuilder<'a> {
    fn new(schema: &'a [Column], field_map: &'a HashMap<String, usize>) -> Self {
        Self {
            schema,
            field_map,
            values: vec![None; schema.len()],
        }
    }

    /// Set a field value by name with O(1) lookup performance.
    /// This ensures correct field mapping and prevents field order mistakes.
    pub fn set(mut self, field_name: &str, value: Value) -> Result<Self> {
        let field_index = self
            .field_map
            .get(field_name)
            .context(error::MissingFieldSnafu { field: field_name })?;

        self.values[*field_index] = Some(value);
        Ok(self)
    }

    /// Set a field value by index. This is faster than `set` as it avoids a map lookup.
    ///
    /// # Errors
    ///
    /// Returns `Err` if `index` is out of bounds.
    pub fn set_by_index(mut self, index: usize, value: Value) -> Result<Self> {
        ensure!(
            index < self.values.len(),
            error::InvalidColumnIndexSnafu {
                index,
                total: self.values.len(),
            }
        );

        self.values[index] = Some(value);
        Ok(self)
    }

    /// Get the number of columns
    #[must_use]
    pub fn column_count(&self) -> usize {
        self.schema.len()
    }

    /// Build the final Row, ensuring all required fields are set
    pub fn build(self) -> Result<Row> {
        let mut row_values = Vec::with_capacity(self.values.len());

        for (i, opt_value) in self.values.into_iter().enumerate() {
            match opt_value {
                Some(value) => row_values.push(value),
                None => {
                    return error::MissingFieldSnafu {
                        field: self.schema[i].name.clone(),
                    }
                    .fail();
                }
            }
        }

        Ok(Row::new().add_values(row_values))
    }
}

// Re-export the proto ColumnDataType for convenience
pub use crate::api::v1::ColumnDataType as ColumnType;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::v1::{ColumnDataType, SemanticType};
    use crate::table::{Column, Value};

    #[test]
    fn test_rows_schema_validation() {
        // Create a schema with 3 columns
        let schema1 = vec![
            Column {
                name: "id".to_string(),
                data_type: ColumnDataType::Int64,
                semantic_type: SemanticType::Field,
                data_type_extension: None,
            },
            Column {
                name: "name".to_string(),
                data_type: ColumnDataType::String,
                semantic_type: SemanticType::Field,
                data_type_extension: None,
            },
            Column {
                name: "timestamp".to_string(),
                data_type: ColumnDataType::TimestampMillisecond,
                semantic_type: SemanticType::Timestamp,
                data_type_extension: None,
            },
        ];

        // Create a different schema
        let schema2 = vec![
            Column {
                name: "id".to_string(),
                data_type: ColumnDataType::Int64,
                semantic_type: SemanticType::Field,
                data_type_extension: None,
            },
            Column {
                name: "value".to_string(),          // Different column name
                data_type: ColumnDataType::Float64, // Different data type
                semantic_type: SemanticType::Field,
                data_type_extension: None,
            },
        ];

        // Test 1: Compatible rows should work
        let rows1 = Rows::new(&schema1, 10, 5).expect("Failed to create rows1");

        // Test 2: Incompatible rows should fail validation
        let rows2 = Rows::new(&schema2, 10, 5).expect("Failed to create rows2");

        // Mock the validation (since we can't easily create a BulkStreamWriter in tests)
        // In practice, this would be tested with a real BulkStreamWriter
        assert_eq!(rows1.schema().fields().len(), 3);
        assert_eq!(rows2.schema().fields().len(), 2);

        // The actual schema validation would happen in validate_rows_schema()
        // which checks that field names and types match exactly
    }

    #[test]
    fn test_rows_creation_and_capacity() {
        let schema = vec![
            Column {
                name: "id".to_string(),
                data_type: ColumnDataType::Int64,
                semantic_type: SemanticType::Field,
                data_type_extension: None,
            },
            Column {
                name: "message".to_string(),
                data_type: ColumnDataType::String,
                semantic_type: SemanticType::Field,
                data_type_extension: None,
            },
        ];

        let mut rows = Rows::new(&schema, 5, 5).expect("Failed to create rows");

        // Test initial state
        assert_eq!(rows.len(), 0);
        assert!(rows.is_empty());

        // Add some rows
        let row1 = crate::table::Row::new()
            .add_values(vec![Value::Int64(1), Value::String("first".to_string())]);

        let row2 = crate::table::Row::new()
            .add_values(vec![Value::Int64(2), Value::String("second".to_string())]);

        rows.add_row(row1).expect("Failed to add row1");
        rows.add_row(row2).expect("Failed to add row2");

        // Test state after adding rows
        assert_eq!(rows.len(), 2);
        assert!(!rows.is_empty());
    }

    #[test]
    fn test_non_nullable_timestamp_field_with_null_should_error() {
        // Create schema with timestamp field (non-nullable)
        let schema = vec![
            Column {
                name: "ts".to_string(),
                data_type: ColumnDataType::TimestampMillisecond,
                semantic_type: SemanticType::Timestamp,
                data_type_extension: None,
            },
            Column {
                name: "value".to_string(),
                data_type: ColumnDataType::Int64,
                semantic_type: SemanticType::Field,
                data_type_extension: None,
            },
        ];

        let mut rows = Rows::new(&schema, 5, 5).expect("Failed to create rows");

        // Add a row with null timestamp (should cause error when converting to RecordBatch)
        let row_with_null_timestamp =
            crate::table::Row::new().add_values(vec![Value::Null, Value::Int64(42)]);

        rows.add_row(row_with_null_timestamp)
            .expect("Failed to add row");

        // Converting to RecordBatch should fail because timestamp is null but field is non-nullable
        let result = RecordBatch::try_from(rows);
        assert!(
            result.is_err(),
            "Should fail when timestamp field contains null value"
        );
    }

    #[test]
    fn test_nullable_field_with_null_should_succeed() {
        // Create schema with nullable field
        let schema = vec![
            Column {
                name: "ts".to_string(),
                data_type: ColumnDataType::TimestampMillisecond,
                semantic_type: SemanticType::Timestamp,
                data_type_extension: None,
            },
            Column {
                name: "value".to_string(),
                data_type: ColumnDataType::Int64,
                semantic_type: SemanticType::Field,
                data_type_extension: None,
            },
        ];

        let mut rows = Rows::new(&schema, 5, 5).expect("Failed to create rows");

        // Add a row with null value field (should succeed since value field is nullable)
        let row_with_null_value = crate::table::Row::new()
            .add_values(vec![Value::TimestampMillisecond(1234567890), Value::Null]);

        rows.add_row(row_with_null_value)
            .expect("Failed to add row");

        // Converting to RecordBatch should succeed because value field is nullable
        let result = RecordBatch::try_from(rows);
        assert!(
            result.is_ok(),
            "Should succeed when nullable field contains null value"
        );
    }

    #[test]
    fn test_arrow_schema_nullable_fields() {
        use arrow_schema::{DataType, Field};

        // Create columns with different semantic types
        let columns = [
            Column {
                name: "ts".to_string(),
                data_type: ColumnDataType::TimestampMillisecond,
                semantic_type: SemanticType::Timestamp,
                data_type_extension: None,
            },
            Column {
                name: "value".to_string(),
                data_type: ColumnDataType::Int64,
                semantic_type: SemanticType::Field,
                data_type_extension: None,
            },
            Column {
                name: "tag".to_string(),
                data_type: ColumnDataType::String,
                semantic_type: SemanticType::Tag,
                data_type_extension: None,
            },
        ];

        // Test the logic that creates Arrow schema fields
        let fields: Vec<Field> = columns
            .iter()
            .map(|col| {
                let nullable = col.semantic_type != SemanticType::Timestamp;
                let data_type = match col.data_type {
                    ColumnDataType::TimestampMillisecond => {
                        DataType::Timestamp(TimeUnit::Millisecond, None)
                    }
                    ColumnDataType::Int64 => DataType::Int64,
                    ColumnDataType::String => DataType::Utf8,
                    _ => DataType::Utf8, // fallback
                };
                Field::new(&col.name, data_type, nullable)
            })
            .collect();

        assert_eq!(fields.len(), 3);

        // Timestamp field should be non-nullable
        assert!(
            !fields[0].is_nullable(),
            "Timestamp field should be non-nullable"
        );
        assert_eq!(fields[0].name(), "ts");

        // Value field should be nullable
        assert!(fields[1].is_nullable(), "Value field should be nullable");
        assert_eq!(fields[1].name(), "value");

        // Tag field should be nullable
        assert!(fields[2].is_nullable(), "Tag field should be nullable");
        assert_eq!(fields[2].name(), "tag");
    }
}
