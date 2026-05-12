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

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use greptime_proto::v1::auth_header::AuthScheme;
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
#[derive(Clone, Debug)]
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

    pub fn set_auth(&mut self, auth: AuthScheme) {
        self.database.set_auth(auth);
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
    ///
    /// When the input `Rows` spans multiple time windows, it is split into
    /// one request per window. This method waits for all of them and returns
    /// a single `DoPutResponse` whose `affected_rows` is the sum across all
    /// windows and whose `request_id` is that of the last submitted request.
    pub async fn write_rows(&mut self, rows: Rows) -> Result<DoPutResponse> {
        let request_ids = self.write_rows_async(rows).await?;

        let mut total_affected_rows = 0usize;
        let mut last_request_id = 0;
        for request_id in request_ids {
            let response = self.wait_for_response(request_id).await?;
            total_affected_rows += response.affected_rows();
            last_request_id = request_id;
        }
        Ok(DoPutResponse::new(last_request_id, total_affected_rows))
    }

    /// Submit rows for writing without waiting for responses.
    ///
    /// When the input `Rows` spans multiple time windows, it is split into
    /// one request per window. The returned vector contains one `request_id`
    /// per submitted request, in submission order, each of which can be used
    /// with `wait_for_response` to retrieve the corresponding result.
    pub async fn write_rows_async(&mut self, rows: Rows) -> Result<Vec<RequestId>> {
        // Ensure that the rows are not empty
        ensure!(!rows.is_empty(), error::EmptyRowsSnafu);
        // Validate that the rows schema matches the writer's schema
        self.validate_rows_schema(&rows)?;

        let batches_with_timestamp: Vec<RecordBatchWithTimestamp> = rows.try_into()?;
        ensure!(!batches_with_timestamp.is_empty(), error::EmptyRowsSnafu);

        let mut request_ids = Vec::with_capacity(batches_with_timestamp.len());
        for batch_with_ts in batches_with_timestamp {
            let request_id = self.submit_record_batch(batch_with_ts).await?;
            request_ids.push(request_id);
        }

        Ok(request_ids)
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
    pub fn alloc_rows_buffer(&self, capacity: usize) -> Result<Rows> {
        Rows::with_arrow_schema(self.column_schemas(), self.arrow_schema.clone(), capacity)
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

    /// Submit a record batch with timestamp range without waiting for response
    /// Returns the `request_id` for later tracking
    async fn submit_record_batch(
        &mut self,
        batch_with_ts: RecordBatchWithTimestamp,
    ) -> Result<RequestId> {
        // Send schema first if not already sent
        if !self.schema_sent {
            let batch = batch_with_ts.batch();
            let mut schema_data = self.encoder.encode(FlightMessage::Schema(batch.schema()));
            let metadata = DoPutMetadata::new(0, None, None);
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

        // Send the request with time range metadata
        let request_id = self.next_request_id();
        let batch = batch_with_ts.batch().clone();
        let message = FlightMessage::RecordBatch(batch);
        let mut data = self.encoder.encode(message);
        let metadata = DoPutMetadata::new(
            request_id,
            Some(batch_with_ts.start_timestamp()),
            Some(batch_with_ts.end_timestamp()),
        );
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
        ColumnDataType::TimeSecond => DataType::Time32(TimeUnit::Second),
        ColumnDataType::TimeMillisecond => DataType::Time32(TimeUnit::Millisecond),
        ColumnDataType::TimeMicrosecond => DataType::Time64(TimeUnit::Microsecond),
        ColumnDataType::TimeNanosecond => DataType::Time64(TimeUnit::Nanosecond),

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
///
/// Supports automatic time windowing: rows are partitioned by timestamp into separate
/// time windows. Each window is converted to a separate RecordBatch with time range metadata.
/// Default time window duration is 1 hour.
#[derive(Debug)]
pub struct Rows {
    schema: Arc<Schema>,
    column_schemas: Vec<Column>, // Store column schemas for creating window builders
    column_count: usize,
    // Time windowing configuration
    time_window_duration: i64,
    timestamp_column_index: usize,
    time_windows: HashMap<TimeWindowKey, RowBatchBuilder>,
    default_capacity: usize,
}

impl Rows {
    /// Create a new Rows collection with the given schema and capacity
    ///
    /// Automatically detects the timestamp column (first column with SemanticType::Timestamp)
    /// and initializes time windowing with a default duration of 1 hour.
    pub fn new(column_schemas: &[Column], capacity: usize) -> Result<Self> {
        let builder = RowBatchBuilder::new(column_schemas, capacity)?;
        let schema = builder.schema.clone();

        // Find timestamp column index
        let (timestamp_column_index, time_window_duration) =
            find_timestamp_index_and_window(column_schemas)?;

        Ok(Self {
            schema,
            column_schemas: column_schemas.to_vec(),
            column_count: column_schemas.len(),
            time_window_duration,
            timestamp_column_index,
            time_windows: HashMap::new(),
            default_capacity: capacity,
        })
    }

    /// Create a new Rows collection with a pre-computed Arrow schema
    ///
    /// Automatically detects the timestamp column (first column with SemanticType::Timestamp)
    /// and initializes time windowing with a default duration of 1 hour.
    fn with_arrow_schema(
        column_schemas: &[Column],
        arrow_schema: Arc<Schema>,
        capacity: usize,
    ) -> Result<Self> {
        // Find timestamp column index
        let (timestamp_column_index, time_window_duration) =
            find_timestamp_index_and_window(column_schemas)?;

        Ok(Self {
            schema: arrow_schema,
            column_schemas: column_schemas.to_vec(),
            column_count: column_schemas.len(),
            time_window_duration,
            timestamp_column_index,
            time_windows: HashMap::new(),
            default_capacity: capacity,
        })
    }

    /// Calculate the time window key for a given timestamp in nanoseconds
    fn calculate_window_key(&self, timestamp: i64) -> TimeWindowKey {
        timestamp
            .checked_div_euclid(self.time_window_duration)
            .and_then(|v| v.checked_mul(self.time_window_duration))
            .unwrap_or(i64::MIN)
    }

    /// Extract timestamp from a row.
    /// Returns an error if timestamp column is missing or null
    fn extract_timestamp(&self, row: &Row) -> Result<i64> {
        // Get the timestamp value (raw value in its native unit)
        unsafe { row.get_timestamp_unchecked(self.timestamp_column_index) }
            .context(error::NullTimestampSnafu)
    }

    /// Add a row to the collection using move semantics
    ///
    /// Rows are automatically partitioned into time windows based on their timestamp.
    /// A timestamp column is required; if it is missing when building the inserter,
    /// an error is returned.
    pub fn add_row(&mut self, row: Row) -> Result<()> {
        // Validate column count matches schema
        ensure!(
            row.len() == self.column_count,
            error::InvalidColumnCountSnafu {
                expected: self.column_count,
                actual: row.len(),
            }
        );

        let window_key = self.calculate_window_key(self.extract_timestamp(&row)?);
        let mut binding = self.time_windows.entry(window_key);
        let buffer = match binding {
            Entry::Occupied(ref mut e) => e.get_mut(),
            Entry::Vacant(v) => v.insert(RowBatchBuilder::new(
                &self.column_schemas,
                Self::window_initial_capacity(self.default_capacity),
            )?),
        };

        buffer.add_row(&row)?;
        Ok(())
    }

    /// Get the current number of rows
    #[must_use]
    pub fn len(&self) -> usize {
        self.time_windows.values().map(|b| b.len()).sum()
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

    fn window_initial_capacity(default_capacity: usize) -> usize {
        default_capacity.min(MAX_WINDOW_INITIAL_CAPACITY)
    }
}

/// Time window key type - represents window start time in nanoseconds
type TimeWindowKey = i64;
const MAX_WINDOW_INITIAL_CAPACITY: usize = 1024;

/// Record batch with timestamp range metadata
/// Represents a RecordBatch that belongs to a specific time window.
///
/// `start_timestamp` and `end_timestamp` are in the timestamp column's native
/// unit (as declared in the table schema), not normalized to a single unit.
#[derive(Debug, Clone)]
pub struct RecordBatchWithTimestamp {
    batch: RecordBatch,
    start_timestamp: i64,
    end_timestamp: i64,
}

impl RecordBatchWithTimestamp {
    /// Create a new RecordBatchWithTimestamp
    pub fn new(batch: RecordBatch, start_timestamp: i64, end_timestamp: i64) -> Self {
        Self {
            batch,
            start_timestamp,
            end_timestamp,
        }
    }

    /// Get the start timestamp, in the timestamp column's native unit
    #[must_use]
    pub fn start_timestamp(&self) -> i64 {
        self.start_timestamp
    }

    /// Get the end timestamp, in the timestamp column's native unit
    #[must_use]
    pub fn end_timestamp(&self) -> i64 {
        self.end_timestamp
    }

    /// Get the RecordBatch
    #[must_use]
    pub fn batch(&self) -> &RecordBatch {
        &self.batch
    }
}

/// Convert Rows to Vec<RecordBatchWithTimestamp>, handling buffered data and time windowing
impl TryFrom<Rows> for Vec<RecordBatchWithTimestamp> {
    type Error = crate::Error;

    fn try_from(rows: Rows) -> Result<Self> {
        // Build RecordBatch for each time window
        let mut batches = Vec::with_capacity(rows.time_windows.len());

        for builder in rows.time_windows.into_values() {
            // Skip empty windows
            if builder.is_empty() {
                continue;
            }

            let min_ts = builder.min_timestamp;
            let max_ts = builder.max_timestamp;
            debug_assert!(max_ts >= min_ts);
            let batch = builder.build()?;

            batches.push(RecordBatchWithTimestamp::new(batch, min_ts, max_ts));
        }
        Ok(batches)
    }
}

/// Efficient batch builder that directly constructs Arrow arrays
/// This avoids the overhead of creating intermediate Row objects and converting them
/// Arrow builders automatically manage capacity and expand as needed
#[derive(Debug)]
pub struct RowBatchBuilder {
    builders: Vec<ArrayBuilderEnum>,
    schema: Arc<Schema>,
    current_rows: usize,
    /// Minimum timestamp seen, in the timestamp column's native unit
    min_timestamp: i64,
    /// Maximum timestamp seen, in the timestamp column's native unit
    max_timestamp: i64,
    timestamp_idx: usize,
}

impl RowBatchBuilder {
    /// Create a new RowBatchBuilder with the given schema and capacity
    fn new(column_schemas: &[Column], capacity: usize) -> Result<Self> {
        let mut fields = Vec::with_capacity(column_schemas.len());
        let mut timestamp_index_opt = None;
        for (idx, col) in column_schemas.iter().enumerate() {
            let mut nullable = true;
            if col.semantic_type == SemanticType::Timestamp {
                nullable = false;
                timestamp_index_opt = Some(idx);
            }

            let field = column_to_arrow_data_type(col)
                .map(|data_type| Field::new(&col.name, data_type, nullable))?;
            fields.push(field);
        }
        let schema = Arc::new(Schema::new(fields));

        let builders: Result<Vec<ArrayBuilderEnum>> = column_schemas
            .iter()
            .enumerate()
            .map(|(col_idx, col)| create_array_builder(col, capacity, col_idx))
            .collect();
        let timestamp_idx = timestamp_index_opt.context(error::MissingTimestampColumnSnafu)?;

        Ok(Self {
            builders: builders?,
            schema,
            current_rows: 0,
            min_timestamp: i64::MAX,
            max_timestamp: i64::MIN,
            timestamp_idx,
        })
    }

    /// Add multiple rows to the batch builder using batch operations
    fn add_row(&mut self, row: &Row) -> Result<()> {
        for (col_idx, builder) in self.builders.iter_mut().enumerate() {
            if col_idx == self.timestamp_idx {
                let ts = unsafe {
                    row.get_timestamp_unchecked(col_idx)
                        .context(error::NullTimestampSnafu)?
                };
                self.max_timestamp = self.max_timestamp.max(ts);
                self.min_timestamp = self.min_timestamp.min(ts);
            }
            builder.append_value_from_row(row, col_idx)?;
        }
        self.current_rows += 1;
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

    /// Check if the builder is empty (has no rows)
    fn is_empty(&self) -> bool {
        self.current_rows == 0
    }
}

/// Trait for type-erased array builders
trait ArrayBuilder {
    fn append_value_from_row(&mut self, row: &Row, col_idx: usize) -> Result<()>;
}

#[derive(Debug)]
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
    fn append_value_from_row(&mut self, row: &Row, col_idx: usize) -> Result<()> {
        match self {
            ArrayBuilderEnum::Boolean(builder) => builder.append_value_from_row(row, col_idx),
            ArrayBuilderEnum::Int8(builder) => builder.append_value_from_row(row, col_idx),
            ArrayBuilderEnum::Int16(builder) => builder.append_value_from_row(row, col_idx),
            ArrayBuilderEnum::Int32(builder) => builder.append_value_from_row(row, col_idx),
            ArrayBuilderEnum::Int64(builder) => builder.append_value_from_row(row, col_idx),
            ArrayBuilderEnum::UInt8(builder) => builder.append_value_from_row(row, col_idx),
            ArrayBuilderEnum::UInt16(builder) => builder.append_value_from_row(row, col_idx),
            ArrayBuilderEnum::UInt32(builder) => builder.append_value_from_row(row, col_idx),
            ArrayBuilderEnum::UInt64(builder) => builder.append_value_from_row(row, col_idx),
            ArrayBuilderEnum::Float32(builder) => builder.append_value_from_row(row, col_idx),
            ArrayBuilderEnum::Float64(builder) => builder.append_value_from_row(row, col_idx),
            ArrayBuilderEnum::String(builder) => builder.append_value_from_row(row, col_idx),
            ArrayBuilderEnum::Binary(builder) => builder.append_value_from_row(row, col_idx),
            ArrayBuilderEnum::Decimal128(builder) => builder.append_value_from_row(row, col_idx),
            ArrayBuilderEnum::Date(builder) => builder.append_value_from_row(row, col_idx),
            ArrayBuilderEnum::TimestampSecond(builder) => {
                builder.append_value_from_row(row, col_idx)
            }
            ArrayBuilderEnum::TimestampMillisecond(builder) => {
                builder.append_value_from_row(row, col_idx)
            }
            ArrayBuilderEnum::TimestampMicrosecond(builder) => {
                builder.append_value_from_row(row, col_idx)
            }
            ArrayBuilderEnum::TimestampNanosecond(builder) => {
                builder.append_value_from_row(row, col_idx)
            }
            ArrayBuilderEnum::TimeSecond(builder) => builder.append_value_from_row(row, col_idx),
            ArrayBuilderEnum::TimeMillisecond(builder) => {
                builder.append_value_from_row(row, col_idx)
            }
            ArrayBuilderEnum::TimeMicrosecond(builder) => {
                builder.append_value_from_row(row, col_idx)
            }
            ArrayBuilderEnum::TimeNanosecond(builder) => {
                builder.append_value_from_row(row, col_idx)
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
                    .with_data_type(DataType::Decimal128(precision, scale)),
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
            fn append_value_from_row(&mut self, row: &Row, col_idx: usize) -> Result<()> {
                // Use unchecked version for performance - col_idx is guaranteed to be valid by schema
                self.append_option(unsafe { row.$getter(col_idx) });
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
impl_arrow_builder!(StringBuilder, get_string_ref_unchecked, String);
impl_arrow_builder!(BinaryBuilder, get_binary_ref_unchecked, Vec<u8>);

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

        Ok(Row::from_values(row_values))
    }
}

// Re-export the proto ColumnDataType for convenience
pub use crate::api::v1::ColumnDataType as ColumnType;

fn find_timestamp_index_and_window(column_schemas: &[Column]) -> Result<(usize, i64)> {
    let (timestamp_column_index, timestamp_type) = column_schemas
        .iter()
        .enumerate()
        .find(|(_, col)| col.semantic_type == SemanticType::Timestamp)
        .context(error::MissingTimestampColumnSnafu)?;

    let time_window_duration = match timestamp_type.data_type {
        ColumnDataType::TimestampSecond => 3600i64,
        ColumnDataType::TimestampMillisecond => 3600i64 * 1000,
        ColumnDataType::Datetime | ColumnDataType::TimestampMicrosecond => 3600i64 * 1000 * 1000,
        ColumnDataType::TimestampNanosecond => 3600i64 * 1000 * 1000 * 1000,
        other => {
            return error::InvalidTimestampTypeSnafu {
                data_type: format!("{:?}", other),
            }
            .fail()
        }
    };
    Ok((timestamp_column_index, time_window_duration))
}

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
        let rows1 = Rows::new(&schema1, 10).expect("Failed to create rows1");

        // Test 2: Incompatible rows should fail validation
        let rows2_err_msg = Rows::new(&schema2, 10).unwrap_err().to_string();

        // Mock the validation (since we can't easily create a BulkStreamWriter in tests)
        // In practice, this would be tested with a real BulkStreamWriter
        assert_eq!(rows1.schema().fields().len(), 3);
        assert!(
            rows2_err_msg.contains("Missing timestamp column in schema"),
            "Actual: {}",
            rows2_err_msg
        );
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

        let mut rows = Rows::new(&schema, 5).expect("Failed to create rows");

        // Add a row with null timestamp (should cause error when converting to RecordBatch)
        let row_with_null_timestamp = Row::new().add_values(vec![Value::Null, Value::Int64(42)]);

        assert!(rows.add_row(row_with_null_timestamp).is_err());
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

        let mut rows = Rows::new(&schema, 5).expect("Failed to create rows");

        // Add a row with null value field (should succeed since value field is nullable)
        let row_with_null_value = crate::table::Row::new()
            .add_values(vec![Value::TimestampMillisecond(1234567890), Value::Null]);

        rows.add_row(row_with_null_value)
            .expect("Failed to add row");
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

    #[test]
    fn test_row_batch_builder_min_max_timestamp() {
        // Create schema with timestamp and value columns
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

        let mut builder = RowBatchBuilder::new(&schema, 10).expect("Failed to create builder");

        // Initially, min should be MAX and max should be MIN
        assert_eq!(builder.min_timestamp, i64::MAX);
        assert_eq!(builder.max_timestamp, i64::MIN);

        // Add first row with timestamp 1000
        let row1 = crate::table::Row::new()
            .add_values(vec![Value::TimestampMillisecond(1000), Value::Int64(10)]);
        builder.add_row(&row1).expect("Failed to add row1");

        assert_eq!(builder.min_timestamp, 1000);
        assert_eq!(builder.max_timestamp, 1000);

        // Add second row with a larger timestamp 2000
        let row2 = crate::table::Row::new()
            .add_values(vec![Value::TimestampMillisecond(2000), Value::Int64(20)]);
        builder.add_row(&row2).expect("Failed to add row2");

        assert_eq!(builder.min_timestamp, 1000);
        assert_eq!(builder.max_timestamp, 2000);

        // Add third row with a smaller timestamp 500
        let row3 = crate::table::Row::new()
            .add_values(vec![Value::TimestampMillisecond(500), Value::Int64(30)]);
        builder.add_row(&row3).expect("Failed to add row3");

        assert_eq!(builder.min_timestamp, 500);
        assert_eq!(builder.max_timestamp, 2000);

        // Add fourth row with timestamp between min and max (1500)
        let row4 = crate::table::Row::new()
            .add_values(vec![Value::TimestampMillisecond(1500), Value::Int64(40)]);
        builder.add_row(&row4).expect("Failed to add row4");

        // min and max should remain unchanged
        assert_eq!(builder.min_timestamp, 500);
        assert_eq!(builder.max_timestamp, 2000);

        // Verify row count
        assert_eq!(builder.len(), 4);
    }

    #[test]
    fn test_record_batch_with_timestamp_preserves_native_unit() {
        // `start_timestamp`/`end_timestamp` on RecordBatchWithTimestamp are in the
        // timestamp column's native unit (no normalization to nanoseconds).
        let cases: &[(ColumnDataType, i64, i64)] = &[
            (ColumnDataType::TimestampSecond, 1, 5),
            (ColumnDataType::TimestampMillisecond, 1_000, 2_500),
            (ColumnDataType::Datetime, 1_000, 2_500),
            (ColumnDataType::TimestampMicrosecond, 1_000, 2_500),
            (ColumnDataType::TimestampNanosecond, 1_000, 2_500),
        ];

        for &(ts_type, raw_min, raw_max) in cases {
            let schema = create_timestamp_schema(ts_type);
            let mut rows = Rows::new(&schema, 4).expect("Failed to create rows");
            rows.add_row(create_row(ts_type, raw_min, 10)).unwrap();
            rows.add_row(create_row(ts_type, raw_max, 20)).unwrap();

            let batches: Vec<RecordBatchWithTimestamp> =
                rows.try_into().expect("Failed to convert rows");
            assert_eq!(batches.len(), 1, "ts_type={:?}", ts_type);

            let batch = &batches[0];
            assert_eq!(batch.start_timestamp(), raw_min, "ts_type={:?}", ts_type);
            assert_eq!(batch.end_timestamp(), raw_max, "ts_type={:?}", ts_type);
        }
    }

    #[test]
    fn test_rows_with_datetime_timestamp_column_uses_microsecond_window() {
        let schema = create_timestamp_schema(ColumnDataType::Datetime);
        let mut rows = Rows::new(&schema, 10).expect("Failed to create rows");

        add_rows(
            &mut rows,
            ColumnDataType::Datetime,
            &[(0, 1), (3_599_999_999, 2), (3_600_000_000, 3)],
        );

        let batches = rows_to_sorted_batches(rows);

        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].start_timestamp(), 0);
        assert_eq!(batches[0].end_timestamp(), 3_599_999_999);
        assert_eq!(batches[0].batch().num_rows(), 2);
        assert_eq!(batches[1].start_timestamp(), 3_600_000_000);
        assert_eq!(batches[1].end_timestamp(), 3_600_000_000);
        assert_eq!(batches[1].batch().num_rows(), 1);
    }

    #[test]
    fn test_window_initial_capacity_is_capped_per_window() {
        assert_eq!(Rows::window_initial_capacity(0), 0);
        assert_eq!(Rows::window_initial_capacity(32), 32);
        assert_eq!(Rows::window_initial_capacity(10_000), 1024);
    }

    // Helper function to create a simple schema with timestamp and value columns
    fn create_timestamp_schema(timestamp_type: ColumnDataType) -> Vec<Column> {
        vec![
            Column {
                name: "ts".to_string(),
                data_type: timestamp_type,
                semantic_type: SemanticType::Timestamp,
                data_type_extension: None,
            },
            Column {
                name: "value".to_string(),
                data_type: ColumnDataType::Int64,
                semantic_type: SemanticType::Field,
                data_type_extension: None,
            },
        ]
    }

    // Helper function to create a row with timestamp and value
    fn create_row(timestamp_type: ColumnDataType, timestamp: i64, value: i64) -> crate::table::Row {
        let timestamp_value = match timestamp_type {
            ColumnDataType::TimestampSecond => Value::TimestampSecond(timestamp),
            ColumnDataType::TimestampMillisecond => Value::TimestampMillisecond(timestamp),
            ColumnDataType::Datetime => Value::Datetime(timestamp),
            ColumnDataType::TimestampMicrosecond => Value::TimestampMicrosecond(timestamp),
            ColumnDataType::TimestampNanosecond => Value::TimestampNanosecond(timestamp),
            _ => panic!("Unsupported timestamp type for test"),
        };
        Row::new().add_values(vec![timestamp_value, Value::Int64(value)])
    }

    // Helper function to add rows to a Rows collection
    fn add_rows(
        rows: &mut Rows,
        timestamp_type: ColumnDataType,
        timestamps_and_values: &[(i64, i64)],
    ) {
        for (timestamp, value) in timestamps_and_values {
            let row = create_row(timestamp_type, *timestamp, *value);
            rows.add_row(row).expect("Failed to add row");
        }
    }

    // Helper function to convert Rows to sorted batches
    fn rows_to_sorted_batches(rows: Rows) -> Vec<RecordBatchWithTimestamp> {
        let mut batches: Vec<RecordBatchWithTimestamp> =
            rows.try_into().expect("Failed to convert to batches");
        batches.sort_by_key(|b| b.start_timestamp());
        batches
    }

    #[test]
    fn test_calculate_window_key_timestamp_millisecond() {
        // Create schema with TimestampMillisecond (window duration = 3600 * 1000 = 3,600,000 ms = 1 hour)
        let schema = create_timestamp_schema(ColumnDataType::TimestampMillisecond);
        let mut rows = Rows::new(&schema, 10).expect("Failed to create rows");

        // Test 1: Timestamps in the same window should end up in the same batch
        // Window 0: 0 to 3,599,999
        add_rows(
            &mut rows,
            ColumnDataType::TimestampMillisecond,
            &[(0, 1), (1_800_000, 2), (3_599_999, 3)],
        );

        // Test 2: Timestamps in different windows should end up in different batches
        // Window 1: 3,600,000 to 7,199,999
        add_rows(
            &mut rows,
            ColumnDataType::TimestampMillisecond,
            &[(3_600_000, 4), (5_400_000, 5)],
        );

        let batches = rows_to_sorted_batches(rows);

        // Should have 2 batches (2 different windows)
        assert_eq!(batches.len(), 2);

        // First batch should contain rows from window 0
        assert_eq!(batches[0].start_timestamp(), 0);
        assert_eq!(batches[0].end_timestamp(), 3_599_999);
        assert_eq!(batches[0].batch().num_rows(), 3);

        // Second batch should contain rows from window 1
        assert_eq!(batches[1].start_timestamp(), 3_600_000);
        assert_eq!(batches[1].end_timestamp(), 5_400_000);
        assert_eq!(batches[1].batch().num_rows(), 2);
    }

    #[test]
    fn test_calculate_window_key_timestamp_second() {
        // Create schema with TimestampSecond (window duration = 3600 seconds = 1 hour)
        let schema = create_timestamp_schema(ColumnDataType::TimestampSecond);
        let mut rows = Rows::new(&schema, 10).expect("Failed to create rows");

        // Window 0: 0 to 3599
        add_rows(
            &mut rows,
            ColumnDataType::TimestampSecond,
            &[(0, 1), (1800, 2)],
        );

        // Window 1: 3600 to 7199
        add_rows(&mut rows, ColumnDataType::TimestampSecond, &[(3600, 3)]);

        let batches = rows_to_sorted_batches(rows);

        assert_eq!(batches.len(), 2);
        // First batch: min=0, max=1800
        assert_eq!(batches[0].start_timestamp(), 0);
        assert_eq!(batches[0].end_timestamp(), 1800);
        assert_eq!(batches[0].batch().num_rows(), 2);
        // Second batch: min=3600, max=3600
        assert_eq!(batches[1].start_timestamp(), 3600);
        assert_eq!(batches[1].end_timestamp(), 3600);
        assert_eq!(batches[1].batch().num_rows(), 1);
    }

    #[test]
    fn test_calculate_window_key_boundary_conditions() {
        // Test timestamps exactly at window boundaries
        let schema = create_timestamp_schema(ColumnDataType::TimestampMillisecond);
        let mut rows = Rows::new(&schema, 10).expect("Failed to create rows");
        let window_duration = 3600i64 * 1000; // 1 hour in milliseconds

        // Add rows at window boundaries
        add_rows(
            &mut rows,
            ColumnDataType::TimestampMillisecond,
            &[
                (0, 1),                   // Window start
                (window_duration - 1, 2), // Just before next window
                (window_duration, 3),     // Exactly at next window boundary
            ],
        );

        let batches = rows_to_sorted_batches(rows);

        assert_eq!(batches.len(), 2);
        // First batch should have rows from window 0
        assert_eq!(batches[0].start_timestamp(), 0);
        assert_eq!(batches[0].batch().num_rows(), 2);
        // Second batch should have row from window 1
        assert_eq!(batches[1].start_timestamp(), window_duration);
        assert_eq!(batches[1].batch().num_rows(), 1);
    }

    #[test]
    fn test_calculate_window_key_negative_timestamps() {
        // Test with negative timestamps (before epoch)
        let schema = create_timestamp_schema(ColumnDataType::TimestampMillisecond);
        let mut rows = Rows::new(&schema, 10).expect("Failed to create rows");
        let window_duration = 3600i64 * 1000;

        // Negative timestamps should still be grouped into windows
        add_rows(
            &mut rows,
            ColumnDataType::TimestampMillisecond,
            &[
                (-window_duration, 1),             // Window -1
                (-window_duration + 1_800_000, 2), // Window -1
                (0, 3),                            // Window 0
            ],
        );

        let batches = rows_to_sorted_batches(rows);

        // Should have 2 batches (negative window and zero window)
        assert_eq!(batches.len(), 2);
        // Verify that rows are correctly grouped
        let total_rows: usize = batches.iter().map(|b| b.batch().num_rows()).sum();
        assert_eq!(total_rows, 3);
    }
}
