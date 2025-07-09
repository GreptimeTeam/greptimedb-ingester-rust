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

use std::io;

use snafu::{Location, Snafu};
use tonic::{metadata::errors::InvalidMetadataValue, Status};

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Invalid client tls config, {}", msg))]
    InvalidTlsConfig {
        msg: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid config file path, {}", source))]
    InvalidConfigFilePath {
        source: io::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to create gRPC channel"))]
    CreateChannel {
        #[snafu(implicit)]
        location: Location,
        source: tonic::transport::Error,
    },

    #[snafu(display("Illegal gRPC client state: {}", err_msg))]
    IllegalGrpcClientState {
        err_msg: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Missing required field in protobuf, field: {}", field))]
    MissingField {
        field: String,
        #[snafu(implicit)]
        location: Location,
    },

    // Server error carried in Tonic Status's metadata.
    #[snafu(display("{}", msg))]
    Server { status: Box<Status>, msg: String },

    #[snafu(display("Illegal Database response: {err_msg}"))]
    IllegalDatabaseResponse {
        err_msg: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid Tonic metadata value"))]
    InvalidTonicMetadataValue {
        #[snafu(source)]
        error: InvalidMetadataValue,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to serde Json"))]
    SerdeJson {
        #[snafu(source)]
        error: serde_json::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to create Arrow RecordBatch"))]
    CreateRecordBatch {
        #[snafu(source)]
        error: arrow_schema::ArrowError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Unsupported data type: {:?}", data_type))]
    UnsupportedDataType {
        data_type: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to serialize metadata"))]
    SerializeMetadata {
        #[snafu(source)]
        error: serde_json::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to send data to stream: {}", source))]
    SendData {
        source: futures::channel::mpsc::SendError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Response stream ended unexpectedly"))]
    StreamEnded {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display(
        "Response stream ended unexpectedly with pending requests: {:?}",
        request_ids
    ))]
    StreamEndedWithPendingRequests {
        request_ids: Vec<i64>,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display(
        "Request timeout after {:?} for request IDs: {:?}",
        timeout,
        request_ids
    ))]
    RequestTimeout {
        request_ids: Vec<i64>,
        timeout: std::time::Duration,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display(
        "Schema mismatch: BulkStreamWriter expects schema {} but got {}",
        expected,
        actual
    ))]
    SchemaMismatch {
        expected: String,
        actual: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid column count: expected {}, got {}", expected, actual))]
    InvalidColumnCount {
        expected: usize,
        actual: usize,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid column index: {}, total columns: {}", index, total))]
    InvalidColumnIndex {
        index: usize,
        total: usize,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Cannot write empty rows"))]
    EmptyRows {
        #[snafu(implicit)]
        location: Location,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

pub const INNER_ERROR_MSG: &str = "INNER_ERROR_MSG";

impl From<Status> for Error {
    fn from(e: Status) -> Self {
        fn get_metadata_value(e: &Status, key: &str) -> Option<String> {
            e.metadata()
                .get(key)
                .and_then(|v| String::from_utf8(v.as_bytes().to_vec()).ok())
        }

        let msg = get_metadata_value(&e, INNER_ERROR_MSG).unwrap_or(e.to_string());

        Self::Server {
            status: Box::new(e),
            msg,
        }
    }
}

impl Error {
    /// Indicate if the error is retriable
    pub fn is_retriable(&self) -> bool {
        !matches!(
            self,
            Self::InvalidTlsConfig { .. }
                | Self::MissingField { .. }
                | Self::InvalidConfigFilePath { .. }
        )
    }
}
