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

use arrow_flight::PutResult;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;

use crate::error;

/// The metadata for "DoPut" requests and responses.
///
/// Contains a "request_id" for coordinating requests and responses in the streams.
/// Optionally includes time range metadata (start_timestamp and end_timestamp in nanoseconds)
/// for time-windowed batches.
/// Client can set a unique request id in this metadata, and the server will return the same id in
/// the corresponding response. In doing so, a client can know how to do with its pending requests.
#[derive(Serialize, Deserialize)]
pub struct DoPutMetadata {
    request_id: i64,
    /// Start timestamp of the batch (optional, for time-windowed batches)
    #[serde(skip_serializing_if = "Option::is_none")]
    start_timestamp: Option<i64>,
    /// End timestamp of the batch (optional, for time-windowed batches)
    #[serde(skip_serializing_if = "Option::is_none")]
    end_timestamp: Option<i64>,
}

impl DoPutMetadata {
    /// Create a new DoPutMetadata with request_id and optional time range
    pub fn new(request_id: i64, start_timestamp: Option<i64>, end_timestamp: Option<i64>) -> Self {
        Self {
            request_id,
            start_timestamp,
            end_timestamp,
        }
    }

    pub fn request_id(&self) -> i64 {
        self.request_id
    }

    /// Get the start timestamp in nanoseconds, if available
    #[must_use]
    pub fn start_timestamp(&self) -> Option<i64> {
        self.start_timestamp
    }

    /// Get the end timestamp in nanoseconds, if available
    #[must_use]
    pub fn end_timestamp(&self) -> Option<i64> {
        self.end_timestamp
    }
}

/// The response in the "DoPut" returned stream.
#[derive(Serialize, Deserialize, Debug)]
pub struct DoPutResponse {
    /// The same "request_id" in the request; see the [DoPutMetadata].
    request_id: i64,
    /// The successfully ingested rows number.
    affected_rows: usize,
}

impl DoPutResponse {
    pub fn new(request_id: i64, affected_rows: usize) -> Self {
        Self {
            request_id,
            affected_rows,
        }
    }

    pub fn request_id(&self) -> i64 {
        self.request_id
    }

    pub fn affected_rows(&self) -> usize {
        self.affected_rows
    }
}

impl TryFrom<PutResult> for DoPutResponse {
    type Error = error::Error;

    fn try_from(value: PutResult) -> Result<Self, Self::Error> {
        serde_json::from_slice(&value.app_metadata).context(error::SerdeJsonSnafu)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serde_do_put_metadata() {
        // Test backward compatibility: old format without time range
        let serialized = r#"{"request_id":42}"#;
        let metadata = serde_json::from_str::<DoPutMetadata>(serialized).unwrap();
        assert_eq!(metadata.request_id(), 42);
        assert_eq!(metadata.start_timestamp(), None);
        assert_eq!(metadata.end_timestamp(), None);

        // Test new format with time range
        let metadata_with_ts = DoPutMetadata::new(42, Some(1000), Some(2000));
        let serialized = serde_json::to_string(&metadata_with_ts).unwrap();
        let deserialized = serde_json::from_str::<DoPutMetadata>(&serialized).unwrap();
        assert_eq!(deserialized.request_id(), 42);
        assert_eq!(deserialized.start_timestamp(), Some(1000));
        assert_eq!(deserialized.end_timestamp(), Some(2000));
    }

    #[test]
    fn test_serde_do_put_response() {
        let x = DoPutResponse::new(42, 88);
        let serialized = serde_json::to_string(&x).unwrap();
        assert_eq!(serialized, r#"{"request_id":42,"affected_rows":88}"#);
    }
}
