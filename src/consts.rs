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

/// Metadata key used to specify the database name in gRPC request headers.
///
/// This constant defines the header key `"x-greptime-db-name"` that is used
/// to identify which database the request should be executed against.
/// The value associated with this key should be the name of the target database.
pub const REQUEST_METADATA_KEY_DATABASE_NAME: &str = "x-greptime-db-name";

/// Metadata key used for authentication in gRPC request headers.
///
/// This constant defines the header key `"x-greptime-auth"` that is used
/// to send authentication credentials to the GreptimeDB server.
/// The value should be a Basic authentication string (e.g., "Basic <base64-encoded-credentials>").
pub const REQUEST_METADATA_KEY_AUTH: &str = "x-greptime-auth";

/// Metadata key used for hints in gRPC request headers.
///
/// This constant defines the header key `"x-greptime-hints"` that is used
/// to pass additional hints to the GreptimeDB server for request processing.
/// The value should be a comma-separated list of key-value pairs (e.g., "key1=value1,key2=value2").
pub const REQUEST_METADATA_KEY_HINTS: &str = "x-greptime-hints";
