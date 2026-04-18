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

use std::time::{SystemTime, UNIX_EPOCH};

use greptimedb_ingester::api::v1::{Row, RowInsertRequest, RowInsertRequests, Rows};
use greptimedb_ingester::client::Client;
use greptimedb_ingester::database::Database;
use greptimedb_ingester::helpers::schema::{field, timestamp};
use greptimedb_ingester::helpers::values::{f64_value, string_value, timestamp_millisecond_value};
use greptimedb_ingester::{
    BulkInserter, BulkWriteOptions, ChannelConfig, ChannelManager, ColumnDataType, CompressionType,
    GrpcCompression, Row as BulkRow, TableSchema, Value,
};
use uuid::Uuid;

struct TestConfig {
    endpoint: String,
    database: String,
}

impl TestConfig {
    fn from_env() -> Self {
        Self {
            endpoint: std::env::var("GREPTIMEDB_TEST_ENDPOINT")
                .unwrap_or_else(|_| "127.0.0.1:4001".to_string()),
            database: std::env::var("GREPTIMEDB_TEST_DATABASE")
                .unwrap_or_else(|_| "public".to_string()),
        }
    }
}

fn unique_table_name(prefix: &str) -> String {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    format!("{prefix}_{ts}_{}", Uuid::new_v4().simple())
}

fn make_client(config: &TestConfig, compression: Option<GrpcCompression>) -> Client {
    let channel_config = match compression {
        Some(compression) => ChannelConfig::default()
            .with_send_compression(compression)
            .with_accept_compression(compression),
        None => ChannelConfig::default(),
    };
    Client::with_manager_and_urls(
        ChannelManager::with_config(channel_config),
        [&config.endpoint],
    )
}

async fn insert_with_grpc_compression(compression: GrpcCompression) {
    let config = TestConfig::from_env();
    let client = make_client(&config, Some(compression));
    let database = Database::new_with_dbname(&config.database, client);

    let table_name = unique_table_name("grpc_compression");
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    let request = RowInsertRequests {
        inserts: vec![RowInsertRequest {
            table_name,
            rows: Some(Rows {
                schema: vec![
                    field("host", ColumnDataType::String),
                    timestamp("ts", ColumnDataType::TimestampMillisecond),
                    field("cpu", ColumnDataType::Float64),
                ],
                rows: vec![Row {
                    values: vec![
                        string_value("node-1".to_string()),
                        timestamp_millisecond_value(now),
                        f64_value(0.42),
                    ],
                }],
            }),
        }],
    };

    let affected_rows = database.insert(request).await.unwrap();
    assert_eq!(1, affected_rows);
}

async fn insert_with_bulk_compression(
    grpc_compression: Option<GrpcCompression>,
    payload_compression: CompressionType,
    table_prefix: &str,
) {
    let config = TestConfig::from_env();
    let client = make_client(&config, grpc_compression);
    let database = Database::new_with_dbname(&config.database, client.clone());
    let table_name = unique_table_name(table_prefix);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    let init_request = RowInsertRequests {
        inserts: vec![RowInsertRequest {
            table_name: table_name.clone(),
            rows: Some(Rows {
                schema: vec![
                    timestamp("ts", ColumnDataType::TimestampMillisecond),
                    field("host", ColumnDataType::String),
                    field("cpu", ColumnDataType::Float64),
                ],
                rows: vec![Row {
                    values: vec![
                        timestamp_millisecond_value(now),
                        string_value("bootstrap-node".to_string()),
                        f64_value(0.11),
                    ],
                }],
            }),
        }],
    };

    let affected_rows = database.insert(init_request).await.unwrap();
    assert_eq!(1, affected_rows);

    let bulk_inserter = BulkInserter::new(client, &config.database);
    let table_schema = TableSchema::builder()
        .name(&table_name)
        .build()
        .unwrap()
        .add_timestamp("ts", ColumnDataType::TimestampMillisecond)
        .add_field("host", ColumnDataType::String)
        .add_field("cpu", ColumnDataType::Float64);

    let mut bulk_writer = bulk_inserter
        .create_bulk_stream_writer(
            &table_schema,
            Some(
                BulkWriteOptions::default()
                    .with_parallelism(1)
                    .with_compression(payload_compression),
            ),
        )
        .await
        .unwrap();

    let mut rows = bulk_writer.alloc_rows_buffer(1, 128).unwrap();
    let row = BulkRow::new().add_values(vec![
        Value::TimestampMillisecond(now + 1),
        Value::String("bulk-node".to_string()),
        Value::Float64(0.42),
    ]);
    rows.add_row(row).unwrap();

    let response = bulk_writer.write_rows(rows).await.unwrap();
    assert_eq!(1, response.affected_rows());

    bulk_writer.finish().await.unwrap();
}

#[tokio::test]
async fn test_insert_with_gzip_grpc_compression() {
    insert_with_grpc_compression(GrpcCompression::Gzip).await;
}

#[tokio::test]
async fn test_insert_with_zstd_grpc_compression() {
    insert_with_grpc_compression(GrpcCompression::Zstd).await;
}

#[tokio::test]
async fn test_bulk_insert_with_gzip_grpc_compression() {
    insert_with_bulk_compression(
        Some(GrpcCompression::Gzip),
        CompressionType::None,
        "grpc_bulk_compression",
    )
    .await;
}

#[tokio::test]
async fn test_bulk_insert_with_zstd_grpc_compression() {
    insert_with_bulk_compression(
        Some(GrpcCompression::Zstd),
        CompressionType::None,
        "grpc_bulk_compression",
    )
    .await;
}

#[tokio::test]
async fn test_bulk_insert_with_lz4_payload_compression() {
    insert_with_bulk_compression(None, CompressionType::Lz4, "payload_bulk_compression").await;
}

#[tokio::test]
async fn test_bulk_insert_with_zstd_payload_compression() {
    insert_with_bulk_compression(None, CompressionType::Zstd, "payload_bulk_compression").await;
}
