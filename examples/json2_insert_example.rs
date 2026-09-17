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

//! Write JSON strings as JSON2 over gRPC.
//!
//! Requires a server that supports JSON2 and allows automatic table creation.
//! The first insert automatically creates `json2_logs` if it does not exist.
//! Run with `cargo run --example json2_insert_example`.
//! Uses the same GREPTIMEDB_* environment variables and examples/db-connection.toml
//! as the other examples.

#[path = "util/mod.rs"]
mod util;

use greptimedb_ingester::api::v1::{
    auth_header::AuthScheme, Basic, ColumnDataType, Row, RowInsertRequest, RowInsertRequests, Rows,
};
use greptimedb_ingester::client::Client;
use greptimedb_ingester::helpers::schema::{json2_field, timestamp};
use greptimedb_ingester::helpers::values::{json2_value, none_value, timestamp_millisecond_value};
use greptimedb_ingester::{database::Database, Result};
use util::DbConfig;

#[tokio::main]
async fn main() -> Result<()> {
    let config = DbConfig::from_env();
    config.display();
    let client = Client::with_urls(&[config.endpoint]);
    let mut database = Database::new_with_dbname(config.dbname, client);
    if let (Some(username), Some(password)) = (config.username, config.password) {
        database.set_auth(AuthScheme::Basic(Basic { username, password }));
    }

    // The client parses JSON strings into native Protobuf JSON2 values.
    // Top-level values must be objects or null; nested arrays and scalars are supported.
    let payloads = vec![
        json2_value(r#"{"message":"hello","nested":{"items":[1,"two",null]},"ok":true}"#)?,
        json2_value("{}")?,
        none_value(), // SQL NULL; json2_value("null")? also produces SQL NULL.
    ];
    let rows = payloads
        .into_iter()
        .enumerate()
        .map(|(index, payload)| Row {
            values: vec![
                timestamp_millisecond_value(1234567890000 + index as i64),
                payload,
            ],
        })
        .collect();
    let affected_rows = database
        .insert_with_hints(
            RowInsertRequests {
                inserts: vec![RowInsertRequest {
                    table_name: "json2_logs".into(),
                    rows: Some(Rows {
                        schema: vec![
                            timestamp("ts", ColumnDataType::TimestampMillisecond),
                            json2_field("payload"),
                        ],
                        rows,
                    }),
                }],
            },
            &[("append_mode", "true")],
        )
        .await?;
    assert_eq!(affected_rows, 3);
    println!("Inserted {affected_rows} JSON2 rows");
    Ok(())
}
