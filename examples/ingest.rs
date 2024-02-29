// Copyright 2024 Greptime Team
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

use derive_new::new;

use greptimedb_client::api::v1::*;
use greptimedb_client::helpers::schema::*;
use greptimedb_client::helpers::values::*;
use greptimedb_client::{
    ChannelConfig, ChannelManager, Client, ClientTlsOption, Database, DEFAULT_SCHEMA_NAME,
};

#[tokio::main]
async fn main() {
    let greptimedb_endpoint =
        std::env::var("GREPTIMEDB_ENDPOINT").unwrap_or_else(|_| "localhost:4001".to_owned());
    let greptimedb_dbname =
        std::env::var("GREPTIMEDB_DBNAME").unwrap_or_else(|_| DEFAULT_SCHEMA_NAME.to_owned());
    let greptimedb_secure = std::env::var("GREPTIMEDB_TLS")
        .map(|s| s == "1")
        .unwrap_or(false);

    let grpc_client = if greptimedb_secure {
        let channel_config = ChannelConfig::default().client_tls_config(ClientTlsOption::default());

        let channel_manager = ChannelManager::with_tls_config(channel_config)
            .expect("Failed to create channel manager");
        Client::with_manager_and_urls(channel_manager, vec![&greptimedb_endpoint])
    } else {
        Client::with_urls(vec![&greptimedb_endpoint])
    };

    let client = Database::new_with_dbname(greptimedb_dbname, grpc_client);

    let records = weather_records();
    let result = client.row_insert(to_insert_request(records)).await;
    match result {
        Ok(rows) => {
            println!("Rows written: {rows}");
        }
        Err(e) => {
            eprintln!("Error: {e}");
        }
    };
}

#[derive(new)]
struct WeatherRecord {
    timestamp_millis: i64,
    collector: String,
    temperature: f32,
    humidity: i32,
}

fn weather_records() -> Vec<WeatherRecord> {
    vec![
        WeatherRecord::new(1686109527000, "c1".to_owned(), 26.4, 15),
        WeatherRecord::new(1686023127000, "c1".to_owned(), 29.3, 20),
        WeatherRecord::new(1685936727010, "c1".to_owned(), 31.8, 13),
        WeatherRecord::new(1686109527000, "c2".to_owned(), 20.4, 67),
        WeatherRecord::new(1686023127000, "c2".to_owned(), 18.0, 74),
        WeatherRecord::new(1685936727000, "c2".to_owned(), 19.2, 81),
    ]
}

fn weather_schema() -> Vec<ColumnSchema> {
    vec![
        timestamp("ts", ColumnDataType::TimestampMillisecond),
        tag("collector", ColumnDataType::String),
        field("temperature", ColumnDataType::Float32),
        field("humidity", ColumnDataType::Int32),
    ]
}

/// This function generates some random data and bundle them into a
/// `InsertRequest`.
///
/// Data structure:
///
/// - `ts`: a timestamp column
/// - `collector`: a tag column
/// - `temperature`: a value field of f32
/// - `humidity`: a value field of i32
///
fn to_insert_request(records: Vec<WeatherRecord>) -> RowInsertRequests {
    let rows = records
        .into_iter()
        .map(|record| Row {
            values: vec![
                timestamp_millisecond_value(record.timestamp_millis),
                string_value(record.collector),
                f32_value(record.temperature),
                i32_value(record.humidity),
            ],
        })
        .collect();

    RowInsertRequests {
        inserts: vec![RowInsertRequest {
            table_name: "weather_demo".to_owned(),
            rows: Some(Rows {
                schema: weather_schema(),
                rows,
            }),
        }],
    }
}
