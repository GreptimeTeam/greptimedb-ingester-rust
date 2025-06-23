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

mod config_utils;
use arrow_flight::FlightDescriptor;
use config_utils::DbConfig;

use arrow_array::{RecordBatch, TimestampMillisecondArray};
use futures::StreamExt;
use greptimedb_ingester::client::Client;
use greptimedb_ingester::database::Database;
use greptimedb_ingester::flight::do_put::DoPutMetadata;
use greptimedb_ingester::flight::{FlightEncoder, FlightMessage};
use greptimedb_ingester::Result;

fn create_record_batch(batch_num: usize) -> Vec<RecordBatch> {
    use arrow_array::{Float64Array, Int64Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    // Create schema
    let schema = Arc::new(Schema::new(vec![
        Field::new("aid", DataType::Int64, false),
        Field::new("aname", DataType::Utf8, false),
        Field::new("avalue", DataType::Float64, false),
        Field::new(
            "ts",
            DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("category", DataType::Utf8, false),
    ]));

    let mut record_batches = Vec::with_capacity(batch_num);

    // Get current timestamp in milliseconds
    let current_time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    for batch_idx in 0..batch_num {
        // Generate 1000 rows of data for each batch
        let mut ids = Vec::with_capacity(1000);
        let mut names = Vec::with_capacity(1000);
        let mut values = Vec::with_capacity(1000);
        let mut timestamps = Vec::with_capacity(1000);
        let mut categories = Vec::with_capacity(1000);

        for i in 0..1000 {
            let global_idx = batch_idx * 1000 + i;
            ids.push(global_idx as i64);
            names.push(format!("item_{}", global_idx));
            values.push(global_idx as f64 * 1.5);
            // Use realistic timestamps starting from current time, with 1 second intervals
            timestamps.push(current_time + (global_idx as i64 * 1000));
            categories.push(
                match global_idx % 3 {
                    0 => "A",
                    1 => "B",
                    _ => "C",
                }
                .to_string(),
            );
        }

        // Create arrays
        let id_array = Int64Array::from(ids);
        let name_array = StringArray::from(names);
        let value_array = Float64Array::from(values);
        let timestamp_array = TimestampMillisecondArray::from(timestamps);
        let category_array = StringArray::from(categories);

        // Create RecordBatch
        let record_batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(id_array),
                Arc::new(name_array),
                Arc::new(value_array),
                Arc::new(timestamp_array),
                Arc::new(category_array),
            ],
        )
        .unwrap();

        record_batches.push(record_batch);
    }

    record_batches
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = DbConfig::from_env();
    let urls = vec![config.endpoint.clone()];

    let grpc_client = Client::with_urls(&urls);

    let database = Database::new_with_dbname(&config.database, grpc_client);

    let record_batches = create_record_batch(10);

    let requests_count = record_batches.len();
    let schema = record_batches[0].schema();

    let stream = futures::stream::once(async move {
        let mut schema_data = FlightEncoder::default().encode(FlightMessage::Schema(schema));
        let metadata = DoPutMetadata::new(0);
        schema_data.app_metadata = serde_json::to_vec(&metadata).unwrap().into();
        // first message in "DoPut" stream should carry table name in flight descriptor
        schema_data.flight_descriptor = Some(FlightDescriptor {
            r#type: arrow_flight::flight_descriptor::DescriptorType::Path as i32,
            path: vec!["my_table".to_string()],
            ..Default::default()
        });
        schema_data
    })
    .chain(
        tokio_stream::iter(record_batches)
            .enumerate()
            .map(|(i, x)| {
                let mut encoder = FlightEncoder::default();
                let message = FlightMessage::RecordBatch(x);
                let mut data = encoder.encode(message);
                let metadata = DoPutMetadata::new((i + 1) as i64);
                data.app_metadata = serde_json::to_vec(&metadata).unwrap().into();
                data
            })
            .boxed(),
    )
    .boxed();

    let response_stream = database.do_put(stream).await.unwrap();

    let responses = response_stream.collect::<Vec<_>>().await;

    println!("responses: {:?}", responses);
    println!("requests_count: {}", requests_count);

    Ok(())
}
