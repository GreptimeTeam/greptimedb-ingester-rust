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

pub mod do_put;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use arrow_flight::{FlightData, SchemaAsIpc};
use arrow_ipc::writer;

use crate::bulk::CompressionType;

#[derive(Debug, Clone)]
pub enum FlightMessage {
    Schema(SchemaRef),
    RecordBatch(RecordBatch),
}

pub struct FlightEncoder {
    write_options: writer::IpcWriteOptions,
    data_gen: writer::IpcDataGenerator,
    dictionary_tracker: writer::DictionaryTracker,
}

impl Default for FlightEncoder {
    fn default() -> Self {
        let write_options = writer::IpcWriteOptions::default()
            .try_with_compression(Some(arrow::ipc::CompressionType::LZ4_FRAME))
            .unwrap();

        Self {
            write_options,
            data_gen: writer::IpcDataGenerator::default(),
            dictionary_tracker: writer::DictionaryTracker::new(false),
        }
    }
}

impl FlightEncoder {
    /// Creates new [FlightEncoder] with specified compression type.
    pub fn with_compression(compression: CompressionType) -> Self {
        let arrow_compression = match compression {
            CompressionType::None => None,
            CompressionType::Lz4 => Some(arrow::ipc::CompressionType::LZ4_FRAME),
            CompressionType::Zstd => Some(arrow::ipc::CompressionType::ZSTD),
        };

        let write_options = writer::IpcWriteOptions::default()
            .try_with_compression(arrow_compression)
            .unwrap();

        Self {
            write_options,
            data_gen: writer::IpcDataGenerator::default(),
            dictionary_tracker: writer::DictionaryTracker::new(false),
        }
    }

    /// Creates new [FlightEncoder] with compression disabled.
    /// This is a convenience method for `with_compression(CompressionType::None)`.
    pub fn without_compression() -> Self {
        Self::with_compression(CompressionType::None)
    }

    pub fn encode(&mut self, flight_message: FlightMessage) -> FlightData {
        match flight_message {
            FlightMessage::Schema(schema) => SchemaAsIpc::new(&schema, &self.write_options).into(),
            FlightMessage::RecordBatch(record_batch) => {
                let (encoded_dictionaries, encoded_batch) = self
                    .data_gen
                    .encoded_batch(
                        &record_batch,
                        &mut self.dictionary_tracker,
                        &self.write_options,
                    )
                    .expect("DictionaryTracker configured above to not fail on replacement");

                // TODO(LFC): Handle dictionary as FlightData here, when we supported Arrow's Dictionary DataType.
                // Currently we don't have a datatype corresponding to Arrow's Dictionary DataType,
                // so there won't be any "dictionaries" here. Assert to be sure about it, and
                // perform a "testing guard" in case we forgot to handle the possible "dictionaries"
                // here in the future.
                debug_assert_eq!(encoded_dictionaries.len(), 0);

                encoded_batch.into()
            }
        }
    }
}
