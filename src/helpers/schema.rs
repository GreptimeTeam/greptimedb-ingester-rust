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

use arrow_schema::extension::{EXTENSION_TYPE_METADATA_KEY, EXTENSION_TYPE_NAME_KEY};

use crate::api::v1::*;

pub fn tag(name: &str, datatype: ColumnDataType) -> ColumnSchema {
    ColumnSchema {
        column_name: name.to_string(),
        semantic_type: SemanticType::Tag as i32,
        datatype: datatype as i32,
        ..Default::default()
    }
}

pub fn timestamp(name: &str, datatype: ColumnDataType) -> ColumnSchema {
    ColumnSchema {
        column_name: name.to_string(),
        semantic_type: SemanticType::Timestamp as i32,
        datatype: datatype as i32,
        ..Default::default()
    }
}

pub fn field(name: &str, datatype: ColumnDataType) -> ColumnSchema {
    ColumnSchema {
        column_name: name.to_string(),
        semantic_type: SemanticType::Field as i32,
        datatype: datatype as i32,
        ..Default::default()
    }
}

/// A JSON2 field for row inserts.
pub fn json2_field(name: &str) -> ColumnSchema {
    ColumnSchema {
        // Match SQL JSON2 column metadata so auto-created tables use the JSON2 builder.
        options: Some(ColumnOptions {
            options: [
                (EXTENSION_TYPE_NAME_KEY.into(), "greptime.json2".into()),
                (
                    EXTENSION_TYPE_METADATA_KEY.into(),
                    r#"{"json_settings":{"type_hints":[],"max_auto_expanded_paths":100},"layout_version":2}"#.into(),
                ),
            ].into(),
        }),
        datatype_extension: Some(ColumnDataTypeExtension {
            type_ext: Some(column_data_type_extension::TypeExt::JsonNativeType(
                Box::new(JsonNativeTypeExtension {
                    datatype: ColumnDataType::Json as i32,
                    datatype_extension: None,
                }),
            )),
        }),
        ..field(name, ColumnDataType::Json)
    }
}
