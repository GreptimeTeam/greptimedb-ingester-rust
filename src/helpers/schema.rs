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
