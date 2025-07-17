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

//! Benchmark modules
//!
//! This module provides benchmark framework and data providers for GreptimeDB performance testing.

pub mod benchmark_runner;
pub mod log_table_data_provider;
pub mod log_text_helper;
pub mod table_data_provider;

// Re-export main components
pub use benchmark_runner::{show_benchmark_result, BenchmarkConfig};
pub use log_table_data_provider::LogTableDataProvider;
