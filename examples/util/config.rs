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

//! Database configuration utilities for examples
//!
//! This module provides utilities for loading GreptimeDB connection configuration
//! from environment variables and configuration files.

use std::fs;
use std::io;

/// Database connection configuration
#[derive(Debug, Clone)]
pub struct DbConfig {
    pub endpoint: String,
    pub dbname: String,
}

impl Default for DbConfig {
    fn default() -> Self {
        Self {
            endpoint: "localhost:4001".to_string(),
            dbname: "public".to_string(),
        }
    }
}

impl DbConfig {
    /// Load configuration from environment variables, falling back to file and defaults
    ///
    /// Environment variables:
    /// - `GREPTIMEDB_ENDPOINT`: Database endpoint (default: localhost:4001)
    /// - `GREPTIMEDB_DBNAME`: Database name (default: public)
    pub fn from_env() -> Self {
        let config = Self::from_file().unwrap_or_default();
        Self {
            endpoint: std::env::var("GREPTIMEDB_ENDPOINT").unwrap_or(config.endpoint),
            dbname: std::env::var("GREPTIMEDB_DBNAME").unwrap_or(config.dbname),
        }
    }

    /// Load configuration from a TOML file
    ///
    /// Expected file format:
    /// ```toml
    /// endpoints = ["127.0.0.1:4001"]
    /// dbname = "public"
    /// ```
    pub fn from_file() -> io::Result<Self> {
        Self::from_file_path("examples/db-connection.toml")
    }

    /// Load configuration from a specific file path
    pub fn from_file_path(path: &str) -> io::Result<Self> {
        let content = fs::read_to_string(path)?;
        let mut endpoint = String::new();
        let mut database = String::new();

        for line in content.lines() {
            let line = line.trim();
            if line.starts_with("endpoints") {
                endpoint = line
                    .split('=')
                    .nth(1)
                    .and_then(|s| {
                        s.trim()
                            .trim_matches(&['[', ']', '"'][..])
                            .split(',')
                            .next()
                    })
                    .unwrap_or("127.0.0.1:4001")
                    .to_string();
            } else if line.starts_with("dbname") {
                database = line
                    .split('=')
                    .nth(1)
                    .map(|s| s.trim().trim_matches('"'))
                    .unwrap_or("public")
                    .to_string();
            }
        }

        Ok(Self {
            endpoint,
            dbname: database,
        })
    }

    /// Display current configuration
    pub fn display(&self) {
        println!("Using GreptimeDB endpoint: {}", self.endpoint);
        println!("Using dbname: {}", self.dbname);
    }
}
