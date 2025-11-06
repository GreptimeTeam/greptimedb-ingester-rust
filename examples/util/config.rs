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

use serde::Deserialize;
use std::fs;
use std::io;

/// Database connection configuration
#[derive(Clone)]
pub struct DbConfig {
    pub endpoint: String,
    pub dbname: String,
    pub username: Option<String>,
    pub password: Option<String>,
}

impl std::fmt::Debug for DbConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DbConfig")
            .field("endpoint", &self.endpoint)
            .field("dbname", &self.dbname)
            .field("username", &self.username)
            .field("password", &"******")
            .finish()
    }
}

impl Default for DbConfig {
    fn default() -> Self {
        Self {
            endpoint: "localhost:4001".to_string(),
            dbname: "public".to_string(),
            username: None,
            password: None,
        }
    }
}

#[derive(Debug, Deserialize)]
struct ConfigFile {
    database: Option<DatabaseConfig>,
}

#[derive(Deserialize)]
struct DatabaseConfig {
    endpoints: Option<Vec<String>>,
    dbname: Option<String>,
    username: Option<String>,
    password: Option<String>,
}

impl std::fmt::Debug for DatabaseConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DatabaseConfig")
            .field("endpoints", &self.endpoints)
            .field("dbname", &self.dbname)
            .field("username", &self.username)
            .field("password", &"******")
            .finish()
    }
}

impl DbConfig {
    /// Load configuration from environment variables, falling back to file and defaults
    ///
    /// Environment variables:
    /// - `GREPTIMEDB_ENDPOINT`: Database endpoint (default: localhost:4001)
    /// - `GREPTIMEDB_DBNAME`: Database name (default: public)
    /// - `GREPTIMEDB_USERNAME`: Username for authentication
    /// - `GREPTIMEDB_PASSWORD`: Password for authentication
    pub fn from_env() -> Self {
        let file_config = Self::from_file().unwrap_or_default();

        Self {
            endpoint: std::env::var("GREPTIMEDB_ENDPOINT").unwrap_or(file_config.endpoint),
            dbname: std::env::var("GREPTIMEDB_DBNAME").unwrap_or(file_config.dbname),
            username: std::env::var("GREPTIMEDB_USERNAME")
                .ok()
                .or(file_config.username),
            password: std::env::var("GREPTIMEDB_PASSWORD")
                .ok()
                .or(file_config.password),
        }
    }

    /// Load configuration from a TOML file
    ///
    /// Expected file format:
    /// ```toml
    /// [database]
    /// endpoints = ["127.0.0.1:4001"]
    /// dbname = "public"
    /// username = "user"
    /// password = "pass"
    /// ```
    pub fn from_file() -> io::Result<Self> {
        Self::from_file_path("examples/db-connection.toml")
    }

    /// Load configuration from a specific file path
    pub fn from_file_path(path: &str) -> io::Result<Self> {
        let content = fs::read_to_string(path)?;
        let config_file: ConfigFile = toml::from_str(&content)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;

        let mut config = DbConfig::default();

        if let Some(db_config) = config_file.database {
            if let Some(endpoints) = db_config.endpoints {
                if let Some(endpoint) = endpoints.first() {
                    config.endpoint = endpoint.clone();
                }
            }
            if let Some(dbname) = db_config.dbname {
                config.dbname = dbname;
            }
            if let Some(username) = db_config.username {
                config.username = Some(username.clone());
            }
            if let Some(password) = db_config.password {
                config.password = Some(password.clone());
            }
        }

        Ok(config)
    }

    /// Display current configuration
    pub fn display(&self) {
        println!("Using GreptimeDB endpoint: {}", self.endpoint);
        println!("Using dbname: {}", self.dbname);
        if let Some(username) = &self.username {
            println!("Using username: {username}");
        }
        if self.password.is_some() {
            println!("Using password: ******");
        }
    }
}
