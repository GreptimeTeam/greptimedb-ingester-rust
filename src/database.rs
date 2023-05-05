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

use crate::api::v1::auth_header::AuthScheme;
use crate::api::v1::greptime_request::Request;
use crate::api::v1::{
    greptime_response, AffectedRows, AuthHeader, DeleteRequest, GreptimeRequest, InsertRequest,
    RequestHeader,
};

use snafu::OptionExt;

use crate::error::IllegalDatabaseResponseSnafu;
use crate::{Client, Result};

#[derive(Clone, Debug, Default)]
pub struct Database {
    // The dbname follows naming rule as out mysql, postgres and http
    // protocol. The server treat dbname in priority of catalog/schema.
    dbname: String,

    client: Client,
    auth_header: Option<AuthHeader>,
}

impl Database {
    /// Create database service client using dbname.
    ///
    /// This API is designed for external usage. `dbname` is:
    ///
    /// - the name of database when using GreptimeDB standalone or cluster
    /// - the name provided by GreptimeCloud or other multi-tenant GreptimeDB
    /// environment
    pub fn new_with_dbname(dbname: impl Into<String>, client: Client) -> Self {
        Self {
            dbname: dbname.into(),
            client,
            ..Default::default()
        }
    }

    pub fn dbname(&self) -> &String {
        &self.dbname
    }

    pub fn set_dbname(&mut self, dbname: impl Into<String>) {
        self.dbname = dbname.into();
    }

    pub fn set_auth(&mut self, auth: AuthScheme) {
        self.auth_header = Some(AuthHeader {
            auth_scheme: Some(auth),
        });
    }

    pub async fn insert(&self, request: InsertRequest) -> Result<u32> {
        self.handle(Request::Insert(request)).await
    }

    pub async fn delete(&self, request: DeleteRequest) -> Result<u32> {
        self.handle(Request::Delete(request)).await
    }

    async fn handle(&self, request: Request) -> Result<u32> {
        let mut client = self.client.make_database_client()?.inner;
        let request = GreptimeRequest {
            header: Some(RequestHeader {
                authorization: self.auth_header.clone(),
                dbname: self.dbname.clone(),
                ..Default::default()
            }),
            request: Some(request),
        };
        let response = client
            .handle(request)
            .await?
            .into_inner()
            .response
            .context(IllegalDatabaseResponseSnafu {
                err_msg: "GreptimeResponse is empty",
            })?;
        let greptime_response::Response::AffectedRows(AffectedRows { value }) = response;
        Ok(value)
    }
}

#[cfg(test)]
mod tests {}
