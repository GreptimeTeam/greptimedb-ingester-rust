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
    InsertRequests, RequestHeader,
};
use crate::stream_insert::StreamInsertor;

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
            auth_header: None,
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

    pub async fn insert(&self, requests: Vec<InsertRequest>) -> Result<u32> {
        self.handle(Request::Inserts(InsertRequests { inserts: requests }))
            .await
    }

    pub fn streaming_insertor(&self) -> Result<StreamInsertor> {
        self.streaming_insertor_with_channel_size(1024)
    }

    pub fn streaming_insertor_with_channel_size(
        &self,
        channel_size: usize,
    ) -> Result<StreamInsertor> {
        let client = self.client.make_database_client()?.inner;

        let stream_inserter = StreamInsertor::new(
            client,
            self.dbname().to_string(),
            self.auth_header.clone(),
            channel_size,
        );

        Ok(stream_inserter)
    }

    pub async fn delete(&self, request: DeleteRequest) -> Result<u32> {
        self.handle(Request::Delete(request)).await
    }

    async fn handle(&self, request: Request) -> Result<u32> {
        let mut client = self.client.make_database_client()?.inner;
        let request = self.to_rpc_request(request);
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

    #[inline]
    fn to_rpc_request(&self, request: Request) -> GreptimeRequest {
        GreptimeRequest {
            header: Some(RequestHeader {
                authorization: self.auth_header.clone(),
                dbname: self.dbname.clone(),
                ..Default::default()
            }),
            request: Some(request),
        }
    }
}

#[cfg(test)]
mod tests {}
