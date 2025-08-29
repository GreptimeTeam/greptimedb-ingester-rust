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

use std::pin::Pin;
use std::str::FromStr;

use arrow_flight::FlightData;
use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use futures::future;
use futures_util::{Stream, StreamExt, TryStreamExt};
use greptime_proto::v1::auth_header::AuthScheme;
use greptime_proto::v1::greptime_database_client::GreptimeDatabaseClient;
use greptime_proto::v1::greptime_request::Request;
use greptime_proto::v1::{
    greptime_response, AffectedRows, AuthHeader, Basic, DeleteRequests, GreptimeRequest,
    RequestHeader, RowInsertRequests,
};
use snafu::{OptionExt, ResultExt};
use tonic::metadata::{AsciiMetadataKey, AsciiMetadataValue, MetadataMap, MetadataValue};
use tonic::transport::Channel;

use crate::client::Client;
use crate::error::{self, IllegalDatabaseResponseSnafu};
use crate::flight::do_put::DoPutResponse;
use crate::Result;

type FlightDataStream = Pin<Box<dyn Stream<Item = FlightData> + Send>>;

type DoPutResponseStream = Pin<Box<dyn Stream<Item = Result<DoPutResponse>>>>;

/// The Client for GreptimeDB Database API.
#[derive(Clone, Debug, Default)]
pub struct Database {
    // The dbname follows naming rule as out mysql, postgres and http
    // protocol. The server treat dbname in priority of catalog/schema.
    dbname: String,

    client: Client,
    auth_header: Option<AuthHeader>,
}

pub struct DatabaseClient {
    pub inner: GreptimeDatabaseClient<Channel>,
}

fn make_database_client(client: &Client) -> Result<DatabaseClient> {
    let (_, channel) = client.find_channel()?;
    let mut inner = GreptimeDatabaseClient::new(channel)
        .max_decoding_message_size(client.max_grpc_recv_message_size())
        .max_encoding_message_size(client.max_grpc_send_message_size());
    if let Some(send_compression) = client.send_compression() {
        inner = inner.send_compressed(send_compression);
    }
    if let Some(accept_compression) = client.accept_compression() {
        inner = inner.accept_compressed(accept_compression);
    }
    Ok(DatabaseClient { inner })
}

impl Database {
    /// Create database service client using dbname.
    ///
    /// This API is designed for external usage. `dbname` is:
    ///
    /// - the name of database when using GreptimeDB standalone or cluster
    /// - the name provided by GreptimeCloud or other multi-tenant GreptimeDB
    ///   environment
    pub fn new_with_dbname(dbname: impl Into<String>, client: Client) -> Self {
        Self {
            dbname: dbname.into(),
            client,
            auth_header: None,
        }
    }

    /// Get associated dbname of this client
    pub fn dbname(&self) -> &String {
        &self.dbname
    }

    /// Update dbname of this client
    pub fn set_dbname(&mut self, dbname: impl Into<String>) {
        self.dbname = dbname.into();
    }

    /// Set authentication information
    pub fn set_auth(&mut self, auth: AuthScheme) {
        self.auth_header = Some(AuthHeader {
            auth_scheme: Some(auth),
        });
    }

    /// Write Row based insert requests to GreptimeDB and get rows written
    pub async fn insert(&self, requests: RowInsertRequests) -> Result<u32> {
        self.handle(Request::RowInserts(requests), &[]).await
    }

    /// Write Row based insert requests with hint to GreptimeDB and get rows written
    pub async fn insert_with_hints(
        &self,
        requests: RowInsertRequests,
        hints: &[(&str, &str)],
    ) -> Result<u32> {
        self.handle(Request::RowInserts(requests), hints).await
    }

    /// Issue a delete to database
    pub async fn delete(&self, request: DeleteRequests) -> Result<u32> {
        self.handle(Request::Deletes(request), &[]).await
    }

    /// Ingest a stream of [RecordBatch]es that belong to a table, using Arrow Flight's "`DoPut`"
    /// method. The return value is also a stream, produces [DoPutResponse]s.
    pub async fn do_put(&self, stream: FlightDataStream) -> Result<DoPutResponseStream> {
        let mut request = tonic::Request::new(stream);

        if let Some(AuthHeader {
            auth_scheme: Some(AuthScheme::Basic(Basic { username, password })),
        }) = &self.auth_header
        {
            let auth_str = format!("Basic {username}:{password}");
            let encoded = BASE64_STANDARD.encode(auth_str);
            let value =
                MetadataValue::from_str(&encoded).context(error::InvalidTonicMetadataValueSnafu)?;
            request.metadata_mut().insert("x-greptime-auth", value);
        }

        request.metadata_mut().insert(
            "x-greptime-db-name",
            MetadataValue::from_str(&self.dbname).context(error::InvalidTonicMetadataValueSnafu)?,
        );

        let mut client = self.client.make_flight_client()?;
        let response = client.mut_inner().do_put(request).await?;
        let response = response
            .into_inner()
            .map_err(Into::into)
            .and_then(|x| future::ready(DoPutResponse::try_from(x)))
            .boxed();
        Ok(response)
    }

    async fn handle(&self, request: Request, hints: &[(&str, &str)]) -> Result<u32> {
        let mut client = make_database_client(&self.client)?;
        let request = self.to_rpc_request(request);
        let mut request = tonic::Request::new(request);
        if !hints.is_empty() {
            Self::put_hints(request.metadata_mut(), hints)?;
        }

        let response = client
            .inner
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

    fn put_hints(metadata: &mut MetadataMap, hints: &[(&str, &str)]) -> Result<()> {
        if hints.is_empty() {
            return Ok(());
        }

        let hint_strings: Vec<String> = hints.iter().map(|(k, v)| format!("{k}={v}")).collect();
        let value = hint_strings.join(",");

        let key = AsciiMetadataKey::from_static("x-greptime-hints");
        let value =
            AsciiMetadataValue::from_str(&value).context(error::InvalidTonicMetadataValueSnafu)?;
        metadata.insert(key, value);
        Ok(())
    }
}
