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

use greptime_proto::v1::greptime_request::Request;
use greptime_proto::v1::{greptime_database_client::GreptimeDatabaseClient, InsertRequest};
use greptime_proto::v1::{
    greptime_response, AffectedRows, AuthHeader, GreptimeRequest, GreptimeResponse, InsertRequests,
    RequestHeader,
};
use snafu::OptionExt;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;
use tonic::{Response, Status};

use crate::error::Result;
use crate::error::{self, IllegalDatabaseResponseSnafu};

pub struct StreamInsertor {
    sender: mpsc::Sender<GreptimeRequest>,

    auth_header: Option<AuthHeader>,

    dbname: String,

    join: JoinHandle<std::result::Result<Response<GreptimeResponse>, Status>>,
}

impl StreamInsertor {
    pub fn new(
        mut client: GreptimeDatabaseClient<Channel>,
        dbname: String,
        auth_header: Option<AuthHeader>,
    ) -> StreamInsertor {
        let (send, recv) = tokio::sync::mpsc::channel(1024);

        let join: JoinHandle<std::result::Result<Response<GreptimeResponse>, Status>> =
            tokio::spawn(async move {
                let recv_stream = ReceiverStream::new(recv);
                client.handle_requests(recv_stream).await
            });

        StreamInsertor {
            sender: send,
            auth_header,
            dbname,
            join,
        }
    }

    pub async fn insert(&self, requests: Vec<InsertRequest>) -> Result<()> {
        let inserts = InsertRequests { inserts: requests };
        let request = self.to_rpc_request(Request::Inserts(inserts));

        self.sender.send(request).await.map_err(|e| {
            error::ClientStreamingSnafu {
                err_msg: e.to_string(),
            }
            .build()
        })
    }

    pub async fn finish(self) -> Result<u32> {
        drop(self.sender);

        let response = self.join.await.unwrap()?;

        let response = response
            .into_inner()
            .response
            .context(IllegalDatabaseResponseSnafu {
                err_msg: "GreptimeResponse is empty",
            })?;

        let greptime_response::Response::AffectedRows(AffectedRows { value }) = response;

        Ok(value)
    }

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
