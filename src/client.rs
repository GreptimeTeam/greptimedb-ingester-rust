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

use std::sync::Arc;

use arrow_flight::flight_service_client::FlightServiceClient;
use greptime_proto::v1::health_check_client::HealthCheckClient;
use greptime_proto::v1::HealthCheckRequest;
use parking_lot::RwLock;
use snafu::OptionExt;
use tonic::codec::CompressionEncoding;
use tonic::transport::Channel;

use crate::channel_manager::{ChannelConfig, ChannelManager, ClientTlsOption};
use crate::load_balance::{LoadBalance, Loadbalancer};
use crate::{error, Result};

pub struct FlightClient {
    addr: String,
    client: FlightServiceClient<Channel>,
}

impl FlightClient {
    pub fn addr(&self) -> &str {
        &self.addr
    }

    pub fn mut_inner(&mut self) -> &mut FlightServiceClient<Channel> {
        &mut self.client
    }
}

#[derive(Clone, Debug, Default)]
pub struct Client {
    inner: Arc<Inner>,
}

impl Client {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn with_urls<U, A>(urls: A) -> Self
    where
        U: AsRef<str>,
        A: AsRef<[U]>,
    {
        Self::with_manager_and_urls(ChannelManager::new(), urls)
    }

    pub fn with_tls_and_urls<U, A>(urls: A, client_tls: ClientTlsOption) -> Result<Self>
    where
        U: AsRef<str>,
        A: AsRef<[U]>,
    {
        let channel_config = ChannelConfig::default().client_tls_config(client_tls);
        let channel_manager = ChannelManager::with_tls_config(channel_config)?;
        Ok(Self::with_manager_and_urls(channel_manager, urls))
    }

    pub fn with_manager_and_urls<U, A>(channel_manager: ChannelManager, urls: A) -> Self
    where
        U: AsRef<str>,
        A: AsRef<[U]>,
    {
        let inner = Inner::with_manager(channel_manager);
        let urls: Vec<String> = urls
            .as_ref()
            .iter()
            .map(|peer| peer.as_ref().to_string())
            .collect();
        inner.set_peers(urls);
        Self {
            inner: Arc::new(inner),
        }
    }

    pub fn start<U, A>(&self, urls: A)
    where
        U: AsRef<str>,
        A: AsRef<[U]>,
    {
        let urls: Vec<String> = urls
            .as_ref()
            .iter()
            .map(|peer| peer.as_ref().to_string())
            .collect();

        self.inner.set_peers(urls);
    }

    pub fn find_channel(&self) -> Result<(String, Channel)> {
        let addr = self
            .inner
            .get_peer()
            .context(error::IllegalGrpcClientStateSnafu {
                err_msg: "No available peer found",
            })?;

        let channel = self.inner.channel_manager.get(&addr)?;
        Ok((addr, channel))
    }

    pub fn max_grpc_recv_message_size(&self) -> usize {
        self.inner.channel_manager.config().max_recv_message_size as usize
    }

    pub fn max_grpc_send_message_size(&self) -> usize {
        self.inner.channel_manager.config().max_send_message_size as usize
    }

    pub fn send_compression(&self) -> Option<CompressionEncoding> {
        if self.inner.channel_manager.config().send_compression {
            Some(CompressionEncoding::Zstd)
        } else {
            None
        }
    }

    pub fn accept_compression(&self) -> Option<CompressionEncoding> {
        if self.inner.channel_manager.config().accept_compression {
            Some(CompressionEncoding::Zstd)
        } else {
            None
        }
    }

    pub fn make_flight_client(&self) -> Result<FlightClient> {
        let (addr, channel) = self.find_channel()?;

        let mut client = FlightServiceClient::new(channel)
            .max_decoding_message_size(self.max_grpc_recv_message_size())
            .max_encoding_message_size(self.max_grpc_send_message_size());
        if let Some(send_compression) = self.send_compression() {
            client = client.send_compressed(send_compression);
        }
        if let Some(accept_compression) = self.accept_compression() {
            client = client.accept_compressed(accept_compression);
        }

        Ok(FlightClient { addr, client })
    }

    pub async fn health_check(&self) -> Result<()> {
        let (_, channel) = self.find_channel()?;
        let mut client = HealthCheckClient::new(channel);
        let _ = client.health_check(HealthCheckRequest {}).await?;
        Ok(())
    }
}

#[derive(Debug, Default)]
struct Inner {
    channel_manager: ChannelManager,
    peers: Arc<RwLock<Vec<String>>>,
    load_balance: Loadbalancer,
}

impl Inner {
    fn with_manager(channel_manager: ChannelManager) -> Self {
        Self {
            channel_manager,
            ..Default::default()
        }
    }

    fn set_peers(&self, peers: Vec<String>) {
        let mut guard = self.peers.write();
        *guard = peers;
    }

    fn get_peer(&self) -> Option<String> {
        let guard = self.peers.read();
        self.load_balance.get_peer(&guard).cloned()
    }
}
