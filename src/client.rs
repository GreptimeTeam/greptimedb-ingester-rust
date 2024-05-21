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

use crate::api::v1::greptime_database_client::GreptimeDatabaseClient;
use crate::api::v1::health_check_client::HealthCheckClient;
use crate::api::v1::HealthCheckRequest;
use crate::channel_manager::ChannelManager;
use parking_lot::RwLock;
use snafu::OptionExt;
use tonic::codec::CompressionEncoding;
use tonic::transport::Channel;

use crate::load_balance::{LoadBalance, Loadbalancer};
use crate::{error, Result};
use derive_builder::Builder;

const MAX_MESSAGE_SIZE: usize = 512 * 1024 * 1024;

pub(crate) struct DatabaseClient {
    pub(crate) inner: GreptimeDatabaseClient<Channel>,
}

#[derive(Clone, Debug, Default)]
pub struct Client {
    inner: Arc<Inner>,
}

#[derive(Default)]
pub struct ClientBuilder {
    channel_manager: ChannelManager,
    load_balance: Loadbalancer,
    compression: Compression,
    peers: Vec<String>,
}

impl ClientBuilder {
    pub fn channel_manager(mut self, channel_manager: ChannelManager) -> Self {
        self.channel_manager = channel_manager;
        self
    }

    pub fn load_balance(mut self, load_balance: Loadbalancer) -> Self {
        self.load_balance = load_balance;
        self
    }

    pub fn compression(mut self, compression: Compression) -> Self {
        self.compression = compression;
        self
    }

    pub fn peers<U, A>(mut self, peers: A) -> Self
    where
        U: AsRef<str>,
        A: AsRef<[U]>,
    {
        self.peers = normalize_urls(peers);
        self
    }

    pub fn build(self) -> Client {
        let inner = InnerBuilder::default()
            .channel_manager(self.channel_manager)
            .load_balance(self.load_balance)
            .compression(self.compression)
            .peers(self.peers)
            .build()
            .unwrap();
        Client {
            inner: Arc::new(inner),
        }
    }
}

#[derive(Debug, Clone)]
pub enum Compression {
    Gzip,
    Zstd,
    None,
}

impl Default for Compression {
    fn default() -> Self {
        Self::Gzip
    }
}

#[derive(Debug, Default, Builder)]
struct Inner {
    channel_manager: ChannelManager,
    #[builder(setter(custom))]
    peers: Arc<RwLock<Vec<String>>>,
    load_balance: Loadbalancer,
    compression: Compression,
}

impl InnerBuilder {
    pub fn peers(&mut self, peers: Vec<String>) -> &mut Self {
        self.peers = Some(Arc::new(RwLock::new(peers)));
        self
    }
}

impl Inner {
    fn set_peers(&self, peers: Vec<String>) {
        let mut guard = self.peers.write();
        *guard = peers;
    }

    fn get_peer(&self) -> Option<String> {
        let guard = self.peers.read();
        self.load_balance.get_peer(&guard).cloned()
    }
}

impl Client {
    #[deprecated(since = "0.1.0", note = "use `ClientBuilder` instead of this method")]
    pub fn new() -> Self {
        Default::default()
    }

    #[deprecated(since = "0.1.0", note = "use `ClientBuilder` instead of this method")]
    pub fn with_manager(channel_manager: ChannelManager) -> Self {
        let inner = InnerBuilder::default()
            .channel_manager(channel_manager)
            .build()
            .unwrap();
        Self {
            inner: Arc::new(inner),
        }
    }

    #[deprecated(since = "0.1.0", note = "use `ClientBuilder` instead of this method")]
    pub fn with_urls<U, A>(urls: A) -> Self
    where
        U: AsRef<str>,
        A: AsRef<[U]>,
    {
        ClientBuilder::default().peers(urls).build()
    }

    #[deprecated(since = "0.1.0", note = "use `ClientBuilder` instead of this method")]
    pub fn with_manager_and_urls<U, A>(channel_manager: ChannelManager, urls: A) -> Self
    where
        U: AsRef<str>,
        A: AsRef<[U]>,
    {
        let inner = InnerBuilder::default()
            .channel_manager(channel_manager)
            .peers(normalize_urls(urls))
            .build()
            .unwrap();

        Self {
            inner: Arc::new(inner),
        }
    }

    #[deprecated(since = "0.1.0", note = "use `ClientBuilder` instead of this method")]
    pub fn start<U, A>(&self, urls: A)
    where
        U: AsRef<str>,
        A: AsRef<[U]>,
    {
        let urls: Vec<String> = normalize_urls(urls);

        self.inner.set_peers(urls);
    }

    fn find_channel(&self) -> Result<(String, Channel)> {
        let addr = self
            .inner
            .get_peer()
            .context(error::IllegalGrpcClientStateSnafu {
                err_msg: "No available peer found",
            })?;

        let channel = self.inner.channel_manager.get(&addr)?;

        Ok((addr, channel))
    }

    pub(crate) fn make_database_client(&self) -> Result<DatabaseClient> {
        let (_, channel) = self.find_channel()?;
        let mut client = GreptimeDatabaseClient::new(channel)
            .max_decoding_message_size(MAX_MESSAGE_SIZE)
            .accept_compressed(CompressionEncoding::Gzip)
            .accept_compressed(CompressionEncoding::Zstd);
        match self.inner.compression {
            Compression::Gzip => {
                client = client.send_compressed(CompressionEncoding::Gzip);
            }
            Compression::Zstd => {
                client = client.send_compressed(CompressionEncoding::Zstd);
            }
            Compression::None => {}
        }
        Ok(DatabaseClient { inner: client })
    }

    pub async fn health_check(&self) -> Result<()> {
        let (_, channel) = self.find_channel()?;
        let mut client = HealthCheckClient::new(channel);
        client.health_check(HealthCheckRequest {}).await?;
        Ok(())
    }
}

fn normalize_urls<U, A>(urls: A) -> Vec<String>
where
    U: AsRef<str>,
    A: AsRef<[U]>,
{
    urls.as_ref()
        .iter()
        .map(|peer| peer.as_ref().to_string())
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::Inner;
    use crate::load_balance::Loadbalancer;

    fn mock_peers() -> Vec<String> {
        vec![
            "127.0.0.1:3001".to_string(),
            "127.0.0.1:3002".to_string(),
            "127.0.0.1:3003".to_string(),
        ]
    }

    #[tokio::test]
    async fn test_inner() {
        let inner = Inner::default();

        assert!(matches!(
            inner.load_balance,
            Loadbalancer::Random(crate::load_balance::Random)
        ));
        assert!(inner.get_peer().is_none());

        let peers = mock_peers();
        inner.set_peers(peers.clone());
        let all: HashSet<String> = peers.into_iter().collect();

        for _ in 0..20 {
            assert!(all.contains(&inner.get_peer().unwrap()));
        }
    }
}
