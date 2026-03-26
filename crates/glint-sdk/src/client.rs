use alloy_primitives::B256;

use crate::rpc::{EntityInfo, GlintRpcClient};

// TODO: add write methods once alloy-signer is a workspace dep
#[derive(Debug, Clone)]
pub struct GlintClient {
    rpc: GlintRpcClient,
}

impl GlintClient {
    pub fn new(url: &str) -> eyre::Result<Self> {
        let rpc = GlintRpcClient::new(url)?;
        Ok(Self { rpc })
    }

    pub async fn get_entity(&self, key: B256) -> eyre::Result<Option<EntityInfo>> {
        self.rpc.get_entity(key).await
    }

    pub async fn get_entity_count(&self) -> eyre::Result<u64> {
        self.rpc.get_entity_count().await
    }

    pub const fn rpc(&self) -> &GlintRpcClient {
        &self.rpc
    }
}
