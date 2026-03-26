use alloy_primitives::{B256, keccak256};
use std::sync::LazyLock;

pub static USED_SLOTS_KEY: LazyLock<B256> = LazyLock::new(|| keccak256(b"moteUsedSlots"));

/// Metadata + content hash.
pub const SLOTS_PER_ENTITY: u64 = 2;
