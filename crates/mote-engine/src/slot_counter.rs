use alloy_primitives::{B256, keccak256};

pub fn used_slots_key() -> B256 {
    keccak256(b"moteUsedSlots")
}

/// Metadata + content hash.
pub const SLOTS_PER_ENTITY: u64 = 2;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn used_slots_key_is_deterministic() {
        assert_eq!(used_slots_key(), used_slots_key());
    }

    #[test]
    fn used_slots_key_not_zero() {
        assert_ne!(used_slots_key(), B256::ZERO);
    }
}
