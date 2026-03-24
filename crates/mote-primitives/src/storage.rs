use alloy_primitives::{B256, U256, keccak256};

const STORAGE_PREFIX: &[u8] = b"moteEntityMetaData";

/// Compute the trie storage key for an entity's metadata slot (slot 1).
/// `keccak256("moteEntityMetaData" || entity_key)`
pub fn entity_storage_key(entity_key: &B256) -> B256 {
    let mut preimage = Vec::with_capacity(STORAGE_PREFIX.len() + 32);
    preimage.extend_from_slice(STORAGE_PREFIX);
    preimage.extend_from_slice(entity_key.as_slice());
    keccak256(&preimage)
}

/// Compute the trie storage key for an entity's content hash slot (slot 2).
/// This is `entity_storage_key + 1`.
pub fn entity_content_hash_key(entity_key: &B256) -> B256 {
    let meta_key = entity_storage_key(entity_key);
    let num = U256::from_be_bytes(meta_key.0) + U256::from(1);
    B256::from(num.to_be_bytes())
}

/// Compute content hash from raw wire bytes.
///
/// **Actual formula:**
/// `keccak256(rlp(payload) || rlp(content_type) || rlp(string_annotations) || rlp(numeric_annotations))`
///
/// All four inputs are contiguous sub-slices of the transaction calldata's RLP structure.
/// They must NEVER be decoded and re-encoded — non-canonical RLP encoding would
/// produce different hashes on different nodes.
pub fn compute_content_hash_from_raw(
    payload_rlp: &[u8],
    content_type_rlp: &[u8],
    string_annotations_rlp: &[u8],
    numeric_annotations_rlp: &[u8],
) -> B256 {
    let mut preimage = Vec::with_capacity(
        payload_rlp.len()
            + content_type_rlp.len()
            + string_annotations_rlp.len()
            + numeric_annotations_rlp.len(),
    );
    preimage.extend_from_slice(payload_rlp);
    preimage.extend_from_slice(content_type_rlp);
    preimage.extend_from_slice(string_annotations_rlp);
    preimage.extend_from_slice(numeric_annotations_rlp);
    keccak256(&preimage)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn storage_key_is_deterministic() {
        let entity_key = B256::repeat_byte(0x42);
        let key1 = entity_storage_key(&entity_key);
        let key2 = entity_storage_key(&entity_key);
        assert_eq!(key1, key2);
    }

    #[test]
    fn storage_key_matches_spec() {
        let entity_key = B256::repeat_byte(0x01);
        let mut preimage = Vec::new();
        preimage.extend_from_slice(b"moteEntityMetaData");
        preimage.extend_from_slice(entity_key.as_slice());
        let expected = keccak256(&preimage);
        assert_eq!(entity_storage_key(&entity_key), expected);
    }

    #[test]
    fn content_hash_from_raw_bytes() {
        let payload_rlp = &[0x85, b'h', b'e', b'l', b'l', b'o'];
        let content_type_rlp = &[
            0x8a, b't', b'e', b'x', b't', b'/', b'p', b'l', b'a', b'i', b'n',
        ];
        let string_annotations_rlp = &[0xc0];
        let numeric_annotations_rlp = &[0xc0];

        let hash = compute_content_hash_from_raw(
            payload_rlp,
            content_type_rlp,
            string_annotations_rlp,
            numeric_annotations_rlp,
        );

        let mut expected_preimage = Vec::new();
        expected_preimage.extend_from_slice(payload_rlp);
        expected_preimage.extend_from_slice(content_type_rlp);
        expected_preimage.extend_from_slice(string_annotations_rlp);
        expected_preimage.extend_from_slice(numeric_annotations_rlp);
        assert_eq!(hash, keccak256(&expected_preimage));
    }

    #[test]
    fn content_hash_slot_offset() {
        let entity_key = B256::repeat_byte(0x01);
        let meta_key = entity_storage_key(&entity_key);
        let content_key = entity_content_hash_key(&entity_key);

        let meta_num = U256::from_be_bytes(meta_key.0);
        let content_num = U256::from_be_bytes(content_key.0);
        assert_eq!(content_num, meta_num + U256::from(1));
    }
}
