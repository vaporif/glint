use alloy_primitives::{address, Address};

/// Processor address: 0x00000000000000000000000000000000006d6f7465
/// ASCII "mote" (0x6d6f7465), right-aligned in a 20-byte address.
///
/// NOTE: The spec v0.1 lists 0x0000000000000000000000000000000000006d6f7465
/// (42 hex digits = 21 bytes). This is a spec typo — Ethereum addresses are
/// 20 bytes (40 hex digits). The correct address drops one leading zero byte.
/// The address!() macro enforces 20 bytes at compile time.
pub const PROCESSOR_ADDRESS: Address = address!("000000000000000000000000000000006d6f7465");

/// Maximum blocks-to-live. ~1 week at 2s blocks.
pub const MAX_BTL: u64 = 302_400;

/// Maximum CRUD operations per transaction.
pub const MAX_OPS_PER_TX: usize = 100;

/// Maximum payload size in bytes (128 KB).
pub const MAX_PAYLOAD_SIZE: usize = 131_072;

/// Maximum content_type length in bytes.
pub const MAX_CONTENT_TYPE_SIZE: usize = 128;

/// Maximum annotations (string + numeric combined) per entity.
pub const MAX_ANNOTATIONS_PER_ENTITY: usize = 64;

/// Maximum annotation key size in bytes.
pub const MAX_ANNOTATION_KEY_SIZE: usize = 256;

/// Maximum annotation value size in bytes (for string annotations).
pub const MAX_ANNOTATION_VALUE_SIZE: usize = 1024;
