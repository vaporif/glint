# Arkiv vs Glint

Arkiv = op-geth fork (Go). Glint = reth plugin (Rust), wraps `BlockExecutorFactory` with no source mods.

## Key differences

**Entity model:** Arkiv stores payloads in SQLite (unverifiable). Glint emits them as event logs and puts a content hash on-chain (slot 2) so payloads are reconstructable and fraud-provable. Glint entities use 2 slots vs Arkiv's ~3.

**Entity key:** Arkiv uses `keccak256(txHash, payload, opIndex)`. Glint adds `payloadLen` to prevent collisions from prefix overlap.

**Gas:** Arkiv charges zero gas for CRUD ops -- a DoS vector with no economic signal for BTL or payload size. Glint meters everything (50k create, 40k update, 10k delete/extend, plus per-byte and per-BTL costs).

**Expiration:** Arkiv uses an on-chain EnumerableSet (~2 trie slots per entity, ~500M gas for 10k expirations/block -- exceeds block limit). Glint keeps it in-memory (~38B RAM per entity, ~170ms for 10k expirations), rebuilt from logs on cold start.

**Limits:** Arkiv has unbounded BTL, annotations, and payload sizes. Glint caps everything: BTL at 302,400 blocks (~1 week), 128KB payload, 64 annotations, 256B keys, 1024B values.

**Streaming:** Arkiv's SQLite ETL goroutine is fire-and-forget (crash = stale data forever). Glint uses ExEx + Arrow IPC with ring buffer replay, backpressure, and health checks.

**Query:** Arkiv has `arkiv_query` with a custom filter language backed by SQLite (unverifiable). Glint skips that, offers Flight SQL via DataFusion instead. `glint_*` JSON-RPC is planned but not yet implemented.

**Not ported (intentionally):** ChangeOwner (unnecessary for ephemeral storage), Brotli compression (batch submitter handles this), zero-gas CRUD, unbounded BTL, on-chain EnumerableSet.

## Arkiv bugs that motivated the rewrite

- `Validate()` return discarded in txpool -- invalid txs enter mempool
- Wrong error variable after `ExecuteArkivTransaction` -- failures silently swallowed
- `blockHash` passed as `txHash` in tracer path -- produces wrong entity keys
- Double execution when tracing is attached
- Brotli decompression bomb (no size limit in mempool)
- `GetEntityMetaData` returns zero-value for missing entities instead of an error
- SQLite ETL goroutine has no crash recovery
- CLI calls `golembase_*` but server registers `arkiv_*` (never worked)
- `arkiv_getEntityCount` returns hardcoded 0
- `GasUsed = 0` for all Arkiv txs

## TODO

- Make Extend owner-only (permissionless extend is a griefing vector)
- Expiration index checkpointing (persist on shutdown, skip cold-start scan)
- Basic `glint_*` JSON-RPC for entity reads
- HashSet in expiration index (O(n) remove -> O(1))
