use arrow::{
    array::{
        Array, AsArray, BinaryArray, FixedSizeBinaryArray, MapArray, StringArray, UInt8Array,
        UInt32Array, UInt64Array,
    },
    record_batch::RecordBatch,
};
use eyre::WrapErr;
use rusqlite::Connection;

use crate::schema;

#[allow(clippy::cast_possible_wrap)]
pub fn insert_batch(conn: &Connection, batch: &RecordBatch) -> eyre::Result<()> {
    if batch.num_rows() == 0 {
        return Ok(());
    }

    let block_number_col = col_u64(batch, "block_number")?;
    let block_hash_col = col_fsb(batch, "block_hash")?;
    let tx_index_col = col_u32(batch, "tx_index")?;
    let tx_hash_col = col_fsb(batch, "tx_hash")?;
    let log_index_col = col_u32(batch, "log_index")?;
    let event_type_col = col_u8(batch, "event_type")?;
    let entity_key_col = col_fsb(batch, "entity_key")?;
    let owner_col = col_fsb(batch, "owner")?;
    let expires_col = col_u64(batch, "expires_at_block")?;
    let old_expires_col = col_u64(batch, "old_expires_at_block")?;
    let content_type_col = col_string(batch, "content_type")?;
    let payload_col = col_binary(batch, "payload")?;
    let str_ann_col = col_map(batch, "string_annotations")?;
    let num_ann_col = col_map(batch, "numeric_annotations")?;
    let extend_policy_col = col_u8(batch, "extend_policy")?;
    let operator_col = col_fsb(batch, "operator")?;

    let tx = conn
        .unchecked_transaction()
        .wrap_err("starting SQLite transaction")?;

    {
        let mut stmt = tx.prepare_cached(
            "INSERT OR IGNORE INTO entity_events (
                block_number, block_hash, tx_index, tx_hash, log_index,
                event_type, entity_key, owner, expires_at_block, old_expires_at_block,
                content_type, payload, string_annotations, numeric_annotations,
                extend_policy, operator
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)",
        )?;

        for i in 0..batch.num_rows() {
            let block_number = block_number_col.value(i);
            let block_hash = block_hash_col.value(i);
            let tx_index = i64::from(tx_index_col.value(i));
            let tx_hash = tx_hash_col.value(i);
            let log_index = i64::from(log_index_col.value(i));
            let event_type = i64::from(event_type_col.value(i));
            let entity_key = entity_key_col.value(i);

            validate_blob_len(block_hash, 32, "block_hash")?;
            validate_blob_len(tx_hash, 32, "tx_hash")?;
            validate_blob_len(entity_key, 32, "entity_key")?;

            let owner: Option<&[u8]> = if owner_col.is_null(i) {
                None
            } else {
                let v = owner_col.value(i);
                validate_blob_len(v, 20, "owner")?;
                Some(v)
            };

            let expires_at: Option<i64> = if expires_col.is_null(i) {
                None
            } else {
                Some(expires_col.value(i) as i64)
            };

            let old_expires_at: Option<i64> = if old_expires_col.is_null(i) {
                None
            } else {
                Some(old_expires_col.value(i) as i64)
            };

            let content_type: Option<&str> = if content_type_col.is_null(i) {
                None
            } else {
                Some(content_type_col.value(i))
            };

            let payload: Option<&[u8]> = if payload_col.is_null(i) {
                None
            } else {
                Some(payload_col.value(i))
            };

            let string_annotations = encode_string_map(str_ann_col, i)?;
            let numeric_annotations = encode_numeric_map(num_ann_col, i)?;

            let extend_policy: Option<i64> = if extend_policy_col.is_null(i) {
                None
            } else {
                Some(i64::from(extend_policy_col.value(i)))
            };

            let operator: Option<&[u8]> = if operator_col.is_null(i) {
                None
            } else {
                let v = operator_col.value(i);
                validate_blob_len(v, 20, "operator")?;
                Some(v)
            };

            stmt.execute(rusqlite::params![
                (block_number as i64),
                block_hash,
                tx_index,
                tx_hash,
                log_index,
                event_type,
                entity_key,
                owner,
                expires_at,
                old_expires_at,
                content_type,
                payload,
                string_annotations,
                numeric_annotations,
                extend_policy,
                operator,
            ])?;
        }
    }

    let last_block = block_number_col.value(batch.num_rows() - 1);
    schema::set_last_processed_block(&tx, last_block)?;

    tx.commit().wrap_err("committing SQLite transaction")?;
    Ok(())
}

fn validate_blob_len(blob: &[u8], expected: usize, name: &str) -> eyre::Result<()> {
    if blob.len() != expected {
        eyre::bail!("{name} blob length {}, expected {expected}", blob.len());
    }
    Ok(())
}

macro_rules! col {
    ($batch:expr, $name:expr, $ty:ty) => {
        $batch
            .column_by_name($name)
            .ok_or_else(|| eyre::eyre!("missing column: {}", $name))?
            .as_any()
            .downcast_ref::<$ty>()
            .ok_or_else(|| eyre::eyre!("column {} is not {}", $name, stringify!($ty)))
    };
}

fn col_u8<'a>(batch: &'a RecordBatch, name: &str) -> eyre::Result<&'a UInt8Array> {
    col!(batch, name, UInt8Array)
}

fn col_u32<'a>(batch: &'a RecordBatch, name: &str) -> eyre::Result<&'a UInt32Array> {
    col!(batch, name, UInt32Array)
}

fn col_u64<'a>(batch: &'a RecordBatch, name: &str) -> eyre::Result<&'a UInt64Array> {
    col!(batch, name, UInt64Array)
}

fn col_fsb<'a>(batch: &'a RecordBatch, name: &str) -> eyre::Result<&'a FixedSizeBinaryArray> {
    col!(batch, name, FixedSizeBinaryArray)
}

fn col_string<'a>(batch: &'a RecordBatch, name: &str) -> eyre::Result<&'a StringArray> {
    col!(batch, name, StringArray)
}

fn col_binary<'a>(batch: &'a RecordBatch, name: &str) -> eyre::Result<&'a BinaryArray> {
    col!(batch, name, BinaryArray)
}

fn col_map<'a>(batch: &'a RecordBatch, name: &str) -> eyre::Result<&'a MapArray> {
    batch
        .column_by_name(name)
        .ok_or_else(|| eyre::eyre!("missing column: {name}"))?
        .as_map_opt()
        .ok_or_else(|| eyre::eyre!("column {name} is not MapArray"))
}

#[allow(clippy::cast_sign_loss)]
fn encode_string_map(col: &MapArray, i: usize) -> eyre::Result<Option<String>> {
    if col.is_null(i) {
        return Ok(None);
    }
    let offsets = col.value_offsets();
    let start = offsets[i] as usize;
    let end = offsets[i + 1] as usize;
    if start == end {
        return Ok(Some("[]".to_owned()));
    }
    let keys = col.keys().as_string::<i32>();
    let values = col.values().as_string::<i32>();
    let pairs: Vec<[&str; 2]> = (start..end)
        .map(|j| [keys.value(j), values.value(j)])
        .collect();
    Ok(Some(serde_json::to_string(&pairs)?))
}

#[allow(clippy::cast_sign_loss)]
fn encode_numeric_map(col: &MapArray, i: usize) -> eyre::Result<Option<String>> {
    if col.is_null(i) {
        return Ok(None);
    }
    let offsets = col.value_offsets();
    let start = offsets[i] as usize;
    let end = offsets[i + 1] as usize;
    if start == end {
        return Ok(Some("[]".to_owned()));
    }
    let keys = col.keys().as_string::<i32>();
    let values = col.values().as_primitive::<arrow::datatypes::UInt64Type>();
    let pairs: Vec<serde_json::Value> = (start..end)
        .map(|j| serde_json::json!([keys.value(j), values.value(j)]))
        .collect();
    Ok(Some(serde_json::to_string(&pairs)?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema;

    use std::sync::Arc;

    use alloy_primitives::{Address, B256};
    use arrow::{
        array::{
            ArrayRef, BinaryBuilder, FixedSizeBinaryBuilder, StringBuilder, UInt8Builder,
            UInt32Builder, UInt64Builder,
            builder::{MapBuilder, MapFieldNames},
        },
        datatypes::{DataType, Field, Schema},
    };
    use glint_primitives::exex_types::{BatchOp, EntityEventType};

    fn map_field_names() -> MapFieldNames {
        MapFieldNames {
            entry: "entries".into(),
            key: "key".into(),
            value: "value".into(),
        }
    }

    fn exex_schema() -> Schema {
        Schema::new(vec![
            Field::new("block_number", DataType::UInt64, false),
            Field::new("block_hash", DataType::FixedSizeBinary(32), false),
            Field::new("tx_index", DataType::UInt32, false),
            Field::new("tx_hash", DataType::FixedSizeBinary(32), false),
            Field::new("log_index", DataType::UInt32, false),
            Field::new("event_type", DataType::UInt8, false),
            Field::new("entity_key", DataType::FixedSizeBinary(32), false),
            Field::new("owner", DataType::FixedSizeBinary(20), true),
            Field::new("expires_at_block", DataType::UInt64, true),
            Field::new("old_expires_at_block", DataType::UInt64, true),
            Field::new("content_type", DataType::Utf8, true),
            Field::new("payload", DataType::Binary, true),
            Field::new(
                "string_annotations",
                DataType::Map(
                    Arc::new(Field::new(
                        "entries",
                        DataType::Struct(
                            vec![
                                Field::new("key", DataType::Utf8, false),
                                Field::new("value", DataType::Utf8, true),
                            ]
                            .into(),
                        ),
                        false,
                    )),
                    false,
                ),
                true,
            ),
            Field::new(
                "numeric_annotations",
                DataType::Map(
                    Arc::new(Field::new(
                        "entries",
                        DataType::Struct(
                            vec![
                                Field::new("key", DataType::Utf8, false),
                                Field::new("value", DataType::UInt64, true),
                            ]
                            .into(),
                        ),
                        false,
                    )),
                    false,
                ),
                true,
            ),
            Field::new("extend_policy", DataType::UInt8, true),
            Field::new("operator", DataType::FixedSizeBinary(20), true),
            Field::new("tip_block", DataType::UInt64, false),
            Field::new("op", DataType::UInt8, false),
        ])
    }

    fn build_created_batch(block_number: u64) -> RecordBatch {
        let schema = Arc::new(exex_schema());
        let entity_key = B256::repeat_byte(0x01);
        let owner = Address::repeat_byte(0x02);
        let tx_hash = B256::repeat_byte(0xAA);

        let mut block_number_b = UInt64Builder::with_capacity(1);
        let mut block_hash_b = FixedSizeBinaryBuilder::with_capacity(1, 32);
        let mut tx_index_b = UInt32Builder::with_capacity(1);
        let mut tx_hash_b = FixedSizeBinaryBuilder::with_capacity(1, 32);
        let mut log_index_b = UInt32Builder::with_capacity(1);
        let mut event_type_b = UInt8Builder::with_capacity(1);
        let mut entity_key_b = FixedSizeBinaryBuilder::with_capacity(1, 32);
        let mut owner_b = FixedSizeBinaryBuilder::with_capacity(1, 20);
        let mut expires_b = UInt64Builder::with_capacity(1);
        let mut old_expires_b = UInt64Builder::with_capacity(1);
        let mut content_type_b = StringBuilder::with_capacity(1, 64);
        let mut payload_b = BinaryBuilder::with_capacity(1, 256);
        let mut str_ann_b = MapBuilder::new(
            Some(map_field_names()),
            StringBuilder::new(),
            StringBuilder::new(),
        );
        let mut num_ann_b = MapBuilder::new(
            Some(map_field_names()),
            StringBuilder::new(),
            UInt64Builder::new(),
        );
        let mut extend_policy_b = UInt8Builder::with_capacity(1);
        let mut operator_b = FixedSizeBinaryBuilder::with_capacity(1, 20);
        let mut tip_block_b = UInt64Builder::with_capacity(1);
        let mut op_b = UInt8Builder::with_capacity(1);

        block_number_b.append_value(block_number);
        block_hash_b.append_value(B256::ZERO.as_slice()).unwrap();
        tx_index_b.append_value(0);
        tx_hash_b.append_value(tx_hash.as_slice()).unwrap();
        log_index_b.append_value(0);
        event_type_b.append_value(EntityEventType::Created as u8);
        entity_key_b.append_value(entity_key.as_slice()).unwrap();
        owner_b.append_value(owner.as_slice()).unwrap();
        expires_b.append_value(200);
        old_expires_b.append_null();
        content_type_b.append_value("text/plain");
        payload_b.append_value(b"hello");
        str_ann_b.keys().append_value("sk");
        str_ann_b.values().append_value("sv");
        str_ann_b.append(true).unwrap();
        num_ann_b.keys().append_value("nk");
        num_ann_b.values().append_value(99);
        num_ann_b.append(true).unwrap();
        extend_policy_b.append_value(0);
        operator_b.append_value(Address::ZERO.as_slice()).unwrap();
        tip_block_b.append_value(block_number);
        op_b.append_value(BatchOp::Commit as u8);

        let columns: Vec<ArrayRef> = vec![
            Arc::new(block_number_b.finish()),
            Arc::new(block_hash_b.finish()),
            Arc::new(tx_index_b.finish()),
            Arc::new(tx_hash_b.finish()),
            Arc::new(log_index_b.finish()),
            Arc::new(event_type_b.finish()),
            Arc::new(entity_key_b.finish()),
            Arc::new(owner_b.finish()),
            Arc::new(expires_b.finish()),
            Arc::new(old_expires_b.finish()),
            Arc::new(content_type_b.finish()),
            Arc::new(payload_b.finish()),
            Arc::new(str_ann_b.finish()),
            Arc::new(num_ann_b.finish()),
            Arc::new(extend_policy_b.finish()),
            Arc::new(operator_b.finish()),
            Arc::new(tip_block_b.finish()),
            Arc::new(op_b.finish()),
        ];
        RecordBatch::try_new(schema, columns).unwrap()
    }

    fn setup_db() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        schema::create_tables(&conn).unwrap();
        conn
    }

    #[test]
    fn insert_batch_single_row() {
        let conn = setup_db();
        let batch = build_created_batch(10);
        insert_batch(&conn, &batch).unwrap();

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM entity_events", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 1);

        let block: i64 = conn
            .query_row("SELECT block_number FROM entity_events", [], |r| r.get(0))
            .unwrap();
        assert_eq!(block, 10);

        assert_eq!(schema::get_last_processed_block(&conn).unwrap(), Some(10));
    }

    #[test]
    fn insert_batch_empty_is_noop() {
        let conn = setup_db();
        let schema = Arc::new(exex_schema());
        let batch = RecordBatch::new_empty(schema);
        insert_batch(&conn, &batch).unwrap();

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM entity_events", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn insert_batch_duplicate_is_ignored() {
        let conn = setup_db();
        let batch = build_created_batch(10);
        insert_batch(&conn, &batch).unwrap();
        insert_batch(&conn, &batch).unwrap();

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM entity_events", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn annotations_stored_as_json() {
        let conn = setup_db();
        let batch = build_created_batch(10);
        insert_batch(&conn, &batch).unwrap();

        let str_ann: String = conn
            .query_row("SELECT string_annotations FROM entity_events", [], |r| {
                r.get(0)
            })
            .unwrap();
        let parsed: Vec<Vec<String>> = serde_json::from_str(&str_ann).unwrap();
        assert_eq!(parsed, vec![vec!["sk", "sv"]]);

        let num_ann: String = conn
            .query_row("SELECT numeric_annotations FROM entity_events", [], |r| {
                r.get(0)
            })
            .unwrap();
        let parsed: Vec<serde_json::Value> = serde_json::from_str(&num_ann).unwrap();
        assert_eq!(parsed, vec![serde_json::json!(["nk", 99])]);
    }
}
