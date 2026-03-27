use std::sync::Arc;

use parking_lot::Mutex;

use alloy_primitives::{Address, B256};
use arrow::{
    array::{
        ArrayRef, BinaryBuilder, FixedSizeBinaryBuilder, StringBuilder, UInt8Builder,
        UInt32Builder, UInt64Builder,
        builder::{MapBuilder, MapFieldNames},
    },
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use datafusion::prelude::*;
use glint_historical::{provider::HistoricalTableProvider, schema, writer};
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

fn build_created_batch(block_number: u64, entity_byte: u8) -> RecordBatch {
    let schema = Arc::new(exex_schema());
    let entity_key = B256::repeat_byte(entity_byte);
    let owner = Address::repeat_byte(entity_byte);
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

#[tokio::test]
async fn write_then_query_via_datafusion() {
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    schema::create_tables(&conn).unwrap();

    writer::insert_batch(&conn, &build_created_batch(10, 0x01)).unwrap();
    writer::insert_batch(&conn, &build_created_batch(20, 0x02)).unwrap();
    writer::insert_batch(&conn, &build_created_batch(30, 0x03)).unwrap();

    let conn = Arc::new(Mutex::new(conn));
    let provider = HistoricalTableProvider::new(conn);

    let ctx = SessionContext::new();
    ctx.register_table("entities", Arc::new(provider)).unwrap();

    let df = ctx
        .sql("SELECT block_number, event_type FROM entities WHERE block_number BETWEEN 10 AND 20")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 2, "expected 2 events in block range 10-20");
}

#[tokio::test]
async fn query_without_block_range_errors() {
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    schema::create_tables(&conn).unwrap();

    let conn = Arc::new(Mutex::new(conn));
    let provider = HistoricalTableProvider::new(conn);

    let ctx = SessionContext::new();
    ctx.register_table("entities", Arc::new(provider)).unwrap();

    let result = ctx
        .sql("SELECT * FROM entities WHERE event_type = 0")
        .await
        .unwrap()
        .collect()
        .await;

    assert!(
        result.is_err(),
        "expected error for query without block range"
    );
}
