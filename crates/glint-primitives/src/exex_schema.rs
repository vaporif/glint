use std::sync::{Arc, LazyLock};

use arrow::array::builder::MapFieldNames;
use arrow::datatypes::{DataType, Field, Schema};

static SCHEMA: LazyLock<Arc<Schema>> = LazyLock::new(|| Arc::new(build_schema()));

#[must_use]
pub fn entity_events_schema() -> Arc<Schema> {
    Arc::clone(&SCHEMA)
}

#[must_use]
pub fn map_field_names() -> MapFieldNames {
    MapFieldNames {
        entry: "entries".into(),
        key: "key".into(),
        value: "value".into(),
    }
}

fn build_schema() -> Schema {
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
