use crate::odps::models::TunnelTableSchema;
use arrow_schema::{ArrowError, DataType, Field, Schema, TimeUnit};
use lazy_static::lazy_static;
use std::collections::BTreeMap;

pub struct OdpsToArrowSchema<'a> {
    odps_to_arrow_type_mapping: BTreeMap<&'a str, DataType>,
}

impl<'a> OdpsToArrowSchema<'a> {
    pub fn new() -> Self {
        let mut m = BTreeMap::new();
        m.insert("string", DataType::Utf8);
        m.insert("binary", DataType::Binary);
        m.insert("tinyint", DataType::Int8);
        m.insert("smallint", DataType::Int16);
        m.insert("int", DataType::Int32);
        m.insert("bigint", DataType::Int64);
        m.insert("boolean", DataType::Boolean);
        m.insert("float", DataType::Float32);
        m.insert("double", DataType::Float64);
        m.insert("date", DataType::Date32);
        m.insert(
            "datetime",
            DataType::Timestamp(TimeUnit::Millisecond, Some("string".into())),
        );
        m.insert(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("string".into())),
        );
        m.insert(
            "timestamp_ntz",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("string".into())),
        );
        Self {
            odps_to_arrow_type_mapping: m,
        }
    }

    pub fn odps_to_arrow_schema(
        &self,
        tunnel_schema: &TunnelTableSchema,
    ) -> Result<Schema, ArrowError> {
        let mut fields = vec![];
        for col in tunnel_schema.columns.iter() {
            let filed_type = self
                .odps_to_arrow_type_mapping
                .get(col.r#type.as_str())
                .unwrap_or(&DataType::Binary);
            let field = Field::new(col.name.as_str(), filed_type.clone(), col.nullable);
            fields.push(field);
        }
        Ok(Schema::new(fields))
    }
}

lazy_static! {
    pub static ref ODPS_TO_ARROW_MAPPING: OdpsToArrowSchema<'static> = OdpsToArrowSchema::new();
}
