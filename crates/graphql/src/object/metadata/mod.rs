use async_graphql::connection::PageInfo;
use async_graphql::dynamic::{Field, FieldFuture, TypeRef};
use async_graphql::{Name, Value};
use convert_case::{Case, Casing};
use sqlx::sqlite::SqliteRow;
use sqlx::{Pool, Row, Sqlite, SqliteConnection};
use torii_sqlite::constants::WORLD_CONTRACT_TYPE;
use tracing::debug;

use super::connection::page_info::PageInfoObject;
use super::connection::{connection_arguments, cursor, parse_connection_arguments};
use super::{BasicObject, ResolvableObject};
use crate::constants::{
    ID_COLUMN, JSON_COLUMN, METADATA_NAMES, METADATA_TABLE, METADATA_TYPE_NAME,
};
use crate::mapping::METADATA_TYPE_MAPPING;
use crate::query::data::{count_rows, fetch_multiple_rows, fetch_world_address};
use crate::query::value_mapping_from_row;
use crate::types::{TypeMapping, ValueMapping};

pub mod content;
pub mod social;

#[derive(Debug)]
pub struct MetadataObject;

impl MetadataObject {
    fn row_types(&self) -> TypeMapping {
        let mut row_types = self.type_mapping().clone();
        row_types.swap_remove("worldAddress");
        row_types
    }
}

impl BasicObject for MetadataObject {
    fn name(&self) -> (&str, &str) {
        METADATA_NAMES
    }

    fn type_name(&self) -> &str {
        METADATA_TYPE_NAME
    }

    fn type_mapping(&self) -> &TypeMapping {
        &METADATA_TYPE_MAPPING
    }
}

impl ResolvableObject for MetadataObject {
    fn resolvers(&self) -> Vec<Field> {
        let row_types = self.row_types();

        let mut field = Field::new(
            self.name().1,
            TypeRef::named(format!("{}Connection", self.type_name())),
            move |ctx| {
                let row_types = row_types.clone();

                FieldFuture::new(async move {
                    let mut conn = ctx.data::<Pool<Sqlite>>()?.acquire().await?;
                    let connection = parse_connection_arguments(&ctx)?;
                    let total_count = count_rows(&mut conn, METADATA_TABLE, &None, &None).await?;
                    let (data, page_info) = fetch_multiple_rows(
                        &mut conn,
                        METADATA_TABLE,
                        ID_COLUMN,
                        &None,
                        &None,
                        &None,
                        &connection,
                        total_count,
                    )
                    .await?;
                    let world_address = if data.is_empty() {
                        String::new()
                    } else {
                        world_address(&mut conn).await?
                    };

                    // convert json field to value_mapping expected by content object
                    let results = metadata_connection_output(
                        &data,
                        &row_types,
                        total_count,
                        page_info,
                        &world_address,
                    )?;

                    Ok(Some(Value::Object(results)))
                })
            },
        );

        field = connection_arguments(field);

        vec![field]
    }
}

/// Address of the world the metadata belongs to.
///
/// The `metadata` table isn't scoped by world (rows are keyed by resource selector only), so rows
/// can only be attributed when a single world is indexed. With no world or several worlds, the
/// address is left empty rather than failing or picking an arbitrary world.
async fn world_address(conn: &mut SqliteConnection) -> sqlx::Result<String> {
    let worlds: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM contracts WHERE contract_type = ?")
        .bind(WORLD_CONTRACT_TYPE)
        .fetch_one(&mut *conn)
        .await?;

    if worlds == 1 {
        fetch_world_address(conn).await
    } else {
        debug!(
            worlds,
            "Metadata world address is ambiguous, leaving it empty."
        );
        Ok(String::new())
    }
}

// NOTE: need to generalize `connection_output` or maybe preprocess to support both predefined
// objects AND dynamic model objects
fn metadata_connection_output(
    data: &[SqliteRow],
    row_types: &TypeMapping,
    total_count: i64,
    page_info: PageInfo,
    world_address: &String,
) -> sqlx::Result<ValueMapping> {
    let edges = data
        .iter()
        .map(|row| {
            let order = row.try_get::<String, &str>(ID_COLUMN)?;
            let cursor = cursor::encode(&order, &order);
            let mut value_mapping = value_mapping_from_row(row, row_types, false, true)?;
            value_mapping.insert(Name::new("worldAddress"), Value::from(world_address));

            let json_str = row.try_get::<String, &str>(JSON_COLUMN)?;
            let serde_value = serde_json::from_str(&json_str).unwrap_or_default();

            let content = ValueMapping::from([
                extract_str_mapping("name", &serde_value),
                extract_str_mapping("description", &serde_value),
                extract_str_mapping("website", &serde_value),
                extract_str_mapping("icon_uri", &serde_value),
                extract_str_mapping("cover_uri", &serde_value),
                extract_socials_mapping("socials", &serde_value),
            ]);

            value_mapping.insert(Name::new("content"), Value::Object(content));

            let edge = ValueMapping::from([
                (Name::new("node"), Value::Object(value_mapping)),
                (Name::new("cursor"), Value::String(cursor)),
            ]);

            Ok(Value::Object(edge))
        })
        .collect::<sqlx::Result<Vec<Value>>>();

    Ok(ValueMapping::from([
        (Name::new("totalCount"), Value::from(total_count)),
        (Name::new("edges"), Value::List(edges?)),
        (Name::new("pageInfo"), PageInfoObject::value(page_info)),
    ]))
}

fn extract_str_mapping(name: &str, serde_value: &serde_json::Value) -> (Name, Value) {
    let name_lower_camel = name.to_case(Case::Camel);
    if let Some(serde_json::Value::String(str)) = serde_value.get(name) {
        (Name::new(name_lower_camel), Value::String(str.to_owned()))
    } else {
        (Name::new(name_lower_camel), Value::Null)
    }
}

fn extract_socials_mapping(name: &str, serde_value: &serde_json::Value) -> (Name, Value) {
    if let Some(serde_json::Value::Object(obj)) = serde_value.get(name) {
        let list = obj
            .iter()
            .map(|(social_name, social_url)| {
                Value::Object(ValueMapping::from([
                    (Name::new("name"), Value::String(social_name.to_string())),
                    (
                        Name::new("url"),
                        social_url
                            .as_str()
                            .map_or(Value::Null, |url| Value::String(url.to_string())),
                    ),
                ]))
            })
            .collect::<Vec<Value>>();

        return (Name::new(name), Value::List(list));
    }

    (Name::new(name), Value::List(vec![]))
}
