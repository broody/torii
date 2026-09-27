use async_graphql::connection::PageInfo;
use async_graphql::dynamic::indexmap::IndexMap;
use async_graphql::dynamic::{Field, InputValue, ResolverContext, TypeRef};
use async_graphql::{Error, Name, Value};
use sqlx::sqlite::SqliteRow;
use sqlx::Row;

use self::page_info::PageInfoObject;
use super::BasicObject;
use crate::constants::{MAX_PAGE_SIZE, PAGE_INFO_TYPE_NAME};
use crate::query::order::Order;
use crate::query::value_mapping_from_row;
use crate::types::{GraphqlType, TypeData, TypeMapping, ValueMapping};
use crate::utils::extract;

pub mod cursor;
pub mod edge;
pub mod page_info;

#[derive(Debug)]
pub struct ConnectionArguments {
    pub first: Option<u64>,
    pub last: Option<u64>,
    pub after: Option<String>,
    pub before: Option<String>,
    pub offset: Option<u64>,
    pub limit: Option<u64>,
}

#[derive(Debug)]
pub struct ConnectionObject {
    pub name: String,
    pub type_name: String,
    pub type_mapping: TypeMapping,
}

impl ConnectionObject {
    pub fn new(name: String, type_name: String) -> Self {
        let type_mapping = TypeMapping::from([
            (
                Name::new("edges"),
                TypeData::Simple(TypeRef::named_list(format!("{}Edge", type_name))),
            ),
            (
                Name::new("totalCount"),
                TypeData::Simple(TypeRef::named_nn(TypeRef::INT)),
            ),
            (
                Name::new("pageInfo"),
                TypeData::Nested((TypeRef::named_nn(PAGE_INFO_TYPE_NAME), IndexMap::new())),
            ),
        ]);

        Self {
            name: format!("{}Connection", name),
            type_name: format!("{}Connection", type_name),
            type_mapping,
        }
    }
}

impl BasicObject for ConnectionObject {
    fn name(&self) -> (&str, &str) {
        (&self.name, "")
    }

    fn type_name(&self) -> &str {
        &self.type_name
    }

    fn type_mapping(&self) -> &TypeMapping {
        &self.type_mapping
    }
}

pub fn parse_connection_arguments(ctx: &ResolverContext<'_>) -> Result<ConnectionArguments, Error> {
    connection_arguments_from_map(ctx.args.as_index_map())
}

/// Reads an optional non-negative integer argument, capped at `max` when given.
fn page_argument(args: &ValueMapping, name: &str, max: Option<u64>) -> Result<Option<u64>, Error> {
    if matches!(args.get(name), None | Some(Value::Null)) {
        return Ok(None);
    }

    let value = extract::<u64>(args, name)
        .map_err(|_| Error::new(format!("`{name}` must be a non-negative integer.")))?;

    match max {
        Some(max) if value > max => Err(Error::new(format!(
            "`{name}` must not exceed {max}, got {value}."
        ))),
        _ => Ok(Some(value)),
    }
}

fn connection_arguments_from_map(args: &ValueMapping) -> Result<ConnectionArguments, Error> {
    let first = page_argument(args, "first", Some(MAX_PAGE_SIZE))?;
    let last = page_argument(args, "last", Some(MAX_PAGE_SIZE))?;
    let after = extract::<String>(args, "after").ok();
    let before = extract::<String>(args, "before").ok();
    let offset = page_argument(args, "offset", None)?;
    let limit = page_argument(args, "limit", Some(MAX_PAGE_SIZE))?;

    if first.is_some() && last.is_some() {
        return Err(
            "Passing both `first` and `last` to paginate a connection is not supported.".into(),
        );
    }

    if after.is_some() && before.is_some() {
        return Err(
            "Passing both `after` and `before` to paginate a connection is not supported.".into(),
        );
    }

    if (offset.is_some() || limit.is_some())
        && (first.is_some() || last.is_some() || before.is_some() || after.is_some())
    {
        return Err(
            "Pass either `offset`/`limit` OR `first`/`last`/`after`/`before` to paginate \
                    a connection."
                .into(),
        );
    }

    Ok(ConnectionArguments {
        first,
        last,
        after,
        before,
        offset,
        limit,
    })
}

pub fn connection_arguments(field: Field) -> Field {
    field
        .argument(InputValue::new("first", TypeRef::named(TypeRef::INT)))
        .argument(InputValue::new("last", TypeRef::named(TypeRef::INT)))
        .argument(InputValue::new(
            "before",
            TypeRef::named(GraphqlType::Cursor.to_string()),
        ))
        .argument(InputValue::new(
            "after",
            TypeRef::named(GraphqlType::Cursor.to_string()),
        ))
        .argument(InputValue::new("offset", TypeRef::named(TypeRef::INT)))
        .argument(InputValue::new("limit", TypeRef::named(TypeRef::INT)))
}

#[allow(clippy::too_many_arguments)]
pub fn connection_output(
    data: &[SqliteRow],
    types: &TypeMapping,
    order: &Option<Order>,
    id_column: &str,
    total_count: i64,
    is_internal: bool,
    snake_case: bool,
    page_info: PageInfo,
) -> sqlx::Result<ValueMapping> {
    let model_edges = data
        .iter()
        .map(|row| {
            let order_field = match order {
                Some(order) => {
                    if is_internal {
                        format!("internal_{}", order.field)
                    } else {
                        order.field.to_string()
                    }
                }
                None => id_column.to_string(),
            };
            let primary_order = row.try_get::<String, &str>(id_column)?;
            let secondary_order = row.try_get_unchecked::<String, &str>(&order_field)?;
            let cursor = cursor::encode(&primary_order, &secondary_order);
            let value_mapping = value_mapping_from_row(row, types, is_internal, snake_case)?;

            let mut edge = ValueMapping::new();
            edge.insert(Name::new("node"), Value::Object(value_mapping));
            edge.insert(Name::new("cursor"), Value::String(cursor));

            Ok(Value::Object(edge))
        })
        .collect::<sqlx::Result<Vec<Value>>>();

    Ok(ValueMapping::from([
        (Name::new("totalCount"), Value::from(total_count)),
        (Name::new("edges"), Value::List(model_edges?)),
        (Name::new("pageInfo"), PageInfoObject::value(page_info)),
    ]))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn arguments(values: &[(&str, Value)]) -> Result<ConnectionArguments, Error> {
        let args = values
            .iter()
            .map(|(name, value)| (Name::new(name), value.clone()))
            .collect::<ValueMapping>();
        connection_arguments_from_map(&args)
    }

    #[test]
    fn page_arguments_are_parsed() {
        let args = arguments(&[("first", Value::from(5)), ("after", Value::from("c"))]).unwrap();
        assert_eq!(args.first, Some(5));
        assert_eq!(args.after.as_deref(), Some("c"));

        let args = arguments(&[
            ("limit", Value::from(MAX_PAGE_SIZE)),
            ("offset", Value::from(20_000)),
        ])
        .unwrap();
        assert_eq!(args.limit, Some(MAX_PAGE_SIZE));
        assert_eq!(args.offset, Some(20_000));

        // explicit nulls are the same as missing arguments
        let args = arguments(&[("first", Value::Null), ("last", Value::Null)]).unwrap();
        assert_eq!(args.first, None);
        assert_eq!(args.last, None);
    }

    #[test]
    fn negative_page_arguments_are_rejected() {
        for name in ["first", "last", "offset", "limit"] {
            let err = arguments(&[(name, Value::from(-1))]).err().unwrap();
            assert_eq!(
                err.message,
                format!("`{name}` must be a non-negative integer.")
            );
        }
    }

    #[test]
    fn page_sizes_above_max_are_rejected() {
        for name in ["first", "last", "limit"] {
            let err = arguments(&[(name, Value::from(MAX_PAGE_SIZE + 1))])
                .err()
                .unwrap();
            assert_eq!(
                err.message,
                format!(
                    "`{name}` must not exceed {MAX_PAGE_SIZE}, got {}.",
                    MAX_PAGE_SIZE + 1
                )
            );
        }
    }
}
