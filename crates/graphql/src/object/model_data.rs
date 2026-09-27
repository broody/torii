use std::collections::HashMap;
use std::sync::Arc;

use async_graphql::dynamic::{Enum, Field, FieldFuture, FieldValue, InputObject, Object, TypeRef};
use async_graphql::Value;
use dojo_types::schema::Ty;
use sqlx::{Pool, Sqlite};
use tracing::debug;

use super::connection::{connection_arguments, connection_output, parse_connection_arguments};
use super::inputs::order_input::{order_argument, parse_order_argument, OrderInputObject};
use super::inputs::where_input::{parse_where_argument, where_argument, WhereInputObject};
use super::inputs::InputObjectTrait;
use super::{BasicObject, ResolvableObject, TypeMapping, ValueMapping};
use crate::constants::{
    ENTITY_ID_COLUMN, ENTITY_TABLE, ENTITY_TYPE_NAME, EVENT_MESSAGE_ID_COLUMN, EVENT_MESSAGE_TABLE,
    EVENT_MESSAGE_TYPE_NAME, ID_COLUMN, INTERNAL_ENTITY_ID_KEY,
};
use crate::mapping::ENTITY_TYPE_MAPPING;
use crate::query::data::{count_rows, fetch_multiple_rows, fetch_single_row};
use crate::query::value_mapping_from_row;
use crate::types::TypeData;
use crate::utils;

#[derive(Debug)]
pub struct ModelDataObject {
    pub name: String,
    pub plural_name: String,
    pub type_name: String,
    /// Model tag (`{namespace}-{name}`), which is also the name of the model table. It can't be
    /// derived back from the type name (`{namespace}_{name}`) as both parts may contain '_'.
    pub table_name: String,
    pub type_mapping: TypeMapping,
    pub schema: Ty,
    pub where_input: WhereInputObject,
    pub order_input: OrderInputObject,
}

impl ModelDataObject {
    pub fn new(
        name: String,
        type_name: String,
        table_name: String,
        type_mapping: TypeMapping,
        schema: Ty,
    ) -> Self {
        let where_input = WhereInputObject::new(type_name.as_str(), &type_mapping);
        let order_input = OrderInputObject::new(type_name.as_str(), &type_mapping);
        let plural_name = format!("{}Models", name);
        Self {
            name,
            plural_name,
            type_name,
            table_name,
            type_mapping,
            schema,
            where_input,
            order_input,
        }
    }
}

impl BasicObject for ModelDataObject {
    fn name(&self) -> (&str, &str) {
        (&self.name, &self.plural_name)
    }

    fn type_name(&self) -> &str {
        &self.type_name
    }

    fn type_mapping(&self) -> &TypeMapping {
        &self.type_mapping
    }

    fn objects(&self) -> Vec<Object> {
        let mut objects = data_objects_recursion(&TypeData::Nested((
            TypeRef::named(self.type_name()),
            self.type_mapping.clone(),
        )));

        // root object requires entity_field association
        let mut root = objects.pop().unwrap();
        root = root.field(entity_field());
        root = root.field(event_message_field());

        objects.push(root);
        objects
    }
}

impl ResolvableObject for ModelDataObject {
    fn input_objects(&self) -> Option<Vec<InputObject>> {
        let mut objects = vec![];
        objects.extend(self.where_input.input_objects());
        objects.push(self.order_input.input_object());
        Some(objects)
    }

    fn enum_objects(&self) -> Option<Vec<Enum>> {
        self.order_input.enum_objects()
    }

    fn resolvers(&self) -> Vec<Field> {
        let table_name = self.table_name.clone();
        let type_mapping = self.type_mapping.clone();
        let where_mapping = self.where_input.type_mapping.clone();
        let field_type = format!("{}Connection", self.type_name());

        let mut field = Field::new(self.name().1, TypeRef::named(field_type), move |ctx| {
            let type_mapping = type_mapping.clone();
            let where_mapping = where_mapping.clone();
            let table_name = table_name.clone();

            FieldFuture::new(async move {
                let mut conn = ctx.data::<Pool<Sqlite>>()?.acquire().await?;
                let order = parse_order_argument(&ctx);
                let filters = parse_where_argument(&ctx, &where_mapping)?;
                let connection = parse_connection_arguments(&ctx)?;

                let total_count = count_rows(&mut conn, &table_name, &None, &filters).await?;
                let (data, page_info) = fetch_multiple_rows(
                    &mut conn,
                    &table_name,
                    "internal_event_id",
                    &None,
                    &order,
                    &filters,
                    &connection,
                    total_count,
                )
                .await?;
                let connection = connection_output(
                    &data,
                    &type_mapping,
                    &order,
                    "internal_event_id",
                    total_count,
                    false,
                    false,
                    page_info,
                )?;

                Ok(Some(Value::Object(connection)))
            })
        });

        // Add relay connection fields (first, last, before, after, where)
        field = connection_arguments(field);
        field = where_argument(field, self.type_name());
        field = order_argument(field, self.type_name());

        vec![field]
    }
}

fn data_objects_recursion(type_data: &TypeData) -> Vec<Object> {
    let mut objects: Vec<Object> = vec![];

    match type_data {
        TypeData::Nested((nested_type, nested_mapping)) => {
            let nested_objects = nested_mapping.values().flat_map(data_objects_recursion);

            objects.extend(nested_objects);
            objects.push(object(&nested_type.to_string(), nested_mapping));
        }
        TypeData::List(inner) => {
            let nested_objects = data_objects_recursion(inner);

            objects.extend(nested_objects);
        }
        _ => {}
    }

    objects
}

/// Builds the object of a model, or of a type nested in a model.
///
/// Every field, nested ones included, resolves from the parent value: `value_mapping_from_row`
/// builds nested values from the same row as the model itself.
pub fn object(type_name: &str, type_mapping: &TypeMapping) -> Object {
    let mut object = Object::new(type_name);

    for (field_name, type_data) in type_mapping {
        let field_name = field_name.clone();

        let field = Field::new(field_name.to_string(), type_data.type_ref(), move |ctx| {
            let field_name = field_name.clone();

            FieldFuture::new(async move {
                let value_mapping = parent_value_mapping(ctx.parent_value)?;
                // a missing value resolves to null instead of failing the whole query
                Ok(value_mapping.get(&field_name).cloned())
            })
        });

        object = object.field(field);
    }

    object
}

/// Model data reaches resolvers as a `Value::Object` when queried through a model or its nested
/// types, and as an owned `ValueMapping` when resolved as a member of `ModelUnion`.
fn parent_value_mapping<'a>(parent: &'a FieldValue<'_>) -> async_graphql::Result<&'a ValueMapping> {
    match parent.as_value() {
        Some(Value::Object(value_mapping)) => Ok(value_mapping),
        Some(_) => Err("incorrect value, requires Value::Object".into()),
        None => parent
            .downcast_ref::<ValueMapping>()
            .ok_or_else(|| "Field resolver only accepts Value or IndexMap".into()),
    }
}

fn entity_field() -> Field {
    Field::new("entity", TypeRef::named(ENTITY_TYPE_NAME), |ctx| {
        FieldFuture::new(async move {
            match ctx.parent_value.try_to_value()? {
                Value::Object(indexmap) => {
                    let mut conn = ctx.data::<Pool<Sqlite>>()?.acquire().await?;
                    let entity_id = utils::extract::<String>(indexmap, INTERNAL_ENTITY_ID_KEY)?;
                    let data =
                        fetch_single_row(&mut conn, ENTITY_TABLE, ID_COLUMN, &entity_id).await?;
                    let entity = value_mapping_from_row(&data, &ENTITY_TYPE_MAPPING, false, true)?;

                    Ok(Some(Value::Object(entity)))
                }
                _ => Err("incorrect value, requires Value::Object".into()),
            }
        })
    })
}

fn event_message_field() -> Field {
    Field::new(
        "eventMessage",
        TypeRef::named(EVENT_MESSAGE_TYPE_NAME),
        |ctx| {
            FieldFuture::new(async move {
                match ctx.parent_value.try_to_value()? {
                    Value::Object(indexmap) => {
                        let mut conn = ctx.data::<Pool<Sqlite>>()?.acquire().await?;
                        let entity_id = utils::extract::<String>(indexmap, INTERNAL_ENTITY_ID_KEY)?;
                        let data =
                            fetch_single_row(&mut conn, EVENT_MESSAGE_TABLE, ID_COLUMN, &entity_id)
                                .await?;
                        let event_message =
                            value_mapping_from_row(&data, &ENTITY_TYPE_MAPPING, false, true)?;

                        Ok(Some(Value::Object(event_message)))
                    }
                    _ => Err("incorrect value, requires Value::Object".into()),
                }
            })
        },
    )
}

/// A model exposed by the schema, as a member of `ModelUnion`.
#[derive(Debug, Clone)]
pub struct RegisteredModel {
    /// GraphQL type name of the model (`{namespace}_{name}`).
    pub type_name: String,
    /// Model tag (`{namespace}-{name}`), which is also the name of the model table.
    pub table_name: String,
    pub type_mapping: TypeMapping,
}

/// Models the schema was built with, keyed by model id.
///
/// Registered as schema data so that resolving the models of an entity or event message doesn't
/// have to load and parse model schemas on every request.
#[derive(Debug, Default)]
pub struct ModelRegistry {
    models: HashMap<String, RegisteredModel>,
}

impl ModelRegistry {
    pub fn insert(&mut self, model_id: String, model: RegisteredModel) {
        self.models.insert(model_id, model);
    }

    pub fn get(&self, model_id: &str) -> Option<&RegisteredModel> {
        self.models.get(model_id)
    }

    pub fn is_empty(&self) -> bool {
        self.models.is_empty()
    }
}

/// Where the `models` union field finds the models of its parent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelUnionSource {
    /// Models of an entity, through `entity_model`.
    Entity,
    /// Models of an event message, through `event_model`.
    EventMessage,
}

impl ModelUnionSource {
    /// Table relating the parent to its models.
    fn relation_table(self) -> &'static str {
        match self {
            Self::Entity => "entity_model",
            Self::EventMessage => "event_model",
        }
    }

    /// Column of the model tables referencing the parent.
    fn model_table_column(self) -> &'static str {
        match self {
            Self::Entity => ENTITY_ID_COLUMN,
            Self::EventMessage => EVENT_MESSAGE_ID_COLUMN,
        }
    }
}

/// The `models` field of entities and event messages, resolving the parent's models as members of
/// `ModelUnion`.
pub fn model_union_field(source: ModelUnionSource) -> Field {
    Field::new("models", TypeRef::named_list("ModelUnion"), move |ctx| {
        FieldFuture::new(async move {
            let Value::Object(indexmap) = ctx.parent_value.try_to_value()? else {
                return Err("incorrect value, requires Value::Object".into());
            };
            let entity_id = utils::extract::<String>(indexmap, "id")?;
            let registry = ctx.data::<Arc<ModelRegistry>>()?;
            let mut conn = ctx.data::<Pool<Sqlite>>()?.acquire().await?;

            // ordered by model id, as models were listed before the registry existed
            let model_ids: Vec<String> = sqlx::query_scalar(&format!(
                "SELECT model_id FROM {} WHERE entity_id = ? ORDER BY model_id",
                source.relation_table()
            ))
            .bind(&entity_id)
            .fetch_all(&mut *conn)
            .await?;

            let mut results: Vec<FieldValue<'_>> = Vec::with_capacity(model_ids.len());
            for model_id in model_ids {
                // Models registered after the schema was built aren't members of `ModelUnion`.
                let Some(model) = registry.get(&model_id) else {
                    debug!(%model_id, "Skipping model missing from the GraphQL schema.");
                    continue;
                };

                let row = sqlx::query(&format!(
                    "SELECT * FROM [{}] WHERE {} = ?",
                    model.table_name,
                    source.model_table_column()
                ))
                .bind(&entity_id)
                .fetch_one(&mut *conn)
                .await?;

                // Use value_mapping_from_row to handle nested structures
                let data = value_mapping_from_row(&row, &model.type_mapping, false, false)?;

                results.push(FieldValue::with_type(
                    FieldValue::owned_any(data),
                    model.type_name.clone(),
                ));
            }

            Ok(Some(FieldValue::list(results)))
        })
    })
}
