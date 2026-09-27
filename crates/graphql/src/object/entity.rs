use async_graphql::dynamic::indexmap::IndexMap;
use async_graphql::dynamic::{
    Field, InputValue, SubscriptionField, SubscriptionFieldFuture, TypeRef,
};
use async_graphql::{Name, Value};
use sqlx::{Pool, Sqlite};
use tokio_stream::StreamExt;
use torii_broker::types::EntityUpdate;
use torii_broker::MemoryBroker;
use torii_sqlite::types::Entity;
use torii_sqlite::utils::felt_to_sql_string;
use torii_storage::utils::format_world_scoped_id;

use super::inputs::keys_input::keys_argument;
use super::model_data::{model_union_field, ModelUnionSource};
use super::{BasicObject, ResolvableObject, TypeMapping, ValueMapping};
use crate::constants::{
    DATETIME_FORMAT, ENTITY_NAMES, ENTITY_TABLE, ENTITY_TYPE_NAME, EVENT_ID_COLUMN, ID_COLUMN,
};
use crate::mapping::ENTITY_TYPE_MAPPING;
use crate::object::{resolve_many, resolve_one};
#[derive(Debug)]
pub struct EntityObject;

impl BasicObject for EntityObject {
    fn name(&self) -> (&str, &str) {
        ENTITY_NAMES
    }

    fn type_name(&self) -> &str {
        ENTITY_TYPE_NAME
    }

    fn type_mapping(&self) -> &TypeMapping {
        &ENTITY_TYPE_MAPPING
    }

    fn related_fields(&self) -> Option<Vec<Field>> {
        Some(vec![model_union_field(ModelUnionSource::Entity)])
    }
}

impl ResolvableObject for EntityObject {
    fn resolvers(&self) -> Vec<Field> {
        let resolve_one = resolve_one(
            ENTITY_TABLE,
            ID_COLUMN,
            self.name().0,
            self.type_name(),
            self.type_mapping(),
        );

        let mut resolve_many = resolve_many(
            ENTITY_TABLE,
            EVENT_ID_COLUMN,
            self.name().1,
            self.type_name(),
            self.type_mapping(),
        );
        resolve_many = keys_argument(resolve_many);

        vec![resolve_one, resolve_many]
    }

    fn subscriptions(&self) -> Option<Vec<SubscriptionField>> {
        Some(vec![SubscriptionField::new(
            "entityUpdated",
            TypeRef::named_nn(self.type_name()),
            |ctx| {
                SubscriptionFieldFuture::new(async move {
                    let id = match ctx.args.get("id") {
                        Some(id) => Some(id.string()?.to_string()),
                        None => None,
                    };
                    let pool = ctx.data::<Pool<Sqlite>>()?.clone();
                    // if id is None, then subscribe to all entities
                    // if id is Some, then subscribe to only the entity with that id
                    Ok(MemoryBroker::<EntityUpdate>::subscribe()
                        .then(move |entity| {
                            let pool = pool.clone();
                            let id = id.clone();
                            async move {
                                let entity_id = felt_to_sql_string(&entity.entity.hashed_keys);
                                let scoped_entity_id = format_world_scoped_id(
                                    &entity.entity.world_address,
                                    &entity.entity.hashed_keys,
                                );
                                if id.is_none()
                                    || id == Some(entity_id.clone())
                                    || id == Some(scoped_entity_id.clone())
                                {
                                    let mut conn = match pool.acquire().await {
                                        Ok(conn) => conn,
                                        Err(_) => return None,
                                    };

                                    let entity = match sqlx::query_as::<_, Entity>(
                                        "SELECT * FROM entities WHERE id = ?",
                                    )
                                    .bind(&scoped_entity_id)
                                    .fetch_one(&mut *conn)
                                    .await
                                    {
                                        Ok(entity) => entity,
                                        Err(_) => return None,
                                    };

                                    Some(Ok(Value::Object(EntityObject::value_mapping(entity))))
                                } else {
                                    None
                                }
                            }
                        })
                        .filter_map(|result| result))
                })
            },
        )
        .argument(InputValue::new("id", TypeRef::named(TypeRef::ID)))])
    }
}

impl EntityObject {
    pub fn value_mapping(entity: Entity) -> ValueMapping {
        let keys: Vec<&str> = entity.keys.split('/').filter(|&k| !k.is_empty()).collect();
        IndexMap::from([
            (Name::new("id"), Value::from(entity.id)),
            (Name::new("keys"), Value::from(keys)),
            (Name::new("eventId"), Value::from(entity.event_id)),
            (
                Name::new("createdAt"),
                Value::from(entity.created_at.format(DATETIME_FORMAT).to_string()),
            ),
            (
                Name::new("updatedAt"),
                Value::from(entity.updated_at.format(DATETIME_FORMAT).to_string()),
            ),
            (
                Name::new("executedAt"),
                Value::from(entity.executed_at.format(DATETIME_FORMAT).to_string()),
            ),
        ])
    }
}
