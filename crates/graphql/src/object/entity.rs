use async_graphql::dynamic::indexmap::IndexMap;
use async_graphql::dynamic::{
    Field, InputValue, SubscriptionField, SubscriptionFieldFuture, TypeRef,
};
use async_graphql::{Name, Value};
use tokio_stream::StreamExt;
use torii_broker::types::EntityUpdate;
use torii_broker::MemoryBroker;
use torii_sqlite::types::Entity;
use torii_sqlite::utils::{felt_to_sql_string, felts_to_sql_string};
use torii_storage::proto::schema::EntityWithMetadata;
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
                    // if id is None, then subscribe to all entities
                    // if id is Some, then subscribe to only the entity with that id
                    Ok(
                        MemoryBroker::<EntityUpdate>::subscribe().filter_map(move |update| {
                            let entity = entity_row_from_update(&update);
                            let matches = match &id {
                                None => true,
                                Some(id) => *id == entity.entity_id || *id == entity.id,
                            };
                            // a deleted entity has no row left to be resolved from
                            (matches && !update.deleted)
                                .then(|| Ok(Value::Object(EntityObject::value_mapping(entity))))
                        }),
                    )
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

/// Rebuilds the `entities` (or `event_messages`) row an update was published for.
///
/// The executor publishes updates, once committed, from the rows it wrote: the rebuilt row holds
/// the values the table had for this update, without querying it again for every subscriber.
pub(crate) fn entity_row_from_update<const EVENT_MESSAGE: bool>(
    update: &EntityWithMetadata<EVENT_MESSAGE>,
) -> Entity {
    let entity = &update.entity;
    Entity {
        id: format_world_scoped_id(&entity.world_address, &entity.hashed_keys),
        entity_id: felt_to_sql_string(&entity.hashed_keys),
        world_address: felt_to_sql_string(&entity.world_address),
        // the update's keys are parsed from the keys column, written by `felts_to_sql_string`
        keys: felts_to_sql_string(&update.keys),
        event_id: update.event_id.clone(),
        executed_at: entity.executed_at,
        created_at: entity.created_at,
        updated_at: entity.updated_at,
        updated_model: None,
        deleted: update.deleted,
        match_model: None,
    }
}
