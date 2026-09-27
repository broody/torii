use std::str::FromStr;

use async_graphql::dynamic::indexmap::IndexMap;
use async_graphql::dynamic::{
    Field, InputValue, SubscriptionField, SubscriptionFieldFuture, TypeRef,
};
use async_graphql::{Name, Value};
use starknet_crypto::Felt;
use tokio_stream::StreamExt;
use torii_broker::types::EventMessageUpdate;
use torii_broker::MemoryBroker;
use torii_sqlite::types::Entity;

use super::entity::entity_row_from_update;
use super::inputs::keys_input::keys_argument;
use super::model_data::{model_union_field, ModelUnionSource};
use super::{BasicObject, ResolvableObject, TypeMapping, ValueMapping};
use crate::constants::{
    DATETIME_FORMAT, EVENT_ID_COLUMN, EVENT_MESSAGE_NAMES, EVENT_MESSAGE_TABLE,
    EVENT_MESSAGE_TYPE_NAME, ID_COLUMN,
};
use crate::mapping::ENTITY_TYPE_MAPPING;
use crate::object::{resolve_many, resolve_one};

#[derive(Debug)]
pub struct EventMessageObject;

impl BasicObject for EventMessageObject {
    fn name(&self) -> (&str, &str) {
        EVENT_MESSAGE_NAMES
    }

    fn type_name(&self) -> &str {
        EVENT_MESSAGE_TYPE_NAME
    }

    fn type_mapping(&self) -> &TypeMapping {
        &ENTITY_TYPE_MAPPING
    }

    fn related_fields(&self) -> Option<Vec<Field>> {
        Some(vec![model_union_field(ModelUnionSource::EventMessage)])
    }
}

impl ResolvableObject for EventMessageObject {
    fn resolvers(&self) -> Vec<Field> {
        let resolve_one = resolve_one(
            EVENT_MESSAGE_TABLE,
            ID_COLUMN,
            self.name().0,
            self.type_name(),
            self.type_mapping(),
        );

        let mut resolve_many = resolve_many(
            EVENT_MESSAGE_TABLE,
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
            "eventMessageUpdated",
            TypeRef::named_nn(self.type_name()),
            |ctx| {
                SubscriptionFieldFuture::new(async move {
                    let id = match ctx.args.get("id") {
                        Some(id) => Some(id.string()?.to_string()),
                        None => None,
                    };
                    // the id is either the event message id or its hashed keys
                    let hashed_keys = id.as_deref().and_then(|id| Felt::from_str(id).ok());
                    Ok(
                        MemoryBroker::<EventMessageUpdate>::subscribe().filter_map(move |update| {
                            let event_message = entity_row_from_update(&update);
                            let matches = match &id {
                                None => true,
                                Some(id) => {
                                    hashed_keys == Some(update.entity.hashed_keys)
                                        || *id == event_message.id
                                }
                            };
                            matches.then(|| {
                                Ok(Value::Object(EventMessageObject::value_mapping(
                                    event_message,
                                )))
                            })
                        }),
                    )
                })
            },
        )
        .argument(InputValue::new("id", TypeRef::named(TypeRef::ID)))])
    }
}

impl EventMessageObject {
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
