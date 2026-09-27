use std::str::FromStr;

use async_graphql::dynamic::{
    Field, InputValue, SubscriptionField, SubscriptionFieldFuture, TypeRef,
};
use async_graphql::{Error, Name, Value};
use starknet_crypto::Felt;
use tokio_stream::StreamExt;
use torii_broker::types::{EventUpdate, InnerType};
use torii_broker::MemoryBroker;
use torii_sqlite::utils::felt_to_sql_string;

use super::inputs::keys_input::{keys_argument, parse_keys_argument};
use super::{resolve_many, BasicObject, ResolvableObject, TypeMapping};
use crate::constants::{DATETIME_FORMAT, EVENT_NAMES, EVENT_TABLE, EVENT_TYPE_NAME, ID_COLUMN};
use crate::mapping::EVENT_TYPE_MAPPING;
use crate::types::ValueMapping;

#[derive(Debug)]
pub struct EventObject;

impl BasicObject for EventObject {
    fn name(&self) -> (&str, &str) {
        EVENT_NAMES
    }

    fn type_name(&self) -> &str {
        EVENT_TYPE_NAME
    }

    fn type_mapping(&self) -> &TypeMapping {
        &EVENT_TYPE_MAPPING
    }
}

impl ResolvableObject for EventObject {
    fn resolvers(&self) -> Vec<Field> {
        let mut resolve_many = resolve_many(
            EVENT_TABLE,
            ID_COLUMN,
            self.name().1,
            self.type_name(),
            self.type_mapping(),
        );
        resolve_many = keys_argument(resolve_many);

        vec![resolve_many]
    }

    fn subscriptions(&self) -> Option<Vec<SubscriptionField>> {
        Some(vec![SubscriptionField::new(
            "eventEmitted",
            TypeRef::named_nn(self.type_name()),
            |ctx| {
                SubscriptionFieldFuture::new(async move {
                    // Parsed once per subscription rather than for every event.
                    let input_keys = parse_keys_argument(&ctx)?
                        .map(|keys| EventObject::parse_keys(&keys))
                        .transpose()?;
                    Ok(
                        MemoryBroker::<EventUpdate>::subscribe().filter_map(move |event| {
                            EventObject::match_and_map_event(&input_keys, event)
                                .map(|value_mapping| Ok(Value::Object(value_mapping)))
                        }),
                    )
                })
            },
        )
        .argument(InputValue::new(
            "keys",
            TypeRef::named_list(TypeRef::STRING),
        ))])
    }
}

impl EventObject {
    fn value_mapping(event: <EventUpdate as InnerType>::Inner) -> ValueMapping {
        let keys: Vec<String> = event.event.keys.iter().map(felt_to_sql_string).collect();
        let data: Vec<String> = event.event.data.iter().map(felt_to_sql_string).collect();
        ValueMapping::from([
            (Name::new("id"), Value::from(event.id)),
            (Name::new("keys"), Value::from(keys)),
            (Name::new("data"), Value::from(data)),
            (
                Name::new("transactionHash"),
                Value::from(felt_to_sql_string(&event.event.transaction_hash)),
            ),
            (
                Name::new("executedAt"),
                Value::from(event.executed_at.format(DATETIME_FORMAT).to_string()),
            ),
            (
                Name::new("createdAt"),
                Value::from(event.created_at.format(DATETIME_FORMAT).to_string()),
            ),
        ])
    }

    /// Parses subscription keys into felts, `None` standing for the `*` wildcard.
    fn parse_keys(keys: &[String]) -> Result<Vec<Option<Felt>>, Error> {
        keys.iter()
            .map(|key| {
                if key == "*" {
                    Ok(None)
                } else {
                    Felt::from_str(key)
                        .map(Some)
                        .map_err(|e| Error::new(format!("Failed to parse key as felt: {e}")))
                }
            })
            .collect()
    }

    fn match_and_map_event(
        input_keys: &Option<Vec<Option<Felt>>>,
        event: <EventUpdate as InnerType>::Inner,
    ) -> Option<ValueMapping> {
        if let Some(ref keys) = input_keys {
            if EventObject::match_keys(keys, &event) {
                return Some(EventObject::value_mapping(event));
            }

            // no match, keep listening
            None
        } else {
            // subscribed to all events
            Some(EventObject::value_mapping(event))
        }
    }

    // Checks if the provided keys match the event's keys, `None` being a wildcard. Returns true
    // if all keys match or if a wildcard is present at the respective position.
    pub fn match_keys(
        input_keys: &[Option<Felt>],
        event: &<EventUpdate as InnerType>::Inner,
    ) -> bool {
        if input_keys.len() > event.event.keys.len() {
            return false;
        }

        input_keys
            .iter()
            .zip(event.event.keys.iter())
            .all(|(input_key, event_key)| input_key.is_none_or(|input_key| input_key == *event_key))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(keys: &[u64]) -> <EventUpdate as InnerType>::Inner {
        let mut event = <EventUpdate as InnerType>::Inner::default();
        event.event.keys = keys.iter().map(|key| Felt::from(*key)).collect();
        event
    }

    fn keys(keys: &[&str]) -> Vec<Option<Felt>> {
        let keys: Vec<String> = keys.iter().map(|key| key.to_string()).collect();
        EventObject::parse_keys(&keys).unwrap()
    }

    #[test]
    fn match_keys_with_wildcards_and_prefixes() {
        let event = event(&[0xdead, 0xbeef]);

        assert!(EventObject::match_keys(&keys(&[]), &event));
        assert!(EventObject::match_keys(&keys(&["0xdead"]), &event));
        assert!(EventObject::match_keys(&keys(&["*", "0xbeef"]), &event));
        // Keys are compared as felts, whatever their spelling.
        assert!(EventObject::match_keys(
            &keys(&["0x000000000000000000000000000000000000000000000000000000000000DEAD"]),
            &event
        ));

        assert!(!EventObject::match_keys(&keys(&["0xbeef"]), &event));
        assert!(!EventObject::match_keys(&keys(&["*", "*", "*"]), &event));
    }

    #[test]
    fn parse_keys_rejects_invalid_felts() {
        assert!(EventObject::parse_keys(&["0xnope".to_string()]).is_err());
    }
}
