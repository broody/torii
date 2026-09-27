use std::str::FromStr;

use async_graphql::dynamic::{
    Field, FieldFuture, FieldValue, InputValue, ResolverContext, SubscriptionField,
    SubscriptionFieldFuture, TypeRef,
};
use async_graphql::{Name, Value};
use chrono::{DateTime, Utc};
use sqlx::{FromRow, Pool, Sqlite};
use starknet_crypto::Felt;
use tokio_stream::StreamExt;
use torii_broker::types::{InnerType, TransactionUpdate};
use torii_broker::MemoryBroker;
use torii_sqlite::utils::felt_to_sql_string;
use tracing::warn;

use super::{BasicObject, ResolvableObject, TypeMapping, ValueMapping};
use crate::constants::{
    CALL_TYPE_NAME, ID_COLUMN, TOKEN_TRANSFER_TYPE_NAME, TRANSACTION_CALLS_TABLE,
    TRANSACTION_HASH_COLUMN, TRANSACTION_NAMES, TRANSACTION_TABLE, TRANSACTION_TYPE_NAME,
};
use crate::mapping::{CALL_MAPPING, TRANSACTION_MAPPING};
use crate::object::erc::token_transfer::{
    mappable_transfer_condition, token_transfer_mapping_from_row, transfers_from,
    TransferQueryResultRaw, TRANSFER_COLUMNS,
};
use crate::object::{felt_id, resolve_many, resolve_one_with_id};
use crate::query::value_mapping_from_row;
use crate::utils;

#[derive(Debug)]
pub struct CallObject;

impl BasicObject for CallObject {
    fn name(&self) -> (&str, &str) {
        ("call", "calls")
    }

    fn type_name(&self) -> &str {
        CALL_TYPE_NAME
    }

    fn type_mapping(&self) -> &TypeMapping {
        &CALL_MAPPING
    }

    fn related_fields(&self) -> Option<Vec<Field>> {
        None
    }
}
#[derive(Debug)]
pub struct TransactionObject;

impl BasicObject for TransactionObject {
    fn name(&self) -> (&str, &str) {
        TRANSACTION_NAMES
    }

    fn type_name(&self) -> &str {
        TRANSACTION_TYPE_NAME
    }

    fn type_mapping(&self) -> &TypeMapping {
        &TRANSACTION_MAPPING
    }

    fn related_fields(&self) -> Option<Vec<Field>> {
        Some(vec![calls_field(), token_transfers_field()])
    }
}

impl ResolvableObject for TransactionObject {
    fn resolvers(&self) -> Vec<Field> {
        let resolve_one = resolve_one_with_id(
            TRANSACTION_TABLE,
            TRANSACTION_HASH_COLUMN,
            self.name().0,
            self.type_name(),
            self.type_mapping(),
            felt_id,
        );

        let resolve_many = resolve_many(
            TRANSACTION_TABLE,
            ID_COLUMN,
            self.name().1,
            self.type_name(),
            self.type_mapping(),
        );

        vec![resolve_one, resolve_many]
    }

    fn subscriptions(&self) -> Option<Vec<SubscriptionField>> {
        Some(vec![SubscriptionField::new(
            "transaction",
            TypeRef::named_nn(self.type_name()),
            |ctx| {
                SubscriptionFieldFuture::new(async move {
                    // Parsed once per subscription, and compared as felts so that any spelling
                    // of a hash or address (unpadded, upper case) matches.
                    let hash = felt_argument(&ctx, "hash")?;
                    let caller = felt_argument(&ctx, "hasCaller")?;
                    // `createdAt` is the only field the published transaction doesn't carry.
                    let fetch_created_at = ctx.look_ahead().field("createdAt").exists();

                    let pool = ctx.data::<Pool<Sqlite>>()?.clone();
                    // if hash is None, then subscribe to all transactions
                    // if hash is Some, then subscribe to only the transaction with that hash
                    Ok(MemoryBroker::<TransactionUpdate>::subscribe()
                        .then(move |transaction| {
                            let pool = pool.clone();
                            async move {
                                if hash.is_some_and(|hash| hash != transaction.transaction_hash)
                                    || caller.is_some_and(|caller| {
                                        !transaction
                                            .calls
                                            .iter()
                                            .any(|call| call.caller_address == caller)
                                    })
                                {
                                    return None;
                                }

                                let created_at = if fetch_created_at {
                                    match sqlx::query_scalar::<_, DateTime<Utc>>(
                                        "SELECT created_at FROM transactions WHERE id = ?",
                                    )
                                    .bind(felt_to_sql_string(&transaction.transaction_hash))
                                    .fetch_one(&pool)
                                    .await
                                    {
                                        Ok(created_at) => Some(created_at),
                                        Err(error) => {
                                            warn!(
                                                %error,
                                                transaction_hash = %format!(
                                                    "{:#x}",
                                                    transaction.transaction_hash
                                                ),
                                                "Failed to fetch transaction creation time."
                                            );
                                            return None;
                                        }
                                    }
                                } else {
                                    None
                                };

                                Some(Ok(Value::Object(TransactionObject::value_mapping(
                                    &transaction,
                                    created_at,
                                ))))
                            }
                        })
                        .filter_map(|result| result))
                })
            },
        )
        .argument(InputValue::new("hash", TypeRef::named(TypeRef::ID)))
        .argument(InputValue::new(
            "hasCaller",
            TypeRef::named(TypeRef::STRING),
        ))])
    }
}

/// Parses an optional felt argument, treating null as absent.
fn felt_argument(ctx: &ResolverContext<'_>, name: &str) -> async_graphql::Result<Option<Felt>> {
    match ctx.args.get(name) {
        Some(value) if !value.is_null() => {
            let value = value.string()?;
            Felt::from_str(value)
                .map(Some)
                .map_err(|_| async_graphql::Error::new(format!("Invalid {name}: {value}")))
        }
        _ => Ok(None),
    }
}

impl TransactionObject {
    /// Maps a transaction published on the broker. It holds the same data as its row in the
    /// `transactions` table, except for `created_at`.
    pub fn value_mapping(
        transaction: &<TransactionUpdate as InnerType>::Inner,
        created_at: Option<DateTime<Utc>>,
    ) -> ValueMapping {
        let transaction_hash = felt_to_sql_string(&transaction.transaction_hash);
        let felts = |felts: &[Felt]| felts.iter().map(felt_to_sql_string).collect::<Vec<_>>();

        async_graphql::dynamic::indexmap::IndexMap::from([
            (Name::new("id"), Value::from(transaction_hash.clone())),
            (Name::new("transactionHash"), Value::from(transaction_hash)),
            (
                Name::new("senderAddress"),
                Value::from(felt_to_sql_string(&transaction.sender_address)),
            ),
            (
                Name::new("calldata"),
                Value::from(felts(&transaction.calldata)),
            ),
            (
                Name::new("maxFee"),
                Value::from(felt_to_sql_string(&transaction.max_fee)),
            ),
            (
                Name::new("signature"),
                Value::from(felts(&transaction.signature)),
            ),
            (
                Name::new("nonce"),
                Value::from(felt_to_sql_string(&transaction.nonce)),
            ),
            (
                Name::new("executedAt"),
                Value::from(transaction.block_timestamp.to_rfc3339()),
            ),
            (
                Name::new("createdAt"),
                created_at.map_or(Value::Null, |created_at| {
                    Value::from(created_at.to_rfc3339())
                }),
            ),
            (
                Name::new("transactionType"),
                Value::from(transaction.transaction_type.clone()),
            ),
            (
                Name::new("blockNumber"),
                Value::from(transaction.block_number),
            ),
        ])
    }
}

/// Returns the hash of the parent transaction in the format it is stored in: the transaction
/// queries strip leading zeros from felts.
fn parent_transaction_hash(indexmap: &ValueMapping) -> async_graphql::Result<String> {
    felt_id(&utils::extract::<String>(indexmap, "transactionHash")?)
}

// NOTE: `calls` and `tokenTransfers` run one query per transaction. Batching them across the
// transactions of a page needs a DataLoader (async-graphql `dataloader` feature) or support from
// the shared `resolve_many`.
fn calls_field() -> Field {
    Field::new("calls", TypeRef::named_list(CALL_TYPE_NAME), move |ctx| {
        FieldFuture::new(async move {
            match ctx.parent_value.try_to_value()? {
                Value::Object(indexmap) => {
                    let mut conn = ctx.data::<Pool<Sqlite>>()?.acquire().await?;

                    let transaction_hash = parent_transaction_hash(indexmap)?;

                    // Fetch all function calls for this transaction
                    let query = &format!(
                        "SELECT * FROM {TRANSACTION_CALLS_TABLE} WHERE transaction_hash = ?"
                    );
                    let rows = sqlx::query(query)
                        .bind(&transaction_hash)
                        .fetch_all(&mut *conn)
                        .await?;

                    let results = rows
                        .iter()
                        .map(|row| {
                            value_mapping_from_row(row, &CALL_MAPPING, false, true)
                                .map(Value::Object)
                        })
                        .collect::<Result<Vec<_>, _>>()?;

                    Ok(Some(Value::List(results)))
                }
                _ => Err("incorrect value, requires Value::Object".into()),
            }
        })
    })
}

fn token_transfers_field() -> Field {
    Field::new(
        "tokenTransfers",
        TypeRef::named_list(TOKEN_TRANSFER_TYPE_NAME),
        move |ctx| {
            FieldFuture::new(async move {
                match ctx.parent_value.try_to_value()? {
                    Value::Object(indexmap) => {
                        let mut conn = ctx.data::<Pool<Sqlite>>()?.acquire().await?;

                        let transaction_hash = parent_transaction_hash(indexmap)?;

                        // Fetch all token transfers for this transaction. Event ids are
                        // `block_number:transaction_hash:contract_address:event_idx`, and a
                        // formatted felt contains no LIKE wildcard.
                        let query = format!(
                            "SELECT {TRANSFER_COLUMNS} {} WHERE et.event_id LIKE ? AND {}",
                            transfers_from(),
                            mappable_transfer_condition()
                        );

                        let rows = sqlx::query(&query)
                            .bind(format!("%:{transaction_hash}:%"))
                            .fetch_all(&mut *conn)
                            .await?;

                        let mut results = Vec::new();
                        for row in &rows {
                            let row = TransferQueryResultRaw::from_row(row)?;
                            match token_transfer_mapping_from_row(&row) {
                                Ok(result) => results.push(FieldValue::owned_any(result)),
                                Err(error) => {
                                    warn!(%error, "Failed to map token transfer.");
                                }
                            }
                        }

                        Ok(Some(FieldValue::list(results)))
                    }
                    _ => Err("incorrect value, requires Value::Object".into()),
                }
            })
        },
    )
}
