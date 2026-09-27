use async_graphql::connection::PageInfo;
use async_graphql::dynamic::{Field, FieldFuture, FieldValue, InputValue, TypeRef};
use convert_case::{Case, Casing};
use serde::Deserialize;
use sqlx::sqlite::SqliteRow;
use sqlx::{FromRow, Pool, Row, Sqlite, SqliteConnection};
use starknet_crypto::Felt;
use torii_sqlite::utils::felt_to_sql_string;
use torii_storage::utils::parse_event_id;
use tracing::warn;

use super::erc_token::ErcTokenType;
use super::{
    handle_cursor, map_token, mappable_token_condition, Connection, ConnectionEdge, TokenColumns,
    TokenMappingError,
};
use crate::constants::{
    DEFAULT_LIMIT, ID_COLUMN, TOKEN_TRANSFER_NAME, TOKEN_TRANSFER_TABLE, TOKEN_TRANSFER_TYPE_NAME,
};
use crate::mapping::TOKEN_TRANSFER_TYPE_MAPPING;
use crate::object::connection::page_info::PageInfoObject;
use crate::object::connection::{
    connection_arguments, cursor, parse_connection_arguments, ConnectionArguments,
};
use crate::object::{BasicObject, ResolvableObject};
use crate::query::order::{CursorDirection, Direction};
use crate::types::TypeMapping;
use crate::utils::extract;

#[derive(Debug)]
pub struct ErcTransferObject;

impl BasicObject for ErcTransferObject {
    fn name(&self) -> (&str, &str) {
        TOKEN_TRANSFER_NAME
    }

    fn type_name(&self) -> &str {
        TOKEN_TRANSFER_TYPE_NAME
    }

    fn type_mapping(&self) -> &TypeMapping {
        &TOKEN_TRANSFER_TYPE_MAPPING
    }
}

impl ResolvableObject for ErcTransferObject {
    fn resolvers(&self) -> Vec<Field> {
        let account_address = "account_address";
        let arg_addr = InputValue::new(
            account_address.to_case(Case::Camel),
            TypeRef::named_nn(TypeRef::STRING),
        );

        let mut field = Field::new(
            self.name().1,
            TypeRef::named(format!("{}Connection", self.type_name())),
            move |ctx| {
                FieldFuture::new(async move {
                    let mut conn = ctx.data::<Pool<Sqlite>>()?.acquire().await?;
                    let connection = parse_connection_arguments(&ctx)?;
                    let address = extract::<Felt>(
                        ctx.args.as_index_map(),
                        &account_address.to_case(Case::Camel),
                    )?;

                    let total_count = count_token_transfers(&mut conn, address).await?;

                    let (data, page_info) =
                        fetch_token_transfers(&mut conn, address, &connection, total_count).await?;
                    let results = token_transfers_connection_output(&data, total_count, page_info)?;

                    Ok(Some(results))
                })
            },
        )
        .argument(arg_addr);

        field = connection_arguments(field);
        vec![field]
    }
}

/// `FROM` clause of the token transfer queries, joining the tables [`TransferQueryResultRaw`]
/// reads from.
pub(crate) fn transfers_from() -> String {
    format!(
        "FROM {TOKEN_TRANSFER_TABLE} et JOIN tokens t ON et.token_id = t.id JOIN contracts c ON \
         t.contract_address = c.contract_address"
    )
}

/// Columns read by [`TransferQueryResultRaw`].
pub(crate) const TRANSFER_COLUMNS: &str = "et.id, et.contract_address, et.from_address, \
                                           et.to_address, et.amount, et.token_id, \
                                           et.executed_at, t.name, t.symbol, t.decimals, \
                                           c.contract_type, t.metadata";

/// Keeps the transfers [`token_transfer_mapping_from_row`] can map.
///
/// Transfers of NFTs whose metadata couldn't be fetched (stored as `''`) are kept: their metadata
/// fields are null.
pub(crate) fn mappable_transfer_condition() -> String {
    mappable_token_condition("et.token_id")
}

/// Conditions (and their bound values) shared by the `tokenTransfers` count and page queries.
fn transfer_conditions(address: Felt) -> (Vec<String>, Vec<String>) {
    let conditions = vec![
        "(et.from_address = ? OR et.to_address = ?)".to_string(),
        mappable_transfer_condition(),
    ];
    let binds = vec![felt_to_sql_string(&address), felt_to_sql_string(&address)];
    (conditions, binds)
}

async fn count_token_transfers(conn: &mut SqliteConnection, address: Felt) -> sqlx::Result<i64> {
    let (conditions, binds) = transfer_conditions(address);
    let query = format!(
        "SELECT COUNT(*) {} WHERE {}",
        transfers_from(),
        conditions.join(" AND ")
    );

    let mut count_query = sqlx::query_scalar(&query);
    for value in &binds {
        count_query = count_query.bind(value);
    }
    count_query.fetch_one(conn).await
}

async fn fetch_token_transfers(
    conn: &mut SqliteConnection,
    address: Felt,
    connection: &ConnectionArguments,
    total_count: i64,
) -> sqlx::Result<(Vec<SqliteRow>, PageInfo)> {
    let id_column = format!("et.{}", ID_COLUMN);

    let mut query = format!("SELECT {TRANSFER_COLUMNS} {}", transfers_from());

    let (mut conditions, mut binds) = transfer_conditions(address);

    let mut cursor_param = &connection.after;
    if let Some(after_cursor) = &connection.after {
        let (condition, value) = handle_cursor(after_cursor, CursorDirection::After, &id_column)?;
        conditions.push(condition);
        binds.push(value);
    }

    if let Some(before_cursor) = &connection.before {
        cursor_param = &connection.before;
        let (condition, value) = handle_cursor(before_cursor, CursorDirection::Before, &id_column)?;
        conditions.push(condition);
        binds.push(value);
    }

    if !conditions.is_empty() {
        query.push_str(&format!(" WHERE {}", conditions.join(" AND ")));
    }

    let is_cursor_based = connection.first.or(connection.last).is_some() || cursor_param.is_some();

    let data_limit = connection
        .first
        .or(connection.last)
        .or(connection.limit)
        .unwrap_or(DEFAULT_LIMIT);
    let limit = if is_cursor_based {
        match &cursor_param {
            Some(_) => data_limit + 2,
            None => data_limit + 1, // prev page does not exist
        }
    } else {
        data_limit
    };

    let order_direction = match (connection.first, connection.last) {
        (Some(_), _) => Direction::Desc,
        (_, Some(_)) => Direction::Asc,
        _ => Direction::Desc,
    };

    query.push_str(&format!(
        " ORDER BY {id_column} {} LIMIT {limit}",
        order_direction.as_ref()
    ));

    if let Some(offset) = connection.offset {
        query.push_str(&format!(" OFFSET {}", offset));
    }

    let mut data_query = sqlx::query(&query);
    for value in &binds {
        data_query = data_query.bind(value);
    }
    let mut data = data_query.fetch_all(conn).await?;

    let mut page_info = PageInfo {
        has_previous_page: false,
        has_next_page: false,
        start_cursor: None,
        end_cursor: None,
    };

    if data.is_empty() {
        Ok((data, page_info))
    } else if is_cursor_based {
        match cursor_param {
            Some(cursor_query) => {
                let first_cursor = cursor::encode(
                    &data[0].try_get::<String, &str>(ID_COLUMN)?,
                    &data[0].try_get_unchecked::<String, &str>(ID_COLUMN)?,
                );

                if &first_cursor == cursor_query && data.len() != 1 {
                    data.remove(0);
                    page_info.has_previous_page = true;
                } else {
                    data.pop();
                }

                if data.len() as u64 == limit - 1 {
                    page_info.has_next_page = true;
                    data.pop();
                }
            }
            None => {
                if data.len() as u64 == limit {
                    page_info.has_next_page = true;
                    data.pop();
                }
            }
        }

        if !data.is_empty() {
            page_info.start_cursor = Some(cursor::encode(
                &data[0].try_get::<String, &str>(ID_COLUMN)?,
                &data[0].try_get_unchecked::<String, &str>(ID_COLUMN)?,
            ));
            page_info.end_cursor = Some(cursor::encode(
                &data[data.len() - 1].try_get::<String, &str>(ID_COLUMN)?,
                &data[data.len() - 1].try_get_unchecked::<String, &str>(ID_COLUMN)?,
            ));
        }

        Ok((data, page_info))
    } else {
        let offset = connection.offset.unwrap_or(0);
        if 0 < offset && offset < total_count as u64 {
            page_info.has_previous_page = true;
        }
        if limit + offset < total_count as u64 {
            page_info.has_next_page = true;
        }

        Ok((data, page_info))
    }
}

fn token_transfers_connection_output<'a>(
    data: &[SqliteRow],
    total_count: i64,
    page_info: PageInfo,
) -> sqlx::Result<FieldValue<'a>> {
    let mut edges = Vec::new();

    for row in data {
        let row = TransferQueryResultRaw::from_row(row)?;
        let cursor = cursor::encode(&row.id, &row.id);

        match token_transfer_mapping_from_row(&row) {
            Ok(transfer_node) => {
                edges.push(ConnectionEdge {
                    node: transfer_node,
                    cursor,
                });
            }
            Err(err) => {
                warn!("Failed to transform row to TokenTransferNode: {}", err);
                continue;
            }
        }
    }

    Ok(FieldValue::owned_any(Connection {
        total_count,
        edges,
        page_info: PageInfoObject::value(page_info),
    }))
}

/// Transforms a TransferQueryResultRaw into a TokenTransferNode
pub fn token_transfer_mapping_from_row(
    row: &TransferQueryResultRaw,
) -> Result<TokenTransferNode, TokenMappingError> {
    let (_, transaction_hash, _, _) = parse_event_id(&row.id);
    let transaction_hash = felt_to_sql_string(&transaction_hash);

    let token_metadata = map_token(&TokenColumns {
        contract_type: &row.contract_type,
        token_key: &row.token_id,
        contract_address: &row.contract_address,
        name: &row.name,
        symbol: &row.symbol,
        decimals: row.decimals,
        metadata: &row.metadata,
        amount: &row.amount,
    })?;

    Ok(TokenTransferNode {
        from: row.from_address.clone(),
        to: row.to_address.clone(),
        executed_at: row.executed_at.clone(),
        token_metadata,
        transaction_hash,
    })
}

// TODO: This would be required when subscriptions are needed
// impl ErcTransferObject {
//     pub fn value_mapping(entity: ErcBalance) -> ValueMapping {
//         IndexMap::from([
//         ])
//     }
// }

#[derive(FromRow, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct TransferQueryResultRaw {
    pub id: String,
    pub contract_address: String,
    pub from_address: String,
    pub to_address: String,
    pub token_id: String,
    pub amount: String,
    pub executed_at: String,
    pub name: String,
    pub symbol: String,
    pub decimals: u8,
    pub contract_type: String,
    pub metadata: String,
}

#[derive(Debug, Clone)]
pub struct TokenTransferNode {
    pub from: String,
    pub to: String,
    pub executed_at: String,
    pub token_metadata: ErcTokenType,
    pub transaction_hash: String,
}
