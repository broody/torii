use async_graphql::connection::PageInfo;
use async_graphql::dynamic::{
    Field, FieldFuture, FieldValue, InputValue, SubscriptionField, SubscriptionFieldFuture, TypeRef,
};
use async_graphql::{Name, Value};
use sqlx::sqlite::SqliteRow;
use sqlx::{Pool, Row, Sqlite, SqliteConnection};
use tokio_stream::StreamExt;
use torii_broker::types::TokenUpdate;
use torii_broker::MemoryBroker;
use torii_sqlite::utils::{felt_and_u256_to_sql_string, felt_to_sql_string};
use tracing::{debug, warn};

use super::{handle_cursor, map_token, TokenColumns, TokenMappingError};
use crate::constants::{
    DEFAULT_LIMIT, ERC1155_TOKEN_NAME, ERC1155_TYPE_NAME, ERC20_TOKEN_NAME, ERC20_TYPE_NAME,
    ERC721_TOKEN_NAME, ERC721_TYPE_NAME, ID_COLUMN,
};
use crate::mapping::{
    ERC1155_TOKEN_TYPE_MAPPING, ERC20_TOKEN_TYPE_MAPPING, ERC721_TOKEN_TYPE_MAPPING,
    TOKEN_TYPE_MAPPING,
};
use crate::object::connection::page_info::PageInfoObject;
use crate::object::connection::{
    connection_arguments, cursor, parse_connection_arguments, ConnectionArguments,
};
use crate::object::erc::{Connection, ConnectionEdge};
use crate::object::{BasicObject, ResolvableObject};
use crate::query::order::{CursorDirection, Direction};
use crate::types::{TypeMapping, ValueMapping};

#[derive(Debug)]
pub struct Erc20TokenObject;

impl BasicObject for Erc20TokenObject {
    fn name(&self) -> (&str, &str) {
        ERC20_TOKEN_NAME
    }

    fn type_name(&self) -> &str {
        ERC20_TYPE_NAME
    }

    fn type_mapping(&self) -> &TypeMapping {
        &ERC20_TOKEN_TYPE_MAPPING
    }
}

#[derive(Debug)]
pub struct Erc721TokenObject;

impl BasicObject for Erc721TokenObject {
    fn name(&self) -> (&str, &str) {
        ERC721_TOKEN_NAME
    }

    fn type_name(&self) -> &str {
        ERC721_TYPE_NAME
    }

    fn type_mapping(&self) -> &TypeMapping {
        &ERC721_TOKEN_TYPE_MAPPING
    }
}

#[derive(Debug)]
pub struct Erc1155TokenObject;

impl BasicObject for Erc1155TokenObject {
    fn name(&self) -> (&str, &str) {
        ERC1155_TOKEN_NAME
    }

    fn type_name(&self) -> &str {
        ERC1155_TYPE_NAME
    }

    fn type_mapping(&self) -> &TypeMapping {
        &ERC1155_TOKEN_TYPE_MAPPING
    }
}

#[derive(Debug, Clone)]
pub enum ErcTokenType {
    Erc20(Erc20Token),
    Erc721(Erc721Token),
    Erc1155(Erc1155Token),
}

#[derive(Debug, Clone)]
pub struct Erc20Token {
    pub name: String,
    pub symbol: String,
    pub decimals: u8,
    pub contract_address: String,
    pub amount: String,
}

#[derive(Debug, Clone)]
pub struct Erc721Token {
    pub name: String,
    pub symbol: String,
    pub token_id: String,
    pub contract_address: String,
    pub metadata: String,
    pub metadata_name: Option<String>,
    pub metadata_description: Option<String>,
    pub metadata_attributes: Option<String>,
    pub image_path: String,
}

#[derive(Debug, Clone)]
pub struct Erc1155Token {
    pub name: String,
    pub symbol: String,
    pub token_id: String,
    pub contract_address: String,
    pub amount: String,
    pub metadata: String,
    pub metadata_name: Option<String>,
    pub metadata_description: Option<String>,
    pub metadata_attributes: Option<String>,
    pub image_path: String,
}

impl ErcTokenType {
    pub fn to_field_value<'a>(self) -> FieldValue<'a> {
        match self {
            ErcTokenType::Erc20(token) => FieldValue::with_type(
                FieldValue::value(Value::Object(ValueMapping::from([
                    (Name::new("name"), Value::String(token.name)),
                    (Name::new("symbol"), Value::String(token.symbol)),
                    (Name::new("decimals"), Value::from(token.decimals)),
                    (
                        Name::new("contractAddress"),
                        Value::String(token.contract_address),
                    ),
                    (Name::new("amount"), Value::String(token.amount)),
                ]))),
                ERC20_TYPE_NAME.to_string(),
            ),
            ErcTokenType::Erc721(token) => FieldValue::with_type(
                FieldValue::value(Value::Object(ValueMapping::from([
                    (Name::new("name"), Value::String(token.name)),
                    (Name::new("symbol"), Value::String(token.symbol)),
                    (Name::new("tokenId"), Value::String(token.token_id)),
                    (
                        Name::new("contractAddress"),
                        Value::String(token.contract_address),
                    ),
                    (Name::new("metadata"), Value::String(token.metadata)),
                    (
                        Name::new("metadataName"),
                        token
                            .metadata_name
                            .map(Value::String)
                            .unwrap_or(Value::Null),
                    ),
                    (
                        Name::new("metadataDescription"),
                        token
                            .metadata_description
                            .map(Value::String)
                            .unwrap_or(Value::Null),
                    ),
                    (
                        Name::new("metadataAttributes"),
                        token
                            .metadata_attributes
                            .map(Value::String)
                            .unwrap_or(Value::Null),
                    ),
                    (Name::new("imagePath"), Value::String(token.image_path)),
                ]))),
                ERC721_TYPE_NAME.to_string(),
            ),
            ErcTokenType::Erc1155(token) => FieldValue::with_type(
                FieldValue::value(Value::Object(ValueMapping::from([
                    (Name::new("name"), Value::String(token.name)),
                    (Name::new("symbol"), Value::String(token.symbol)),
                    (Name::new("tokenId"), Value::String(token.token_id)),
                    (
                        Name::new("contractAddress"),
                        Value::String(token.contract_address),
                    ),
                    (Name::new("amount"), Value::String(token.amount)),
                    (Name::new("metadata"), Value::String(token.metadata)),
                    (
                        Name::new("metadataName"),
                        token
                            .metadata_name
                            .map(Value::String)
                            .unwrap_or(Value::Null),
                    ),
                    (
                        Name::new("metadataDescription"),
                        token
                            .metadata_description
                            .map(Value::String)
                            .unwrap_or(Value::Null),
                    ),
                    (
                        Name::new("metadataAttributes"),
                        token
                            .metadata_attributes
                            .map(Value::String)
                            .unwrap_or(Value::Null),
                    ),
                    (Name::new("imagePath"), Value::String(token.image_path)),
                ]))),
                ERC1155_TYPE_NAME.to_string(),
            ),
        }
    }
}

#[derive(Debug)]
pub struct TokenObject;

impl BasicObject for TokenObject {
    fn name(&self) -> (&str, &str) {
        ("tokens", "token")
    }

    fn type_name(&self) -> &str {
        "Token"
    }

    fn type_mapping(&self) -> &TypeMapping {
        &TOKEN_TYPE_MAPPING
    }
}

async fn fetch_tokens(
    conn: &mut SqliteConnection,
    contract_address: Option<String>,
    connection: &ConnectionArguments,
    total_count: i64,
) -> sqlx::Result<(Vec<SqliteRow>, PageInfo)> {
    let mut query = "SELECT t.*, c.contract_type 
                    FROM tokens t 
                    JOIN contracts c ON t.contract_address = c.contract_address"
        .to_string();

    let id_column = format!("t.{}", ID_COLUMN);
    let mut conditions = vec![];
    let mut binds = vec![];
    if let Some(addr) = contract_address {
        conditions.push(format!("t.contract_address = '{}'", addr));
    }

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
            None => data_limit + 1,
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
        " ORDER BY {} {} LIMIT {}",
        id_column,
        order_direction.as_ref(),
        limit
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
        if 1 < offset && offset < total_count as u64 {
            page_info.has_previous_page = true;
        }
        if limit + offset < total_count as u64 {
            page_info.has_next_page = true;
        }

        Ok((data, page_info))
    }
}

impl ResolvableObject for TokenObject {
    fn resolvers(&self) -> Vec<Field> {
        vec![
            // Query for multiple tokens with optional contract address filter
            connection_arguments(
                Field::new(
                    self.name().0, // "tokens"
                    TypeRef::named_nn(format!("{}Connection", self.type_name())),
                    move |ctx| {
                        FieldFuture::new(async move {
                            let mut conn = ctx.data::<Pool<Sqlite>>()?.acquire().await?;
                            let connection = parse_connection_arguments(&ctx)?;
                            let contract_address = ctx
                                .args
                                .get("contractAddress")
                                .map(|v| v.string().unwrap().to_string());

                            let mut count_query =
                                "SELECT COUNT(*) as count FROM tokens t".to_string();
                            if let Some(addr) = &contract_address {
                                count_query
                                    .push_str(&format!(" WHERE t.contract_address = '{}'", addr));
                            }

                            let total_count: i64 = sqlx::query(&count_query)
                                .fetch_one(&mut *conn)
                                .await?
                                .get("count");

                            let (data, page_info) =
                                fetch_tokens(&mut conn, contract_address, &connection, total_count)
                                    .await?;

                            let mut edges = Vec::new();
                            for row in data {
                                let token_metadata = token_from_row(&row)?;

                                edges.push(ConnectionEdge {
                                    node: token_metadata,
                                    cursor: cursor::encode(
                                        &row.get::<String, _>("id"),
                                        &row.get::<String, _>("id"),
                                    ),
                                });
                            }

                            Ok(Some(FieldValue::owned_any(Connection {
                                total_count,
                                edges,
                                page_info: PageInfoObject::value(page_info),
                            })))
                        })
                    },
                )
                .argument(InputValue::new(
                    "contractAddress",
                    TypeRef::named(TypeRef::STRING),
                )),
            ),
            // Query for single token by ID
            Field::new(
                self.name().1, // "token"
                TypeRef::named_nn(self.type_name()),
                move |ctx| {
                    FieldFuture::new(async move {
                        let pool = ctx.data::<Pool<Sqlite>>()?;
                        let token_id = ctx
                            .args
                            .get("id")
                            .and_then(|v| v.string().ok())
                            .ok_or_else(|| async_graphql::Error::new("Token ID is required"))?;

                        let query = "SELECT t.*, c.contract_type 
                                   FROM tokens t 
                                   JOIN contracts c ON t.contract_address = c.contract_address 
                                   WHERE t.id = ?";

                        let row = sqlx::query(query)
                            .bind(token_id)
                            .fetch_optional(pool)
                            .await?;

                        match row {
                            Some(row) => {
                                let token_metadata = token_from_row(&row)?;

                                Ok(Some(FieldValue::owned_any(token_metadata)))
                            }
                            None => Ok(None),
                        }
                    })
                },
            )
            .argument(InputValue::new("id", TypeRef::named_nn(TypeRef::STRING))),
        ]
    }

    fn subscriptions(&self) -> Option<Vec<SubscriptionField>> {
        Some(vec![SubscriptionField::new(
            "tokenUpdated",
            TypeRef::named_nn(self.type_name()),
            |ctx| {
                SubscriptionFieldFuture::new(async move {
                    let pool = ctx.data::<Pool<Sqlite>>()?;
                    Ok(MemoryBroker::<TokenUpdate>::subscribe()
                        .then(move |token| {
                            let pool = pool.clone();
                            async move {
                                // Fetch complete token data including contract type
                                let query = "SELECT t.*, c.contract_type
                                               FROM tokens t
                                               JOIN contracts c ON t.contract_address = \
                                             c.contract_address
                                               WHERE t.id = ?";

                                let token_id = if let Some(token_id) = token.token_id {
                                    felt_and_u256_to_sql_string(
                                        &token.contract_address,
                                        &token_id.into(),
                                    )
                                } else {
                                    felt_to_sql_string(&token.contract_address)
                                };
                                let row = match sqlx::query(query)
                                    .bind(token_id)
                                    .fetch_one(&pool)
                                    .await
                                {
                                    Ok(row) => row,
                                    Err(_) => return None,
                                };

                                let token_metadata = match token_from_row(&row) {
                                    Ok(token_metadata) => token_metadata,
                                    // Contract-level rows of NFT collections don't describe an
                                    // individual token and are not exposed by `tokens` either.
                                    Err(TokenRowError::Mapping(
                                        error @ TokenMappingError::ContractLevelNft { .. },
                                    )) => {
                                        debug!(%error, "Skipping token update.");
                                        return None;
                                    }
                                    Err(error) => {
                                        warn!(%error, "Failed to map updated token.");
                                        return None;
                                    }
                                };

                                Some(Ok(FieldValue::owned_any(token_metadata)))
                            }
                        })
                        .filter_map(|result| result))
                })
            },
        )])
    }
}

#[derive(Debug, thiserror::Error)]
enum TokenRowError {
    #[error(transparent)]
    Sql(#[from] sqlx::Error),
    #[error(transparent)]
    Mapping(#[from] TokenMappingError),
}

// Helper function to create token metadata from a `tokens` row joined with `contracts`
fn token_from_row(row: &SqliteRow) -> Result<ErcTokenType, TokenRowError> {
    let contract_type: String = row.try_get("contract_type")?;
    let token_key: String = row.try_get(ID_COLUMN)?;
    let contract_address: String = row.try_get("contract_address")?;
    let name: String = row.try_get("name")?;
    let symbol: String = row.try_get("symbol")?;
    let decimals: u8 = row.try_get("decimals")?;
    let metadata: Option<String> = row.try_get("metadata")?;

    Ok(map_token(&TokenColumns {
        contract_type: &contract_type,
        token_key: &token_key,
        contract_address: &contract_address,
        name: &name,
        symbol: &symbol,
        decimals,
        metadata: metadata.as_deref().unwrap_or_default(),
        // A token is not tied to an account, so there is no amount to report.
        amount: "0",
    })?)
}
