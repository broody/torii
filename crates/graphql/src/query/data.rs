use async_graphql::connection::PageInfo;
use sqlx::sqlite::{SqliteArguments, SqliteRow};
use sqlx::{Arguments, Result, SqliteConnection};
use torii_sqlite::constants::WORLD_CONTRACT_TYPE;

use super::filter::{Filter, FilterValue};
use super::order::{CursorDirection, Direction, Order};
use crate::constants::DEFAULT_LIMIT;
use crate::object::connection::{cursor, ConnectionArguments};

/// A value bound to a `?` placeholder.
#[derive(Clone, Debug, PartialEq)]
enum SqlValue {
    Int(i64),
    Text(String),
}

/// A SQL condition whose `?` placeholders are bound, in order, to `binds`.
#[derive(Clone, Debug, PartialEq)]
struct SqlCondition {
    sql: String,
    binds: Vec<SqlValue>,
}

/// Bracket-quotes a SQL identifier (table or column name).
///
/// Identifiers cannot be bound as parameters and SQLite has no escape for `]` inside a
/// bracket-quoted identifier, so names containing it are rejected.
pub fn quote_identifier(name: &str) -> Result<String> {
    if name.contains(']') {
        return Err(sqlx::Error::Encode(
            format!("invalid SQL identifier: {name}").into(),
        ));
    }

    Ok(format!("[{name}]"))
}

fn arguments(binds: &[SqlValue]) -> Result<SqliteArguments<'static>> {
    let mut arguments = SqliteArguments::default();
    for bind in binds {
        match bind {
            SqlValue::Int(i) => arguments.add(*i),
            SqlValue::Text(s) => arguments.add(s.clone()),
        }
        .map_err(sqlx::Error::Encode)?;
    }

    Ok(arguments)
}

/// Appends ` WHERE c1 AND c2 ...` to `sql` and returns the values to bind, in order.
fn push_where_clause(sql: &mut String, conditions: &[SqlCondition]) -> Vec<SqlValue> {
    if conditions.is_empty() {
        return Vec::new();
    }

    let clause = conditions
        .iter()
        .map(|condition| condition.sql.as_str())
        .collect::<Vec<_>>()
        .join(" AND ");
    sql.push_str(" WHERE ");
    sql.push_str(&clause);

    conditions
        .iter()
        .flat_map(|condition| condition.binds.iter().cloned())
        .collect()
}

fn sql_integer(value: u64) -> SqlValue {
    SqlValue::Int(i64::try_from(value).unwrap_or(i64::MAX))
}

pub async fn count_rows(
    conn: &mut SqliteConnection,
    table_name: &str,
    keys: &Option<Vec<String>>,
    filters: &Option<Vec<Filter>>,
) -> Result<i64> {
    let mut query = format!("SELECT COUNT(*) FROM {}", quote_identifier(table_name)?);
    let conditions = build_conditions(keys, filters)?;
    let binds = push_where_clause(&mut query, &conditions);

    sqlx::query_scalar_with(&query, arguments(&binds)?)
        .fetch_one(conn)
        .await
}

pub async fn fetch_world_address(conn: &mut SqliteConnection) -> Result<String> {
    let query = "SELECT contract_address FROM contracts where contract_type = ?".to_string();
    // for now we only have one world contract so this works
    let res: (String,) = sqlx::query_as(&query)
        .bind(WORLD_CONTRACT_TYPE)
        .fetch_one(conn)
        .await?;
    Ok(res.0)
}

#[derive(Clone, Debug)]
pub struct JoinConfig {
    pub table: String,
    pub alias: Option<String>,
    pub on_condition: String,
    pub join_type: JoinType,
}

#[derive(Clone, Debug)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
}

impl JoinType {
    fn as_sql(&self) -> &'static str {
        match self {
            JoinType::Inner => "INNER JOIN",
            JoinType::Left => "LEFT JOIN",
            JoinType::Right => "LEFT JOIN", // SQLite doesn't support RIGHT JOIN, so we use LEFT
            JoinType::Full => "LEFT JOIN",  // SQLite doesn't support FULL JOIN, so we use LEFT
        }
    }
}

pub async fn fetch_single_row(
    conn: &mut SqliteConnection,
    table_name: &str,
    id_column: &str,
    id: &str,
) -> sqlx::Result<SqliteRow> {
    let query = format!(
        "SELECT * FROM {} WHERE {} = ?",
        quote_identifier(table_name)?,
        quote_identifier(id_column)?
    );
    sqlx::query(&query).bind(id).fetch_one(conn).await
}

pub async fn fetch_single_row_with_joins(
    conn: &mut SqliteConnection,
    table_name: &str,
    id_column: &str,
    id: &str,
    joins: Vec<JoinConfig>,
    select_columns: Option<Vec<String>>,
) -> sqlx::Result<SqliteRow> {
    let table = quote_identifier(table_name)?;

    // Build the SELECT clause
    let select = match select_columns {
        Some(columns) => columns.join(", "),
        None => format!("{table}.*"),
    };

    // Build the JOIN clauses
    let join_clauses = joins
        .iter()
        .map(|join| {
            let join_table = quote_identifier(&join.table)?;
            let table_ref = match &join.alias {
                Some(alias) => format!("{join_table} AS {alias}"),
                None => join_table,
            };
            Ok(format!(
                "{} {} ON {}",
                join.join_type.as_sql(),
                table_ref,
                join.on_condition
            ))
        })
        .collect::<Result<Vec<String>>>()?
        .join(" ");

    // Build the complete query
    let query = format!(
        "SELECT {select} FROM {table} {join_clauses} WHERE {table}.{} = ?",
        quote_identifier(id_column)?
    );

    sqlx::query(&query).bind(id).fetch_one(conn).await
}

/// Fetches one page of rows using keyset (cursor) or offset/limit pagination.
///
/// Rows are listed by `order` (ties broken by `id_column` in the same direction) or, without an
/// explicit order, by `id_column` descending. `first` reads the listing from its start and `last`
/// from its end, returning rows in the order they were read. `after`/`before` restrict the
/// listing to rows strictly after/before the cursor row.
///
/// `hasNextPage` reports whether more rows follow the page in read order. `hasPreviousPage`
/// reports whether rows exist on the other side of the cursor the page starts from (`after` when
/// reading forward, `before` with `last`), or whether `offset` skipped rows.
///
/// `_total_count` is unused and kept for signature compatibility.
#[allow(clippy::too_many_arguments)]
pub async fn fetch_multiple_rows(
    conn: &mut SqliteConnection,
    table_name: &str,
    id_column: &str,
    keys: &Option<Vec<String>>,
    order: &Option<Order>,
    filters: &Option<Vec<Filter>>,
    connection: &ConnectionArguments,
    _total_count: i64,
) -> Result<(Vec<SqliteRow>, PageInfo)> {
    let table = quote_identifier(table_name)?;
    let id_sql_column = quote_identifier(id_column)?;
    let base_conditions = build_conditions(keys, filters)?;

    let listing_direction = order
        .as_ref()
        .map_or(Direction::Desc, |order| order.direction);
    let is_backward = connection.last.is_some();
    let read_direction = if is_backward {
        listing_direction.reverse()
    } else {
        listing_direction
    };

    let after = connection
        .after
        .as_deref()
        .map(|c| {
            cursor_condition(
                c,
                order,
                CursorDirection::After,
                listing_direction,
                id_column,
            )
        })
        .transpose()?;
    let before = connection
        .before
        .as_deref()
        .map(|c| {
            cursor_condition(
                c,
                order,
                CursorDirection::Before,
                listing_direction,
                id_column,
            )
        })
        .transpose()?;
    // The cursor the page is read from. Rows on its other side form the previous page.
    let start_cursor_condition = if is_backward {
        before.clone()
    } else {
        after.clone()
    };

    let mut conditions = base_conditions.clone();
    conditions.extend(after);
    conditions.extend(before);

    let is_cursor_based = connection.first.is_some()
        || connection.last.is_some()
        || connection.after.is_some()
        || connection.before.is_some();

    let page_size = connection
        .first
        .or(connection.last)
        .or(connection.limit)
        .unwrap_or(DEFAULT_LIMIT);

    let direction = read_direction.as_ref();
    let order_by = match order {
        Some(order) => format!(
            "{} {direction}, {id_sql_column} {direction}",
            quote_identifier(&order.field)?
        ),
        None => format!("{id_sql_column} {direction}"),
    };

    let mut query = format!("SELECT * FROM {table}");
    let mut binds = push_where_clause(&mut query, &conditions);
    // Fetch one extra row to know whether a next page exists.
    query.push_str(&format!(" ORDER BY {order_by} LIMIT ?"));
    binds.push(sql_integer(page_size.saturating_add(1)));
    if let Some(offset) = connection.offset {
        query.push_str(" OFFSET ?");
        binds.push(sql_integer(offset));
    }

    let mut data = sqlx::query_with(&query, arguments(&binds)?)
        .fetch_all(&mut *conn)
        .await?;
    let mut page_info = PageInfo {
        has_previous_page: false,
        has_next_page: false,
        start_cursor: None,
        end_cursor: None,
    };

    if data.is_empty() {
        return Ok((data, page_info));
    }

    let page_size = usize::try_from(page_size).unwrap_or(usize::MAX);
    if data.len() > page_size {
        page_info.has_next_page = true;
        data.truncate(page_size);
    }

    if !is_cursor_based {
        page_info.has_previous_page = connection.offset.unwrap_or(0) > 0;
        return Ok((data, page_info));
    }

    if let Some(start_cursor_condition) = start_cursor_condition {
        page_info.has_previous_page =
            rows_exist_outside(conn, &table, &base_conditions, &start_cursor_condition).await?;
    }

    let order_column = match order {
        Some(order) => order.field.as_str(),
        None => id_column,
    };
    if let (Some(first), Some(last)) = (data.first(), data.last()) {
        page_info.start_cursor = Some(cursor::encode_row(first, id_column, order_column)?);
        page_info.end_cursor = Some(cursor::encode_row(last, id_column, order_column)?);
    }

    Ok((data, page_info))
}

/// Whether any row matching `conditions` falls outside `cursor_condition`, i.e. on the other side
/// of the cursor (the cursor row included).
async fn rows_exist_outside(
    conn: &mut SqliteConnection,
    table: &str,
    conditions: &[SqlCondition],
    cursor_condition: &SqlCondition,
) -> Result<bool> {
    let mut conditions = conditions.to_vec();
    conditions.push(SqlCondition {
        sql: format!("NOT ({})", cursor_condition.sql),
        binds: cursor_condition.binds.clone(),
    });

    let mut query = format!("SELECT EXISTS(SELECT 1 FROM {table}");
    let binds = push_where_clause(&mut query, &conditions);
    query.push(')');

    sqlx::query_scalar_with(&query, arguments(&binds)?)
        .fetch_one(conn)
        .await
}

/// Builds the keyset condition selecting rows strictly after (or before) the cursor row in a
/// listing ordered by `listing_direction`.
///
/// Cursor values are bound as text. Comparisons are made directly against the column, so SQLite
/// applies the column's affinity to the bound value: INTEGER columns compare numerically and TEXT
/// columns (zero-padded hex, strings) compare as text.
fn cursor_condition(
    cursor: &str,
    order: &Option<Order>,
    direction: CursorDirection,
    listing_direction: Direction,
    id_column: &str,
) -> Result<SqlCondition> {
    let (id_value, order_value) =
        cursor::decode(cursor).map_err(|_| sqlx::Error::Decode("Invalid cursor format".into()))?;
    let id_column = quote_identifier(id_column)?;
    let op = direction.comparator(listing_direction);

    match order {
        Some(order) => {
            let order_column = quote_identifier(&order.field)?;
            Ok(SqlCondition {
                sql: format!(
                    "({order_column} {op} ? OR ({order_column} = ? AND {id_column} {op} ?))"
                ),
                binds: vec![
                    SqlValue::Text(order_value.clone()),
                    SqlValue::Text(order_value),
                    SqlValue::Text(id_value),
                ],
            })
        }
        None => Ok(SqlCondition {
            sql: format!("{id_column} {op} ?"),
            binds: vec![SqlValue::Text(id_value)],
        }),
    }
}

fn build_conditions(
    keys: &Option<Vec<String>>,
    filters: &Option<Vec<Filter>>,
) -> Result<Vec<SqlCondition>> {
    let mut conditions = Vec::new();

    if let Some(keys) = keys {
        if !keys.is_empty() {
            // regex is used if first element is wildcard, otherwise default to `like` which is more
            // performant
            let use_regex = keys.first().is_some_and(|k| k == "*");
            let pattern = keys_to_pattern(keys, use_regex);

            let condition_type = if use_regex { "REGEXP" } else { "LIKE" };
            conditions.push(SqlCondition {
                sql: format!("keys {condition_type} ?"),
                binds: vec![SqlValue::Text(pattern)],
            });
        }
    }

    if let Some(filters) = filters {
        for filter in filters {
            let column = quote_identifier(&filter.field)?;
            let comparator = &filter.comparator;
            let condition = match &filter.value {
                FilterValue::List(list) => {
                    let binds = list
                        .iter()
                        .map(|value| match value {
                            FilterValue::Int(i) => Ok(SqlValue::Int(*i)),
                            FilterValue::String(s) => Ok(SqlValue::Text(s.clone())),
                            FilterValue::List(_) => Err(sqlx::Error::Encode(
                                format!(
                                    "nested list values are not supported for {}",
                                    filter.field
                                )
                                .into(),
                            )),
                        })
                        .collect::<Result<Vec<_>>>()?;
                    let placeholders = vec!["?"; binds.len()].join(", ");
                    SqlCondition {
                        sql: format!("{column} {comparator} ({placeholders})"),
                        binds,
                    }
                }
                FilterValue::Int(i) => SqlCondition {
                    sql: format!("{column} {comparator} ?"),
                    binds: vec![SqlValue::Int(*i)],
                },
                FilterValue::String(s) => SqlCondition {
                    sql: format!("{column} {comparator} ?"),
                    binds: vec![SqlValue::Text(s.clone())],
                },
            };
            conditions.push(condition);
        }
    }

    Ok(conditions)
}

fn keys_to_pattern(keys: &[String], use_regex: bool) -> String {
    let pattern = keys
        .iter()
        .map(|key| {
            if use_regex {
                match key.as_str() {
                    "*" => "([^/]*)".to_string(),
                    _ => regex::escape(key),
                }
            } else {
                key.replace('*', "%")
            }
        })
        .collect::<Vec<_>>()
        .join("/");

    match use_regex {
        true => format!("^{}.*", pattern),
        false => format!("{}/%", pattern),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use sqlx::{Connection, Row};

    use super::*;
    use crate::query::filter::Comparator;

    const TABLE: &str = "ns-Item";
    const ROWS: i64 = 25;

    fn row_id(i: i64) -> String {
        // ids contain '/' and ':' like token balance ids, to exercise the cursor format
        format!("0x{i:02x}/0x1:0x{i:02x}")
    }

    /// Items `0..ROWS` with: `amount = i` (INTEGER with values >= 10), `score = i % 4` (ties),
    /// `order = i` (reserved keyword column), `typeCamel = ROWS - i` (camelCase column).
    async fn setup() -> SqliteConnection {
        let mut conn = SqliteConnection::connect("sqlite::memory:").await.unwrap();
        sqlx::query(
            "CREATE TABLE [ns-Item] (id TEXT NOT NULL PRIMARY KEY, amount INTEGER NOT NULL, score \
             INTEGER NOT NULL, [order] INTEGER NOT NULL, typeCamel INTEGER NOT NULL, name TEXT \
             NOT NULL, keys TEXT NOT NULL)",
        )
        .execute(&mut conn)
        .await
        .unwrap();

        for i in 0..ROWS {
            sqlx::query("INSERT INTO [ns-Item] VALUES (?, ?, ?, ?, ?, ?, ?)")
                .bind(row_id(i))
                .bind(i)
                .bind(i % 4)
                .bind(i)
                .bind(ROWS - i)
                .bind(format!("item-{i}"))
                .bind(format!("0x{:x}/0x{i:x}/", i % 2))
                .execute(&mut conn)
                .await
                .unwrap();
        }

        conn
    }

    fn order(field: &str, direction: Direction) -> Option<Order> {
        Some(Order {
            field: field.to_string(),
            direction,
        })
    }

    fn args() -> ConnectionArguments {
        ConnectionArguments {
            first: None,
            last: None,
            after: None,
            before: None,
            offset: None,
            limit: None,
        }
    }

    fn ids(rows: &[SqliteRow]) -> Vec<String> {
        rows.iter().map(|row| row.get::<String, _>("id")).collect()
    }

    /// Row ids sorted by `(field, id)` in `direction`, as the listing should return them.
    fn expected_listing(field: impl Fn(i64) -> i64, direction: Direction) -> Vec<String> {
        let mut items: Vec<(i64, String)> = (0..ROWS).map(|i| (field(i), row_id(i))).collect();
        items.sort();
        if direction == Direction::Desc {
            items.reverse();
        }
        items.into_iter().map(|(_, id)| id).collect()
    }

    async fn fetch(
        conn: &mut SqliteConnection,
        order: &Option<Order>,
        filters: &Option<Vec<Filter>>,
        connection: &ConnectionArguments,
    ) -> (Vec<SqliteRow>, PageInfo) {
        fetch_multiple_rows(conn, TABLE, "id", &None, order, filters, connection, 0)
            .await
            .unwrap()
    }

    /// Follows `endCursor` with `first: page_size` until `hasNextPage` is false.
    async fn paginate_forward(
        conn: &mut SqliteConnection,
        order: &Option<Order>,
        page_size: u64,
    ) -> Vec<String> {
        let mut all = Vec::new();
        let mut after = None;
        for _ in 0..=ROWS {
            let connection = ConnectionArguments {
                first: Some(page_size),
                after: after.clone(),
                ..args()
            };
            let (rows, page_info) = fetch(conn, order, &None, &connection).await;
            assert!(rows.len() as u64 <= page_size);
            assert_eq!(page_info.has_previous_page, after.is_some());
            all.extend(ids(&rows));
            if !page_info.has_next_page {
                return all;
            }
            after = page_info.end_cursor;
        }
        panic!("pagination did not terminate");
    }

    #[tokio::test]
    async fn asc_order_after_cursor_returns_next_page() {
        let mut conn = setup().await;
        let order = order("amount", Direction::Asc);

        let first_page = ConnectionArguments {
            first: Some(10),
            ..args()
        };
        let (page1, info1) = fetch(&mut conn, &order, &None, &first_page).await;
        assert_eq!(ids(&page1), (0..10).map(row_id).collect::<Vec<_>>());
        assert!(!info1.has_previous_page);
        assert!(info1.has_next_page);

        // amount is an INTEGER column: 10 must sort after 9, not lexically before it
        let second_page = ConnectionArguments {
            first: Some(10),
            after: info1.end_cursor.clone(),
            ..args()
        };
        let (page2, info2) = fetch(&mut conn, &order, &None, &second_page).await;
        assert_eq!(ids(&page2), (10..20).map(row_id).collect::<Vec<_>>());
        assert!(info2.has_previous_page);
        assert!(info2.has_next_page);

        let third_page = ConnectionArguments {
            first: Some(10),
            after: info2.end_cursor.clone(),
            ..args()
        };
        let (page3, info3) = fetch(&mut conn, &order, &None, &third_page).await;
        assert_eq!(ids(&page3), (20..ROWS).map(row_id).collect::<Vec<_>>());
        assert!(info3.has_previous_page);
        assert!(!info3.has_next_page);
    }

    #[tokio::test]
    async fn integer_order_paginates_numerically_in_both_directions() {
        let mut conn = setup().await;
        for direction in [Direction::Asc, Direction::Desc] {
            let listing = paginate_forward(&mut conn, &order("amount", direction), 3).await;
            assert_eq!(listing, expected_listing(|i| i, direction));
        }
    }

    #[tokio::test]
    async fn ties_in_order_field_paginate_without_duplicates_or_gaps() {
        let mut conn = setup().await;
        for direction in [Direction::Asc, Direction::Desc] {
            for page_size in [1, 2, 3, 4, 5, 7] {
                let listing =
                    paginate_forward(&mut conn, &order("score", direction), page_size).await;
                assert_eq!(
                    listing,
                    expected_listing(|i| i % 4, direction),
                    "direction {direction:?}, page size {page_size}"
                );
                assert_eq!(listing.iter().collect::<HashSet<_>>().len(), ROWS as usize);
            }
        }
    }

    #[tokio::test]
    async fn default_order_paginates_by_id() {
        let mut conn = setup().await;
        let listing = paginate_forward(&mut conn, &None, 4).await;
        assert_eq!(listing, expected_listing(|_| 0, Direction::Desc));
    }

    #[tokio::test]
    async fn last_and_before_read_backwards_from_cursor() {
        let mut conn = setup().await;
        let everything = ConnectionArguments {
            first: Some(ROWS as u64),
            ..args()
        };

        // default order: the listing is id DESC and `last` reads it from the end (id ASC)
        let desc = expected_listing(|_| 0, Direction::Desc);
        let (all, _) = fetch(&mut conn, &None, &None, &everything).await;
        assert_eq!(ids(&all), desc);

        let connection = ConnectionArguments {
            last: Some(2),
            before: Some(cursor::encode_row(&all[6], "id", "id").unwrap()),
            ..args()
        };
        let (rows, info) = fetch(&mut conn, &None, &None, &connection).await;
        assert_eq!(ids(&rows), vec![desc[5].clone(), desc[4].clone()]);
        assert!(info.has_previous_page);
        assert!(info.has_next_page);

        let connection = ConnectionArguments {
            last: Some(3),
            ..args()
        };
        let (rows, info) = fetch(&mut conn, &None, &None, &connection).await;
        assert_eq!(
            ids(&rows),
            vec![desc[24].clone(), desc[23].clone(), desc[22].clone()]
        );
        assert!(!info.has_previous_page);
        assert!(info.has_next_page);

        // explicit DESC order with ties: the rows immediately before the cursor, nearest first
        let order = order("score", Direction::Desc);
        let listing = expected_listing(|i| i % 4, Direction::Desc);
        let (all, _) = fetch(&mut conn, &order, &None, &everything).await;
        assert_eq!(ids(&all), listing);
        let cursor = cursor::encode_row(&all[10], "id", "score").unwrap();

        let connection = ConnectionArguments {
            last: Some(4),
            before: Some(cursor.clone()),
            ..args()
        };
        let (rows, info) = fetch(&mut conn, &order, &None, &connection).await;
        let mut expected = listing[6..10].to_vec();
        expected.reverse();
        assert_eq!(ids(&rows), expected);
        assert!(info.has_previous_page);
        assert!(info.has_next_page);

        // reading backwards up to the start of the listing
        let connection = ConnectionArguments {
            last: Some(20),
            before: Some(cursor.clone()),
            ..args()
        };
        let (rows, info) = fetch(&mut conn, &order, &None, &connection).await;
        let mut expected = listing[..10].to_vec();
        expected.reverse();
        assert_eq!(ids(&rows), expected);
        assert!(info.has_previous_page);
        assert!(!info.has_next_page);

        // DESC + after returns the rows following the cursor
        let connection = ConnectionArguments {
            first: Some(3),
            after: Some(cursor),
            ..args()
        };
        let (rows, _) = fetch(&mut conn, &order, &None, &connection).await;
        assert_eq!(ids(&rows), listing[11..14].to_vec());
    }

    #[tokio::test]
    async fn reserved_and_camel_case_columns_can_be_ordered() {
        let mut conn = setup().await;

        // `order` is an SQL keyword
        let listing = paginate_forward(&mut conn, &order("order", Direction::Asc), 6).await;
        assert_eq!(listing, expected_listing(|i| i, Direction::Asc));

        // order fields come lowercased from the GraphQL enum; `typeCamel` must still match
        let listing = paginate_forward(&mut conn, &order("typecamel", Direction::Asc), 6).await;
        assert_eq!(listing, expected_listing(|i| ROWS - i, Direction::Asc));
    }

    #[tokio::test]
    async fn empty_page_after_last_cursor_has_no_page_info() {
        let mut conn = setup().await;
        let everything = ConnectionArguments {
            first: Some(ROWS as u64),
            ..args()
        };
        let (all, _) = fetch(&mut conn, &None, &None, &everything).await;

        let connection = ConnectionArguments {
            first: Some(1),
            after: Some(cursor::encode_row(all.last().unwrap(), "id", "id").unwrap()),
            ..args()
        };
        let (rows, info) = fetch(&mut conn, &None, &None, &connection).await;
        assert!(rows.is_empty());
        assert!(!info.has_previous_page);
        assert!(!info.has_next_page);
        assert_eq!(info.start_cursor, None);
        assert_eq!(info.end_cursor, None);
    }

    #[tokio::test]
    async fn offset_limit_page_info_does_not_need_total_count() {
        let mut conn = setup().await;
        let desc = expected_listing(|_| 0, Direction::Desc);
        let page = |offset: Option<u64>, limit: Option<u64>| ConnectionArguments {
            offset,
            limit,
            ..args()
        };

        // an offset of 1 already skips a row
        let (rows, info) = fetch(&mut conn, &None, &None, &page(Some(1), Some(5))).await;
        assert_eq!(ids(&rows), desc[1..6].to_vec());
        assert!(info.has_previous_page);
        assert!(info.has_next_page);
        assert_eq!(info.start_cursor, None);
        assert_eq!(info.end_cursor, None);

        let (rows, info) = fetch(&mut conn, &None, &None, &page(None, Some(5))).await;
        assert_eq!(ids(&rows), desc[..5].to_vec());
        assert!(!info.has_previous_page);
        assert!(info.has_next_page);

        // exactly the last rows: no next page
        let (rows, info) = fetch(&mut conn, &None, &None, &page(Some(20), Some(5))).await;
        assert_eq!(ids(&rows), desc[20..].to_vec());
        assert!(info.has_previous_page);
        assert!(!info.has_next_page);

        // no arguments: default limit
        let (rows, info) = fetch(&mut conn, &None, &None, &args()).await;
        assert_eq!(rows.len() as u64, DEFAULT_LIMIT);
        assert!(!info.has_previous_page);
        assert!(info.has_next_page);

        let (rows, info) = fetch(&mut conn, &None, &None, &page(Some(ROWS as u64), Some(5))).await;
        assert!(rows.is_empty());
        assert!(!info.has_previous_page);
        assert!(!info.has_next_page);
    }

    #[tokio::test]
    async fn filters_and_cursors_paginate_together() {
        let mut conn = setup().await;
        let filters = Some(vec![Filter {
            field: "amount".to_string(),
            comparator: Comparator::Gte,
            value: FilterValue::Int(10),
        }]);
        let order = order("amount", Direction::Asc);

        let connection = ConnectionArguments {
            first: Some(5),
            ..args()
        };
        let (page1, info1) = fetch(&mut conn, &order, &filters, &connection).await;
        assert_eq!(ids(&page1), (10..15).map(row_id).collect::<Vec<_>>());
        assert!(!info1.has_previous_page);

        let connection = ConnectionArguments {
            first: Some(5),
            after: info1.end_cursor,
            ..args()
        };
        let (page2, info2) = fetch(&mut conn, &order, &filters, &connection).await;
        assert_eq!(ids(&page2), (15..20).map(row_id).collect::<Vec<_>>());
        assert!(info2.has_previous_page);
        assert!(info2.has_next_page);

        let count = count_rows(&mut conn, TABLE, &None, &filters).await.unwrap();
        assert_eq!(count, 15);
    }

    #[tokio::test]
    async fn filter_values_are_bound() {
        let mut conn = setup().await;
        let filter = |field: &str, comparator, value| {
            Some(vec![Filter {
                field: field.to_string(),
                comparator,
                value,
            }])
        };
        let injection = "x' OR '1'='1".to_string();

        let filters = filter(
            "name",
            Comparator::Eq,
            FilterValue::String(injection.clone()),
        );
        assert_eq!(
            count_rows(&mut conn, TABLE, &None, &filters).await.unwrap(),
            0
        );

        let filters = filter(
            "name",
            Comparator::In,
            FilterValue::List(vec![
                FilterValue::String("item-1".to_string()),
                FilterValue::String(injection),
                FilterValue::String("item-2".to_string()),
            ]),
        );
        assert_eq!(
            count_rows(&mut conn, TABLE, &None, &filters).await.unwrap(),
            2
        );

        let filters = filter(
            "amount",
            Comparator::NotIn,
            FilterValue::List(vec![FilterValue::Int(1), FilterValue::Int(2)]),
        );
        assert_eq!(
            count_rows(&mut conn, TABLE, &None, &filters).await.unwrap(),
            ROWS - 2
        );

        let filters = filter(
            "name",
            Comparator::Like,
            FilterValue::String("item-1%".to_string()),
        );
        assert_eq!(
            count_rows(&mut conn, TABLE, &None, &filters).await.unwrap(),
            11
        );

        // identifiers that would escape the bracket quoting are rejected
        let filters = filter("amount] OR 1=1 --", Comparator::Eq, FilterValue::Int(1));
        let err = count_rows(&mut conn, TABLE, &None, &filters)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("invalid SQL identifier"));
    }

    #[tokio::test]
    async fn keys_pattern_is_bound() {
        let mut conn = setup().await;

        let keys = Some(vec!["0x1".to_string()]);
        assert_eq!(
            count_rows(&mut conn, TABLE, &keys, &None).await.unwrap(),
            12
        );

        let keys = Some(vec!["0x1' OR '1'='1".to_string()]);
        assert_eq!(count_rows(&mut conn, TABLE, &keys, &None).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn ids_and_cursor_values_are_bound() {
        let mut conn = setup().await;
        let injection = "x' OR '1'='1";

        let row = fetch_single_row(&mut conn, TABLE, "id", &row_id(3))
            .await
            .unwrap();
        assert_eq!(row.get::<i64, _>("amount"), 3);

        let result = fetch_single_row(&mut conn, TABLE, "id", injection).await;
        assert!(matches!(result, Err(sqlx::Error::RowNotFound)));

        let row = fetch_single_row_with_joins(&mut conn, TABLE, "id", &row_id(4), vec![], None)
            .await
            .unwrap();
        assert_eq!(row.get::<i64, _>("amount"), 4);

        let result =
            fetch_single_row_with_joins(&mut conn, TABLE, "id", injection, vec![], None).await;
        assert!(matches!(result, Err(sqlx::Error::RowNotFound)));

        // an attacker-chosen cursor is only ever compared as a value
        let connection = ConnectionArguments {
            first: Some(ROWS as u64),
            after: Some(cursor::encode(injection, "0) OR 1=1 --")),
            ..args()
        };
        let (rows, _) = fetch(
            &mut conn,
            &order("amount", Direction::Asc),
            &None,
            &connection,
        )
        .await;
        assert!(rows.len() < ROWS as usize);

        let connection = ConnectionArguments {
            first: Some(1),
            after: Some("not a cursor".to_string()),
            ..args()
        };
        let result =
            fetch_multiple_rows(&mut conn, TABLE, "id", &None, &None, &None, &connection, 0).await;
        assert!(matches!(result, Err(sqlx::Error::Decode(_))));
    }

    #[tokio::test]
    async fn legacy_cursor_is_accepted() {
        use base64::engine::general_purpose;
        use base64::Engine as _;

        let mut conn = setup().await;
        let desc = expected_listing(|_| 0, Direction::Desc);
        // legacy cursors only worked for ids without '/'; `0x` sorts below every item id, so it
        // is positioned at the end of the default (id DESC) listing
        let legacy = general_purpose::STANDARD.encode("cursor/0x/0x");

        let connection = ConnectionArguments {
            last: Some(2),
            before: Some(legacy),
            ..args()
        };
        let (rows, _) = fetch(&mut conn, &None, &None, &connection).await;
        assert_eq!(ids(&rows), vec![desc[24].clone(), desc[23].clone()]);
    }
}
