//! GraphQL-level tests of the shared query layer (filters, ordering, pagination, argument
//! validation) against an in-memory database, without spinning up katana.
#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;

    use async_graphql::dynamic::Schema;
    use dojo_types::naming::get_tag;
    use dojo_types::primitive::Primitive;
    use dojo_types::schema::{Member, Struct, Ty};
    use dojo_world::contracts::abigen::model::Layout;
    use dojo_world::contracts::naming::compute_selector_from_tag;
    use serde_json::Value;
    use sqlx::SqlitePool;
    use starknet::providers::jsonrpc::HttpTransport;
    use starknet::providers::JsonRpcClient;
    use starknet_crypto::Felt;
    use tokio::sync::broadcast;
    use torii_messaging::{Messaging, MessagingConfig};
    use torii_sqlite::executor::Executor;
    use torii_sqlite::Sql;
    use torii_storage::proto::{ContractDefinition, ContractType};
    use torii_storage::Storage;
    use url::Url;

    use crate::schema::build_schema;
    use crate::tests::run_graphql_query;

    const ITEMS: u32 = 25;
    const NODE_FIELDS: &str =
        "item_id amount score order typeCamel name big nested { inner } pair { _0 _1 }";

    fn member(name: &str, key: bool, ty: Ty) -> Member {
        Member {
            name: name.to_string(),
            key,
            ty,
        }
    }

    /// Registers the `qtest-Item` model and inserts `ITEMS` rows where, for item `i`:
    /// `amount = i`, `score = i % 4`, `order = i`, `typeCamel = ITEMS - i`, `name = "item-{i}"`,
    /// `big = i` (u64, stored as hex text), `nested.inner = i` and `pair = (i % 5, i)`.
    async fn setup(pool: &SqlitePool) -> Schema {
        let (shutdown_tx, _) = broadcast::channel(1);
        let url: Url = "https://www.example.com".parse().unwrap();
        let provider = Arc::new(JsonRpcClient::new(HttpTransport::new(url)));
        let (mut executor, sender) =
            Executor::new(pool.clone(), shutdown_tx.clone(), provider.clone())
                .await
                .unwrap();
        tokio::spawn(async move {
            executor.run().await.unwrap();
        });

        let db = Sql::new(
            pool.clone(),
            sender,
            &[ContractDefinition {
                address: Felt::ZERO,
                r#type: ContractType::WORLD,
                starting_block: None,
            }],
        )
        .await
        .unwrap();

        let tag = get_tag("qtest", "Item");
        let model = Ty::Struct(Struct {
            name: tag.clone(),
            children: vec![
                member("item_id", true, Ty::Primitive(Primitive::U32(None))),
                member("amount", false, Ty::Primitive(Primitive::U8(None))),
                member("score", false, Ty::Primitive(Primitive::U8(None))),
                member("order", false, Ty::Primitive(Primitive::U16(None))),
                member("typeCamel", false, Ty::Primitive(Primitive::U32(None))),
                member("name", false, Ty::ByteArray(String::new())),
                member("big", false, Ty::Primitive(Primitive::U64(None))),
                member(
                    "nested",
                    false,
                    Ty::Struct(Struct {
                        name: "Inner".to_string(),
                        children: vec![member("inner", false, Ty::Primitive(Primitive::U8(None)))],
                    }),
                ),
                member(
                    "pair",
                    false,
                    Ty::Tuple(vec![
                        Ty::Primitive(Primitive::U8(None)),
                        Ty::Primitive(Primitive::U8(None)),
                    ]),
                ),
            ],
        });

        db.register_model(
            Felt::ZERO,
            compute_selector_from_tag(&tag),
            &model,
            &Layout::Fixed(vec![]),
            Felt::ONE,
            Felt::TWO,
            0,
            0,
            1710754478_u64,
            None,
            None,
            true,
        )
        .await
        .unwrap();
        db.execute().await.unwrap();

        for i in 0..ITEMS {
            sqlx::query(
                "INSERT INTO [qtest-Item] (internal_id, internal_event_id, internal_executed_at, \
                 item_id, amount, score, [order], typeCamel, name, big, [nested.inner], [pair.0], \
                 [pair.1]) VALUES (?, ?, CURRENT_TIMESTAMP, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(format!("0x{i:x}"))
            .bind(format!("0x{i:04x}:0x0:0x0"))
            .bind(i)
            .bind(i)
            .bind(i % 4)
            .bind(i)
            .bind(ITEMS - i)
            .bind(format!("item-{i}"))
            .bind(Primitive::U64(Some(i.into())).to_sql_value())
            .bind(i)
            .bind(i % 5)
            .bind(i)
            .execute(pool)
            .await
            .unwrap();
        }

        let messaging = Arc::new(Messaging::new(
            MessagingConfig::default(),
            Arc::new(db.clone()),
            provider,
        ));
        build_schema(pool, messaging, Arc::new(db)).await.unwrap()
    }

    async fn items_query(schema: &Schema, args: &str) -> Value {
        let query = format!(
            "{{ qtestItemModels {args} {{ totalCount edges {{ cursor node {{ {NODE_FIELDS} }} }} \
             pageInfo {{ hasPreviousPage hasNextPage startCursor endCursor }} }} }}"
        );
        run_graphql_query(schema, &query)
            .await
            .get("qtestItemModels")
            .cloned()
            .unwrap()
    }

    fn item_ids(connection: &Value) -> Vec<u64> {
        connection["edges"]
            .as_array()
            .unwrap()
            .iter()
            .map(|edge| edge["node"]["item_id"].as_u64().unwrap())
            .collect()
    }

    /// Follows `endCursor` until `hasNextPage` is false and returns all item ids in order.
    async fn paginate(schema: &Schema, order: &str, page_size: u32) -> Vec<u64> {
        let mut all = Vec::new();
        let mut after = String::new();
        for _ in 0..=ITEMS {
            let connection =
                items_query(schema, &format!("(first: {page_size}, {order}{after})")).await;
            assert_eq!(connection["totalCount"], ITEMS);
            assert_eq!(connection["pageInfo"]["hasPreviousPage"], !after.is_empty());
            all.extend(item_ids(&connection));
            if connection["pageInfo"]["hasNextPage"] == false {
                return all;
            }
            after = format!(", after: {}", connection["pageInfo"]["endCursor"]);
        }
        panic!("pagination did not terminate");
    }

    async fn query_error(schema: &Schema, query: &str) -> String {
        let response = schema.execute(query).await;
        assert!(!response.errors.is_empty(), "expected an error for {query}");
        response.errors[0].message.clone()
    }

    #[sqlx::test(migrations = "../migrations")]
    async fn asc_order_with_after_returns_next_page(pool: SqlitePool) {
        let schema = setup(&pool).await;

        // amount is an INTEGER column with values >= 10, which must not be compared as text
        let listing = paginate(&schema, "order: {field: AMOUNT, direction: ASC}", 10).await;
        assert_eq!(listing, (0..ITEMS as u64).collect::<Vec<_>>());

        let listing = paginate(&schema, "order: {field: AMOUNT, direction: DESC}", 7).await;
        assert_eq!(listing, (0..ITEMS as u64).rev().collect::<Vec<_>>());
    }

    #[sqlx::test(migrations = "../migrations")]
    async fn ties_paginate_without_duplicates_or_gaps(pool: SqlitePool) {
        let schema = setup(&pool).await;

        for direction in ["ASC", "DESC"] {
            let order = format!("order: {{field: SCORE, direction: {direction}}}");
            let listing = paginate(&schema, &order, 3).await;
            assert_eq!(listing.len(), ITEMS as usize);
            assert_eq!(listing.iter().collect::<HashSet<_>>().len(), ITEMS as usize);

            let scores: Vec<u64> = listing.iter().map(|i| i % 4).collect();
            let mut sorted = scores.clone();
            sorted.sort();
            if direction == "DESC" {
                sorted.reverse();
            }
            assert_eq!(scores, sorted);
        }
    }

    #[sqlx::test(migrations = "../migrations")]
    async fn reserved_and_camel_case_members_can_be_ordered(pool: SqlitePool) {
        let schema = setup(&pool).await;

        let listing = paginate(&schema, "order: {field: ORDER, direction: DESC}", 10).await;
        assert_eq!(listing, (0..ITEMS as u64).rev().collect::<Vec<_>>());

        let listing = paginate(&schema, "order: {field: TYPECAMEL, direction: ASC}", 10).await;
        assert_eq!(listing, (0..ITEMS as u64).rev().collect::<Vec<_>>());
    }

    #[sqlx::test(migrations = "../migrations")]
    async fn order_field_enum_only_lists_orderable_members(pool: SqlitePool) {
        let schema = setup(&pool).await;

        let result = run_graphql_query(
            &schema,
            r#"{ __type(name: "qtest_ItemOrderField") { enumValues { name } } }"#,
        )
        .await;
        let values: HashSet<&str> = result["__type"]["enumValues"]
            .as_array()
            .unwrap()
            .iter()
            .map(|value| value["name"].as_str().unwrap())
            .collect();
        assert_eq!(
            values,
            HashSet::from([
                "ITEM_ID",
                "AMOUNT",
                "SCORE",
                "ORDER",
                "TYPECAMEL",
                "NAME",
                "BIG"
            ])
        );
    }

    #[sqlx::test(migrations = "../migrations")]
    async fn byte_array_members_can_be_filtered(pool: SqlitePool) {
        let schema = setup(&pool).await;

        let count = |connection: Value| connection["totalCount"].as_u64().unwrap();

        let connection = items_query(&schema, r#"(where: {name: "item-3"})"#).await;
        assert_eq!(count(connection.clone()), 1);
        assert_eq!(item_ids(&connection), vec![3]);

        let connection = items_query(&schema, r#"(where: {nameIN: ["item-1", "item-2"]})"#).await;
        assert_eq!(count(connection), 2);

        let connection = items_query(&schema, r#"(where: {nameLIKE: "item-1%"})"#).await;
        assert_eq!(count(connection), 11);

        let connection = items_query(&schema, r#"(where: {nameNEQ: "item-0"})"#).await;
        assert_eq!(count(connection), u64::from(ITEMS) - 1);

        // values are bound, never spliced into the SQL
        let connection = items_query(&schema, r#"(where: {name: "x' OR '1'='1"})"#).await;
        assert_eq!(count(connection), 0);
    }

    #[sqlx::test(migrations = "../migrations")]
    async fn nested_comparators_and_tuple_members_filter(pool: SqlitePool) {
        let schema = setup(&pool).await;

        // comparators inside nested inputs used to be silently ignored, returning every row
        let connection = items_query(&schema, "(where: {nested: {innerGT: 20}})").await;
        assert_eq!(connection["totalCount"], 4);
        let mut ids = item_ids(&connection);
        ids.sort();
        assert_eq!(ids, vec![21, 22, 23, 24]);

        // tuple elements are exposed as `_0`, `_1` but stored in columns `0`, `1`
        let connection = items_query(&schema, "(where: {pair: {_0: 3}}, first: 10)").await;
        let mut ids = item_ids(&connection);
        ids.sort();
        assert_eq!(ids, vec![3, 8, 13, 18, 23]);
        for edge in connection["edges"].as_array().unwrap() {
            assert_eq!(edge["node"]["pair"]["_0"], 3);
        }

        let connection = items_query(&schema, "(where: {pair: {_1LT: 2, _0IN: [0, 1]}})").await;
        let mut ids = item_ids(&connection);
        ids.sort();
        assert_eq!(ids, vec![0, 1]);
    }

    #[sqlx::test(migrations = "../migrations")]
    async fn null_order_columns_paginate(pool: SqlitePool) {
        let schema = setup(&pool).await;

        // Members added by a model upgrade are NULL for rows written before it.
        let null_items = [100u32, 101, 102];
        for i in null_items {
            sqlx::query(
                "INSERT INTO [qtest-Item] (internal_id, internal_event_id, internal_executed_at, \
                 item_id) VALUES (?, ?, CURRENT_TIMESTAMP, ?)",
            )
            .bind(format!("0x{i:x}"))
            .bind(format!("0x{i:04x}:0x0:0x0"))
            .bind(i)
            .execute(&pool)
            .await
            .unwrap();
        }

        // NULL columns are read as the zero value of their type, like a member added by an upgrade
        // reads on-chain for existing entities.
        let connection = items_query(&schema, "(where: {item_idGTE: 100})").await;
        assert_eq!(connection["totalCount"], null_items.len());
        for edge in connection["edges"].as_array().unwrap() {
            assert_eq!(edge["node"]["amount"], 0);
            assert_eq!(edge["node"]["nested"]["inner"], 0);
        }

        // Ordering by a column with NULLs: SQLite sorts NULLs first ascending, last descending,
        // ties broken by the event id. Pages must neither skip nor repeat rows.
        let all_items = ITEMS as usize + null_items.len();
        let non_null = (0..ITEMS as u64).collect::<Vec<_>>();
        let nulls = null_items.iter().map(|&i| i as u64).collect::<Vec<_>>();
        for (direction, expected) in [
            ("ASC", [nulls.clone(), non_null.clone()].concat()),
            (
                "DESC",
                [
                    non_null.iter().rev().copied().collect::<Vec<_>>(),
                    nulls.iter().rev().copied().collect(),
                ]
                .concat(),
            ),
        ] {
            let mut listing = Vec::new();
            let mut after = String::new();
            for page in 0.. {
                assert!(page <= all_items, "pagination did not terminate");
                let connection = items_query(
                    &schema,
                    &format!("(first: 4, order: {{field: AMOUNT, direction: {direction}}}{after})"),
                )
                .await;
                assert_eq!(
                    connection["pageInfo"]["hasPreviousPage"],
                    !after.is_empty(),
                    "{direction} page {page}"
                );
                listing.extend(item_ids(&connection));
                if connection["pageInfo"]["hasNextPage"] == false {
                    break;
                }
                after = format!(", after: {}", connection["pageInfo"]["endCursor"]);
            }
            assert_eq!(listing, expected, "{direction}");
        }
    }

    #[sqlx::test(migrations = "../migrations")]
    async fn invalid_filter_values_are_rejected(pool: SqlitePool) {
        let schema = setup(&pool).await;

        // u64 values are normalized to the stored hex format
        let connection = items_query(&schema, r#"(where: {bigGTE: "20"})"#).await;
        assert_eq!(connection["totalCount"], 5);

        // LIKE patterns are not valid u64 values and are matched verbatim (items 0x10..=0x18)
        let connection = items_query(&schema, r#"(where: {bigLIKE: "0x000000000000001%"})"#).await;
        assert_eq!(connection["totalCount"], 9);

        let message = query_error(
            &schema,
            r#"{ qtestItemModels(where: {big: "not-a-number"}) { totalCount } }"#,
        )
        .await;
        assert!(
            message.contains("Invalid u64 value \"not-a-number\" on field big"),
            "unexpected error: {message}"
        );
    }

    #[sqlx::test(migrations = "../migrations")]
    async fn negative_and_oversized_page_arguments_are_rejected(pool: SqlitePool) {
        let schema = setup(&pool).await;

        for (field, args, expected) in [
            (
                "qtestItemModels",
                "first: -1",
                "`first` must be a non-negative integer",
            ),
            (
                "qtestItemModels",
                "last: -1",
                "`last` must be a non-negative integer",
            ),
            (
                "qtestItemModels",
                "limit: -5",
                "`limit` must be a non-negative integer",
            ),
            (
                "qtestItemModels",
                "offset: -1",
                "`offset` must be a non-negative integer",
            ),
            ("qtestItemModels", "first: 10001", "must not exceed 10000"),
            ("qtestItemModels", "limit: 10001", "must not exceed 10000"),
            (
                "entities",
                "first: -1",
                "`first` must be a non-negative integer",
            ),
            ("entities", "last: 20000", "must not exceed 10000"),
        ] {
            let message =
                query_error(&schema, &format!("{{ {field}({args}) {{ totalCount }} }}")).await;
            assert!(
                message.contains(expected),
                "{field}({args}): unexpected error: {message}"
            );
        }

        // the maximum page size itself is accepted
        let connection = items_query(&schema, "(first: 10000)").await;
        assert_eq!(item_ids(&connection).len(), ITEMS as usize);
    }
}
