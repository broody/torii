#[cfg(test)]
pub(crate) mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use async_graphql::dynamic::Schema;
    use serde_json::Value;
    use serial_test::serial;
    use sqlx::SqlitePool;
    use starknet::core::types::{Felt, U256};
    use starknet::providers::jsonrpc::HttpTransport;
    use starknet::providers::JsonRpcClient;
    use tokio::sync::broadcast;
    use tokio_stream::StreamExt;
    use torii_broker::types::{TokenBalanceUpdate, TokenUpdate, Update};
    use torii_broker::MemoryBroker;
    use torii_messaging::{Messaging, MessagingConfig};
    use torii_sqlite::executor::Executor;
    use torii_sqlite::utils::{
        felt_and_u256_to_sql_string, felt_to_sql_string, u256_to_sql_string,
    };
    use torii_sqlite::Sql;
    use torii_storage::utils::format_event_id;
    use url::Url;

    use crate::schema::build_schema;
    use crate::tests::run_graphql_query;

    const ACCOUNT: Felt = Felt::from_hex_unchecked("0xacc");
    const OTHER: Felt = Felt::from_hex_unchecked("0xbeef");

    pub(crate) const TOKEN_FIELDS: &str = r#"
        tokenMetadata {
          __typename
          ... on ERC20__Token { contractAddress amount }
          ... on ERC721__Token {
            contractAddress tokenId metadata metadataName metadataDescription imagePath
          }
          ... on ERC1155__Token {
            contractAddress tokenId amount metadata metadataName imagePath
          }
        }"#;

    /// Builds the schema over `pool`. Fixtures are inserted with plain SQL, so the executor is only
    /// needed while building the schema and shuts down when this returns.
    pub(crate) async fn schema(pool: &SqlitePool) -> Schema {
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
        let db = Sql::new(pool.clone(), sender, &[]).await.unwrap();
        let messaging = Arc::new(Messaging::new(
            MessagingConfig::default(),
            Arc::new(db.clone()),
            provider,
        ));

        let schema = build_schema(pool, messaging, Arc::new(db)).await.unwrap();
        let _ = shutdown_tx.send(());
        schema
    }

    pub(crate) fn token_key(contract: Felt, token_id: Option<u64>) -> String {
        match token_id {
            Some(id) => felt_and_u256_to_sql_string(&contract, &U256::from(id)),
            None => felt_to_sql_string(&contract),
        }
    }

    pub(crate) async fn insert_contract(pool: &SqlitePool, contract: Felt, contract_type: &str) {
        sqlx::query("INSERT INTO contracts (id, contract_address, contract_type) VALUES (?, ?, ?)")
            .bind(felt_to_sql_string(&contract))
            .bind(felt_to_sql_string(&contract))
            .bind(contract_type)
            .execute(pool)
            .await
            .unwrap();
    }

    /// Inserts a `tokens` row: the contract-level row when `token_id` is `None`.
    pub(crate) async fn insert_token(
        pool: &SqlitePool,
        contract: Felt,
        token_id: Option<u64>,
        metadata: Option<&str>,
    ) {
        sqlx::query(
            "INSERT INTO tokens (id, contract_address, token_id, name, symbol, decimals, \
             metadata) VALUES (?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(token_key(contract, token_id))
        .bind(felt_to_sql_string(&contract))
        .bind(token_id.map(|id| u256_to_sql_string(&U256::from(id))))
        .bind("Name")
        .bind("SYM")
        .bind(if token_id.is_some() { 0 } else { 18 })
        .bind(metadata)
        .execute(pool)
        .await
        .unwrap();
    }

    async fn insert_balance(
        pool: &SqlitePool,
        account: Felt,
        contract: Felt,
        token_id: Option<u64>,
        balance: u64,
    ) {
        let token_key = token_key(contract, token_id);
        sqlx::query(
            "INSERT INTO token_balances (id, balance, account_address, contract_address, \
             token_id) VALUES (?, ?, ?, ?, ?)",
        )
        .bind(format!("{}/{}", felt_to_sql_string(&account), token_key))
        .bind(u256_to_sql_string(&U256::from(balance)))
        .bind(felt_to_sql_string(&account))
        .bind(felt_to_sql_string(&contract))
        .bind(token_key)
        .execute(pool)
        .await
        .unwrap();
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn insert_transfer(
        pool: &SqlitePool,
        block_number: u64,
        transaction_hash: Felt,
        contract: Felt,
        token_id: Option<u64>,
        from: Felt,
        to: Felt,
        amount: u64,
    ) {
        let event_id = format_event_id(block_number, &transaction_hash, &contract, 0);
        let token_key = token_key(contract, token_id);
        sqlx::query(
            "INSERT INTO token_transfers (id, contract_address, from_address, to_address, \
             amount, token_id, event_id, executed_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(format!("{event_id}:{token_key}"))
        .bind(felt_to_sql_string(&contract))
        .bind(felt_to_sql_string(&from))
        .bind(felt_to_sql_string(&to))
        .bind(u256_to_sql_string(&U256::from(amount)))
        .bind(token_key)
        .bind(event_id)
        .bind("2024-03-18T09:34:38Z")
        .execute(pool)
        .await
        .unwrap();
    }

    fn get<'a>(value: &'a Value, path: &[&str]) -> &'a Value {
        path.iter().fold(value, |value, key| &value[*key])
    }

    /// `contract_address[:token_id]` of the token of an edge.
    fn token_of(edge: &Value) -> String {
        let token = get(edge, &["node", "tokenMetadata"]);
        let contract_address = token["contractAddress"].as_str().unwrap();
        match token["tokenId"].as_str() {
            Some(token_id) => format!("{contract_address}:{token_id}"),
            None => contract_address.to_string(),
        }
    }

    fn transaction_hash_of(edge: &Value) -> String {
        get(edge, &["node", "transactionHash"])
            .as_str()
            .unwrap()
            .to_string()
    }

    fn keys(connection: &Value, key: fn(&Value) -> String) -> Vec<String> {
        connection["edges"]
            .as_array()
            .unwrap()
            .iter()
            .map(key)
            .collect()
    }

    async fn tokens(schema: &Schema, args: &str) -> Value {
        let query = format!(
            "{{ tokens{args} {{ totalCount edges {{ cursor node {{ {TOKEN_FIELDS} }} }} \
             pageInfo {{ hasPreviousPage hasNextPage startCursor endCursor }} }} }}"
        );
        run_graphql_query(schema, &query).await["tokens"].clone()
    }

    async fn token_balances(schema: &Schema, account: Felt, args: &str) -> Value {
        let query = format!(
            "{{ tokenBalances(accountAddress: \"{account:#x}\"{args}) {{ totalCount edges {{ \
             cursor node {{ {TOKEN_FIELDS} }} }} pageInfo {{ hasPreviousPage hasNextPage \
             startCursor endCursor }} }} }}"
        );
        run_graphql_query(schema, &query).await["tokenBalances"].clone()
    }

    async fn token_transfers(schema: &Schema, account: Felt, args: &str) -> Value {
        let query = format!(
            "{{ tokenTransfers(accountAddress: \"{account:#x}\"{args}) {{ totalCount edges {{ \
             cursor node {{ from to transactionHash {TOKEN_FIELDS} }} }} pageInfo {{ \
             hasPreviousPage hasNextPage startCursor endCursor }} }} }}"
        );
        run_graphql_query(schema, &query).await["tokenTransfers"].clone()
    }

    /// Walks a connection forward with `first`/`after` and returns the keys of each page.
    async fn walk_pages<F, Fut>(
        page_size: u64,
        key: fn(&Value) -> String,
        fetch: F,
    ) -> Vec<Vec<String>>
    where
        F: Fn(String) -> Fut,
        Fut: std::future::Future<Output = Value>,
    {
        let mut pages = vec![];
        let mut after = String::new();
        loop {
            let connection = fetch(format!("first: {page_size}{after}")).await;
            pages.push(keys(&connection, key));
            if !connection["pageInfo"]["hasNextPage"].as_bool().unwrap() {
                return pages;
            }
            let end_cursor = connection["pageInfo"]["endCursor"].as_str().unwrap();
            after = format!(", after: \"{end_cursor}\"");
            assert!(pages.len() < 10, "pagination does not terminate");
        }
    }

    #[sqlx::test(migrations = "../migrations")]
    async fn test_tokens_cursor_pagination(pool: SqlitePool) {
        let schema = schema(&pool).await;
        let erc20s: Vec<Felt> = (0x10..0x15).map(Felt::from).collect();
        for contract in &erc20s {
            insert_contract(&pool, *contract, "ERC20").await;
            insert_token(&pool, *contract, None, None).await;
        }

        let pages = walk_pages(2, token_of, |args| {
            let schema = schema.clone();
            async move { tokens(&schema, &format!("({args})")).await }
        })
        .await;

        let expected: Vec<String> = erc20s.iter().rev().map(felt_to_sql_string).collect();
        assert_eq!(pages, [&expected[0..2], &expected[2..4], &expected[4..]]);
    }

    #[sqlx::test(migrations = "../migrations")]
    async fn test_token_transfers_cursor_pagination(pool: SqlitePool) {
        let schema = schema(&pool).await;
        let erc20 = Felt::from(0x20);
        insert_contract(&pool, erc20, "ERC20").await;
        insert_token(&pool, erc20, None, None).await;
        for block in 1..=5 {
            insert_transfer(
                &pool,
                block,
                Felt::from(block),
                erc20,
                None,
                OTHER,
                ACCOUNT,
                block,
            )
            .await;
        }

        let pages = walk_pages(2, transaction_hash_of, |args| {
            let schema = schema.clone();
            async move { token_transfers(&schema, ACCOUNT, &format!(", {args}")).await }
        })
        .await;

        // Every transfer is returned exactly once, most recent first.
        let expected: Vec<String> = (1..=5)
            .rev()
            .map(|block| felt_to_sql_string(&Felt::from(block)))
            .collect();
        assert_eq!(pages, [&expected[0..2], &expected[2..4], &expected[4..]]);
    }

    #[sqlx::test(migrations = "../migrations")]
    async fn test_tokens_contract_address_filter(pool: SqlitePool) {
        let schema = schema(&pool).await;
        let (erc20_a, erc20_b) = (Felt::from(0x40), Felt::from(0x41));
        for contract in [erc20_a, erc20_b] {
            insert_contract(&pool, contract, "ERC20").await;
            insert_token(&pool, contract, None, None).await;
        }

        // Unpadded and padded spellings both match the stored (padded) address.
        for address in [format!("{erc20_a:#x}"), felt_to_sql_string(&erc20_a)] {
            let connection = tokens(&schema, &format!("(contractAddress: \"{address}\")")).await;
            assert_eq!(connection["totalCount"], 1);
            assert_eq!(keys(&connection, token_of), [felt_to_sql_string(&erc20_a)]);
        }

        // A null filter (e.g. an unset variable) doesn't filter instead of panicking.
        let res = schema
            .execute("query Tokens($address: String) { tokens(contractAddress: $address) { totalCount } }")
            .await;
        assert!(res.errors.is_empty(), "{:?}", res.errors);
        assert_eq!(res.data.into_json().unwrap()["tokens"]["totalCount"], 2);

        // The address is validated and bound, never interpolated into the SQL.
        let res = schema
            .execute(r#"{ tokens(contractAddress: "0x40' OR '1'='1") { totalCount } }"#)
            .await;
        assert_eq!(res.errors.len(), 1);
        assert!(res.errors[0].message.contains("Invalid contractAddress"));
    }

    const ERC20: Felt = Felt::from_hex_unchecked("0x50");
    const ERC20_B: Felt = Felt::from_hex_unchecked("0x51");
    const ERC721: Felt = Felt::from_hex_unchecked("0x52");
    const ERC1155: Felt = Felt::from_hex_unchecked("0x53");
    const NOT_A_TOKEN: Felt = Felt::from_hex_unchecked("0x54");
    const NFT_METADATA: &str = r#"{"name":"a \"quoted\" name","description":"line\nbreak"}"#;

    /// Tokens of every kind, including the contract-level rows every ERC721/ERC1155 collection
    /// gets and a token row of a contract that is not a token contract.
    async fn insert_tokens(pool: &SqlitePool) {
        for (contract, contract_type) in [
            (ERC20, "ERC20"),
            (ERC20_B, "ERC20"),
            (ERC721, "ERC721"),
            (ERC1155, "ERC1155"),
        ] {
            insert_contract(pool, contract, contract_type).await;
            insert_token(pool, contract, None, Some(r#"{"name":"collection"}"#)).await;
        }
        insert_token(pool, ERC721, Some(1), Some(NFT_METADATA)).await;
        // Failed metadata fetches are stored as ''.
        insert_token(pool, ERC721, Some(2), Some("")).await;
        insert_token(pool, ERC1155, Some(1), Some("")).await;
        insert_contract(pool, NOT_A_TOKEN, "OTHER").await;
        insert_token(pool, NOT_A_TOKEN, None, None).await;
    }

    #[sqlx::test(migrations = "../migrations")]
    async fn test_tokens_skip_contract_level_nft_rows(pool: SqlitePool) {
        let schema = schema(&pool).await;
        insert_tokens(&pool).await;

        let connection = tokens(&schema, "").await;
        let mut expected = vec![
            felt_to_sql_string(&ERC20),
            felt_to_sql_string(&ERC20_B),
            token_key(ERC721, Some(1)),
            token_key(ERC721, Some(2)),
            token_key(ERC1155, Some(1)),
        ];
        expected.reverse();
        assert_eq!(keys(&connection, token_of), expected);
        assert_eq!(connection["totalCount"], 5);
        assert_eq!(connection["pageInfo"]["hasNextPage"], false);

        let connection = tokens(&schema, &format!("(contractAddress: \"{ERC721:#x}\")")).await;
        assert_eq!(connection["totalCount"], 2);
        assert_eq!(connection["edges"].as_array().unwrap().len(), 2);

        // Strings from the metadata are returned verbatim, not JSON-escaped.
        let token = get(&connection["edges"][1], &["node", "tokenMetadata"]);
        assert_eq!(token["metadataName"], "a \"quoted\" name");
        assert_eq!(token["metadataDescription"], "line\nbreak");

        // Looking up the contract-level row of a collection is an error, not a panic.
        let query = |id: String| format!("{{ token(id: \"{id}\") {{ {TOKEN_FIELDS} }} }}");
        let res = schema.execute(query(felt_to_sql_string(&ERC721))).await;
        assert_eq!(res.errors.len(), 1);
        assert!(
            res.errors[0].message.contains("contract-level"),
            "{:?}",
            res.errors
        );

        let res = schema.execute(query(token_key(ERC721, Some(2)))).await;
        assert!(res.errors.is_empty(), "{:?}", res.errors);
        let token = res.data.into_json().unwrap()["token"]["tokenMetadata"].clone();
        assert_eq!(token["__typename"], "ERC721__Token");
        assert_eq!(token["metadataName"], Value::Null);
        assert_eq!(token["imagePath"], "");
    }

    #[sqlx::test(migrations = "../migrations")]
    async fn test_offset_pagination_page_info(pool: SqlitePool) {
        let schema = schema(&pool).await;
        insert_tokens(&pool).await;

        for (offset, has_previous_page, has_next_page) in
            [(0, false, true), (1, true, true), (3, true, false)]
        {
            let connection = tokens(&schema, &format!("(offset: {offset}, limit: 2)")).await;
            assert_eq!(connection["totalCount"], 5);
            assert_eq!(
                connection["pageInfo"]["hasPreviousPage"], has_previous_page,
                "offset {offset}"
            );
            assert_eq!(
                connection["pageInfo"]["hasNextPage"], has_next_page,
                "offset {offset}"
            );
        }
    }

    #[sqlx::test(migrations = "../migrations")]
    async fn test_token_balances_count_matches_page(pool: SqlitePool) {
        let schema = schema(&pool).await;
        insert_tokens(&pool).await;
        insert_balance(&pool, ACCOUNT, ERC20, None, 5).await;
        // ERC20 balances are listed even when empty, NFTs only while held.
        insert_balance(&pool, ACCOUNT, ERC20_B, None, 0).await;
        insert_balance(&pool, ACCOUNT, ERC721, Some(1), 1).await;
        insert_balance(&pool, ACCOUNT, ERC721, Some(2), 0).await;
        insert_balance(&pool, ACCOUNT, ERC1155, Some(1), 3).await;
        // Contract-level balance of a collection, and a balance of a non-token contract.
        insert_balance(&pool, ACCOUNT, ERC721, None, 1).await;
        insert_balance(&pool, ACCOUNT, NOT_A_TOKEN, None, 1).await;
        insert_balance(&pool, OTHER, ERC20, None, 1).await;

        let connection = token_balances(&schema, ACCOUNT, "").await;
        let mut expected = vec![
            felt_to_sql_string(&ERC20),
            felt_to_sql_string(&ERC20_B),
            token_key(ERC721, Some(1)),
            token_key(ERC1155, Some(1)),
        ];
        expected.reverse();
        assert_eq!(keys(&connection, token_of), expected);
        assert_eq!(connection["totalCount"], 4);
        assert_eq!(connection["pageInfo"]["hasNextPage"], false);

        let connection = token_balances(&schema, ACCOUNT, ", limit: 3").await;
        assert_eq!(connection["edges"].as_array().unwrap().len(), 3);
        assert_eq!(connection["pageInfo"]["hasNextPage"], true);
        let connection = token_balances(&schema, ACCOUNT, ", limit: 4").await;
        assert_eq!(connection["pageInfo"]["hasNextPage"], false);
    }

    #[sqlx::test(migrations = "../migrations")]
    async fn test_token_transfers_count_matches_page(pool: SqlitePool) {
        let schema = schema(&pool).await;
        insert_tokens(&pool).await;
        let transfers = [
            (ERC20, None),
            (ERC721, Some(1)),
            // NFTs whose metadata couldn't be fetched are still listed.
            (ERC721, Some(2)),
            (ERC1155, Some(1)),
            // Transfer of a non-token contract.
            (NOT_A_TOKEN, None),
        ];
        for (block, (contract, token_id)) in (1..).zip(transfers) {
            insert_transfer(
                &pool,
                block,
                Felt::from(block),
                contract,
                token_id,
                OTHER,
                ACCOUNT,
                1,
            )
            .await;
        }

        let connection = token_transfers(&schema, ACCOUNT, "").await;
        let expected: Vec<String> = (1..=4)
            .rev()
            .map(|block| felt_to_sql_string(&Felt::from(block)))
            .collect();
        assert_eq!(keys(&connection, transaction_hash_of), expected);
        assert_eq!(connection["totalCount"], 4);
        assert_eq!(connection["pageInfo"]["hasNextPage"], false);

        let token = get(&connection["edges"][1], &["node", "tokenMetadata"]);
        assert_eq!(token["tokenId"], u256_to_sql_string(&U256::from(2u8)));
        assert_eq!(token["metadataName"], Value::Null);
    }

    /// Runs `subscription`, publishes `updates` once it is subscribed and returns its first item.
    pub(crate) async fn first_subscription_item<T, F>(
        schema: &Schema,
        subscription: &str,
        publish: F,
    ) -> Value
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        let mut stream = schema.execute_stream(subscription);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(200)).await;
            publish();
        });
        let response = tokio::time::timeout(Duration::from_secs(5), stream.next())
            .await
            .expect("no subscription item received")
            .unwrap();
        assert!(response.errors.is_empty(), "{:?}", response.errors);
        response.data.into_json().unwrap()
    }

    #[sqlx::test(migrations = "../migrations")]
    #[serial]
    async fn test_token_balance_subscription(pool: SqlitePool) {
        let schema = schema(&pool).await;
        insert_tokens(&pool).await;
        insert_balance(&pool, ACCOUNT, ERC721, None, 1).await;
        insert_balance(&pool, ACCOUNT, ERC1155, Some(1), 3).await;

        let balance = |token_id: Option<u64>, contract_address: Felt| -> TokenBalanceUpdate {
            let balance = torii_storage::proto::TokenBalance {
                balance: 3u64.into(),
                account_address: ACCOUNT,
                contract_address,
                token_id: token_id.map(Into::into),
            };
            Update::new(balance, false)
        };
        let subscription = format!(
            "subscription {{ tokenBalanceUpdated(accountAddress: \"{ACCOUNT:#x}\") {{ \
             {TOKEN_FIELDS} }} }}"
        );
        let data = first_subscription_item(&schema, &subscription, move || {
            // The contract-level balance of a collection is skipped.
            MemoryBroker::publish(balance(None, ERC721));
            MemoryBroker::publish(balance(Some(1), ERC1155));
        })
        .await;
        let token = &data["tokenBalanceUpdated"]["tokenMetadata"];
        assert_eq!(token["__typename"], "ERC1155__Token");
        assert_eq!(token["amount"], u256_to_sql_string(&U256::from(3u8)));

        // An invalid address is an error, not a panic.
        let response = schema
            .execute_stream(
                "subscription { tokenBalanceUpdated(accountAddress: \"foo\") { __typename } }",
            )
            .next()
            .await
            .unwrap();
        assert_eq!(response.errors.len(), 1);
        assert!(response.errors[0]
            .message
            .contains("Invalid accountAddress"));
    }

    #[sqlx::test(migrations = "../migrations")]
    #[serial]
    async fn test_token_subscription_skips_contract_level_nft_rows(pool: SqlitePool) {
        let schema = schema(&pool).await;
        insert_tokens(&pool).await;

        let token = |token_id: Option<u64>| -> TokenUpdate {
            let token = torii_storage::proto::Token {
                token_id: token_id.map(Into::into),
                contract_address: ERC721,
                ..Default::default()
            };
            Update::new(token, false)
        };
        let subscription = format!("subscription {{ tokenUpdated {{ {TOKEN_FIELDS} }} }}");
        let data = first_subscription_item(&schema, &subscription, move || {
            // Registering a collection publishes its contract-level row first.
            MemoryBroker::publish(token(None));
            MemoryBroker::publish(token(Some(1)));
        })
        .await;

        let token = &data["tokenUpdated"]["tokenMetadata"];
        assert_eq!(token["__typename"], "ERC721__Token");
        assert_eq!(token["tokenId"], u256_to_sql_string(&U256::from(1u8)));
        assert_eq!(token["metadataName"], "a \"quoted\" name");
    }

    // Balance ids (`account/contract:token_id`) contain '/', which `cursor::decode` can't handle
    // yet; un-ignore once the connection cursor encoding supports it.
    #[ignore = "requires cursor::decode support for ids containing '/'"]
    #[sqlx::test(migrations = "../migrations")]
    async fn test_token_balances_cursor_pagination(pool: SqlitePool) {
        let schema = schema(&pool).await;
        let erc20s: Vec<Felt> = (0x30..0x35).map(Felt::from).collect();
        for contract in &erc20s {
            insert_contract(&pool, *contract, "ERC20").await;
            insert_token(&pool, *contract, None, None).await;
            insert_balance(&pool, ACCOUNT, *contract, None, 1).await;
        }

        let pages = walk_pages(2, token_of, |args| {
            let schema = schema.clone();
            async move { token_balances(&schema, ACCOUNT, &format!(", {args}")).await }
        })
        .await;

        let expected: Vec<String> = erc20s.iter().rev().map(felt_to_sql_string).collect();
        assert_eq!(pages, [&expected[0..2], &expected[2..4], &expected[4..]]);
    }
}
