#[cfg(test)]
mod tests {
    use chrono::{DateTime, Utc};
    use serde_json::{json, Value};
    use serial_test::serial;
    use sqlx::SqlitePool;
    use starknet::core::types::Felt;
    use tokio_stream::StreamExt;
    use torii_broker::types::{TransactionUpdate, Update};
    use torii_broker::MemoryBroker;
    use torii_sqlite::utils::{felt_to_sql_string, felts_to_sql_string};
    use torii_storage::proto::{CallType, Transaction, TransactionCall};

    use crate::tests::erc_test::tests::{
        first_subscription_item, insert_contract, insert_token, insert_transfer, schema,
        TOKEN_FIELDS,
    };
    use crate::tests::run_graphql_query;

    // Short enough to have leading zeros once padded, which the `transactions` query strips.
    const TRANSACTION_HASH: Felt = Felt::from_hex_unchecked("0x123");
    const OTHER_TRANSACTION_HASH: Felt = Felt::from_hex_unchecked("0x124");
    const CALLER: Felt = Felt::from_hex_unchecked("0xca11");
    const ERC20: Felt = Felt::from_hex_unchecked("0x60");
    const ERC721: Felt = Felt::from_hex_unchecked("0x61");
    const NOT_A_TOKEN: Felt = Felt::from_hex_unchecked("0x62");

    async fn insert_transaction(pool: &SqlitePool, transaction_hash: Felt) {
        sqlx::query(
            "INSERT INTO transactions (id, transaction_hash, sender_address, calldata, max_fee, \
             signature, nonce, executed_at, block_number) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(felt_to_sql_string(&transaction_hash))
        .bind(felt_to_sql_string(&transaction_hash))
        .bind(felt_to_sql_string(&CALLER))
        .bind(felts_to_sql_string(&[Felt::ONE, Felt::TWO]))
        .bind(felt_to_sql_string(&Felt::ZERO))
        .bind(felts_to_sql_string(&[Felt::THREE]))
        .bind(felt_to_sql_string(&Felt::ZERO))
        .bind("2024-03-18T09:34:38Z")
        .bind(7)
        .execute(pool)
        .await
        .unwrap();

        sqlx::query(
            "INSERT INTO transaction_calls (transaction_hash, contract_address, entrypoint, \
             calldata, caller_address) VALUES (?, ?, ?, ?, ?)",
        )
        .bind(felt_to_sql_string(&transaction_hash))
        .bind(felt_to_sql_string(&ERC20))
        .bind("transfer")
        .bind(felts_to_sql_string(&[]))
        .bind(felt_to_sql_string(&CALLER))
        .execute(pool)
        .await
        .unwrap();
    }

    #[sqlx::test(migrations = "../migrations")]
    async fn test_transaction_calls_and_token_transfers(pool: SqlitePool) {
        let schema = schema(&pool).await;
        insert_transaction(&pool, TRANSACTION_HASH).await;
        insert_transaction(&pool, OTHER_TRANSACTION_HASH).await;
        for (contract, contract_type) in
            [(ERC20, "ERC20"), (ERC721, "ERC721"), (NOT_A_TOKEN, "OTHER")]
        {
            insert_contract(&pool, contract, contract_type).await;
            insert_token(&pool, contract, None, None).await;
        }
        // The metadata fetch of this NFT failed.
        insert_token(&pool, ERC721, Some(1), Some("")).await;

        let transfers = [
            (TRANSACTION_HASH, ERC20, None),
            (TRANSACTION_HASH, ERC721, Some(1)),
            // Can't be represented as a token transfer, and must not fail the field.
            (TRANSACTION_HASH, NOT_A_TOKEN, None),
            (OTHER_TRANSACTION_HASH, ERC20, None),
        ];
        for (block, (transaction_hash, contract, token_id)) in (1..).zip(transfers) {
            insert_transfer(
                &pool,
                block,
                transaction_hash,
                contract,
                token_id,
                CALLER,
                Felt::ONE,
                1,
            )
            .await;
        }

        let query = format!(
            "{{ transactions {{ edges {{ node {{ transactionHash calls {{ entrypoint }} \
             tokenTransfers {{ transactionHash {TOKEN_FIELDS} }} }} }} }} }}"
        );
        let result = run_graphql_query(&schema, &query).await;
        let node = result["transactions"]["edges"]
            .as_array()
            .unwrap()
            .iter()
            .map(|edge| &edge["node"])
            .find(|node| node["transactionHash"] == format!("{TRANSACTION_HASH:#x}"))
            .unwrap();

        assert_eq!(node["calls"], json!([{ "entrypoint": "transfer" }]));

        let transfers = node["tokenTransfers"].as_array().unwrap();
        let mut types: Vec<&str> = transfers
            .iter()
            .map(|transfer| transfer["tokenMetadata"]["__typename"].as_str().unwrap())
            .collect();
        types.sort();
        assert_eq!(types, ["ERC20__Token", "ERC721__Token"]);
        for transfer in transfers {
            assert_eq!(
                transfer["transactionHash"],
                felt_to_sql_string(&TRANSACTION_HASH)
            );
        }
    }

    fn transaction(transaction_hash: Felt, caller: Felt) -> TransactionUpdate {
        Update::new(
            Transaction {
                transaction_hash,
                sender_address: caller,
                calldata: vec![Felt::ONE, Felt::TWO],
                max_fee: Felt::ZERO,
                signature: vec![Felt::THREE, Felt::from(4u8)],
                nonce: Felt::ZERO,
                block_number: 7,
                transaction_type: "INVOKE".to_string(),
                block_timestamp: DateTime::from_timestamp(1710754478, 0).unwrap(),
                calls: vec![TransactionCall {
                    contract_address: ERC20,
                    entrypoint: "transfer".to_string(),
                    calldata: vec![],
                    call_type: CallType::Execute,
                    caller_address: caller,
                }],
                unique_models: vec![],
            },
            false,
        )
    }

    #[sqlx::test(migrations = "../migrations")]
    #[serial]
    async fn test_transaction_subscription(pool: SqlitePool) {
        let schema = schema(&pool).await;
        insert_transaction(&pool, TRANSACTION_HASH).await;
        let created_at: DateTime<Utc> =
            sqlx::query_scalar("SELECT created_at FROM transactions WHERE id = ?")
                .bind(felt_to_sql_string(&TRANSACTION_HASH))
                .fetch_one(&pool)
                .await
                .unwrap();

        // Unpadded and upper case spellings match.
        let subscription = "subscription { transaction(hash: \"0x123\", hasCaller: \"0xCA11\") { \
                            id transactionHash calldata signature executedAt createdAt \
                            blockNumber } }";
        let data = first_subscription_item(&schema, subscription, || {
            MemoryBroker::publish(transaction(OTHER_TRANSACTION_HASH, CALLER));
            MemoryBroker::publish(transaction(TRANSACTION_HASH, Felt::ONE));
            MemoryBroker::publish(transaction(TRANSACTION_HASH, CALLER));
        })
        .await;

        let felts = |felts: &[Felt]| felts.iter().map(felt_to_sql_string).collect::<Vec<_>>();
        assert_eq!(
            data["transaction"],
            json!({
                "id": felt_to_sql_string(&TRANSACTION_HASH),
                "transactionHash": felt_to_sql_string(&TRANSACTION_HASH),
                "calldata": felts(&[Felt::ONE, Felt::TWO]),
                "signature": felts(&[Felt::THREE, Felt::from(4u8)]),
                "executedAt": "2024-03-18T09:34:38+00:00",
                "createdAt": created_at.to_rfc3339(),
                "blockNumber": 7,
            })
        );

        // Invalid filters are errors, not panics.
        for arguments in ["hasCaller: \"foo\"", "hash: \"foo\""] {
            let response = schema
                .execute_stream(format!(
                    "subscription {{ transaction({arguments}) {{ transactionHash }} }}"
                ))
                .next()
                .await
                .unwrap();
            assert_eq!(response.errors.len(), 1, "{arguments}");
            assert!(response.errors[0].message.starts_with("Invalid"));
        }
    }

    #[sqlx::test(migrations = "../migrations")]
    #[serial]
    async fn test_transaction_subscription_without_created_at(pool: SqlitePool) {
        // Nothing is stored: the transaction is built from the published update alone.
        let schema = schema(&pool).await;
        let data = first_subscription_item(
            &schema,
            "subscription { transaction { transactionHash signature } }",
            || MemoryBroker::publish(transaction(TRANSACTION_HASH, CALLER)),
        )
        .await;

        assert_eq!(
            data["transaction"]["transactionHash"],
            felt_to_sql_string(&TRANSACTION_HASH)
        );
        assert_eq!(
            data["transaction"]["signature"],
            Value::from(vec![
                felt_to_sql_string(&Felt::THREE),
                felt_to_sql_string(&Felt::from(4u8))
            ])
        );
    }
}
