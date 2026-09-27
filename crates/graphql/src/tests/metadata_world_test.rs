#[cfg(test)]
mod tests {
    use serde_json::{json, Value};
    use sqlx::SqlitePool;
    use starknet::core::types::Felt;
    use torii_sqlite::utils::felt_to_sql_string;

    use crate::tests::erc_test::tests::{insert_contract, schema};
    use crate::tests::run_graphql_query;

    const QUERY: &str = "{ metadatas { totalCount edges { node { worldAddress content { name \
                         socials { name url } } } } } }";

    async fn insert_metadata(pool: &SqlitePool, json: &str) {
        sqlx::query("INSERT INTO metadata (id, uri, executed_at, json) VALUES (?, ?, ?, ?)")
            .bind(felt_to_sql_string(&Felt::ZERO))
            .bind("ipfs://metadata")
            .bind("2024-03-18T09:34:38Z")
            .bind(json)
            .execute(pool)
            .await
            .unwrap();
    }

    fn world_addresses(result: &Value) -> Vec<&str> {
        result["metadatas"]["edges"]
            .as_array()
            .unwrap()
            .iter()
            .map(|edge| edge["node"]["worldAddress"].as_str().unwrap())
            .collect()
    }

    #[sqlx::test(migrations = "../migrations")]
    async fn test_metadatas_without_world(pool: SqlitePool) {
        let schema = schema(&pool).await;

        let result = run_graphql_query(&schema, QUERY).await;
        assert_eq!(result["metadatas"]["totalCount"], 0);

        insert_metadata(&pool, "{}").await;
        let result = run_graphql_query(&schema, QUERY).await;
        assert_eq!(world_addresses(&result), [""]);
    }

    #[sqlx::test(migrations = "../migrations")]
    async fn test_metadatas_world_address(pool: SqlitePool) {
        let schema = schema(&pool).await;
        let world = Felt::from(0x1u8);
        insert_contract(&pool, world, "WORLD").await;
        insert_contract(&pool, Felt::from(0x2u8), "ERC20").await;
        insert_metadata(&pool, "{}").await;

        let result = run_graphql_query(&schema, QUERY).await;
        assert_eq!(world_addresses(&result), [felt_to_sql_string(&world)]);

        // The metadata table isn't scoped by world: with several worlds, the row can't be
        // attributed to one of them.
        insert_contract(&pool, Felt::from(0x3u8), "WORLD").await;
        let result = run_graphql_query(&schema, QUERY).await;
        assert_eq!(world_addresses(&result), [""]);
    }

    #[sqlx::test(migrations = "../migrations")]
    async fn test_metadatas_non_string_social_url(pool: SqlitePool) {
        let schema = schema(&pool).await;
        insert_metadata(
            &pool,
            r#"{"name":"world","socials":{"x":"https://x.com/dojostarknet","discord":1}}"#,
        )
        .await;

        let result = run_graphql_query(&schema, QUERY).await;
        assert_eq!(
            result["metadatas"]["edges"][0]["node"]["content"],
            json!({
                "name": "world",
                "socials": [
                    { "name": "discord", "url": null },
                    { "name": "x", "url": "https://x.com/dojostarknet" },
                ]
            })
        );
    }
}
