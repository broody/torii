#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use async_graphql::dynamic::Schema;
    use dojo_types::naming::get_tag;
    use dojo_types::primitive::Primitive;
    use dojo_types::schema::{Member, Struct, Ty};
    use dojo_world::contracts::abigen::model::Layout;
    use dojo_world::contracts::naming::compute_selector_from_tag;
    use serde_json::json;
    use serial_test::serial;
    use sqlx::SqlitePool;
    use starknet::providers::jsonrpc::HttpTransport;
    use starknet::providers::JsonRpcClient;
    use starknet_crypto::{poseidon_hash_many, Felt};
    use tokio::sync::broadcast;
    use torii_messaging::Messaging;
    use torii_sqlite::executor::Executor;
    use torii_sqlite::Sql;
    use torii_storage::proto::{ContractDefinition, ContractType};
    use torii_storage::utils::format_world_scoped_id;
    use torii_storage::Storage;
    use url::Url;

    use crate::schema::build_schema;
    use crate::tests::run_graphql_query;

    const BLOCK_TIMESTAMP: u64 = 1710754478;

    fn provider() -> Arc<JsonRpcClient<HttpTransport>> {
        // only used to fetch token metadata, which these tests don't need
        let url: Url = "https://www.example.com".parse().unwrap();
        Arc::new(JsonRpcClient::new(HttpTransport::new(url)))
    }

    /// Returns the storage along with the executor shutdown sender, which has to be kept alive
    /// for the executor to keep running.
    async fn setup_db(pool: &SqlitePool) -> (Sql, broadcast::Sender<()>) {
        let (shutdown_tx, _) = broadcast::channel(1);
        let (mut executor, sender) = Executor::new(pool.clone(), shutdown_tx.clone(), provider())
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
        (db, shutdown_tx)
    }

    async fn schema(db: &Sql) -> Schema {
        let messaging = Arc::new(Messaging::new(
            Default::default(),
            Arc::new(db.clone()),
            provider(),
        ));
        build_schema(&db.pool, messaging, Arc::new(db.clone()))
            .await
            .unwrap()
    }

    async fn register_model(db: &Sql, model: &Ty) {
        db.register_model(
            Felt::ZERO,
            compute_selector_from_tag(&model.name()),
            model,
            &Layout::Fixed(vec![]),
            Felt::ONE,
            Felt::TWO,
            0,
            0,
            BLOCK_TIMESTAMP,
            None,
            None,
            true,
        )
        .await
        .unwrap();
    }

    fn keys(model: &Ty) -> Vec<Felt> {
        model
            .as_struct()
            .unwrap()
            .keys()
            .iter()
            .flat_map(|member| member.serialize().unwrap())
            .collect()
    }

    /// Sets `model` as the state of the entity identified by its keys, returns the entity id.
    async fn set_entity(db: &Sql, model: &Ty, event_id: &str) -> String {
        let keys = keys(model);
        let hashed_keys = poseidon_hash_many(&keys);
        db.set_entity(
            Felt::ZERO,
            model.clone(),
            event_id,
            BLOCK_TIMESTAMP,
            hashed_keys,
            compute_selector_from_tag(&model.name()),
            Some(keys),
        )
        .await
        .unwrap();
        format_world_scoped_id(&Felt::ZERO, &hashed_keys)
    }

    fn player_stats(player: Felt, score: Option<u32>) -> Ty {
        Ty::Struct(Struct {
            name: get_tag("ns", "Player_Stats"),
            children: vec![
                Member {
                    name: "player".to_string(),
                    key: true,
                    ty: Ty::Primitive(Primitive::ContractAddress(Some(player))),
                },
                Member {
                    name: "score".to_string(),
                    key: false,
                    ty: Ty::Primitive(Primitive::U32(score)),
                },
            ],
        })
    }

    // Model names may contain '_', so the model table can't be derived back from the GraphQL
    // type name (`ns_Player_Stats` is table `ns-Player_Stats`, not `ns_Player-Stats`).
    #[sqlx::test(migrations = "../migrations")]
    #[serial]
    async fn test_model_name_with_underscore(pool: SqlitePool) {
        let (db, _shutdown_tx) = setup_db(&pool).await;
        register_model(&db, &player_stats(Felt::ZERO, None)).await;
        let entity_id = set_entity(&db, &player_stats(Felt::ONE, Some(7)), "0x1:0x0:0x0").await;
        db.execute().await.unwrap();

        let schema = schema(&db).await;
        let result = run_graphql_query(
            &schema,
            r#"{
                nsPlayerStatsModels {
                    totalCount
                    edges { node { __typename player score entity { id } } }
                }
            }"#,
        )
        .await;

        assert_eq!(
            result,
            json!({
                "nsPlayerStatsModels": {
                    "totalCount": 1,
                    "edges": [{
                        "node": {
                            "__typename": "ns_Player_Stats",
                            "player": format!("{:#x}", Felt::ONE),
                            "score": 7,
                            "entity": { "id": entity_id },
                        }
                    }]
                }
            })
        );
    }
}
