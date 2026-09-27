#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use anyhow::Result;
    use async_graphql::dynamic::Schema;
    use serial_test::serial;
    use sqlx::SqlitePool;
    use starknet::providers::jsonrpc::HttpTransport;
    use starknet::providers::JsonRpcClient;
    use starknet_crypto::Felt;
    use tokio::sync::broadcast;
    use torii_messaging::Messaging;
    use torii_sqlite::executor::Executor as SqlExecutor;
    use torii_sqlite::Sql;
    use torii_storage::proto::{ContractDefinition, ContractType};
    use url::Url;

    use crate::schema::build_schema;
    use crate::tests::model_fixtures;
    use crate::utils;
    /// Sets up storage backed by `pool`. The returned sender keeps the SQL executor running.
    async fn setup_sql(
        pool: &SqlitePool,
    ) -> (
        Sql,
        Arc<JsonRpcClient<HttpTransport>>,
        broadcast::Sender<()>,
    ) {
        let (shutdown_tx, _) = broadcast::channel(1);
        // Never called: only used to build the messaging component of the schema.
        let url: Url = "https://www.example.com".parse().unwrap();
        let provider = Arc::new(JsonRpcClient::new(HttpTransport::new(url)));
        let (mut executor, sender) =
            SqlExecutor::new(pool.clone(), shutdown_tx.clone(), provider.clone())
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

        (db, provider, shutdown_tx)
    }

    async fn build_test_schema(
        pool: &SqlitePool,
        db: &Sql,
        provider: Arc<JsonRpcClient<HttpTransport>>,
    ) -> Result<Schema> {
        let messaging = Arc::new(Messaging::new(
            Default::default(),
            Arc::new(db.clone()),
            provider,
        ));
        build_schema(pool, messaging, Arc::new(db.clone())).await
    }

    async fn has_type(schema: &Schema, type_name: &str) -> bool {
        let response = schema
            .execute(format!(r#"{{ __type(name: "{type_name}") {{ name }} }}"#))
            .await;
        assert!(response.errors.is_empty(), "{:?}", response.errors);
        !response.data.into_json().unwrap()["__type"].is_null()
    }

    #[sqlx::test(migrations = "../migrations")]
    #[serial]
    async fn test_model_with_malformed_schema_is_skipped(pool: SqlitePool) {
        let (db, provider, _executor_shutdown) = setup_sql(&pool).await;
        model_fixtures(&db).await;

        // Duplicate the fixture model with a schema that is not valid JSON.
        let columns: Vec<String> =
            sqlx::query_scalar("SELECT name FROM pragma_table_info('models')")
                .fetch_all(&pool)
                .await
                .unwrap();
        let values = columns
            .iter()
            .map(|column| match column.as_str() {
                "id" => "'0xbad'".to_string(),
                "name" => "'Broken'".to_string(),
                "schema" => "'{not json'".to_string(),
                column => column.to_string(),
            })
            .collect::<Vec<_>>();
        sqlx::query(&format!(
            "INSERT INTO models ({}) SELECT {} FROM models LIMIT 1",
            columns.join(", "),
            values.join(", ")
        ))
        .execute(&pool)
        .await
        .unwrap();

        let schema = build_test_schema(&pool, &db, provider.clone())
            .await
            .expect("a malformed model must not break the schema");
        assert!(
            has_type(
                &schema,
                &utils::type_name_from_names("types_test", "Record")
            )
            .await
        );
        assert!(
            !has_type(
                &schema,
                &utils::type_name_from_names("types_test", "Broken")
            )
            .await
        );

        // Even when no model can be mapped, the (empty) model union keeps the schema valid.
        sqlx::query("UPDATE models SET schema = '{not json'")
            .execute(&pool)
            .await
            .unwrap();
        let schema = build_test_schema(&pool, &db, provider)
            .await
            .expect("malformed models must not break the schema");
        assert!(
            !has_type(
                &schema,
                &utils::type_name_from_names("types_test", "Record")
            )
            .await
        );
    }

    /// The standard introspection query (graphql-js `getIntrospectionQuery` with every option
    /// async-graphql supports enabled, and the deeper 9-level `TypeRef` of graphql-js 17).
    const INTROSPECTION_QUERY: &str = r#"
        query IntrospectionQuery {
          __schema {
            description
            queryType { name }
            mutationType { name }
            subscriptionType { name }
            types { ...FullType }
            directives {
              name
              description
              isRepeatable
              locations
              args { ...InputValue }
            }
          }
        }

        fragment FullType on __Type {
          kind
          name
          description
          specifiedByURL
          isOneOf
          fields(includeDeprecated: true) {
            name
            description
            args { ...InputValue }
            type { ...TypeRef }
            isDeprecated
            deprecationReason
          }
          inputFields { ...InputValue }
          interfaces { ...TypeRef }
          enumValues(includeDeprecated: true) {
            name
            description
            isDeprecated
            deprecationReason
          }
          possibleTypes { ...TypeRef }
        }

        fragment InputValue on __InputValue {
          name
          description
          type { ...TypeRef }
          defaultValue
        }

        fragment TypeRef on __Type {
          kind
          name
          ofType {
            kind
            name
            ofType {
              kind
              name
              ofType {
                kind
                name
                ofType {
                  kind
                  name
                  ofType {
                    kind
                    name
                    ofType {
                      kind
                      name
                      ofType {
                        kind
                        name
                        ofType {
                          kind
                          name
                          ofType {
                            kind
                            name
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
    "#;

    #[sqlx::test(migrations = "../migrations")]
    #[serial]
    async fn test_introspection_query_within_limits(pool: SqlitePool) {
        let (db, provider, _executor_shutdown) = setup_sql(&pool).await;
        model_fixtures(&db).await;
        let schema = build_test_schema(&pool, &db, provider).await.unwrap();

        let response = schema.execute(INTROSPECTION_QUERY).await;
        assert!(response.errors.is_empty(), "{:?}", response.errors);
        let data = response.data.into_json().unwrap();
        assert!(!data["__schema"]["types"].as_array().unwrap().is_empty());
    }

    #[sqlx::test(migrations = "../migrations")]
    #[serial]
    async fn test_overly_deep_query_is_rejected(pool: SqlitePool) {
        let (db, provider, _executor_shutdown) = setup_sql(&pool).await;
        let schema = build_test_schema(&pool, &db, provider).await.unwrap();

        let depth = crate::schema::MAX_QUERY_DEPTH + 1;
        let query = format!(
            "{{ __schema {{ types {} name {} }} }}",
            "{ ofType ".repeat(depth - 2),
            "}".repeat(depth - 2)
        );
        let response = schema.execute(query).await;
        assert!(
            response
                .errors
                .iter()
                .any(|e| e.message.contains("nested too deep")),
            "{:?}",
            response.errors
        );
    }
}
