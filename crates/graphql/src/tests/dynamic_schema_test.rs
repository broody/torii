#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use anyhow::Result;
    use async_graphql::dynamic::{
        Field, FieldFuture, FieldValue, Object, Schema, Subscription, SubscriptionField,
        SubscriptionFieldFuture, TypeRef,
    };
    use async_graphql::{Executor, Request, Value};
    use serial_test::serial;
    use sqlx::SqlitePool;
    use starknet::providers::jsonrpc::HttpTransport;
    use starknet::providers::JsonRpcClient;
    use starknet_crypto::Felt;
    use tokio::sync::{broadcast, mpsc};
    use tokio::task::JoinHandle;
    use tokio_stream::wrappers::UnboundedReceiverStream;
    use tokio_stream::StreamExt;
    use torii_messaging::Messaging;
    use torii_sqlite::executor::Executor as SqlExecutor;
    use torii_sqlite::Sql;
    use torii_storage::proto::{ContractDefinition, ContractType};
    use url::Url;

    use crate::schema::build_schema;
    use crate::server::{self, rebuild_schema_on_updates, DynamicSchema};
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

    const WAIT_TIMEOUT: Duration = Duration::from_secs(10);

    /// A minimal schema whose `version` query/subscription returns `version`. Schemas with
    /// `extra_field` also expose an `extra` query field.
    fn versioned_schema(version: i32, extra_field: bool) -> Schema {
        let mut query = Object::new("Query").field(Field::new(
            "version",
            TypeRef::named_nn(TypeRef::INT),
            move |_| FieldFuture::new(async move { Ok(Some(Value::from(version))) }),
        ));
        if extra_field {
            query = query.field(Field::new(
                "extra",
                TypeRef::named_nn(TypeRef::STRING),
                |_| FieldFuture::new(async { Ok(Some(Value::from("hello"))) }),
            ));
        }

        let subscription = Subscription::new("Subscription").field(SubscriptionField::new(
            "version",
            TypeRef::named_nn(TypeRef::INT),
            move |_| {
                SubscriptionFieldFuture::new(async move {
                    Ok(tokio_stream::iter([Ok(FieldValue::value(version))]))
                })
            },
        ));

        Schema::build("Query", None, Some("Subscription"))
            .register(query)
            .register(subscription)
            .finish()
            .unwrap()
    }

    async fn served_version(schema: &DynamicSchema) -> i64 {
        let response = Executor::execute(schema, Request::new("{ version }")).await;
        assert!(response.errors.is_empty(), "{:?}", response.errors);
        response.data.into_json().unwrap()["version"]
            .as_i64()
            .unwrap()
    }

    async fn wait_for_version(schema: &DynamicSchema, expected: i64) {
        tokio::time::timeout(WAIT_TIMEOUT, async {
            while served_version(schema).await != expected {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("schema version {expected} was never served"));
    }

    /// Drives [`rebuild_schema_on_updates`] with an injected update stream and build function.
    struct RebuildHarness {
        schema: DynamicSchema,
        updates_tx: mpsc::UnboundedSender<()>,
        shutdown_tx: broadcast::Sender<()>,
        attempts: Arc<AtomicUsize>,
        attempts_rx: mpsc::UnboundedReceiver<usize>,
        handle: JoinHandle<()>,
    }

    impl RebuildHarness {
        /// Spawns the rebuild loop. Build attempt `n` (starting at 1) returns `outcome(n)`.
        /// `pending_updates` are queued before the loop starts.
        fn spawn(
            quiet_period: Duration,
            max_delay: Duration,
            pending_updates: usize,
            outcome: impl Fn(usize) -> Result<Schema> + Send + 'static,
        ) -> Self {
            let schema = DynamicSchema::new(versioned_schema(1, false));
            let (updates_tx, updates_rx) = mpsc::unbounded_channel();
            for _ in 0..pending_updates {
                updates_tx.send(()).unwrap();
            }
            let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
            let attempts = Arc::new(AtomicUsize::new(0));
            let (attempts_tx, attempts_rx) = mpsc::unbounded_channel();

            let build = {
                let attempts = attempts.clone();
                move || {
                    let attempt = attempts.fetch_add(1, Ordering::SeqCst) + 1;
                    let result = outcome(attempt);
                    attempts_tx.send(attempt).unwrap();
                    async move { result }
                }
            };

            let handle = tokio::spawn(rebuild_schema_on_updates(
                schema.clone(),
                UnboundedReceiverStream::new(updates_rx),
                shutdown_rx,
                quiet_period,
                max_delay,
                build,
            ));

            Self {
                schema,
                updates_tx,
                shutdown_tx,
                attempts,
                attempts_rx,
                handle,
            }
        }

        async fn next_attempt(&mut self) -> usize {
            tokio::time::timeout(WAIT_TIMEOUT, self.attempts_rx.recv())
                .await
                .expect("timed out waiting for a schema rebuild")
                .unwrap()
        }

        fn attempts(&self) -> usize {
            self.attempts.load(Ordering::SeqCst)
        }

        async fn shutdown(self) {
            self.shutdown_tx.send(()).unwrap();
            tokio::time::timeout(WAIT_TIMEOUT, self.handle)
                .await
                .expect("rebuild loop did not stop on shutdown")
                .unwrap();
        }
    }

    #[tokio::test]
    async fn test_swapped_schema_is_served_by_existing_handle() {
        let schema = DynamicSchema::new(versioned_schema(1, false));
        // Handles are created once (e.g. by the HTTP handler), only the schema behind them changes.
        let handle = schema.clone();

        let query = |schema: DynamicSchema| async move {
            let response = Executor::execute(&schema, Request::new("{ version extra }")).await;
            serde_json::to_value(&response).unwrap()
        };

        let response = query(handle.clone()).await;
        assert!(
            response["errors"][0]["message"]
                .as_str()
                .unwrap()
                .contains("extra"),
            "unexpected response: {response}"
        );

        schema.swap(versioned_schema(2, true));

        let response = query(handle).await;
        assert!(response.get("errors").is_none(), "{response}");
        assert_eq!(response["data"]["version"], 2);
        assert_eq!(response["data"]["extra"], "hello");
    }

    #[tokio::test]
    async fn test_running_subscriptions_keep_their_schema() {
        let schema = DynamicSchema::new(versioned_schema(1, false));

        let mut running =
            Executor::execute_stream(&schema, Request::new("subscription { version }"), None);
        schema.swap(versioned_schema(2, false));
        let mut started_after_swap =
            Executor::execute_stream(&schema, Request::new("subscription { version }"), None);

        let running = running.next().await.unwrap().into_result().unwrap();
        assert_eq!(running.data.into_json().unwrap()["version"], 1);
        let started_after_swap = started_after_swap
            .next()
            .await
            .unwrap()
            .into_result()
            .unwrap();
        assert_eq!(started_after_swap.data.into_json().unwrap()["version"], 2);
    }

    #[tokio::test]
    async fn test_failed_rebuild_keeps_previous_schema() {
        let mut harness = RebuildHarness::spawn(
            Duration::from_millis(20),
            Duration::from_secs(5),
            0,
            |attempt| match attempt {
                1 => Err(anyhow::anyhow!("pool timed out")),
                _ => Ok(versioned_schema(2, false)),
            },
        );

        harness.updates_tx.send(()).unwrap();
        assert_eq!(harness.next_attempt().await, 1);
        assert_eq!(served_version(&harness.schema).await, 1);

        // The loop survives the failure and applies the next successful rebuild.
        harness.updates_tx.send(()).unwrap();
        assert_eq!(harness.next_attempt().await, 2);
        wait_for_version(&harness.schema, 2).await;

        harness.shutdown().await;
    }

    #[tokio::test]
    async fn test_burst_of_updates_triggers_single_rebuild() {
        let quiet_period = Duration::from_millis(200);
        let mut harness =
            RebuildHarness::spawn(quiet_period, Duration::from_secs(30), 50, |attempt| {
                Ok(versioned_schema(attempt as i32 + 1, false))
            });

        assert_eq!(harness.next_attempt().await, 1);
        wait_for_version(&harness.schema, 2).await;

        tokio::time::sleep(quiet_period * 3).await;
        assert_eq!(harness.attempts(), 1);

        harness.shutdown().await;
    }

    #[tokio::test]
    async fn test_trickle_of_updates_is_debounced() {
        let quiet_period = Duration::from_millis(300);
        let mut harness =
            RebuildHarness::spawn(quiet_period, Duration::from_secs(30), 0, |attempt| {
                Ok(versioned_schema(attempt as i32 + 1, false))
            });

        for _ in 0..20 {
            harness.updates_tx.send(()).unwrap();
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        harness.next_attempt().await;
        tokio::time::sleep(quiet_period * 3).await;
        // Ideally exactly one, allow a second one in case the test runner stalls mid-trickle.
        assert!(
            harness.attempts() <= 2,
            "20 updates caused {} rebuilds",
            harness.attempts()
        );

        harness.shutdown().await;
    }

    #[tokio::test]
    async fn test_continuous_updates_do_not_postpone_rebuild_forever() {
        // The quiet period is never reached, only `max_delay` can trigger the rebuild.
        let harness = RebuildHarness::spawn(
            Duration::from_secs(60),
            Duration::from_millis(200),
            0,
            |attempt| Ok(versioned_schema(attempt as i32 + 1, false)),
        );

        tokio::time::timeout(WAIT_TIMEOUT, async {
            loop {
                harness.updates_tx.send(()).unwrap();
                if harness.attempts() > 0 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("rebuild was postponed while updates kept coming");
        wait_for_version(&harness.schema, 2).await;

        harness.shutdown().await;
    }

    #[tokio::test]
    async fn test_rebuild_loop_waits_for_shutdown_when_updates_end() {
        let mut harness =
            RebuildHarness::spawn(Duration::from_millis(20), Duration::from_secs(5), 0, |_| {
                Ok(versioned_schema(2, false))
            });

        // The runner shuts Torii down as soon as the GraphQL task resolves, so the loop must keep
        // running (serving the last schema) once the update stream ends.
        let (updates_tx, _) = mpsc::unbounded_channel();
        drop(std::mem::replace(&mut harness.updates_tx, updates_tx));
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(!harness.handle.is_finished());
        assert_eq!(harness.attempts(), 0);
        assert_eq!(served_version(&harness.schema).await, 1);

        harness.shutdown().await;
    }

    #[sqlx::test(migrations = "../migrations")]
    #[serial]
    async fn test_built_schema_picks_up_registered_model(pool: SqlitePool) {
        let (db, provider, _executor_shutdown) = setup_sql(&pool).await;
        let messaging = Arc::new(Messaging::new(
            Default::default(),
            Arc::new(db.clone()),
            provider,
        ));
        let (shutdown_tx, shutdown_rx) = broadcast::channel(1);

        let (schema, rebuild) = server::build(shutdown_rx, &pool, messaging, Arc::new(db.clone()))
            .await
            .unwrap();
        let rebuild = tokio::spawn(rebuild);

        let type_query = format!(
            r#"{{ __type(name: "{}") {{ name }} }}"#,
            utils::type_name_from_names("types_test", "Record")
        );
        let type_is_null = |schema: DynamicSchema| {
            let request = Request::new(type_query.clone());
            async move {
                let response = Executor::execute(&schema, request).await;
                assert!(response.errors.is_empty(), "{:?}", response.errors);
                response.data.into_json().unwrap()["__type"].is_null()
            }
        };
        assert!(type_is_null(schema.clone()).await);

        // Registering a model publishes a model update, which rebuilds the schema in place.
        model_fixtures(&db).await;
        tokio::time::timeout(WAIT_TIMEOUT, async {
            while type_is_null(schema.clone()).await {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("registered model never showed up in the built schema");

        assert!(!rebuild.is_finished());
        shutdown_tx.send(()).unwrap();
        tokio::time::timeout(WAIT_TIMEOUT, rebuild)
            .await
            .expect("rebuild loop did not stop on shutdown")
            .unwrap();
    }
}
