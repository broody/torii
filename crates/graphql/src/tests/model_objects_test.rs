#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use async_graphql::dynamic::{Field, FieldFuture, FieldValue, Object, Schema, TypeRef, Union};
    use async_graphql::{Name, Value};
    use dojo_types::naming::get_tag;
    use dojo_types::primitive::Primitive;
    use dojo_types::schema::{Enum, EnumOption, Member, Struct, Ty};
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
    use torii_sqlite::utils::felt_to_sql_string;
    use torii_sqlite::Sql;
    use torii_storage::proto::{ContractDefinition, ContractType};
    use torii_storage::utils::format_world_scoped_id;
    use torii_storage::Storage;
    use url::Url;

    use crate::constants::INTERNAL_ENTITY_ID_KEY;
    use crate::object::model_data::{object, EntityRowId, EventMessageRowId, ModelNodeLoader};
    use crate::query::build_type_mapping;
    use crate::schema::build_schema;
    use crate::tests::run_graphql_query;
    use crate::types::{TypeData, TypeMapping, ValueMapping};

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

    /// Emits `model` as an event message, returns the event message id.
    async fn set_event_message(db: &Sql, model: &Ty, event_id: &str) -> String {
        let keys = keys(model);
        let hashed_keys = poseidon_hash_many(&keys);
        db.set_event_message(Felt::ZERO, model.clone(), event_id, BLOCK_TIMESTAMP, keys)
            .await
            .unwrap();
        format_world_scoped_id(&Felt::ZERO, &hashed_keys)
    }

    fn position(player: Felt, vec: Option<(u32, u32)>) -> Ty {
        let coordinate = |name: &str, value: Option<u32>| Member {
            name: name.to_string(),
            key: false,
            ty: Ty::Primitive(Primitive::U32(value)),
        };
        Ty::Struct(Struct {
            name: get_tag("ns", "Position"),
            children: vec![
                Member {
                    name: "player".to_string(),
                    key: true,
                    ty: Ty::Primitive(Primitive::ContractAddress(Some(player))),
                },
                Member {
                    name: "vec".to_string(),
                    key: false,
                    ty: Ty::Struct(Struct {
                        name: "Vec2".to_string(),
                        children: vec![
                            coordinate("x", vec.map(|v| v.0)),
                            coordinate("y", vec.map(|v| v.1)),
                        ],
                    }),
                },
            ],
        })
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

    // Nested values are built from the same row as their model, so nested fields must resolve from
    // the parent value alone: no database access, and missing values resolve to null instead of
    // panicking. Covers both parents: model values and owned `ModelUnion` members.
    #[tokio::test]
    async fn test_model_fields_resolve_from_parent_value() {
        let nested_mapping = TypeMapping::from([
            (
                Name::new("x"),
                TypeData::Simple(TypeRef::named(TypeRef::INT)),
            ),
            (
                Name::new("y"),
                TypeData::Simple(TypeRef::named(TypeRef::INT)),
            ),
        ]);
        let mapping = TypeMapping::from([
            (
                Name::new("value"),
                TypeData::Simple(TypeRef::named(TypeRef::INT)),
            ),
            (
                Name::new("vec"),
                TypeData::Nested((TypeRef::named("ns_Vec2"), nested_mapping.clone())),
            ),
            (
                Name::new("missing"),
                TypeData::Simple(TypeRef::named(TypeRef::INT)),
            ),
        ]);
        let model_value = || {
            ValueMapping::from([
                (Name::new(INTERNAL_ENTITY_ID_KEY), Value::from("0x1")),
                (Name::new("value"), Value::from(1)),
                (
                    Name::new("vec"),
                    Value::Object(ValueMapping::from([
                        (Name::new(INTERNAL_ENTITY_ID_KEY), Value::from("0x1")),
                        (Name::new("x"), Value::from(2)),
                        (Name::new("y"), Value::from(3)),
                    ])),
                ),
            ])
        };

        let query = Object::new("Query")
            .field(Field::new("model", TypeRef::named("ns_Model"), move |_| {
                FieldFuture::new(async move { Ok(Some(Value::Object(model_value()))) })
            }))
            .field(Field::new(
                "models",
                TypeRef::named_list("ModelUnion"),
                move |_| {
                    FieldFuture::new(async move {
                        Ok(Some(FieldValue::list([FieldValue::with_type(
                            FieldValue::owned_any(model_value()),
                            "ns_Model",
                        )])))
                    })
                },
            ));
        // no pool registered: resolving model fields must not touch the database
        let schema = Schema::build("Query", None, None)
            .register(query)
            .register(Union::new("ModelUnion").possible_type("ns_Model"))
            .register(object("ns_Model", &mapping))
            .register(object("ns_Vec2", &nested_mapping))
            .finish()
            .unwrap();

        let result = schema
            .execute(
                r#"{
                    model { value missing vec { x y } }
                    models { ... on ns_Model { value missing vec { x y } } }
                }"#,
            )
            .await;

        assert!(result.errors.is_empty(), "{:?}", result.errors);
        let model = json!({ "value": 1, "missing": null, "vec": { "x": 2, "y": 3 } });
        assert_eq!(
            serde_json::to_value(result.data).unwrap(),
            json!({ "model": model.clone(), "models": [model] })
        );
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

    // The `models` union of entities and event messages resolves models from the registry built
    // with the schema. Models registered after the schema was built aren't members of its
    // `ModelUnion` and must be skipped rather than failing the query.
    #[sqlx::test(migrations = "../migrations")]
    #[serial]
    async fn test_models_union_uses_schema_registry(pool: SqlitePool) {
        let (db, _shutdown_tx) = setup_db(&pool).await;
        register_model(&db, &player_stats(Felt::ZERO, None)).await;
        let entity_id = set_entity(&db, &player_stats(Felt::ONE, Some(7)), "0x1:0x0:0x0").await;
        let event_message_id =
            set_event_message(&db, &player_stats(Felt::TWO, Some(9)), "0x2:0x0:0x0").await;
        db.execute().await.unwrap();

        let schema_before = schema(&db).await;

        // registered after `schema_before` was built
        register_model(&db, &position(Felt::ZERO, None)).await;
        set_entity(&db, &position(Felt::ONE, Some((3, 4))), "0x3:0x0:0x0").await;
        set_event_message(&db, &position(Felt::TWO, Some((5, 6))), "0x4:0x0:0x0").await;
        db.execute().await.unwrap();

        let schema_after = schema(&db).await;

        // (dynamic schemas don't apply fragments conditioned on a union, hence no named fragment)
        let query = |position_fields: &str| {
            let models = format!(
                "models {{ __typename ... on ns_Player_Stats {{ score }} {position_fields} }}"
            );
            format!(
                r#"{{
                    entity(id: "{entity_id}") {{ {models} }}
                    eventMessage(id: "{event_message_id}") {{ {models} }}
                }}"#
            )
        };
        let models = |result: &serde_json::Value, parent: &str| {
            result[parent]["models"].as_array().unwrap().clone()
        };

        // `ns_Position` isn't a type of `schema_before`
        let result = run_graphql_query(&schema_before, &query("")).await;
        let stats = |score| json!({ "__typename": "ns_Player_Stats", "score": score });
        assert_eq!(models(&result, "entity"), vec![stats(7)]);
        assert_eq!(models(&result, "eventMessage"), vec![stats(9)]);

        // models are listed by model id
        let mut tags = [get_tag("ns", "Player_Stats"), get_tag("ns", "Position")];
        tags.sort_by_key(|tag| {
            format_world_scoped_id(&Felt::ZERO, &compute_selector_from_tag(tag))
        });
        let expected = |score, x, y| {
            tags.iter()
                .map(|tag| match tag.as_str() {
                    "ns-Player_Stats" => stats(score),
                    _ => json!({ "__typename": "ns_Position", "vec": { "x": x, "y": y } }),
                })
                .collect::<Vec<_>>()
        };

        let result =
            run_graphql_query(&schema_after, &query("... on ns_Position { vec { x y } }")).await;
        assert_eq!(models(&result, "entity"), expected(7, 3, 4));
        assert_eq!(models(&result, "eventMessage"), expected(9, 5, 6));
    }

    #[test]
    fn test_build_type_mapping_of_non_struct_schema() {
        let schemas = [
            Ty::Primitive(Primitive::U32(None)),
            Ty::Enum(Enum {
                name: "ns-Direction".to_string(),
                option: None,
                options: vec![EnumOption {
                    name: "Up".to_string(),
                    ty: Ty::Tuple(vec![]),
                }],
            }),
            Ty::Tuple(vec![Ty::Primitive(Primitive::U8(None))]),
        ];
        for schema in schemas {
            assert!(build_type_mapping("ns", &schema).is_empty());
        }
        assert_eq!(
            build_type_mapping("ns", &player_stats(Felt::ZERO, None))
                .keys()
                .map(|name| name.as_str())
                .collect::<Vec<_>>(),
            ["player", "score"]
        );
    }

    // Models whose schema isn't a struct can't be exposed and are skipped when building the
    // schema, which must stay valid even if no model is left for `ModelUnion`.
    #[sqlx::test(migrations = "../migrations")]
    #[serial]
    async fn test_schema_skips_non_struct_models(pool: SqlitePool) {
        let (db, _shutdown_tx) = setup_db(&pool).await;
        let insert_model = |tag: &str, schema: &Ty| {
            let (namespace, name) = tag.split_once('-').unwrap();
            let selector = compute_selector_from_tag(tag);
            sqlx::query(
                "INSERT INTO models (id, model_selector, namespace, name, class_hash, layout, \
                 schema, packed_size, unpacked_size, executed_at) VALUES (?, ?, ?, ?, '0x0', \
                 '[]', ?, 0, 0, '2024-03-18T09:34:38Z')",
            )
            .bind(format_world_scoped_id(&Felt::ZERO, &selector))
            .bind(format!("{selector:#064x}"))
            .bind(namespace.to_string())
            .bind(name.to_string())
            .bind(serde_json::to_string(schema).unwrap())
            .execute(&pool)
        };
        let possible_types = |schema: Schema| async move {
            let result = run_graphql_query(
                &schema,
                r#"{ __type(name: "ModelUnion") { possibleTypes { name } } }"#,
            )
            .await;
            result["__type"]["possibleTypes"].clone()
        };

        insert_model("ns-Counter", &Ty::Primitive(Primitive::U32(None)))
            .await
            .unwrap();
        assert_eq!(
            possible_types(schema(&db).await).await,
            json!([{ "name": "World__Empty" }])
        );

        register_model(&db, &player_stats(Felt::ZERO, None)).await;
        db.execute().await.unwrap();
        assert_eq!(
            possible_types(schema(&db).await).await,
            json!([{ "name": "ns_Player_Stats" }])
        );
    }

    // Model rows of event messages reference them through `internal_event_message_id`, their
    // `internal_entity_id` being NULL.
    #[sqlx::test(migrations = "../migrations")]
    #[serial]
    async fn test_event_message_model_nodes(pool: SqlitePool) {
        let (db, _shutdown_tx) = setup_db(&pool).await;
        register_model(&db, &player_stats(Felt::ZERO, None)).await;
        let event_message_id =
            set_event_message(&db, &player_stats(Felt::ONE, Some(7)), "0x1:0x0:0x0").await;
        db.execute().await.unwrap();

        let result = run_graphql_query(
            &schema(&db).await,
            "{ nsPlayerStatsModels { edges { node { score eventMessage { id keys } } } } }",
        )
        .await;

        assert_eq!(
            result,
            json!({
                "nsPlayerStatsModels": {
                    "edges": [{
                        "node": {
                            "score": 7,
                            "eventMessage": {
                                "id": event_message_id,
                                "keys": [felt_to_sql_string(&Felt::ONE)],
                            },
                        }
                    }]
                }
            })
        );
    }

    // The `entity` and `eventMessage` fields of model nodes are batched through a data loader,
    // which must resolve the same values (and errors) as querying them node by node.
    #[sqlx::test(migrations = "../migrations")]
    #[serial]
    async fn test_model_nodes_entity_and_event_message(pool: SqlitePool) {
        let (db, _shutdown_tx) = setup_db(&pool).await;
        register_model(&db, &player_stats(Felt::ZERO, None)).await;
        let players = [Felt::ONE, Felt::TWO, Felt::THREE];
        for (i, player) in players.iter().enumerate() {
            let model = player_stats(*player, Some(i as u32));
            set_entity(&db, &model, &format!("0x{i:x}:0x0:0x0")).await;
            set_event_message(&db, &model, &format!("0x{i:x}:0x1:0x0")).await;
        }
        db.execute().await.unwrap();

        let schema = schema(&db).await;
        let result = run_graphql_query(
            &schema,
            r#"{
                nsPlayerStatsModels {
                    totalCount
                    edges { node { player entity { id keys } eventMessage { id keys } } }
                }
            }"#,
        )
        .await;

        // each player has an entity row and an event message row, both linked to the entity and
        // the event message of the player
        let connection = &result["nsPlayerStatsModels"];
        assert_eq!(connection["totalCount"], 6);
        let mut nodes: Vec<_> = connection["edges"]
            .as_array()
            .unwrap()
            .iter()
            .map(|edge| edge["node"].clone())
            .collect();
        nodes.sort_by_key(|node| node["player"].as_str().unwrap().to_string());
        let expected: Vec<_> = players
            .iter()
            .flat_map(|player| {
                let hashed_keys = poseidon_hash_many(&[*player]);
                let entity = json!({
                    "id": format_world_scoped_id(&Felt::ZERO, &hashed_keys),
                    "keys": [felt_to_sql_string(player)],
                });
                let node = json!({
                    "player": format!("{player:#x}"),
                    "entity": entity,
                    "eventMessage": entity,
                });
                [node.clone(), node]
            })
            .collect();
        assert_eq!(nodes, expected);

        // an entity that was never emitted as an event message: its node's `eventMessage` fails
        // like a single row lookup would
        set_entity(&db, &player_stats(Felt::from(4u8), Some(4)), "0x4:0x0:0x0").await;
        db.execute().await.unwrap();

        let result = schema
            .execute("{ nsPlayerStatsModels { edges { node { eventMessage { id } } } } }")
            .await;
        let errors: Vec<_> = result.errors.iter().map(|e| e.message.clone()).collect();
        assert_eq!(errors, [sqlx::Error::RowNotFound.to_string()]);
    }

    // The loader is schema data shared by all requests: it must batch lookups, never cache them.
    #[sqlx::test(migrations = "../migrations")]
    async fn test_model_node_loader_does_not_cache(pool: SqlitePool) {
        for id in ["a", "b"] {
            sqlx::query(
                "INSERT INTO entities (id, keys, event_id, executed_at) VALUES (?, '0x1/', \
                 'event', '2024-03-18T09:34:38Z')",
            )
            .bind(id)
            .execute(&pool)
            .await
            .unwrap();
        }
        let id = |id: &str| EntityRowId(id.to_string());
        let loader = ModelNodeLoader::data_loader(pool.clone());

        let loaded = loader
            .load_many([id("a"), id("b"), id("missing")])
            .await
            .unwrap();
        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[&id("a")]["id"], Value::from("a"));
        assert_eq!(
            loaded[&id("a")]["keys"],
            Value::List(vec![Value::from("0x1")])
        );
        assert_eq!(loaded[&id("b")]["eventId"], Value::from("event"));

        sqlx::query("UPDATE entities SET event_id = 'updated' WHERE id = 'a'")
            .execute(&pool)
            .await
            .unwrap();
        let a = loader.load_one(id("a")).await.unwrap().unwrap();
        assert_eq!(a["eventId"], Value::from("updated"));

        // event messages live in their own table
        let event_message = loader
            .load_one(EventMessageRowId("a".to_string()))
            .await
            .unwrap();
        assert!(event_message.is_none());
    }
}
