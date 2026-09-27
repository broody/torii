use std::future::Future;
use std::net::SocketAddr;
use std::sync::{Arc, PoisonError, RwLock};
use std::time::Duration;

use anyhow::{Context, Result};
use async_graphql::dynamic::Schema;
use async_graphql::futures_util::stream::BoxStream;
use async_graphql::{Data, Executor, Request, Response};
use async_graphql_warp::graphql_subscription;
use sqlx::{Pool, Sqlite};
use starknet::providers::Provider;
use tokio::sync::broadcast::Receiver;
use tokio::time::Instant;
use tokio_stream::{Stream, StreamExt};
use torii_broker::types::ModelUpdate;
use torii_broker::MemoryBroker;
use torii_messaging::Messaging;
use torii_storage::ReadOnlyStorage;
use tracing::{debug, error, info, warn};
use warp::{Filter, Rejection, Reply};

use crate::playground::{graphiql::GraphiQLSource, graphiql_plugin::GraphiQLPlugin};

use super::schema::build_schema;

/// How long the model update stream must stay quiet before the schema is rebuilt, so that a burst
/// of model registrations results in a single rebuild.
pub const SCHEMA_REBUILD_QUIET_PERIOD: Duration = Duration::from_millis(500);

/// Upper bound on how long a continuous stream of model updates can postpone a schema rebuild.
pub const SCHEMA_REBUILD_MAX_DELAY: Duration = Duration::from_secs(5);

/// Starts the GraphQL server on an ephemeral local port.
///
/// The schema is built once up front and then rebuilt in the background whenever models are
/// registered or upgraded. Rebuilt schemas are swapped in place, so the server (and its address)
/// stays the same for the whole lifetime of the returned future, which only resolves on shutdown.
pub async fn new<P: Provider + Sync + Send + Clone + 'static>(
    mut shutdown_rx: Receiver<()>,
    pool: &Pool<Sqlite>,
    messaging: Arc<Messaging<P>>,
    storage: Arc<dyn ReadOnlyStorage>,
) -> Result<(SocketAddr, impl Future<Output = ()> + 'static)> {
    // Subscribe before the initial build so that models registered while it runs still trigger a
    // rebuild.
    let model_updates = MemoryBroker::<ModelUpdate>::subscribe();

    let schema = build_schema(pool, messaging.clone(), storage.clone())
        .await
        .context("Failed to build initial GraphQL schema")?;
    let schema = DynamicSchema::new(schema);

    let pool = pool.clone();
    let rebuild = rebuild_schema_on_updates(
        schema.clone(),
        model_updates,
        shutdown_rx.resubscribe(),
        SCHEMA_REBUILD_QUIET_PERIOD,
        SCHEMA_REBUILD_MAX_DELAY,
        move || {
            let pool = pool.clone();
            let messaging = messaging.clone();
            let storage = storage.clone();
            async move { build_schema(&pool, messaging, storage).await }
        },
    );

    let routes = graphql_filter(schema);
    let (addr, server) = warp::serve(routes)
        .try_bind_with_graceful_shutdown(([127, 0, 0, 1], 0), async move {
            shutdown_rx.recv().await.ok();
        })
        .context("Failed to bind GraphQL server")?;

    // The server owns the lifetime of this future: the rebuild loop is dropped once the server
    // stops, and the server keeps serving the last schema if the rebuild loop ends first.
    let serve = async move {
        tokio::pin!(server);
        tokio::select! {
            () = &mut server => {}
            () = rebuild => server.await,
        }
    };

    Ok((addr, serve))
}

/// A GraphQL executor whose underlying schema can be replaced at runtime.
///
/// Every new request or subscription is executed against the latest schema, while subscriptions
/// that are already running keep the schema they were started with.
#[derive(Clone)]
pub(crate) struct DynamicSchema(Arc<RwLock<Schema>>);

impl DynamicSchema {
    pub(crate) fn new(schema: Schema) -> Self {
        Self(Arc::new(RwLock::new(schema)))
    }

    /// Returns the current schema. Cloning a [`Schema`] is cheap (it is reference counted).
    pub(crate) fn current(&self) -> Schema {
        self.0
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Replaces the schema used for subsequent requests.
    pub(crate) fn swap(&self, schema: Schema) {
        let previous = std::mem::replace(
            &mut *self.0.write().unwrap_or_else(PoisonError::into_inner),
            schema,
        );
        // Drop the previous schema outside of the lock.
        drop(previous);
    }
}

impl Executor for DynamicSchema {
    async fn execute(&self, request: Request) -> Response {
        // Clone the schema out of the lock so it is never held across an await point.
        let schema = self.current();
        Executor::execute(&schema, request).await
    }

    fn execute_stream(
        &self,
        request: Request,
        session_data: Option<Arc<Data>>,
    ) -> BoxStream<'static, Response> {
        Executor::execute_stream(&self.current(), request, session_data)
    }
}

/// Rebuilds the schema with `build` whenever `updates` yields, and swaps it into `schema`.
///
/// Updates are debounced: after an update, further updates are drained until `quiet_period`
/// passes without any (or `max_delay` passes since the first one), then the schema is rebuilt
/// once. A failed rebuild is logged and the previous schema keeps being served.
///
/// Returns when `shutdown_rx` receives a message (or is closed), or when `updates` ends.
pub(crate) async fn rebuild_schema_on_updates<S, F, Fut>(
    schema: DynamicSchema,
    updates: S,
    mut shutdown_rx: Receiver<()>,
    quiet_period: Duration,
    max_delay: Duration,
    build: F,
) where
    S: Stream,
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<Schema>>,
{
    tokio::select! {
        _ = shutdown_rx.recv() => {
            debug!("Stopping GraphQL schema rebuild loop.");
        }
        _ = rebuild_loop(schema, updates, quiet_period, max_delay, build) => {
            warn!("Model update stream ended, GraphQL schema will no longer be rebuilt.");
        }
    }
}

async fn rebuild_loop<S, F, Fut>(
    schema: DynamicSchema,
    updates: S,
    quiet_period: Duration,
    max_delay: Duration,
    mut build: F,
) where
    S: Stream,
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<Schema>>,
{
    let mut updates = std::pin::pin!(updates);

    while updates.next().await.is_some() {
        let mut updates_count = 1usize;
        let deadline = Instant::now() + max_delay;
        let mut stream_open = true;

        // Debounce: keep draining updates until the stream has been quiet for `quiet_period`.
        loop {
            let now = Instant::now();
            if now >= deadline {
                break;
            }

            match tokio::time::timeout_at((now + quiet_period).min(deadline), updates.next()).await
            {
                Ok(Some(_)) => updates_count += 1,
                Ok(None) => {
                    stream_open = false;
                    break;
                }
                Err(_) => break,
            }
        }

        let started = Instant::now();
        match build().await {
            Ok(new_schema) => {
                schema.swap(new_schema);
                info!(
                    updates = updates_count,
                    elapsed = ?started.elapsed(),
                    "Rebuilt GraphQL schema."
                );
            }
            Err(e) => {
                error!(
                    error = ?e,
                    updates = updates_count,
                    "Failed to rebuild GraphQL schema, keeping the previous schema."
                );
            }
        }

        if !stream_open {
            break;
        }
    }
}

pub(crate) fn graphql_filter(
    schema: DynamicSchema,
) -> impl Filter<Extract = impl Reply, Error = Rejection> + Clone {
    let graphql_post = async_graphql_warp::graphql(schema.clone()).and_then(
        move |(schema, request): (DynamicSchema, Request)| async move {
            // Execute query
            let response = schema.execute(request).await;
            // Return result
            Ok::<_, Rejection>(warp::reply::json(&response))
        },
    );

    let playground_filter = warp::path("graphql").map(move || {
        warp::reply::html(
            GraphiQLSource::build()
                .version("6.0.0-canary-d779fd3f.0")
                .plugins(&[GraphiQLPlugin::explorer("6.0.0-canary-d779fd3f.0")])
                // Queries and subscriptions (over ws/wss) go to the URL the page is served from,
                // so that it keeps working behind hosted services like Slot, which serve Torii
                // under a path prefix.
                .subscription_endpoint("")
                .title("Torii GraphQL Playground")
                .finish(),
        )
    });

    graphql_subscription(schema)
        .or(graphql_post)
        .or(playground_filter)
}
