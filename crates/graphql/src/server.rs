use std::future::Future;
use std::sync::{Arc, PoisonError, RwLock};
use std::time::Duration;

use anyhow::{Context, Result};
use async_graphql::dynamic::Schema;
use async_graphql::futures_util::stream::BoxStream;
use async_graphql::{Data, Executor, Request, Response};
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

use crate::playground::{graphiql::GraphiQLSource, graphiql_plugin::GraphiQLPlugin};

use super::schema::build_schema;

/// How long the model update stream must stay quiet before the schema is rebuilt, so that a burst
/// of model registrations results in a single rebuild.
pub const SCHEMA_REBUILD_QUIET_PERIOD: Duration = Duration::from_millis(500);

/// Upper bound on how long a continuous stream of model updates can postpone a schema rebuild.
pub const SCHEMA_REBUILD_MAX_DELAY: Duration = Duration::from_secs(5);

/// Delay before retrying a failed schema rebuild, doubled after each failure up to
/// [`SCHEMA_REBUILD_RETRY_MAX`], so that a transient error (e.g. a pool timeout) doesn't leave the
/// schema stale until the next model update.
pub const SCHEMA_REBUILD_RETRY_MIN: Duration = Duration::from_secs(1);

/// Upper bound on the delay between retries of a failed schema rebuild.
pub const SCHEMA_REBUILD_RETRY_MAX: Duration = Duration::from_secs(60);

/// Timing of the schema rebuild loop, see [`rebuild_schema_on_updates`].
#[derive(Clone, Copy, Debug)]
pub(crate) struct RebuildTiming {
    pub(crate) quiet_period: Duration,
    pub(crate) max_delay: Duration,
    pub(crate) retry_min: Duration,
    pub(crate) retry_max: Duration,
}

impl Default for RebuildTiming {
    fn default() -> Self {
        Self {
            quiet_period: SCHEMA_REBUILD_QUIET_PERIOD,
            max_delay: SCHEMA_REBUILD_MAX_DELAY,
            retry_min: SCHEMA_REBUILD_RETRY_MIN,
            retry_max: SCHEMA_REBUILD_RETRY_MAX,
        }
    }
}

/// Version of GraphiQL (and of its explorer plugin) served by the playground.
const GRAPHIQL_VERSION: &str = "6.0.0-canary-d779fd3f.0";

/// Builds the GraphQL schema, to be served by the caller.
///
/// The schema is built once up front and then rebuilt in the background whenever models are
/// registered or upgraded, by the returned future. Rebuilt schemas are swapped in place, so the
/// returned [`DynamicSchema`] stays valid for the whole lifetime of the future, which only resolves
/// on shutdown.
pub async fn build<P: Provider + Sync + Send + Clone + 'static>(
    shutdown_rx: Receiver<()>,
    pool: &Pool<Sqlite>,
    messaging: Arc<Messaging<P>>,
    storage: Arc<dyn ReadOnlyStorage>,
) -> Result<(DynamicSchema, impl Future<Output = ()> + 'static)> {
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
        shutdown_rx,
        RebuildTiming::default(),
        move || {
            let pool = pool.clone();
            let messaging = messaging.clone();
            let storage = storage.clone();
            async move { build_schema(&pool, messaging, storage).await }
        },
    );

    Ok((schema, rebuild))
}

/// Returns the GraphiQL playground page.
///
/// The page sends queries and subscriptions to the URL it is served from (over `ws`/`wss` for
/// subscriptions) rather than to a fixed path on the origin, so that it keeps working behind hosted
/// services like Slot, which serve Torii under a path prefix.
pub fn playground_html() -> String {
    GraphiQLSource::build()
        .version(GRAPHIQL_VERSION)
        .plugins(&[GraphiQLPlugin::explorer(GRAPHIQL_VERSION)])
        .subscription_endpoint("")
        .title("Torii GraphQL Playground")
        .finish()
}

/// A GraphQL executor whose underlying schema can be replaced at runtime.
///
/// Every new request or subscription is executed against the latest schema, while subscriptions
/// that are already running keep the schema they were started with.
#[derive(Clone)]
pub struct DynamicSchema(Arc<RwLock<Schema>>);

impl DynamicSchema {
    pub fn new(schema: Schema) -> Self {
        Self(Arc::new(RwLock::new(schema)))
    }

    /// Returns the current schema. Cloning a [`Schema`] is cheap (it is reference counted).
    pub fn current(&self) -> Schema {
        self.0
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Replaces the schema used for subsequent requests.
    pub fn swap(&self, schema: Schema) {
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
/// Returns when `shutdown_rx` receives a message (or is closed). If `updates` ends first, the
/// schema is no longer rebuilt but the loop still waits for shutdown, so that callers can treat
/// its completion as a shutdown.
pub(crate) async fn rebuild_schema_on_updates<S, F, Fut>(
    schema: DynamicSchema,
    updates: S,
    mut shutdown_rx: Receiver<()>,
    timing: RebuildTiming,
    build: F,
) where
    S: Stream,
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<Schema>>,
{
    tokio::select! {
        _ = shutdown_rx.recv() => {
            debug!("Stopping GraphQL schema rebuild loop.");
            return;
        }
        _ = rebuild_loop(schema, updates, timing, build) => {
            warn!("Model update stream ended, GraphQL schema will no longer be rebuilt.");
        }
    }

    // The last schema keeps being served until shutdown.
    let _ = shutdown_rx.recv().await;
}

async fn rebuild_loop<S, F, Fut>(
    schema: DynamicSchema,
    updates: S,
    timing: RebuildTiming,
    mut build: F,
) where
    S: Stream,
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<Schema>>,
{
    let mut updates = std::pin::pin!(updates);
    // Set after a failed rebuild: the delay before retrying it.
    let mut retry_delay: Option<Duration> = None;

    loop {
        // Wait for a model update, or for the retry of a failed rebuild.
        let update = match retry_delay {
            Some(delay) => tokio::select! {
                update = updates.next() => update.map(|_| true),
                () = tokio::time::sleep(delay) => Some(false),
            },
            None => updates.next().await.map(|_| true),
        };
        let Some(is_update) = update else {
            break;
        };

        let mut updates_count = usize::from(is_update);
        // A retry rebuilds right away, an update waits for the stream to settle.
        let deadline = Instant::now()
            + if is_update {
                timing.max_delay
            } else {
                Duration::ZERO
            };
        let mut stream_open = true;

        // Debounce: keep draining updates until the stream has been quiet for `quiet_period`.
        loop {
            let now = Instant::now();
            if now >= deadline {
                break;
            }

            match tokio::time::timeout_at((now + timing.quiet_period).min(deadline), updates.next())
                .await
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
                retry_delay = None;
                info!(
                    updates = updates_count,
                    elapsed = ?started.elapsed(),
                    "Rebuilt GraphQL schema."
                );
            }
            Err(e) => {
                let delay = retry_delay
                    .map_or(timing.retry_min, |delay| delay * 2)
                    .min(timing.retry_max);
                retry_delay = Some(delay);
                error!(
                    error = ?e,
                    updates = updates_count,
                    retry_in = ?delay,
                    "Failed to rebuild GraphQL schema, keeping the previous schema."
                );
            }
        }

        if !stream_open {
            break;
        }
    }
}
