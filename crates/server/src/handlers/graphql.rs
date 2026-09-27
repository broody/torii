use std::future::Future;
use std::net::IpAddr;
use std::str::FromStr;
use std::sync::LazyLock;
use std::time::Duration;

use async_graphql::http::{
    parse_query_string, receive_batch_body, MultipartOptions, WebSocket, WebSocketProtocols,
    WsMessage,
};
use async_graphql::{BatchRequest, Executor, ParseRequestError};
use futures_util::{future, StreamExt};
use http::header::{ALLOW, CONTENT_TYPE, SEC_WEBSOCKET_PROTOCOL};
use http::{HeaderValue, Method, StatusCode};
use http_body::{LengthLimitError, Limited};
use hyper::{Body, Request, Response};
use hyper_tungstenite::HyperWebsocket;
use tokio::runtime::Handle;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::{Error as WsError, Message};
use tokio_util::task::AbortOnDropHandle;
use torii_graphql::server::{playground_html, DynamicSchema};
use tracing::{debug, error};

use super::Handler;

pub(crate) const LOG_TARGET: &str = "torii::server::handlers::graphql";

// Timeout for regular GraphQL requests (not WebSocket which are long-lived)
const GRAPHQL_REQUEST_TIMEOUT: Duration = Duration::from_secs(60);

/// Maximum size of a GraphQL request body. Queries are small; this bounds the memory a single
/// request can make the server buffer.
const MAX_REQUEST_BODY_SIZE: usize = 2 * 1024 * 1024;

static PLAYGROUND: LazyLock<String> = LazyLock::new(playground_html);

/// Serves GraphQL queries (`GET` with a query string and `POST`, including batches), subscriptions
/// over WebSocket and the GraphiQL playground (`GET` without a query string).
pub struct GraphQLHandler {
    /// `None` until the schema is built, requests are answered with 404 in the meantime.
    schema: Option<DynamicSchema>,
    /// Runtime GraphQL operations are executed on, the proxy's runtime if `None`.
    runtime: Option<Handle>,
}

impl GraphQLHandler {
    pub fn new(schema: Option<DynamicSchema>, runtime: Option<Handle>) -> Self {
        Self { schema, runtime }
    }

    fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        match &self.runtime {
            Some(runtime) => runtime.spawn(future),
            None => tokio::spawn(future),
        }
    }

    async fn execute(&self, schema: DynamicSchema, req: Request<Body>) -> Response<Body> {
        let batch = match parse_request(req).await {
            Ok(batch) => batch,
            Err(e) => {
                let status = match e {
                    ParseRequestError::PayloadTooLarge => StatusCode::PAYLOAD_TOO_LARGE,
                    _ => StatusCode::BAD_REQUEST,
                };
                return Response::builder()
                    .status(status)
                    .body(Body::from(e.to_string()))
                    .unwrap();
            }
        };

        // Execute and serialize on the GraphQL runtime. The task is aborted if this future is
        // dropped, i.e. when the request times out or the connection is closed.
        let task = AbortOnDropHandle::new(
            self.spawn(async move { serde_json::to_vec(&schema.execute_batch(batch).await) }),
        );

        match task.await {
            Ok(Ok(body)) => Response::builder()
                .status(StatusCode::OK)
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(body))
                .unwrap(),
            Ok(Err(e)) => {
                error!(target: LOG_TARGET, error = ?e, "Failed to serialize GraphQL response.");
                empty_response(StatusCode::INTERNAL_SERVER_ERROR)
            }
            Err(e) => {
                error!(target: LOG_TARGET, error = ?e, "GraphQL execution task failed.");
                empty_response(StatusCode::INTERNAL_SERVER_ERROR)
            }
        }
    }

    fn upgrade(&self, schema: DynamicSchema, req: Request<Body>) -> Response<Body> {
        let offered_protocol = offered_protocol(&req);

        let (mut response, websocket) = match hyper_tungstenite::upgrade(req, None) {
            Ok(upgrade) => upgrade,
            Err(e) => {
                debug!(target: LOG_TARGET, error = ?e, "Invalid WebSocket upgrade request.");
                return Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .body(Body::from(e.to_string()))
                    .unwrap();
            }
        };

        // Only confirm a protocol the client offered, clients must fail the connection otherwise.
        // Clients that did not offer any are served the legacy protocol, like the async-graphql
        // integrations do.
        let protocol = match offered_protocol {
            Some(protocol) => {
                response.headers_mut().insert(
                    SEC_WEBSOCKET_PROTOCOL,
                    HeaderValue::from_static(protocol.sec_websocket_protocol()),
                );
                protocol
            }
            None => WebSocketProtocols::SubscriptionsTransportWS,
        };

        self.spawn(serve_websocket(schema, websocket, protocol));

        response
    }
}

impl std::fmt::Debug for GraphQLHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GraphQLHandler")
            .field("has_schema", &self.schema.is_some())
            .field("runtime", &self.runtime)
            .finish()
    }
}

#[async_trait::async_trait]
impl Handler for GraphQLHandler {
    fn should_handle(&self, req: &Request<Body>) -> bool {
        req.uri().path().starts_with("/graphql")
    }

    async fn handle(&self, req: Request<Body>, _client_addr: IpAddr) -> Response<Body> {
        let Some(schema) = self.schema.clone() else {
            return empty_response(StatusCode::NOT_FOUND);
        };

        if hyper_tungstenite::is_upgrade_request(&req) {
            return self.upgrade(schema, req);
        }

        match *req.method() {
            Method::GET if req.uri().query().is_none_or(str::is_empty) => Response::builder()
                .status(StatusCode::OK)
                .header(CONTENT_TYPE, "text/html; charset=utf-8")
                .body(Body::from(PLAYGROUND.as_str()))
                .unwrap(),
            Method::GET | Method::POST => {
                match timeout(GRAPHQL_REQUEST_TIMEOUT, self.execute(schema, req)).await {
                    Ok(response) => response,
                    Err(_) => {
                        error!(target: LOG_TARGET, "GraphQL request timeout after {:?}", GRAPHQL_REQUEST_TIMEOUT);
                        empty_response(StatusCode::GATEWAY_TIMEOUT)
                    }
                }
            }
            _ => Response::builder()
                .status(StatusCode::METHOD_NOT_ALLOWED)
                .header(ALLOW, "GET, POST")
                .body(Body::empty())
                .unwrap(),
        }
    }
}

fn empty_response(status: StatusCode) -> Response<Body> {
    Response::builder()
        .status(status)
        .body(Body::empty())
        .unwrap()
}

/// Reads a GraphQL request from the query string of a `GET` request, or from the body of a `POST`
/// request (JSON, or multipart as determined by its content type).
async fn parse_request(req: Request<Body>) -> Result<BatchRequest, ParseRequestError> {
    if req.method() == Method::GET {
        return parse_query_string(req.uri().query().unwrap_or_default()).map(Into::into);
    }

    let content_type = req
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(ToOwned::to_owned);
    let body = hyper::body::to_bytes(Limited::new(req.into_body(), MAX_REQUEST_BODY_SIZE))
        .await
        .map_err(|e| {
            if e.is::<LengthLimitError>() {
                ParseRequestError::PayloadTooLarge
            } else {
                ParseRequestError::Io(std::io::Error::other(e))
            }
        })?;

    receive_batch_body(content_type, body.as_ref(), MultipartOptions::default()).await
}

/// Returns the first GraphQL over WebSocket protocol offered by the client in its
/// `Sec-WebSocket-Protocol` headers.
fn offered_protocol(req: &Request<Body>) -> Option<WebSocketProtocols> {
    req.headers()
        .get_all(SEC_WEBSOCKET_PROTOCOL)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .find_map(|protocol| WebSocketProtocols::from_str(protocol.trim()).ok())
}

/// Serves GraphQL subscriptions over `websocket` once the upgrade completes, until either side
/// closes the connection.
async fn serve_websocket(
    schema: DynamicSchema,
    websocket: HyperWebsocket,
    protocol: WebSocketProtocols,
) {
    let websocket = match websocket.await {
        Ok(websocket) => websocket,
        Err(e) => {
            debug!(target: LOG_TARGET, error = ?e, "WebSocket upgrade failed.");
            return;
        }
    };
    let (sink, stream) = websocket.split();

    // Like the async-graphql integrations: the connection ends with the first read error, and only
    // data frames are passed on, as tungstenite answers ping and close frames itself.
    let messages = stream
        .take_while(|message| future::ready(message.is_ok()))
        .filter_map(|message| {
            future::ready(match message {
                Ok(Message::Text(text)) => Some(text.into_bytes()),
                Ok(Message::Binary(data)) => Some(data),
                _ => None,
            })
        });

    let replies = WebSocket::new(schema, messages, protocol).map(|message| {
        Ok::<_, WsError>(match message {
            WsMessage::Text(text) => Message::Text(text),
            WsMessage::Close(code, reason) => Message::Close(Some(CloseFrame {
                code: CloseCode::from(code),
                reason: reason.into(),
            })),
        })
    });

    if let Err(e) = replies.forward(sink).await {
        debug!(target: LOG_TARGET, error = ?e, "GraphQL WebSocket connection closed.");
    }
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;
    use std::net::{Ipv4Addr, SocketAddr};
    use std::sync::Arc;

    use async_graphql::dynamic::{
        Field, FieldFuture, FieldValue, Object, Schema, Subscription, SubscriptionField,
        SubscriptionFieldFuture, TypeRef,
    };
    use async_graphql::Value;
    use futures_util::SinkExt;
    use hyper::server::conn::AddrStream;
    use hyper::service::{make_service_fn, service_fn};
    use hyper::Server;
    use serde_json::json;
    use tokio::net::TcpStream;
    use tokio::sync::broadcast;
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

    use super::*;

    const WAIT_TIMEOUT: Duration = Duration::from_secs(10);

    const CLIENT_ADDR: IpAddr = IpAddr::V4(Ipv4Addr::LOCALHOST);

    /// A minimal schema: `version` returns `version`, `extra` only exists with `extra_field`,
    /// `thread` returns the name of the thread it is resolved on, and the `events` subscription
    /// streams the values sent on `events`.
    fn test_schema(version: i32, extra_field: bool, events: broadcast::Sender<i32>) -> Schema {
        let mut query = Object::new("Query")
            .field(Field::new(
                "version",
                TypeRef::named_nn(TypeRef::INT),
                move |_| FieldFuture::new(async move { Ok(Some(Value::from(version))) }),
            ))
            .field(Field::new(
                "thread",
                TypeRef::named_nn(TypeRef::STRING),
                |_| {
                    FieldFuture::new(async {
                        let thread = std::thread::current().name().unwrap_or_default().to_owned();
                        Ok(Some(Value::from(thread)))
                    })
                },
            ));
        if extra_field {
            query = query.field(Field::new(
                "extra",
                TypeRef::named_nn(TypeRef::STRING),
                |_| FieldFuture::new(async { Ok(Some(Value::from("hello"))) }),
            ));
        }

        let subscription = Subscription::new("Subscription").field(SubscriptionField::new(
            "events",
            TypeRef::named_nn(TypeRef::INT),
            move |_| {
                let events = events.subscribe();
                SubscriptionFieldFuture::new(async move {
                    Ok(futures_util::stream::unfold(
                        events,
                        |mut events| async move {
                            let event = events.recv().await.ok()?;
                            Some((Ok(FieldValue::value(event)), events))
                        },
                    ))
                })
            },
        ));

        Schema::build("Query", None, Some("Subscription"))
            .register(query)
            .register(subscription)
            .finish()
            .unwrap()
    }

    fn handler(schema: Schema) -> GraphQLHandler {
        GraphQLHandler::new(Some(DynamicSchema::new(schema)), None)
    }

    fn post(body: &str) -> Request<Body> {
        Request::post("/graphql")
            .header(CONTENT_TYPE, "application/json")
            .body(Body::from(body.to_owned()))
            .unwrap()
    }

    async fn json_body(response: Response<Body>) -> serde_json::Value {
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()[CONTENT_TYPE], "application/json");
        let body = hyper::body::to_bytes(response.into_body()).await.unwrap();
        serde_json::from_slice(&body).unwrap()
    }

    #[tokio::test]
    async fn test_post_query() {
        let (events, _) = broadcast::channel(1);
        let handler = handler(test_schema(1, false, events));

        let response = handler
            .handle(post(r#"{"query":"{ version }"}"#), CLIENT_ADDR)
            .await;

        assert_eq!(
            json_body(response).await,
            json!({ "data": { "version": 1 } })
        );
    }

    #[tokio::test]
    async fn test_post_batch_query() {
        let (events, _) = broadcast::channel(1);
        let handler = handler(test_schema(1, true, events));

        let response = handler
            .handle(
                post(r#"[{"query":"{ version }"},{"query":"{ extra }"}]"#),
                CLIENT_ADDR,
            )
            .await;

        assert_eq!(
            json_body(response).await,
            json!([{ "data": { "version": 1 } }, { "data": { "extra": "hello" } }])
        );
    }

    #[tokio::test]
    async fn test_get_query() {
        let (events, _) = broadcast::channel(1);
        let handler = handler(test_schema(1, false, events));

        let request = Request::get("/graphql?query=%7B%20version%20%7D")
            .body(Body::empty())
            .unwrap();
        let response = handler.handle(request, CLIENT_ADDR).await;

        assert_eq!(
            json_body(response).await,
            json!({ "data": { "version": 1 } })
        );
    }

    #[tokio::test]
    async fn test_get_without_query_serves_playground() {
        let (events, _) = broadcast::channel(1);
        let handler = handler(test_schema(1, false, events));

        let request = Request::get("/graphql").body(Body::empty()).unwrap();
        let response = handler.handle(request, CLIENT_ADDR).await;

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()[CONTENT_TYPE], "text/html; charset=utf-8");
        let body = hyper::body::to_bytes(response.into_body()).await.unwrap();
        let body = std::str::from_utf8(&body).unwrap();
        assert!(body.contains("<title>Torii GraphQL Playground</title>"));
        // Queries and subscriptions go to the URL the playground is served from.
        assert!(body.contains("url: createUrl(''),"));
        assert!(body.contains("subscriptionUrl: createUrl('', true),"));
    }

    #[tokio::test]
    async fn test_invalid_requests_are_rejected() {
        let (events, _) = broadcast::channel(1);
        let handler = handler(test_schema(1, false, events));

        let response = handler.handle(post("{ version }"), CLIENT_ADDR).await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let request = Request::put("/graphql").body(Body::empty()).unwrap();
        let response = handler.handle(request, CLIENT_ADDR).await;
        assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);

        let response = GraphQLHandler::new(None, None)
            .handle(post(r#"{"query":"{ version }"}"#), CLIENT_ADDR)
            .await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);

        let oversized = format!(
            r#"{{"query":"{{ version }}","variables":{{"padding":"{}"}}}}"#,
            "x".repeat(MAX_REQUEST_BODY_SIZE)
        );
        let response = handler.handle(post(&oversized), CLIENT_ADDR).await;
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    }

    #[tokio::test]
    async fn test_swapped_schema_is_served_by_same_handler() {
        let (events, _) = broadcast::channel(1);
        let schema = DynamicSchema::new(test_schema(1, false, events.clone()));
        let handler = GraphQLHandler::new(Some(schema.clone()), None);
        let query = r#"{"query":"{ version extra }"}"#;

        let response = json_body(handler.handle(post(query), CLIENT_ADDR).await).await;
        assert!(
            response["errors"][0]["message"]
                .as_str()
                .unwrap()
                .contains("extra"),
            "unexpected response: {response}"
        );

        schema.swap(test_schema(2, true, events));

        let response = json_body(handler.handle(post(query), CLIENT_ADDR).await).await;
        assert_eq!(
            response,
            json!({ "data": { "version": 2, "extra": "hello" } })
        );
    }

    #[tokio::test]
    async fn test_operations_run_on_given_runtime() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .thread_name("graphql-test-runtime")
            .enable_all()
            .build()
            .unwrap();
        let (events, _) = broadcast::channel(1);
        let handler = GraphQLHandler::new(
            Some(DynamicSchema::new(test_schema(1, false, events))),
            Some(runtime.handle().clone()),
        );

        let response = handler
            .handle(post(r#"{"query":"{ thread }"}"#), CLIENT_ADDR)
            .await;

        assert_eq!(
            json_body(response).await,
            json!({ "data": { "thread": "graphql-test-runtime" } })
        );
        runtime.shutdown_background();
    }

    /// Serves `handler` over HTTP on an ephemeral local port.
    fn serve(handler: GraphQLHandler) -> SocketAddr {
        let handler = Arc::new(handler);
        let make_service = make_service_fn(move |conn: &AddrStream| {
            let handler = handler.clone();
            let client_addr = conn.remote_addr().ip();
            async move {
                Ok::<_, Infallible>(service_fn(move |req| {
                    let handler = handler.clone();
                    async move { Ok::<_, Infallible>(handler.handle(req, client_addr).await) }
                }))
            }
        });

        let server = Server::bind(&SocketAddr::from((Ipv4Addr::LOCALHOST, 0))).serve(make_service);
        let addr = server.local_addr();
        tokio::spawn(server);
        addr
    }

    type Client = WebSocketStream<MaybeTlsStream<TcpStream>>;

    async fn connect(addr: SocketAddr, protocol: &'static str) -> Client {
        let mut request = format!("ws://{addr}/graphql")
            .into_client_request()
            .unwrap();
        request
            .headers_mut()
            .insert(SEC_WEBSOCKET_PROTOCOL, HeaderValue::from_static(protocol));

        let (client, response) = tokio_tungstenite::connect_async(request).await.unwrap();
        assert_eq!(response.headers()[SEC_WEBSOCKET_PROTOCOL], protocol);
        client
    }

    async fn send(client: &mut Client, message: serde_json::Value) {
        client
            .send(Message::Text(message.to_string()))
            .await
            .unwrap();
    }

    async fn receive(client: &mut Client) -> serde_json::Value {
        let message = timeout(WAIT_TIMEOUT, client.next())
            .await
            .expect("timed out waiting for a message")
            .unwrap()
            .unwrap();
        serde_json::from_str(message.to_text().unwrap()).unwrap()
    }

    async fn wait_for_subscribers(events: &broadcast::Sender<i32>, count: usize) {
        timeout(WAIT_TIMEOUT, async {
            while events.receiver_count() != count {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("expected {count} running subscriptions"));
    }

    /// Initializes the connection, subscribes to `events` with a `subscribe_type` message and
    /// returns the message carrying an event published once the subscription is running.
    async fn subscribe_and_receive_event(
        client: &mut Client,
        events: &broadcast::Sender<i32>,
        subscribe_type: &str,
    ) -> serde_json::Value {
        send(client, json!({ "type": "connection_init" })).await;
        assert_eq!(receive(client).await, json!({ "type": "connection_ack" }));

        send(
            client,
            json!({
                "id": "1",
                "type": subscribe_type,
                "payload": { "query": "subscription { events }" },
            }),
        )
        .await;
        wait_for_subscribers(events, 1).await;
        events.send(42).unwrap();

        receive(client).await
    }

    #[tokio::test]
    async fn test_subscription_over_graphql_transport_ws() {
        let (events, _) = broadcast::channel(16);
        let addr = serve(handler(test_schema(1, false, events.clone())));
        let mut client = connect(addr, "graphql-transport-ws").await;

        let message = subscribe_and_receive_event(&mut client, &events, "subscribe").await;
        assert_eq!(
            message,
            json!({ "type": "next", "id": "1", "payload": { "data": { "events": 42 } } })
        );

        send(&mut client, json!({ "type": "ping" })).await;
        assert_eq!(receive(&mut client).await, json!({ "type": "pong" }));

        // Closing the connection drops its subscriptions.
        client.close(None).await.unwrap();
        wait_for_subscribers(&events, 0).await;
    }

    #[tokio::test]
    async fn test_subscription_over_graphql_ws() {
        let (events, _) = broadcast::channel(16);
        let addr = serve(handler(test_schema(1, false, events.clone())));
        let mut client = connect(addr, "graphql-ws").await;

        let message = subscribe_and_receive_event(&mut client, &events, "start").await;
        assert_eq!(
            message,
            json!({ "type": "data", "id": "1", "payload": { "data": { "events": 42 } } })
        );

        send(&mut client, json!({ "id": "1", "type": "stop" })).await;
        assert_eq!(
            receive(&mut client).await,
            json!({ "type": "complete", "id": "1" })
        );
        wait_for_subscribers(&events, 0).await;
    }

    #[tokio::test]
    async fn test_websocket_protocol_negotiation() {
        let (events, _) = broadcast::channel(1);
        let addr = serve(handler(test_schema(1, false, events)));

        // The first supported protocol offered by the client is picked.
        let mut request = format!("ws://{addr}/graphql")
            .into_client_request()
            .unwrap();
        request.headers_mut().insert(
            SEC_WEBSOCKET_PROTOCOL,
            HeaderValue::from_static("unknown, graphql-transport-ws, graphql-ws"),
        );
        let (_, response) = tokio_tungstenite::connect_async(request).await.unwrap();
        assert_eq!(
            response.headers()[SEC_WEBSOCKET_PROTOCOL],
            "graphql-transport-ws"
        );

        // No protocol is confirmed to clients that did not offer a supported one.
        let request = format!("ws://{addr}/graphql")
            .into_client_request()
            .unwrap();
        let (_, response) = tokio_tungstenite::connect_async(request).await.unwrap();
        assert!(response.headers().get(SEC_WEBSOCKET_PROTOCOL).is_none());
    }
}
