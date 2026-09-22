//! Ledger write path: gRPC commands, operational HTTP and outbox relay.

use ironledger_api_grpc::proto::{
    ledger_admin_service_server::LedgerAdminServiceServer,
    ledger_query_service_server::LedgerQueryServiceServer,
    ledger_write_service_server::LedgerWriteServiceServer,
};
use ironledger_api_grpc::GrpcServices;
use ironledger_api_http::auth::{AllowAllAuthorizer, DevTokenAuthorizer};
use ironledger_api_http::{install_metrics, router, HttpState};
use ironledger_event_bus::{BusConfig, KafkaPublisher, OutboxRelay};
use ironledger_ledger::LedgerService;
use ironledger_reconciler::Reconciler;
use ironledger_storage_postgres::{PostgresBalanceViews, PostgresHistory, PostgresStore};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::signal;
use tokio_util::sync::CancellationToken;
use tonic::transport::Server;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .json()
        .init();

    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://ironledger:ironledger@127.0.0.1:5432/ironledger".into());
    let bus = BusConfig::from_env()?;
    let store = Arc::new(PostgresStore::connect(&database_url, bus.ledger_topic.clone()).await?);
    store.migrate().await?;

    let ledger = Arc::new(LedgerService::new(
        store.clone(),
        store.clone(),
        store.clone(),
        store.clone(),
        store.clone(),
    ));
    let history = PostgresHistory::new(store.pool().clone());
    let views = PostgresBalanceViews::new(store.pool().clone());
    let reconciler = Arc::new(Reconciler::new(history, views));

    let metrics = install_metrics();
    let auth: Arc<dyn ironledger_api_http::auth::Authorizer> =
        match std::env::var("IRONLEDGER_AUTH_MODE").as_deref() {
            Ok("dev") => Arc::new(DevTokenAuthorizer::from_env()),
            _ => Arc::new(AllowAllAuthorizer),
        };

    let http_state = Arc::new(HttpState {
        ledger: ledger.clone(),
        store: store.clone(),
        reconciler: reconciler.clone(),
        metrics,
        auth,
    });

    let grpc = GrpcServices::new(ledger.clone(), reconciler);

    let http_listen: SocketAddr = std::env::var("HTTP_LISTEN")
        .unwrap_or_else(|_| "0.0.0.0:8080".into())
        .parse()?;
    let grpc_listen: SocketAddr = std::env::var("GRPC_LISTEN")
        .unwrap_or_else(|_| "0.0.0.0:50051".into())
        .parse()?;

    let cancel = CancellationToken::new();
    let shutdown = cancel.clone();
    tokio::spawn(async move {
        let _ = signal::ctrl_c().await;
        shutdown.cancel();
    });

    let publisher = KafkaPublisher::new(&bus)?;
    let relay = OutboxRelay::new(
        store.clone(),
        publisher,
        50,
        std::time::Duration::from_secs(30),
        std::time::Duration::from_millis(500),
    );
    let relay_cancel = cancel.clone();
    tokio::spawn(async move {
        relay.run(relay_cancel).await;
    });

    let http_app = router(http_state);
    let http_server = axum::serve(tokio::net::TcpListener::bind(http_listen).await?, http_app);

    let grpc_server = Server::builder()
        .add_service(LedgerWriteServiceServer::new(grpc.clone()))
        .add_service(LedgerQueryServiceServer::new(grpc.clone()))
        .add_service(LedgerAdminServiceServer::new(grpc))
        .serve_with_shutdown(grpc_listen, async move {
            cancel.cancelled().await;
        });

    tokio::select! {
        result = http_server => result?,
        result = grpc_server => result?,
    }
    Ok(())
}
