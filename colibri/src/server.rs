//! The gRPC service: three streams of what the assembler emits.

use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
};

use tokio::sync::broadcast::{self, error::RecvError, Sender};
use tonic::{codegen::tokio_stream::wrappers::ReceiverStream, transport::Server, Request, Response, Status};

pub mod shredstream {
    tonic::include_proto!("shredstream");
}

use shredstream::{
    shredstream_proxy_server::{ShredstreamProxy, ShredstreamProxyServer},
    SubscribeEntriesRequest, SubscribeFootersRequest, SubscribeTransactionsRequest,
};
pub use shredstream::{Entry as ProtoEntry, Footer as ProtoFooter, Transaction as ProtoTransaction};

/// Everything the service serves. One struct because they are always created
/// and passed together.
pub struct Streams {
    pub entries: Arc<Sender<ProtoEntry>>,
    pub txs:     Arc<Sender<ProtoTransaction>>,
    pub footers: Arc<Sender<ProtoFooter>>,
}

struct ColibriGrpcService {
    streams:    Streams,
    auth_token: Option<Arc<String>>,
    /// Lowest slot any subscriber asked to repair from (`from-slot` metadata).
    /// 0 = none yet (the frontier loop falls back to `--depth`).
    requested_from: Arc<AtomicU64>,
}

fn check_auth<T>(auth_token: &Option<Arc<String>>, request: &Request<T>) -> Result<(), Status> {
    let Some(expected) = auth_token else { return Ok(()) };
    let provided = request.metadata()
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .unwrap_or("");
    use subtle::ConstantTimeEq;
    let ok = provided.len() == expected.len() && bool::from(provided.as_bytes().ct_eq(expected.as_bytes()));
    if ok { Ok(()) } else { Err(Status::unauthenticated("invalid token")) }
}

type Stream<T> = ReceiverStream<Result<T, Status>>;

/// A subscriber's stream: `first`, if any, then everything broadcast from now
/// on, `queue` messages deep.
///
/// A slow subscriber is waited for: the backlog flushed after a catch-up
/// overran the queue and cut a healthy consumer off (2026-10-19). One that
/// falls behind the broadcast buffer itself is told what it lost and dropped.
fn forward<T: Clone + Send + 'static>(first: Option<T>, mut bcast: broadcast::Receiver<T>, queue: usize, what: &'static str) -> Response<Stream<T>> {
    let (tx, rx) = tokio::sync::mpsc::channel(queue);
    tokio::spawn(async move {
        if let Some(first) = first
            && tx.send(Ok(first)).await.is_err() { return; }
        loop {
            match bcast.recv().await {
                Ok(message) => if tx.send(Ok(message)).await.is_err() { break },
                Err(RecvError::Lagged(n)) => {
                    eprintln!("[grpc] {what} subscriber lagged by {n}, disconnecting");
                    let _ = tx.send(Err(Status::data_loss(format!("stream lagged: {n} {what} dropped")))).await;
                    break;
                }
                Err(RecvError::Closed) => break,
            }
        }
    });
    Response::new(ReceiverStream::new(rx))
}

#[tonic::async_trait]
impl ShredstreamProxy for ColibriGrpcService {
    type SubscribeEntriesStream = Stream<ProtoEntry>;
    type SubscribeTransactionsStream = Stream<ProtoTransaction>;
    type SubscribeFootersStream = Stream<ProtoFooter>;

    async fn subscribe_entries(&self, request: Request<SubscribeEntriesRequest>) -> Result<Response<Self::SubscribeEntriesStream>, Status> {
        check_auth(&self.auth_token, &request)?;
        // `from-slot`: the consumer's replay frontier — repair from here upward.
        // Keep the lowest any subscriber asked for (0 = none → --depth fallback).
        if let Some(from) = request.metadata().get("from-slot").and_then(|v| v.to_str().ok()).and_then(|v| v.parse::<u64>().ok()).filter(|&f| f > 0) {
            let _ = self.requested_from.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |cur| (cur == 0 || from < cur).then_some(from));
        }
        Ok(forward(None, self.streams.entries.subscribe(), 256, "entries"))
    }

    async fn subscribe_transactions(&self, request: Request<SubscribeTransactionsRequest>) -> Result<Response<Self::SubscribeTransactionsStream>, Status> {
        check_auth(&self.auth_token, &request)?;
        Ok(forward(None, self.streams.txs.subscribe(), 1_024, "transactions"))
    }

    async fn subscribe_footers(&self, request: Request<SubscribeFootersRequest>) -> Result<Response<Self::SubscribeFootersStream>, Status> {
        check_auth(&self.auth_token, &request)?;
        Ok(forward(None, self.streams.footers.subscribe(), 256, "footers"))
    }
}

pub fn start_grpc_server(
    addr:           SocketAddr,
    streams:        Streams,
    auth_token:     Option<String>,
    tls:            Option<(String, String)>,
    requested_from: Arc<AtomicU64>,
    exit:           Arc<AtomicBool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        eprintln!("[grpc] listening on {addr}");
        let svc = ColibriGrpcService { streams, auth_token: auth_token.map(Arc::new), requested_from };
        let shutdown = async move {
            while !exit.load(Ordering::Relaxed) {
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        };
        let mut server = Server::builder();
        if let Some((cert_path, key_path)) = tls {
            let cert = tokio::fs::read(&cert_path).await.unwrap_or_else(|e| panic!("cannot read TLS cert {cert_path:?}: {e}"));
            let key = tokio::fs::read(&key_path).await.unwrap_or_else(|e| panic!("cannot read TLS key {key_path:?}: {e}"));
            let config = tonic::transport::ServerTlsConfig::new().identity(tonic::transport::Identity::from_pem(cert, key));
            server = server.tls_config(config).expect("tls config error");
        }
        if let Err(e) = server.add_service(ShredstreamProxyServer::new(svc)).serve_with_shutdown(addr, shutdown).await {
            eprintln!("[grpc] server error: {e}");
        }
    })
}
