//! The gRPC service: three streams of what the assembler emits, and one of
//! what gossip says each node runs.

use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex,
    },
};

use tokio::sync::broadcast::{self, error::RecvError, Sender};
use tonic::{codegen::tokio_stream::wrappers::ReceiverStream, transport::Server, Request, Response, Status};

pub mod shredstream {
    tonic::include_proto!("shredstream");
}

use shredstream::{
    shredstream_proxy_server::{ShredstreamProxy, ShredstreamProxyServer},
    SubscribeEntriesRequest, SubscribeFootersRequest, SubscribeNodesRequest, SubscribeTransactionsRequest,
};
pub use shredstream::{Entry as ProtoEntry, Footer as ProtoFooter, Node as ProtoNode, Nodes as ProtoNodes, Transaction as ProtoTransaction};

/// What gossip says each node runs, and the subscribers told of every change.
pub struct NodeBook {
    /// The lock also orders a subscription against an update: a subscriber
    /// takes its snapshot and its place in `changes` under it, so no change
    /// falls between the two.
    known:   Mutex<HashMap<Vec<u8>, ProtoNode>>,
    changes: Sender<ProtoNodes>,
}

impl NodeBook {
    pub fn new() -> Self {
        // A change is a handful of nodes every few seconds: 256 is minutes of them.
        Self { known: Mutex::new(HashMap::new()), changes: broadcast::channel(256).0 }
    }

    /// Take all that gossip holds now. The nodes that are new or run something
    /// else than before, and those gossip no longer holds, are sent to the
    /// subscribers; returns how many of each. The book is what gossip holds
    /// and no more: identities cost nothing to make, and one that kept every
    /// node it ever saw could be grown without end.
    pub fn update(&self, nodes: impl IntoIterator<Item = ProtoNode>) -> (usize, usize) {
        let now: HashMap<Vec<u8>, ProtoNode> = nodes.into_iter().map(|n| (n.identity.clone(), n)).collect();
        let mut known = self.known.lock().unwrap_or_else(|e| e.into_inner());
        let changed: Vec<ProtoNode> = now.values().filter(|n| known.get(&n.identity) != Some(n)).cloned().collect();
        let gone: Vec<Vec<u8>> = known.keys().filter(|identity| !now.contains_key(*identity)).cloned().collect();
        *known = now;
        let counts = (changed.len(), gone.len());
        if counts != (0, 0) { let _ = self.changes.send(ProtoNodes { nodes: changed, gone }); }
        counts
    }

    fn subscribe(&self) -> (ProtoNodes, broadcast::Receiver<ProtoNodes>) {
        let known = self.known.lock().unwrap_or_else(|e| e.into_inner());
        (ProtoNodes { nodes: known.values().cloned().collect(), gone: Vec::new() }, self.changes.subscribe())
    }
}

/// Everything the service serves. One struct because they are always created
/// and passed together.
pub struct Streams {
    pub entries: Arc<Sender<ProtoEntry>>,
    pub txs:     Arc<Sender<ProtoTransaction>>,
    pub footers: Arc<Sender<ProtoFooter>>,
    pub nodes:   Arc<NodeBook>,
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
    type SubscribeNodesStream = Stream<ProtoNodes>;

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

    async fn subscribe_nodes(&self, request: Request<SubscribeNodesRequest>) -> Result<Response<Self::SubscribeNodesStream>, Status> {
        check_auth(&self.auth_token, &request)?;
        let (known, changes) = self.streams.nodes.subscribe();
        Ok(forward(Some(known), changes, 256, "node changes"))
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

#[cfg(test)]
mod tests {
    use super::*;

    fn node(id: u8, client: &str) -> ProtoNode {
        ProtoNode { identity: vec![id; 32], client: client.into(), version: "4.3.0".into() }
    }

    #[test]
    fn a_subscriber_gets_all_that_is_known_and_then_only_what_changes() {
        let book = NodeBook::new();
        assert_eq!(book.update([node(1, "Agave"), node(2, "JitoLabs")]), (2, 0));
        let (known, mut changes) = book.subscribe();
        assert_eq!(known.nodes.len(), 2);
        // The same again is no change; a node that runs something else is, and so is a new one.
        assert_eq!(book.update([node(1, "Agave"), node(2, "JitoLabs")]), (0, 0));
        assert!(changes.try_recv().is_err());
        assert_eq!(book.update([node(1, "Agave"), node(2, "AgaveBam"), node(3, "Firedancer")]), (2, 0));
        let mut got: Vec<_> = changes.try_recv().unwrap().nodes.into_iter().map(|n| (n.identity[0], n.client)).collect();
        got.sort();
        assert_eq!(got, [(2, "AgaveBam".to_string()), (3, "Firedancer".to_string())]);
        // A node gossip dropped is told as gone and is no longer held.
        assert_eq!(book.update([node(2, "AgaveBam"), node(3, "Firedancer")]), (0, 1));
        assert_eq!(changes.try_recv().unwrap().gone, [vec![1u8; 32]]);
        assert_eq!(book.subscribe().0.nodes.len(), 2);
    }
}
