use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use tokio::sync::broadcast::Sender;
use tonic::{codegen::tokio_stream::wrappers::ReceiverStream, transport::Server, Request, Response, Status};

pub mod shredstream {
    tonic::include_proto!("shredstream");
}

use shredstream::{
    shredstream_proxy_server::{ShredstreamProxy, ShredstreamProxyServer},
    Entry, SubscribeEntriesRequest, Transaction, SubscribeTransactionsRequest,
};

pub struct ColibriGrpcService {
    entry_sender: Arc<Sender<Entry>>,
    tx_sender:    Arc<Sender<Transaction>>,
    auth_token:   Option<Arc<String>>,
}

fn check_auth(auth_token: &Option<Arc<String>>, request_metadata: &tonic::metadata::MetadataMap) -> Result<(), Status> {
    if let Some(expected) = auth_token {
        let provided = request_metadata
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "))
            .unwrap_or("");

        use subtle::ConstantTimeEq;
        let provided_b = provided.as_bytes();
        let expected_b = expected.as_bytes();
        let ok = provided_b.len() == expected_b.len()
            && provided_b.ct_eq(expected_b).into();
        if !ok {
            return Err(Status::unauthenticated("invalid token"));
        }
    }
    Ok(())
}

#[tonic::async_trait]
impl ShredstreamProxy for ColibriGrpcService {
    type SubscribeEntriesStream = ReceiverStream<Result<Entry, Status>>;
    type SubscribeTransactionsStream = ReceiverStream<Result<Transaction, Status>>;

    async fn subscribe_entries(
        &self,
        request: Request<SubscribeEntriesRequest>,
    ) -> Result<Response<Self::SubscribeEntriesStream>, Status> {
        check_auth(&self.auth_token, request.metadata())?;

        let (tx, rx) = tokio::sync::mpsc::channel(256);
        let mut bcast = self.entry_sender.subscribe();

        tokio::spawn(async move {
            loop {
                match bcast.recv().await {
                    Ok(entry) => {
                        match tx.try_send(Ok(entry)) {
                            Ok(()) => {}
                            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                                eprintln!("[grpc] entries subscriber too slow, disconnecting");
                                break;
                            }
                            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => break,
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        eprintln!("[grpc] entries subscriber lagged by {n}, disconnecting");
                        let _ = tx.try_send(Err(Status::data_loss(
                            format!("stream lagged: {n} entries dropped"),
                        )));
                        break;
                    }
                    Err(_) => break,
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn subscribe_transactions(
        &self,
        request: Request<SubscribeTransactionsRequest>,
    ) -> Result<Response<Self::SubscribeTransactionsStream>, Status> {
        check_auth(&self.auth_token, request.metadata())?;

        let (tx, rx) = tokio::sync::mpsc::channel(1_024);
        let mut bcast = self.tx_sender.subscribe();

        tokio::spawn(async move {
            loop {
                match bcast.recv().await {
                    Ok(transaction) => {
                        match tx.try_send(Ok(transaction)) {
                            Ok(()) => {}
                            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                                eprintln!("[grpc] txs subscriber too slow, disconnecting");
                                break;
                            }
                            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => break,
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        eprintln!("[grpc] txs subscriber lagged by {n}, disconnecting");
                        let _ = tx.try_send(Err(Status::data_loss(
                            format!("stream lagged: {n} transactions dropped"),
                        )));
                        break;
                    }
                    Err(_) => break,
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

pub fn start_grpc_server(
    addr:         SocketAddr,
    entry_sender: Arc<Sender<Entry>>,
    tx_sender:    Arc<Sender<Transaction>>,
    auth_token:   Option<String>,
    tls_cert:     Option<String>,
    tls_key:      Option<String>,
    exit:         Arc<AtomicBool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        eprintln!("[grpc] listening on {addr}");

        let svc = ColibriGrpcService {
            entry_sender,
            tx_sender,
            auth_token: auth_token.map(Arc::new),
        };

        let shutdown = async move {
            loop {
                if exit.load(Ordering::Relaxed) { break; }
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        };

        if let (Some(cert_path), Some(key_path)) = (tls_cert, tls_key) {
            let cert = tokio::fs::read(&cert_path).await
                .unwrap_or_else(|e| panic!("cannot read TLS cert {cert_path:?}: {e}"));
            let key = tokio::fs::read(&key_path).await
                .unwrap_or_else(|e| panic!("cannot read TLS key {key_path:?}: {e}"));
            let tls = tonic::transport::ServerTlsConfig::new()
                .identity(tonic::transport::Identity::from_pem(cert, key));
            let result = Server::builder()
                .tls_config(tls)
                .expect("tls config error")
                .add_service(ShredstreamProxyServer::new(svc))
                .serve_with_shutdown(addr, shutdown)
                .await;
            if let Err(e) = result {
                eprintln!("[grpc] server error: {e}");
            }
        } else {
            let result = Server::builder()
                .add_service(ShredstreamProxyServer::new(svc))
                .serve_with_shutdown(addr, shutdown)
                .await;
            if let Err(e) = result {
                eprintln!("[grpc] server error: {e}");
            }
        }
    })
}

pub use shredstream::Entry as ProtoEntry;
pub use shredstream::Transaction as ProtoTransaction;
