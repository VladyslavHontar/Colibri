//! Subscribe to a Colibri gRPC transaction stream.
//!
//! Usage:
//!   cargo run --example subscribe -- [OPTIONS]
//!
//! Options:
//!   --url <URL>        Colibri gRPC endpoint (default: http://127.0.0.1:8888)
//!   --token <TOKEN>    Bearer auth token (optional)

use {
    std::{env, time::Instant},
    tonic::{metadata::MetadataValue, transport::Channel, Request},
};

mod shredstream {
    tonic::include_proto!("shredstream");
}

use shredstream::{shredstream_proxy_client::ShredstreamProxyClient, SubscribeTransactionsRequest};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    let mut url = "http://127.0.0.1:8888".to_string();
    let mut token: Option<String> = None;

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--url" => { i += 1; url = args[i].clone(); }
            "--token" => { i += 1; token = Some(args[i].clone()); }
            _ => {}
        }
        i += 1;
    }

    eprintln!("[subscribe] connecting to {url}");

    let channel = Channel::from_shared(url)?.connect().await?;
    let mut client = ShredstreamProxyClient::new(channel);

    let mut request = Request::new(SubscribeTransactionsRequest {});
    if let Some(tok) = &token {
        let val: MetadataValue<_> = format!("Bearer {tok}").parse()?;
        request.metadata_mut().insert("authorization", val);
    }

    let mut stream = client.subscribe_transactions(request).await?.into_inner();

    eprintln!("[subscribe] streaming transactions...");

    let start = Instant::now();
    let mut total_txs: u64 = 0;
    let mut last_report = Instant::now();

    while let Some(tx) = stream.message().await? {
        total_txs += 1;

        println!(
            "slot={} sig={} raw_tx={}B",
            tx.slot, tx.signature, tx.raw_tx.len(),
        );

        if last_report.elapsed().as_secs() >= 5 {
            let elapsed = start.elapsed().as_secs();
            let tps = if elapsed > 0 { total_txs / elapsed } else { 0 };
            eprintln!("[subscribe] t={}s  txs={}  (~{} tx/s)", elapsed, total_txs, tps);
            last_report = Instant::now();
        }
    }

    eprintln!("[subscribe] stream ended. txs={total_txs}");
    Ok(())
}
