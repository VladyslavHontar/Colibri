FROM rust:bookworm AS builder

RUN apt-get update && apt-get install -y \
    libclang-dev cmake pkg-config libssl-dev git protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Colibri's own workspace only. `deshredder` is a PUBLIC git dependency, fetched
# during the build — no local Deshreder checkout, no auth needed.
COPY Cargo.toml Cargo.lock ./
COPY colibri ./colibri
COPY shred-net ./shred-net

RUN cargo build --release -p colibri --jobs 4

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y libssl3 ca-certificates openssl \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /build/target/release/colibri .
COPY colibri/docker-entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
