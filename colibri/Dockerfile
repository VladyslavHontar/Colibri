# syntax=docker/dockerfile:1.4
FROM rust:latest AS builder

RUN apt-get update && apt-get install -y \
    libclang-dev cmake pkg-config libssl-dev git protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Colibri's own workspace only. `deshredder` is a PRIVATE git dependency,
# fetched during the build using the `github_token` BuildKit secret — no local
# Deshreder checkout, and the token never lands in an image layer.
COPY Cargo.toml Cargo.lock ./
COPY colibri ./colibri
COPY shred-net ./shred-net

# Fetch the private deshredder dep with the token exposed ONLY through a git
# credential helper that reads the tmpfs-mounted secret at fetch time. The
# stored config holds the helper (a path reference), never the token value, so
# the secret never lands in a build layer. Cargo uses the git CLI so the helper
# is honoured.
ENV CARGO_NET_GIT_FETCH_WITH_CLI=true
RUN --mount=type=secret,id=github_token \
    git config --global credential.helper \
        '!f() { echo username=x-access-token; echo "password=$(cat /run/secrets/github_token)"; }; f' \
    && cargo build --release -p colibri --jobs 4

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y libssl3 ca-certificates openssl \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /build/target/release/colibri .
COPY colibri/docker-entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
