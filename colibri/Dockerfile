FROM rust:latest AS builder

RUN apt-get update && apt-get install -y \
    libclang-dev cmake pkg-config libssl-dev git protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Build context is the parent directory (/Users/newuser/RustroverProjects/)
# which contains both Colibri/ and Deshreder/ repos.

# Copy the Deshreder library (deshredder dep, built from the local checkout)
COPY Deshreder ./Deshreder

# Copy workspace files from Colibri/
COPY Colibri/Cargo.toml Colibri/Cargo.lock ./
COPY Colibri/colibri ./colibri
COPY Colibri/shred-net ./shred-net

# Build deshredder from the local copy instead of fetching the git dep
RUN sed -i 's|deshredder = { git = ".*" }|deshredder = { path = "../Deshreder" }|' colibri/Cargo.toml

RUN cargo build --release -p colibri --jobs 4

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y libssl3 ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /build/target/release/colibri .
CMD ["./colibri"]
