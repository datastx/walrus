FROM rust:1.85-bookworm AS builder

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY crates/ crates/

RUN cargo build --release --workspace

# --- WAL Capture ---
FROM debian:bookworm-slim AS wal-capture

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates libssl3 && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/pgiceberg-wal-capture /usr/local/bin/
COPY config/pgiceberg.toml /etc/pgiceberg/pgiceberg.toml

EXPOSE 8081
ENTRYPOINT ["pgiceberg-wal-capture"]

# --- Iceberg Writer ---
FROM debian:bookworm-slim AS iceberg-writer

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates libssl3 && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/pgiceberg-iceberg-writer /usr/local/bin/
COPY config/pgiceberg.toml /etc/pgiceberg/pgiceberg.toml

EXPOSE 8082
ENTRYPOINT ["pgiceberg-iceberg-writer"]
