# Build stage
FROM rust:1-slim-bookworm AS builder
WORKDIR /app

# Cache dependency compilation: build with a stub main first.
COPY Cargo.toml Cargo.lock ./
RUN mkdir -p src/bin \
    && echo "fn main() {}" > src/bin/collect_spotify_data.rs \
    && echo "" > src/lib.rs \
    && cargo build --release --bin collect-spotify-data \
    && rm -rf src

COPY src ./src
# Touch sources so cargo rebuilds them over the stub artifacts.
RUN touch src/lib.rs src/bin/collect_spotify_data.rs \
    && cargo build --release --bin collect-spotify-data

# Runtime stage
FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates tzdata \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /app/target/release/collect-spotify-data /usr/local/bin/collect-spotify-data

# Data volume is mounted by compose; keep a default dir for bare runs.
RUN mkdir -p /app/data
ENV DATA_DIR=/app/data

CMD ["collect-spotify-data"]
