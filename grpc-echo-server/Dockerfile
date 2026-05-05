# Build stage
FROM rust:1.85 AS builder

WORKDIR /workspace

# Copy the full workspace so cargo can resolve all members and the lockfile
COPY . .

RUN cargo build --release -p grpc-echo-server

# Runtime stage
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /workspace/target/release/grpc-echo-server /usr/local/bin/grpc-echo-server

ENV ECHO_ADDR=0.0.0.0:8137

EXPOSE 8137

CMD ["grpc-echo-server"]
