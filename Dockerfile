# Build stage
FROM --platform=$BUILDPLATFORM rust:1.90-slim AS builder
ARG TARGETPLATFORM
WORKDIR /build

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    protobuf-compiler pkg-config libssl-dev && \
    rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY src ./src

RUN case "$TARGETPLATFORM" in \
        "linux/amd64")  export CARGO_TARGET=x86_64-unknown-linux-gnu ;; \
        "linux/arm64")  export CARGO_TARGET=aarch64-unknown-linux-gnu ;; \
        *) echo "Unsupported platform: $TARGETPLATFORM" && exit 1 ;; \
    esac && \
    rustup target add $CARGO_TARGET && \
    cargo build --release --target $CARGO_TARGET -p umadb-server

# Runtime stage
FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Create a non-root user
RUN useradd -m -u 1000 umadb

# Copy the binary from builder
COPY --from=builder /build/target/release/umadb /usr/local/bin/umadb

# Set ownership
RUN chown umadb:umadb /usr/local/bin/umadb

# Create data directory
RUN mkdir -p /data && chown umadb:umadb /data

# Switch to non-root user
USER umadb

# Set working directory
WORKDIR /data

# Expose default gRPC port
EXPOSE 50051

# Set default command
ENTRYPOINT ["/usr/local/bin/umadb"]
CMD ["--listen", "0.0.0.0:50051", "--db-path", "/data"]
