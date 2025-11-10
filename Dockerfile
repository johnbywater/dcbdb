# Multi-stage build for UmaDB server
# Supports both amd64 and arm64 architectures

# Build stage
FROM --platform=$BUILDPLATFORM rust:1.90-slim AS builder

# Install build dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    protobuf-compiler \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Copy workspace configuration
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY src ./src
#COPY tests ./tests

# Build for release
RUN cargo build --release -p umadb-server

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
CMD ["--help"]
