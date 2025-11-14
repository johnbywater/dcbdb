# ============================================================
# 1. BUILDER STAGE (multi-arch, MUSL, static linking)
# ============================================================
FROM rust:1.90-slim AS builder

# Use Docker Buildx-provided architecture
ARG TARGETARCH

# Install MUSL toolchain, build deps, and ca-certificates
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        musl-tools musl-dev clang pkg-config protobuf-compiler \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Determine target and add it to rustup \
RUN if [ "$TARGETARCH" = "amd64" ]; then \
        rustup target add x86_64-unknown-linux-musl; \
        echo "x86_64-unknown-linux-musl" > /tmp/rust_target; \
    elif [ "$TARGETARCH" = "arm64" ]; then \
        rustup target add aarch64-unknown-linux-musl; \
        echo "aarch64-unknown-linux-musl" > /tmp/rust_target; \
    else \
        echo "Unsupported architecture: $TARGETARCH"; exit 1; \
    fi

WORKDIR /build

# Copy workspace
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY src ./src

# Build the static binary
RUN cargo build --release --target "$(cat /tmp/rust_target)" -p umadb-server
RUN strip /build/target/$(cat /tmp/rust_target)/release/umadb

# ============================================================
# 2. RUNTIME STAGE (SCRATCH, 100% static binary)
# ============================================================
FROM scratch

WORKDIR /data

# Copy the static binary
COPY --from=builder /build/target/*musl*/release/umadb /umadb

# Copy CA certificates from builder
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

# Expose gRPC port
EXPOSE 50051

ENTRYPOINT ["/umadb"]
CMD ["--listen", "0.0.0.0:50051", "--db-path", "/data"]
