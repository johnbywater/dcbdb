# -----------------------------
# Base image (for certificates only)
# -----------------------------
FROM ubuntu:22.04 AS base
RUN apt-get update && \
    apt-get install -y ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# -----------------------------
# Final image (scratch, fully static)
# -----------------------------
FROM scratch AS final

WORKDIR /data

# Copy the correct prebuilt MUSL binary per platform
# BuildKit automatically uses TARGETPLATFORM
COPY --platform=linux/amd64 ./binaries/x86_64-unknown-linux-musl/umadb /umadb
COPY --platform=linux/arm64 ./binaries/aarch64-unknown-linux-musl/umadb /umadb

# Copy CA certificates from base
COPY --from=base /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

# Expose gRPC port
EXPOSE 50051

ENTRYPOINT ["/umadb"]
CMD ["--listen", "0.0.0.0:50051", "--db-path", "/data"]
