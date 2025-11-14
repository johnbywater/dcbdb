# ============================================================
# 1. Base image for CA certificates
# ============================================================
FROM ubuntu:22.04 AS base
RUN apt-get update && \
    apt-get install -y ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# ============================================================
# 2. Final image (scratch, fully static)
# ============================================================
FROM scratch AS final

WORKDIR /data

# Copy prebuilt MUSL binaries per platform
COPY --platform=linux/amd64 ./binaries/x86_64-unknown-linux-musl/umadb /umadb
COPY --platform=linux/arm64 ./binaries/aarch64-unknown-linux-musl/umadb /umadb

# Copy CA certificates from base
COPY --from=base /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

EXPOSE 50051

ENTRYPOINT ["/umadb"]
CMD ["--listen", "0.0.0.0:50051", "--db-path", "/data"]
