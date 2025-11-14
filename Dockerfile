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

# Copy binary, use TARGETPLATFORM to select the correct binary
ARG TARGETPLATFORM
COPY ./binaries/${TARGETPLATFORM}/umadb /umadb

# Copy CA certificates from base
COPY --from=base /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

EXPOSE 50051

ENTRYPOINT ["/umadb"]
CMD ["--listen", "0.0.0.0:50051", "--db-path", "/data"]
