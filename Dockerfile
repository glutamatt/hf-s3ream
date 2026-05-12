# syntax=docker/dockerfile:1.7
#
# Build hf-s3ream in a rust:slim image, then copy the static-ish binary into
# distroless/cc-debian12. Final image is ~30 MiB and pulls quickly on a
# compute node before the job actually starts streaming bytes.

FROM rust:slim-bookworm AS builder
WORKDIR /build

# pkg-config + ca-certificates for cargo's HTTPS fetches; git for the
# xet-core git dependencies pinned in Cargo.lock.
RUN apt-get update && apt-get install -y --no-install-recommends \
        pkg-config \
        ca-certificates \
        git \
    && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock ./
COPY src ./src

RUN cargo build --release \
    && strip target/release/hf-s3ream

# Distroless cc has libgcc + libc + ca-certificates, which is all reqwest
# (rustls-tls) and tokio need at runtime. Runs as root (uid 0) by default;
# Pyxis maps that to the submitting user's uid via subuid/subgid namespaces.
FROM gcr.io/distroless/cc-debian12
COPY --from=builder /build/target/release/hf-s3ream /usr/local/bin/hf-s3ream
ENTRYPOINT ["/usr/local/bin/hf-s3ream"]
