# syntax=docker/dockerfile:1.7
#
# Build hf-s3ream in a rust:slim image, then copy the binary into a tiny
# debian-slim runtime. Final image is ~70 MiB.
#
# Why debian:bookworm-slim and not distroless: enroot/Pyxis (the SLURM
# container runtime we target) invokes /bin/sh during switchroot setup,
# even when the ENTRYPOINT is exec-form. Distroless intentionally has no
# shell, so srun --container-image=... fails with "enroot-switchroot:
# failed to execute: /bin/sh: No such file or directory". debian-slim has
# /bin/dash + ca-certificates and adds ~40 MiB vs distroless/cc-debian12 —
# negligible vs the cluster's NIC.

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

# Runtime: debian-slim with ca-certificates for outbound TLS to S3/Hub/CAS.
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /build/target/release/hf-s3ream /usr/local/bin/hf-s3ream
ENTRYPOINT ["/usr/local/bin/hf-s3ream"]
