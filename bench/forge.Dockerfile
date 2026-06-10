# Build forge-jobs' loadgen and run it against the compose `postgres`.
# Build context is the repo root (see docker-compose.bench.yml).
FROM rust:slim AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
# rustls (no OpenSSL needed). Build only loadgen + its deps.
RUN cargo build --release -p forge-jobs --features postgres --bin loadgen

FROM debian:stable-slim
COPY --from=build /src/target/release/loadgen /usr/local/bin/loadgen
# loadgen reads DATABASE_URL + LOADGEN_* from the environment.
ENTRYPOINT ["loadgen"]
