FROM rust:alpine AS builder
RUN apk add --no-cache musl-dev gcc
WORKDIR /app
COPY Cargo.* ./
RUN mkdir src && echo "fn main() {}" > src/main.rs && \
    cargo build --release && \
    rm -rf src target/release/deps/rust_em* target/release/rust_em*
COPY src ./src
RUN cargo build --release

FROM alpine:latest
RUN apk add --no-cache libgcc
WORKDIR /app
COPY --from=builder /app/target/release/rust_em /usr/local/bin/rust_em
COPY public ./public
EXPOSE 8096
CMD ["rust_em"]
