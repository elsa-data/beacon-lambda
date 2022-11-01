ARG FUNCTION_DIR="/function"

FROM rust:1.63-buster as builder

# some nonsense to do with ring not building
# https://github.com/briansmith/ring/issues/1414
RUN apt update
RUN apt install musl-tools clang llvm -y

ENV CC_aarch64_unknown_linux_musl=clang
ENV AR_aarch64_unknown_linux_musl=llvm-ar
ENV CARGO_TARGET_AARCH64_UNKNOWN_LINUX_MUSL_RUSTFLAGS="-Clink-self-contained=yes -Clinker=rust-lld"

WORKDIR /build

ADD Cargo.toml Cargo.toml
ADD Cargo.lock Cargo.lock

# do a before build setup of all our packages etc
# take advantage of Docker caching so we only need to do all this when our packages change
RUN mkdir ./src && echo 'fn main() { println!("Dummy!"); }' > ./src/main.rs
RUN rustup target add aarch64-unknown-linux-musl && \
    cargo build --release --target aarch64-unknown-linux-musl
RUN rm -rf ./src

# now build the actual project
ADD src src
RUN cargo build --release --target aarch64-unknown-linux-musl

# copy artifacts to a clean image
FROM gcr.io/distroless/static:latest

# Include global arg in this stage of the build
ARG FUNCTION_DIR

# Set working directory to function root directory
WORKDIR ${FUNCTION_DIR}

COPY --from=builder /build/target/aarch64-unknown-linux-musl/release/beacon bootstrap

ENTRYPOINT [ "./bootstrap" ]
