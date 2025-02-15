VERSION 0.7
FROM ubuntu:22.04

RUN apt-get update && apt-get install --yes sudo

WORKDIR /dbsp
ENV RUSTUP_HOME=$HOME/.rustup
ENV CARGO_HOME=$HOME/.cargo
ENV PATH=$HOME/.cargo/bin:$PATH
ENV RUST_VERSION=1.69.0

install-deps:
    RUN apt-get update
    RUN apt-get install --yes build-essential curl libssl-dev build-essential pkg-config \
                              cmake git gcc clang libclang-dev python3-pip python3-plumbum \
                              hub numactl openjdk-19-jre-headless maven netcat jq \
                              libsasl2-dev docker.io
    RUN curl -fsSL https://deb.nodesource.com/setup_19.x | sudo -E bash -
    RUN apt-get install --yes nodejs
    RUN npm install --global yarn
    RUN npm install --global openapi-typescript-codegen
    COPY demo/demo_notebooks/requirements.txt demo/demo_notebooks/requirements.txt
    RUN pip3 install -r demo/demo_notebooks/requirements.txt
    RUN pip3 install openapi-python-client

install-rust:
    FROM +install-deps
    RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- \
        -y \
        --default-toolchain $RUST_VERSION \
        --profile minimal \
        --component clippy \
        --component rustfmt \
        --component llvm-tools-preview
    RUN rustup toolchain install nightly \
        --component clippy \
        --component rustfmt \
        --component llvm-tools-preview
    RUN cargo install --force cargo-udeps
    RUN cargo install --force cargo-audit
    RUN cargo install --force cargo-make
    RUN rustup --version
    RUN cargo --version
    RUN rustc --version

install-chef:
    FROM +install-rust
    RUN cargo install --debug cargo-chef

prepare-cache:
    # We download and pre-build dependencies to cache it using cargo-chef.
    # See also (on why this is so complicated :/):
    # https://github.com/rust-lang/cargo/issues/2644
    # https://hackmd.io/@kobzol/S17NS71bh
    FROM +install-chef

    RUN mkdir -p crates/dataflow-jit
    RUN mkdir -p crates/nexmark
    RUN mkdir -p crates/dbsp
    RUN mkdir -p crates/adapters
    RUN mkdir -p crates/pipeline_manager
    #RUN mkdir -p crates/webui-tester

    COPY --keep-ts Cargo.toml .
    COPY --keep-ts Cargo.lock .
    COPY --keep-ts crates/dataflow-jit/Cargo.toml crates/dataflow-jit/
    COPY --keep-ts crates/nexmark/Cargo.toml crates/nexmark/
    COPY --keep-ts crates/dbsp/Cargo.toml crates/dbsp/
    COPY --keep-ts crates/adapters/Cargo.toml crates/adapters/
    COPY --keep-ts crates/pipeline_manager/Cargo.toml crates/pipeline_manager/
    #COPY --keep-ts crates/webui-tester/Cargo.toml crates/webui-tester/

    RUN mkdir -p crates/dataflow-jit/src && touch crates/dataflow-jit/src/lib.rs
    RUN mkdir -p crates/nexmark/src && touch crates/nexmark/src/lib.rs
    RUN mkdir -p crates/dbsp/src && touch crates/dbsp/src/lib.rs
    RUN mkdir -p crates/adapters/src && touch crates/adapters/src/lib.rs
    RUN mkdir -p crates/dataflow-jit/src && touch crates/dataflow-jit/src/main.rs
    RUN mkdir -p crates/nexmark/benches/nexmark-gen && touch crates/nexmark/benches/nexmark-gen/main.rs
    RUN mkdir -p crates/nexmark/benches/nexmark && touch crates/nexmark/benches/nexmark/main.rs
    RUN mkdir -p crates/dbsp/benches/gdelt && touch crates/dbsp/benches/gdelt/main.rs
    RUN mkdir -p crates/dbsp/benches/ldbc-graphalytics && touch crates/dbsp/benches/ldbc-graphalytics/main.rs
    RUN mkdir -p crates/pipeline_manager/src && touch crates/pipeline_manager/src/main.rs
    #RUN mkdir -p crates/webui-tester/src && touch crates/webui-tester/src/lib.rs

    ENV RUST_LOG=info
    RUN cargo chef prepare

    SAVE ARTIFACT --keep-ts --keep-own recipe.json

build-cache:
    ARG RUST_TOOLCHAIN=$RUST_VERSION
    FROM +install-chef
    COPY --keep-ts +prepare-cache/recipe.json ./
    RUN cargo +$RUST_TOOLCHAIN chef cook --workspace --all-targets
    RUN cargo +$RUST_TOOLCHAIN chef cook --release --package dbsp_pipeline_manager --all-targets
    RUN cargo +$RUST_TOOLCHAIN chef cook --clippy --workspace
    # I have no clue why we need all these commands below to build all
    # dependencies (since we just did a build --workspace --all-targets). But if
    # we don't run all of them, it will go and compile dependencies during
    # the `build-*` targets (which should just compile our crates ideally to
    # maximize the cache benefit). This happens even without earthly (or docker)
    # if you just run cargo in our repo directly on clean target...
    #
    # The only issues for this I found was:
    # https://github.com/rust-lang/cargo/issues/2904
    # https://github.com/rust-lang/cargo/issues/6733
    #
    # When using `RUST_LOG=cargo::ops::cargo_rustc::fingerprint=info` to debug,
    # it looks like the hash for the crates changes.
    RUN cargo +$RUST_TOOLCHAIN build
    RUN cargo +$RUST_TOOLCHAIN test --no-run
    RUN cargo +$RUST_TOOLCHAIN clippy
    RUN cargo +$RUST_TOOLCHAIN build --package dbsp
    RUN cargo +$RUST_TOOLCHAIN test --package dbsp --no-run
    RUN cargo +$RUST_TOOLCHAIN clippy --package dbsp
    RUN cargo +$RUST_TOOLCHAIN build --package dbsp_adapters
    RUN cargo +$RUST_TOOLCHAIN test --package dbsp_adapters --no-run
    RUN cargo +$RUST_TOOLCHAIN clippy --package dbsp_adapters
    RUN cargo +$RUST_TOOLCHAIN build --package dbsp_pipeline_manager
    # Pipeline manager is the only one we compile in release mode from
    # `start_manager.sh`.  As we add benchmarks and other targets that
    # require release builds, we may want to make build profile an ARG.
    RUN cargo +$RUST_TOOLCHAIN build --release --package dbsp_pipeline_manager
    RUN cargo +$RUST_TOOLCHAIN test --package dbsp_pipeline_manager --no-run
    RUN cargo +$RUST_TOOLCHAIN clippy --package dbsp_pipeline_manager
    RUN cargo +$RUST_TOOLCHAIN build --package dataflow-jit
    RUN cargo +$RUST_TOOLCHAIN test --package dataflow-jit --no-run
    RUN cargo +$RUST_TOOLCHAIN clippy --package dataflow-jit
    RUN cargo +$RUST_TOOLCHAIN build --package dbsp_nexmark
    RUN cargo +$RUST_TOOLCHAIN test --package dbsp_nexmark --no-run
    RUN cargo +$RUST_TOOLCHAIN clippy --package dbsp_nexmark

    SAVE ARTIFACT --keep-ts --keep-own $CARGO_HOME
    SAVE ARTIFACT --keep-ts --keep-own ./target

build-dbsp:
    ARG RUST_TOOLCHAIN=$RUST_VERSION
    FROM +build-cache --RUST_TOOLCHAIN=$RUST_TOOLCHAIN

    # Remove the skeleton created in build-cache
    RUN rm -rf crates/dbsp
    # Copy in the actual sources
    COPY --keep-ts --dir crates/dbsp crates/dbsp
    COPY --keep-ts README.md README.md

    RUN cargo +$RUST_TOOLCHAIN build --package dbsp
    RUN cargo +$RUST_TOOLCHAIN test --package dbsp --no-run
    RUN cargo +$RUST_TOOLCHAIN clippy --package dbsp -- -D warnings
    # Update target folder for future tasks, the whole --keep-ts, --keep-own
    # args were an attempt in fixing the dependency problem (see build-cache)
    # but it doesn't seem to make a difference.
    SAVE ARTIFACT --keep-ts --keep-own ./target/* ./target


build-adapters:
    ARG RUST_TOOLCHAIN=$RUST_VERSION
    FROM +build-dbsp --RUST_TOOLCHAIN=$RUST_TOOLCHAIN
    RUN rm -rf crates/adapters
    COPY --keep-ts --dir crates/adapters crates/adapters

    RUN cargo +$RUST_TOOLCHAIN build --package dbsp_adapters
    RUN cargo +$RUST_TOOLCHAIN test --package dbsp_adapters --no-run
    RUN cargo +$RUST_TOOLCHAIN clippy --package dbsp_adapters -- -D warnings
    SAVE ARTIFACT --keep-ts --keep-own ./target/* ./target

build-manager:
    ARG RUST_TOOLCHAIN=$RUST_VERSION
    FROM +build-adapters --RUST_TOOLCHAIN=$RUST_TOOLCHAIN

    RUN rm -rf crates/pipeline_manager
    COPY --keep-ts --dir crates/pipeline_manager crates/pipeline_manager
    COPY --keep-ts --dir web-ui web-ui
    COPY --keep-ts python python

    RUN cargo +$RUST_TOOLCHAIN build --package dbsp_pipeline_manager
    RUN cargo +$RUST_TOOLCHAIN test --package dbsp_pipeline_manager --no-run
    RUN cargo +$RUST_TOOLCHAIN clippy --package dbsp_pipeline_manager -- -D warnings
    RUN cargo make --cwd crates/pipeline_manager/ openapi_python
    SAVE ARTIFACT --keep-ts --keep-own ./target/* ./target

build-sql:
    ARG RUST_TOOLCHAIN=$RUST_VERSION
    FROM +build-manager --RUST_TOOLCHAIN=$RUST_TOOLCHAIN
    COPY --keep-ts sql-to-dbsp-compiler sql-to-dbsp-compiler
    RUN cd "sql-to-dbsp-compiler/SQL-compiler" && mvn -DskipTests package

build-dataflow-jit:
    ARG RUST_TOOLCHAIN=$RUST_VERSION
    FROM +build-dbsp --RUST_TOOLCHAIN=$RUST_TOOLCHAIN

    RUN rm -rf crates/dataflow-jit
    COPY --keep-ts --dir crates/dataflow-jit crates/dataflow-jit

    RUN cargo +$RUST_TOOLCHAIN build --package dataflow-jit
    RUN cargo +$RUST_TOOLCHAIN test --package dataflow-jit --no-run
    RUN cargo +$RUST_TOOLCHAIN clippy --package dataflow-jit -- -D warnings
    SAVE ARTIFACT --keep-ts --keep-own ./target/* ./target

build-nexmark:
    ARG RUST_TOOLCHAIN=$RUST_VERSION
    FROM +build-dbsp --RUST_TOOLCHAIN=$RUST_TOOLCHAIN

    RUN rm -rf crates/nexmark
    COPY --keep-ts --dir crates/nexmark crates/nexmark

    RUN cargo +$RUST_TOOLCHAIN build --package dbsp_nexmark
    RUN cargo +$RUST_TOOLCHAIN test --package dbsp_nexmark --no-run
    RUN cargo +$RUST_TOOLCHAIN clippy --package dbsp_nexmark -- -D warnings
    SAVE ARTIFACT --keep-ts --keep-own ./target/* ./target

audit:
    FROM +install-rust
    COPY --keep-ts Cargo.toml .
    COPY --keep-ts Cargo.lock .
    COPY --keep-ts README.md .
    COPY --keep-ts --dir crates crates
    COPY --keep-ts --dir web-ui web-ui
    # cargo udeps requires nightly Rust.
    RUN cargo +nightly udeps --workspace --all-features --all-targets
    RUN cargo audit

test-dbsp:
    ARG RUST_TOOLCHAIN=$RUST_VERSION
    FROM +build-dbsp --RUST_TOOLCHAIN=$RUST_TOOLCHAIN
    RUN cargo +$RUST_TOOLCHAIN test --package dbsp

test-nexmark:
    ARG RUST_TOOLCHAIN=$RUST_VERSION
    FROM +build-dbsp --RUST_TOOLCHAIN=$RUST_TOOLCHAIN
    RUN cargo +$RUST_TOOLCHAIN test --package dbsp_nexmark

test-dataflow-jit:
    ARG RUST_TOOLCHAIN=$RUST_VERSION
    FROM +build-dataflow-jit --RUST_TOOLCHAIN=$RUST_TOOLCHAIN
    # Tests use this demo directory
    COPY --keep-ts demo/project_demo01-TimeSeriesEnrich demo/project_demo01-TimeSeriesEnrich
    RUN cargo +$RUST_TOOLCHAIN test --package dataflow-jit

test-adapters:
    ARG RUST_TOOLCHAIN=$RUST_VERSION
    FROM +build-adapters --RUST_TOOLCHAIN=$RUST_TOOLCHAIN
    WITH DOCKER --pull docker.redpanda.com/vectorized/redpanda:v22.3.11
        RUN docker run --name redpanda -p 9092:9092 --rm -itd docker.redpanda.com/vectorized/redpanda:v22.3.11 \
            redpanda start --smp 2 && \
            cargo +$RUST_TOOLCHAIN test --package dbsp_adapters
    END

test-manager:
    ARG RUST_TOOLCHAIN=$RUST_VERSION
    FROM +build-manager --RUST_TOOLCHAIN=$RUST_TOOLCHAIN
    ENV PGHOST=localhost
    ENV PGUSER=postgres
    ENV PGCLIENTENCODING=UTF8
    ENV PGPORT=5432
    ENV RUST_LOG=error
    WITH DOCKER --pull postgres
        # We just put the PGDATA in /dev/shm because the docker fs seems very slow (test time goes to 2min vs. shm 40s)
        RUN docker run --shm-size=256MB -p 5432:5432 --name postgres -e POSTGRES_HOST_AUTH_METHOD=trust -e PGDATA=/dev/shm -d postgres && \
            # Sleep until postgres is up (otherwise we get connection reset if we connect too early)
            # (See: https://github.com/docker-library/docs/blob/master/postgres/README.md#caveats)
            sleep 3 && \
            cargo +$RUST_TOOLCHAIN test --package dbsp_pipeline_manager
    END

test-python:
    ARG RUST_TOOLCHAIN=$RUST_VERSION
    FROM +build-sql --RUST_TOOLCHAIN=$RUST_TOOLCHAIN
    COPY --keep-ts scripts scripts
    COPY --keep-ts demo/demo_notebooks demo/demo_notebooks
    ENV PGHOST=localhost
    ENV PGUSER=postgres
    ENV PGCLIENTENCODING=UTF8
    ENV PGPORT=5432
    ENV RUST_LOG=error
    ENV WITH_POSTGRES=1
    WITH DOCKER --pull postgres
        # We just put the PGDATA in /dev/shm because the docker fs seems very slow (test time goes to 2min vs. shm 40s)
        RUN docker run --shm-size=256MB -p 5432:5432 --name postgres -e POSTGRES_HOST_AUTH_METHOD=trust -e PGDATA=/dev/shm -d postgres && \
            sleep 3 && \
            cargo make --cwd crates/pipeline_manager/ python_test
    END

test-rust:
    ARG RUST_TOOLCHAIN=$RUST_VERSION
    BUILD +test-dbsp --RUST_TOOLCHAIN=$RUST_TOOLCHAIN
    BUILD +test-nexmark --RUST_TOOLCHAIN=$RUST_TOOLCHAIN
    BUILD +test-adapters --RUST_TOOLCHAIN=$RUST_TOOLCHAIN
    BUILD +test-dataflow-jit --RUST_TOOLCHAIN=$RUST_TOOLCHAIN
    BUILD +test-manager --RUST_TOOLCHAIN=$RUST_TOOLCHAIN

test-rust-nightly:
    BUILD +test-rust --RUST_TOOLCHAIN=nightly

all-tests:
    BUILD +test-rust
    BUILD +test-python
    BUILD +audit
