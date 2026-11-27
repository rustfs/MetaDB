# Multi-stage Dockerfile for running official Jepsen (jepsen.io) tests against a 5-node MetaDB cluster.
# Stage 1: Build pure-Rust `metadbd` and `metadb-ctl` release binaries.
FROM rust:1.85-bookworm AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
        protobuf-compiler \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY . .
RUN cargo build --release --locked -p metadb-server --bins

# Stage 2: Debian Linux DB node image with SSH, iptables, iproute2 (tc), and MetaDB binaries.
FROM debian:bookworm-slim AS db-node

RUN apt-get update && apt-get install -y --no-install-recommends \
        openssh-server \
        iptables \
        iproute2 \
        procps \
        psmisc \
        curl \
        ca-certificates \
        faketime \
        libfaketime \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /var/run/sshd /var/lib/metadb /etc/metadb /var/log/metadb \
    && echo 'root:root' | chpasswd \
    && sed -i 's/#PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config \
    && sed -i 's/#PasswordAuthentication yes/PasswordAuthentication yes/' /etc/ssh/sshd_config

COPY --from=builder /src/target/release/metadbd /usr/local/bin/metadbd
COPY --from=builder /src/target/release/metadb-ctl /usr/local/bin/metadb-ctl

EXPOSE 22 7800 9100
CMD ["/usr/sbin/sshd", "-D"]

# Stage 3: Jepsen control node with OpenJDK 21, Leiningen, gnuplot, Graphviz, and `jepsen.metadb`.
FROM clojure:temurin-21-lein-bookworm AS control

RUN apt-get update && apt-get install -y --no-install-recommends \
        openssh-client \
        sshpass \
        gnuplot \
        graphviz \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /src/target/release/metadb-ctl /usr/local/bin/metadb-ctl
WORKDIR /jepsen
COPY tests/jepsen/project.clj /jepsen/project.clj
RUN lein deps
COPY tests/jepsen/ /jepsen/

CMD ["sleep", "infinity"]
