# Official Jepsen (`jepsen.io`) Harness for MetaDB

This directory contains the official Clojure Jepsen (`jepsen.core` + `knossos` + `elle`) test suite and 5-node Linux Docker environment for auditing MetaDB under real OS-level network partitions (`iptables`), packet latency/loss (`tc netem`), `SIGKILL` crashes, `SIGSTOP`/`SIGCONT` pauses, clock skew (`bump-clock`), and live shard splits/leader transfers.

MetaDB also ships a self-contained pure-Rust Jepsen suite (`crates/metadb-server/src/jepsen.rs` and `crates/metadb-server/tests/jepsen_tests.rs`) that runs in `cargo test --workspace --locked` without Docker or JVM and exports standard Clojure EDN histories (`history.edn`) directly inspectable by `knossos` and `elle`.

## Quick Start (5-Node Docker Cluster)

```bash
cd tests/jepsen/docker
docker compose up -d --build

# Run linearizable register test (Knossos WGL checker) under partitions, kills, and pauses:
docker exec -it metadb-jepsen-control \
  lein run test \
    --nodes n1,n2,n3,n4,n5 \
    --username root --password root \
    --workload register \
    --nemesis partition,kill,pause,clock \
    --time-limit 120 \
    --concurrency 10

# Run set-full durability test (zero lost acknowledged creates):
docker exec -it metadb-jepsen-control \
  lein run test \
    --nodes n1,n2,n3,n4,n5 \
    --username root --password root \
    --workload set \
    --nemesis partition,kill,split \
    --time-limit 120

# Run cross-shard bank conservation test (QuotaTable + DirStat):
docker exec -it metadb-jepsen-control \
  lein run test \
    --nodes n1,n2,n3,n4,n5 \
    --username root --password root \
    --workload bank \
    --nemesis partition,kill,pause \
    --time-limit 120

# Run Elle atomic cross-shard rename / rw-register cycle detection:
docker exec -it metadb-jepsen-control \
  lein run test \
    --nodes n1,n2,n3,n4,n5 \
    --username root --password root \
    --workload rename-elle \
    --nemesis partition,kill,pause \
    --time-limit 120
```

## Pure-Rust CI & Local Jepsen Runner (No Docker / JVM Required)

```bash
# Run full integration test suite (real 3-process metadbd OS clusters + Swizzle-Clog partitions):
cargo test -p metadb-server --test jepsen_tests --locked -- --nocapture

# Run interactively via metadb-ctl and export history.edn:
cargo run --release -p metadb-server --bin metadb-ctl -- \
  jepsen --workload register --nemesis pause --clients 6 --duration-secs 15 \
  --edn-out /tmp/metadb-jepsen-history.edn
```
