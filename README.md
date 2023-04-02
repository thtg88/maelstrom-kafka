# Fly.io Gossip Gloomers Distributed Systems Challenges - Kafka-Style Log

This repo contains a Go implementation of the Kafka-style log challenge for the [Fly.io Gossip Gloomers](https://fly.io/dist-sys/) series of distributed systems challenges.

## Requirements

### Go 1.20

You can install Go 1.20 using [gvm](https://github.com/moovweb/gvm) with:

```bash
gvm install go1.20
gvm use go1.20
```

### Maelstrom

Maelstrom is built in [Clojure](https://clojure.org/) so you'll need to install [OpenJDK](https://openjdk.org/).

It also provides some plotting and graphing utilities which rely on [Graphviz](https://graphviz.org/) & [gnuplot](http://www.gnuplot.info/).

If you're using Homebrew, you can install these with this command:

```bash
brew install openjdk graphviz gnuplot
```

You can find more details on the [Prerequisites](https://github.com/jepsen-io/maelstrom/blob/main/doc/01-getting-ready/index.md#prerequisites) section on the Maelstrom docs.

Next, you'll need to download Maelstrom itself.

These challenges have been tested against [Maelstrom 0.2.3](https://github.com/jepsen-io/maelstrom/releases/tag/v0.2.3).

Download the tarball & unpack it.

You can run the maelstrom binary from inside this directory.

## Build

From the project's root directory:

```bash
go build .
```

## Test

### Challenge #5a: Single-Node Kafka-Style Log

https://fly.io/dist-sys/5a/

Your nodes will need to store an append-only log in order to handle the "kafka" workload. Each log is identified by a string key (e.g. "k1") and these logs contain a series of messages which are identified by an integer offset. These offsets can be sparse in that not every offset must contain a message.

Maelstrom will check to make sure several anomalies do not occur:

- Lost writes: for example, a client sees offset 10 but not offset 5.
- Monotonic increasing offsets: an offset for a log should always be increasing.
- There are no recency requirements so acknowledged send messages do not need to return in poll messages immediately.

```bash
# Make sure to replace `~/go/bin/maelstrom-kafka`
# with the full path of the executable you built above
./maelstrom test -w kafka --bin ~/go/bin/maelstrom-kafka \
  --node-count 1 \
  --concurrency 2n \
  --time-limit 20 \
  --rate 1000
```
