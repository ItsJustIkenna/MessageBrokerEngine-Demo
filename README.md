# MessageBrokerEngine - Demo Version

> **Note**: This is a public demonstration version with sanitized configurations and example parameters.

## Overview

Message broker built in Rust, for publish/subscribe messaging.

## Performance

No benchmark results are published for this demo. The Criterion benchmarks in `hostbuilder/benches/` exercise standalone mock queue/message types, not the full broker over a network, so they are not an end-to-end throughput or latency measurement.

## Features

- Publisher and subscriber clients
- Topic routing
- Protocol Buffers encoding

## Quick Start

```bash
# Build
cargo build --workspace --release

# Run
cargo run --release --bin program
```

## Disclaimer

For educational purposes only.
