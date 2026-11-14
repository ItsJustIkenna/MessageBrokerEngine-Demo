# MessageBrokerEngine - Demo Version

> **Note**: This is a public demonstration version with sanitized configurations and example parameters.

## Overview

Message broker built in Rust, designed for low-latency publish/subscribe messaging.

## Performance

Measured on an 8-vCPU VM: 300k+ messages/s over loopback TCP.

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
