# Fluvio SmartStreams

This repository is a [`cargo-generate`] template for getting started
with writing Fluvio SmartStreams. To use it, run the following:

```
$ cargo install cargo-generate
$ cargo generate --git https://github.com/infinyon/fluvio-smartstream-template
```

> **Note**: To compile a SmartStream, you will need to install the `wasm32-unknown-unknown`
> target by running `rustup target add wasm32-unknown-unknown`.

## About SmartStreams

Fluvio SmartStreams are custom plugins that can be used to manipulate
streaming data in a topic. SmartStreams are written in Rust and compiled
to WebAssembly. To use a SmartStream, you simply provide the `.wasm` file
to the Fluvio consumer, which uploads it to the Streaming Processing Unit
(SPU) where it runs your SmartStream code on each record before sending
it to the consumer.

Below are the various types of SmartStreams and examples of how to use them.

### Filtering

Filters are functions that are given a reference to each record in the
stream as it is processed, and must return true or false to determine
whether the record should be kept (true) or discarded (false).

```rust
use fluvio_smartstream::{smartstream, SimpleRecord};

#[smartstream(filter)]
pub fn my_filter(record: &SimpleRecord) -> bool {
    let value = String::from_utf8_lossy(record.value.as_ref());
    value.contains('z')
}
```

This filter will keep only records whose data contains the letter `z`.

## Using SmartStreams with the Fluvio CLI

Make sure to follow the [Fluvio getting started] guide, then create a new
topic to send data to.

[Fluvio getting started]: https://www.fluvio.io/docs/getting-started/

```bash
$ fluvio topic create smartstream-test
$ cargo build --release
$ fluvio consume smartstream-test -B --smart-stream="target/wasm32-unknown-unknown/release/fluvio-postgres-map"
```
