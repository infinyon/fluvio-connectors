use std::thread;
use std::time::Duration;
use jemalloc_ctl::{stats, epoch};
use http_connector::HttpOpt;
use fluvio_connectors_common::opt::CommonSourceOpt;

#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

#[tokio::main]
async fn main() -> eyre::Result<()> {
    let opts = HttpOpt {
        endpoint: String::from("localhost:8080"),
        interval: 1,
        common: CommonSourceOpt {
            fluvio_topic: String::from("benchmarks"),
            ..Default::default()
        },
        ..Default::default()
    };
    let _handle = tokio::spawn(async move {
        opts.execute().await
    });
    loop {
        // many statistics are cached and only updated when the epoch is advanced.
        epoch::advance().unwrap();

        let allocated = stats::allocated::read().unwrap();
        let resident = stats::resident::read().unwrap();
        println!("{} bytes allocated/{} bytes resident", allocated, resident);
        thread::sleep(Duration::from_secs(10));
    }
}
