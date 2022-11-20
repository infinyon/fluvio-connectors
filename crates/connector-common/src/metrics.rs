use std::sync::atomic::{AtomicU64, Ordering};

//use fluvio_smartengine::metrics::SmartModuleChainMetrics;
use serde::Serialize;

#[derive(Default, Debug, Serialize)]
pub struct ConnectorMetrics {
    inbound_bytes: AtomicU64,
    outbound_bytes: AtomicU64,
    // smartmodule: SmartModuleChainMetrics,
}

impl ConnectorMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_inbound_bytes(&self, bytes: u64) {
        self.inbound_bytes.fetch_add(bytes, Ordering::SeqCst);
    }

    pub fn add_outbound_bytes(&self, bytes: u64) {
        self.outbound_bytes.fetch_add(bytes, Ordering::SeqCst);
    }
}
