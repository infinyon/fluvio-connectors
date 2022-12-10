use std::sync::Arc;

use fluvio::metrics::ClientMetrics;
use serde::Serialize;

#[derive(Default, Debug, Serialize)]
pub struct ConnectorMetrics {
    #[serde(flatten)]
    fluvio_metrics: Arc<ClientMetrics>,
    // We can add here more metrics specific to the connector
}

impl ConnectorMetrics {
    pub fn new(fluvio_metrics: Arc<ClientMetrics>) -> Self {
        Self { fluvio_metrics }
    }
}
