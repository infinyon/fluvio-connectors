use std::{io::Error as IoError, sync::Arc};

use async_net::unix::UnixListener;
use futures_util::{AsyncWriteExt, StreamExt};

use fluvio_future::task::spawn;
use tracing::{error, info, trace};

use crate::metrics::ConnectorMetrics;

const SOCKET_PATH: &str = "/tmp/fluvio-connector.sock";

pub fn init_monitoring(metrics: Arc<ConnectorMetrics>) {
    spawn(async move {
        if let Err(err) = start_monitoring(metrics).await {
            error!("error running monitoring: {}", err);
        }
    });
}

/// initialize if monitoring flag is set
async fn start_monitoring(metrics: Arc<ConnectorMetrics>) -> Result<(), IoError> {
    let metric_out_path = match std::env::var("FLUVIO_METRIC_CONNECTOR") {
        Ok(path) => {
            info!("using metric path: {}", path);
            path
        }
        Err(_) => {
            info!("using default metric path: {}", SOCKET_PATH);
            SOCKET_PATH.to_owned()
        }
    };

    // check if file exists
    if let Ok(_metadata) = std::fs::metadata(&metric_out_path) {
        info!("metric file already exists, deleting: {}", metric_out_path);
        match std::fs::remove_file(&metric_out_path) {
            Ok(_) => {}
            Err(err) => {
                println!("error deleting metric file: {err}");
                return Err(err);
            }
        }
    }

    let listener = UnixListener::bind(metric_out_path)?;
    let mut incoming = listener.incoming();
    info!("monitoring started");

    while let Some(stream) = incoming.next().await {
        let mut stream = stream?;

        trace!("metrics: {:?}", metrics);
        let bytes = serde_json::to_vec_pretty(metrics.as_ref())?;
        stream.write_all(&bytes).await?;
    }

    Ok(())
}
