mod error;
mod group;

use error::ConnectorError;
use group::SpuGroupCmd as ConnectorOpts;

use structopt::StructOpt;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), ConnectorError> {
    let opts = ConnectorOpts::from_args();
    use fluvio_extension_common::PrintTerminal;
    use std::sync::Arc;
    let out = Arc::new(PrintTerminal::new());
    let fluvio = fluvio::Fluvio::connect().await.expect("Failed to connect to fluvio");

    let _ = opts.process(out, &fluvio).await?;

    Ok(())
}
