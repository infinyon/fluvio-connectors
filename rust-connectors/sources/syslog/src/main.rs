use structopt::StructOpt;

mod config;
mod consume;
mod error;
mod produce;

use consume::ConsumerOpts;
use error::ConnectorError;
use produce::ProducerOpts;

const DEFAULT_TOPIC: &str = "syslog";

#[derive(StructOpt, Debug)]
enum ConnectorOpts {
    #[structopt(name = "produce")]
    Produce(ProducerOpts),

    #[structopt(name = "consume")]
    Consume(ConsumerOpts),
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), ConnectorError> {
    let opts = ConnectorOpts::from_args();
    match opts {
        ConnectorOpts::Produce(opts) => {
            opts.exec().await?;
        }
        ConnectorOpts::Consume(opts) => {
            opts.exec().await?;
        }
    }

    Ok(())
}
