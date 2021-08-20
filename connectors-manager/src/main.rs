mod error;
mod list;
mod start;
mod stop;

use error::ConnectorError;
use list::ListOpts;
use start::StartOpts;
use stop::StopOpts;

use structopt::StructOpt;

#[derive(StructOpt, Debug)]
enum ConnectorOpts {
    #[structopt(name = "start")]
    Start(StartOpts),

    #[structopt(name = "stop")]
    Stop(StopOpts),

    #[structopt(name = "list")]
    List(ListOpts),
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), ConnectorError> {
    let opts = ConnectorOpts::from_args();
    match opts {
        ConnectorOpts::Start(opts) => {
            let _ = opts.exec().await?;
        }
        ConnectorOpts::Stop(opts) => {
            let _ = opts.exec().await?;
        }
        ConnectorOpts::List(opts) => {
            let _ = opts.exec().await?;
        }
    }

    Ok(())
}
