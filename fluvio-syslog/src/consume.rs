use futures_util::stream::StreamExt;
use structopt::StructOpt;
use syslog_loose::parse_message;

use crate::error::ConnectorError;
use crate::DEFAULT_TOPIC;
use fluvio::{Fluvio, Offset};

#[derive(StructOpt, Debug)]
pub struct ConsumerOpts {
    /// The topic where all things should go
    #[structopt(short, long, default_value = DEFAULT_TOPIC)]
    topic: String,
}

impl ConsumerOpts {
    pub async fn exec(self) -> Result<(), ConnectorError> {
        let topic = self.topic;
        let fluvio = Fluvio::connect().await?;
        let consumer = fluvio.partition_consumer(topic, 0).await?;
        let mut stream = consumer.stream(Offset::from_end(1)).await?;
        while let Some(record) = stream.next().await {
            let record = record?;
            let record = String::from_utf8_lossy(record.value());
            let msg = parse_message(&record);
            println!("{:?}", msg);
        }
        Ok(())
    }
}
