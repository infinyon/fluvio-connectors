use fluvio_connectors_common::git_hash_version;
use fluvio_connectors_common::opt::{CommonSourceOpt, Record};
use fluvio_future::tracing::{debug, info};
use schemars::schema_for;
use schemars::JsonSchema;
use std::collections::HashMap;
use structopt::StructOpt;
use tokio_stream::StreamExt;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    if let Some("metadata") = std::env::args().nth(1).as_deref() {
        let schema = serde_json::json!({
            "name": env!("CARGO_PKG_NAME"),
            "version": env!("CARGO_PKG_VERSION"),
            "description": env!("CARGO_PKG_DESCRIPTION"),
            "direction": "Source",
            "schema": schema_for!(SlackOpt),
        });
        println!("{}", serde_json::to_string_pretty(&schema).unwrap());
        return Ok(());
    }
    let opts: SlackOpt = SlackOpt::from_args();
    opts.common.enable_logging();
    info!(
        connector_version = env!("CARGO_PKG_VERSION"),
        git_hash = git_hash_version(),
        "Starting Slack sink connector",
    );
    let _ = opts.execute().await?;
    Ok(())
}

#[derive(StructOpt, Debug, JsonSchema, Clone)]
pub struct SlackOpt {
    #[structopt(long, env = "WEBHOOK_URL", hide_env_values = true)]
    pub webhook_url: String,

    #[structopt(flatten)]
    #[schemars(flatten)]
    pub common: CommonSourceOpt,
}

impl SlackOpt {
    pub async fn execute(&self) -> anyhow::Result<()> {
        let mut stream = self.common.create_consumer_stream().await?;
        info!("Starting stream");
        while let Some(Ok(record)) = stream.next().await {
            let _ = self.send_to_slack(&record).await;
        }
        Ok(())
    }
    pub async fn send_to_slack(&self, record: &Record) -> anyhow::Result<()> {
        let text = String::from_utf8_lossy(record.value());
        debug!("Sending {:?}, to slack", text);
        let mut map = HashMap::new();
        map.insert("text", text);

        let client = reqwest::Client::new();
        let _res = client.post(&self.webhook_url).json(&map).send().await?;
        Ok(())
    }
}
