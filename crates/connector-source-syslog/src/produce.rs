use crate::config::ConnectorConfig;
use crate::error::ConnectorError;
use crate::DEFAULT_TOPIC;
use std::convert::TryFrom;
use std::io::{self, BufRead};
use std::path::Path;
use structopt::StructOpt;

use fluvio::{metadata::topic::TopicSpec, Fluvio};
#[derive(StructOpt, Debug)]
pub struct ProducerOpts {
    /// If this connector is acts as a server, we use this bind address
    #[structopt(short, long)]
    bind: Option<String>,

    /// If this connector simply looks for new things coming in from a file,
    #[structopt(short, long)]
    file: Option<String>,

    /// The topic where all things should go
    #[structopt(short, long, default_value = DEFAULT_TOPIC)]
    topic: String,

    #[structopt(short, long)]
    config: Option<String>,
}

impl ProducerOpts {
    pub async fn exec(self) -> Result<(), ConnectorError> {
        let topic = self.topic;
        let fluvio = Fluvio::connect().await?;
        let admin = fluvio.admin().await;
        let topics = admin
            .list::<TopicSpec, String>(vec![])
            .await?
            .iter()
            .map(|topic| topic.name.clone())
            .collect::<String>();

        if !topics.contains(&topic) {
            admin
                .create(
                    topic.clone(),
                    false,
                    TopicSpec::new_computed(1, 1, Some(false)),
                )
                .await?;
        }

        let producer = fluvio.topic_producer(topic.clone()).await?;

        if let Some(config_file) = self.config {
            let _config = ConnectorConfig::try_from(Path::new(&config_file))?;
        } else if let Some(_bind) = self.bind {
            cfg_if::cfg_if! {
                if #[cfg(target_arch = "wasm32")] {
                    unimplemented!("Not supported on wasm32!");
                } else {
                    todo!();
                }
            }
        } else if let Some(file) = self.file {
            use notify::{
                event::ModifyKind, EventKind, RecommendedWatcher, RecursiveMode,
                Result as NotifyResult, Watcher,
            };

            let (tx, rx) = std::sync::mpsc::channel();
            let mut watcher: RecommendedWatcher =
                RecommendedWatcher::new(move |res: NotifyResult<notify::Event>| match res {
                    Ok(event) => {
                        println!("NEW EVENT: {:?}", event);
                        let _ = tx.send(event);
                    }
                    Err(e) => println!("watch error: {:?}", e),
                })?;
            watcher.watch(Path::new(&file), RecursiveMode::Recursive)?;

            let file = std::fs::File::open(file)?;
            let mut f = std::io::BufReader::new(file);
            println!("Reading to the end of the file");

            // TODO: Figure out how to use SeekFrom here.
            loop {
                let mut line = String::new();
                let _ = f.read_line(&mut line);
                if line.is_empty() {
                    break;
                }
            }
            println!("Now at the end of the file! Watching events");

            while let Ok(event) = rx.recv() {
                match event.kind {
                    EventKind::Modify(ModifyKind::Data(_)) => {
                        // TODO: There's certainly a better way to do this.
                        loop {
                            let mut line = String::new();
                            let _ = f.read_line(&mut line)?;
                            if line.is_empty() {
                                break;
                            }
                            if let Some(line) = line.strip_suffix('\n') {
                                let _ = producer.send("", line).await?;
                            }
                        }
                    }
                    other => {
                        println!("OTHER EVENT {:?}", other);
                    }
                }
            }
        } else {
            let stdin = io::stdin();
            for line in stdin.lock().lines() {
                let line = line?;
                let _ = producer.send("", line).await?;
            }
        }

        Ok(())
    }
}
