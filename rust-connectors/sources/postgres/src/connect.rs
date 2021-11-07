use crate::model::{Column, LogicalReplicationMessage, ReplicationEvent};
use crate::PgConnectorOpt;
use fluvio::{Fluvio, Offset, TopicProducer};
use once_cell::sync::Lazy;
use postgres_protocol::message::backend::{
    LogicalReplicationMessage as PgReplication, ReplicationMessage,
};
use std::collections::BTreeMap;
use std::pin::Pin;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio_postgres::config::ReplicationMode;
use tokio_postgres::replication::LogicalReplicationStream;
use tokio_postgres::types::PgLsn;
use tokio_postgres::{Client, NoTls};
use tokio_stream::StreamExt;

const TIME_SEC_CONVERSION: u64 = 946_684_800;
static EPOCH: Lazy<SystemTime> =
    Lazy::new(|| UNIX_EPOCH + Duration::from_secs(TIME_SEC_CONVERSION));

/// A Fluvio connector for Postgres CDC.
pub struct PgConnector {
    /// The connector configuration.
    config: PgConnectorOpt,
    /// The Postgres client for streaming replication changes.
    pg_client: Client,
    /// The Fluvio producer for recording change events.
    producer: TopicProducer,
    /// The current Log Sequence Number (offset) to read in the replication stream.
    lsn: Option<PgLsn>,
    /// Caches the schema for each new table we see, grouped by relation_id
    relations: BTreeMap<u32, Vec<Column>>,
}

impl PgConnector {
    pub async fn new(config: PgConnectorOpt) -> eyre::Result<Self> {
        tracing::info!("Initializing PgConnector");
        let fluvio = Fluvio::connect().await?;
        tracing::info!("Connected to Fluvio");
        let consumer = fluvio.partition_consumer(&config.topic, 0).await?;
        let mut lsn: Option<PgLsn> = None;

        // Try to get the last item from the Fluvio Topic. Timeout after 1 second
        let stream = consumer.stream(Offset::from_end(1)).await?;
        let timeout = stream.timeout(Duration::from_millis(1_000));
        tokio::pin!(timeout);

        let last_record = StreamExt::try_next(&mut timeout)
            .await
            .ok()
            .flatten()
            .transpose()?;

        if let Some(record) = last_record {
            let event = serde_json::from_slice::<ReplicationEvent>(record.value())?;
            let offset = PgLsn::from(event.wal_end);
            lsn = Some(offset);
            tracing::info!(lsn = event.wal_end, "Discovered LSN to resume PgConnector:");
        } else {
            tracing::info!("No prior LSN discovered, starting PgConnector at beginning");
        }

        let producer = fluvio.topic_producer(&config.topic).await?;

        let (pg_client, conn) = config
            .url
            .as_str()
            .parse::<tokio_postgres::Config>()?
            .replication_mode(ReplicationMode::Logical)
            .connect(NoTls)
            .await?;
        tokio::spawn(conn);
        tracing::info!("Connected to Postgres");

        Ok(Self {
            config,
            pg_client,
            producer,
            lsn,
            relations: BTreeMap::default(),
        })
    }

    pub async fn process_stream(&mut self) -> eyre::Result<()> {
        let mut last_lsn = self.lsn.unwrap_or(PgLsn::from(0));

        // We now switch to consuming the stream
        let options = format!(
            r#"("proto_version" '1', "publication_names" '{}')"#,
            self.config.publication
        );
        let query = format!(
            r#"START_REPLICATION SLOT "{}" LOGICAL {} {}"#,
            self.config.slot, last_lsn, options
        );
        let copy_stream = self
            .pg_client
            .copy_both_simple::<bytes::Bytes>(&query)
            .await
            .map_err(|source| crate::Error::PostgresReplication {
                publication: self.config.publication.to_string(),
                slot: self.config.slot.to_string(),
                source,
            })?;

        let stream = LogicalReplicationStream::new(copy_stream);
        tokio::pin!(stream);

        while let Some(replication_message) = stream.try_next().await? {
            let result = self
                .process_event(stream.as_mut(), replication_message, &mut last_lsn)
                .await;

            if let Err(e) = result {
                tracing::error!("PgConnector error: {:#}", e);
            }
        }

        Ok(())
    }

    async fn process_event(
        &mut self,
        mut stream: Pin<&mut LogicalReplicationStream>,
        event: ReplicationMessage<PgReplication>,
        last_lsn: &mut PgLsn,
    ) -> eyre::Result<()> {
        match event {
            ReplicationMessage::XLogData(xlog_data) => {
                let event = ReplicationEvent::from_pg_event(&self.relations, &xlog_data)?;
                let json = serde_json::to_string(&event)?;

                tracing::info!("Producing event: {}", json);
                self.producer.send(fluvio::RecordKey::NULL, json).await?;

                match event.message {
                    LogicalReplicationMessage::Relation(rel) => {
                        self.relations.insert(rel.rel_id, rel.columns);
                    }
                    LogicalReplicationMessage::Commit(commit) => {
                        *last_lsn = commit.commit_lsn.into();
                    }
                    _ => {}
                }
            }
            ReplicationMessage::PrimaryKeepAlive(keepalive) => {
                if keepalive.reply() == 1 {
                    tracing::debug!("Sending keepalive response");
                    let ts = EPOCH.elapsed().unwrap().as_micros() as i64;
                    stream
                        .as_mut()
                        .standby_status_update(*last_lsn, *last_lsn, *last_lsn, ts, 0)
                        .await?;
                }
            }
            _ => (),
        }

        Ok(())
    }
}
