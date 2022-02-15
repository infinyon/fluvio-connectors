
use fluvio::{Fluvio, Offset, PartitionConsumer};
use tokio_stream::StreamExt;

use crate::PgConnectorOpt;
use fluvio_model_postgres::RelationBody;
use std::collections::BTreeMap;
use std::time::Duration;
use tokio_postgres::{Client, NoTls};
use fluvio_model_postgres::{
    LogicalReplicationMessage, ReplicationEvent,
    convert,
};

/// A Fluvio connector for Postgres CDC.
pub struct PgConnector {
    /// The Postgres client for streaming replication changes.
    pg_client: Client,
    /// The Fluvio producer for recording change events.
    consumer: PartitionConsumer,
    /// The current Log Sequence Number (offset) to read in the replication stream.
    //lsn: Option<PgLsn>,
    /// Caches the schema for each new table we see, grouped by relation_id
    relations: BTreeMap<u32, RelationBody>,
}

impl PgConnector {
    pub async fn new(config: PgConnectorOpt) -> eyre::Result<Self> {
        let fluvio = Fluvio::connect().await?;
        let consumer = fluvio
            .partition_consumer(&config.common.fluvio_topic, 0)
            .await?;
        let (pg_client, conn) = config
            .url
            .as_str()
            .parse::<tokio_postgres::Config>()?
            .connect(NoTls)
            .await?;
        tokio::spawn(conn);
        Ok(Self {
            consumer,
            pg_client,
            relations: BTreeMap::new(),
        })
    }
    pub async fn get_offset(&self) -> eyre::Result<i64> {
        let schema_create = "CREATE SCHEMA IF NOT EXISTS fluvio";
        let _ = self.pg_client.execute(schema_create, &[]).await?;
        let table_create = "CREATE TABLE IF NOT EXISTS fluvio.offset (id INT8 PRIMARY KEY, current_offset INT8 NOT NULL)";
        let _ = self.pg_client.execute(table_create, &[]).await?;
        let sql = "select * from fluvio.offset where id = 1";
        let rows = self.pg_client.query(sql, &[]).await?;
        if let Some(offset) = rows.first() {
            let current_offset: i64 = offset.get("current_offset");
            Ok(current_offset)
        } else {
            let current_offset = 0;
            let sql = "INSERT INTO fluvio.offset (id, current_offset) VALUES(1, $1);";
            let _ = self.pg_client.execute(sql, &[&current_offset]).await?;
            Ok(current_offset)
        }
    }

    pub async fn get_relations(&mut self, offset: i64) -> eyre::Result<()> {
        let stream = self.consumer.stream(Offset::from_beginning(0)).await?;
        let stream = stream.timeout(Duration::from_millis(100));
        tokio::pin!(stream);
        while let Some(Ok(Ok(record))) = stream.next().await {
            if record.offset >= offset {
                break;
            }
            let next = record.value();
            let event: ReplicationEvent = serde_json::de::from_slice(next)?;
            if let LogicalReplicationMessage::Relation(new_rel) = event.message {
                self.relations.insert(new_rel.rel_id, new_rel);
            }
        }

        Ok(())
    }
    pub async fn process_stream(&mut self) -> eyre::Result<()> {
        let offset = self.get_offset().await?;
        if offset >= 0 {
            self.get_relations(offset).await?;
        }
        // Offset needs to be one more than the last.
        let offset = offset + 1;
        let mut stream = self
            .consumer
            .stream(Offset::from_beginning(offset.try_into().unwrap()))
            .await?;
        while let Some(Ok(next)) = stream.next().await {
            let offset = next.offset;
            let next = next.value();
            let event: ReplicationEvent = match serde_json::de::from_slice(next) {
                Ok(next) => next,
                Err(e) => {
                    tracing::error!("Error deseralizing ReplicationEvent {:?}", e);
                    continue;
                }
            };
            let mut sql_statements: Vec<String> = Vec::new();
            match event.message {
                LogicalReplicationMessage::Insert(insert) => {
                    if let Some(table) = self.relations.get(&insert.rel_id) {
                        let sql = convert::to_table_insert(table, &insert);
                        sql_statements.push(sql);
                    } else {
                        tracing::error!("Failed to find table for: {:?}", insert);
                    }
                }
                LogicalReplicationMessage::Relation(new_rel) => {
                    if let Some(old_table) = self.relations.get_mut(&new_rel.rel_id) {
                        let alters = convert::to_table_alter(&new_rel, &old_table);
                        sql_statements.extend(alters);

                        *old_table = new_rel;
                    } else {
                        let sql = convert::to_table_create(&new_rel);
                        sql_statements.push(sql);
                        self.relations.insert(new_rel.rel_id, new_rel);
                    }
                }
                LogicalReplicationMessage::Delete(delete) => {
                    if let Some(table) = self.relations.get(&delete.rel_id) {
                        let sql = convert::to_delete(table, &delete);
                        sql_statements.push(sql);
                    } else {
                        tracing::error!("Failed to find table for delete: {:?}", delete);
                    }
                }
                LogicalReplicationMessage::Update(update) => {
                    if let Some(table) = self.relations.get_mut(&update.rel_id) {
                        let sql = convert::to_update(table, &update);
                        sql_statements.push(sql);
                    } else {
                        tracing::error!("Failed to find table for update: {:?}", update);
                    }
                }
                LogicalReplicationMessage::Truncate(trunk) => {
                    let sql = convert::to_table_trucate(&self.relations, trunk);
                    sql_statements.push(sql);
                }
                LogicalReplicationMessage::Commit(_) | LogicalReplicationMessage::Begin(_) => {}
                other => {
                    tracing::error!("Uncaught replication message: {:?}", other);
                }
            }
            if !sql_statements.is_empty() {
                let sql = format!(
                    "UPDATE fluvio.offset SET current_offset={} where id = 1",
                    offset
                );
                sql_statements.push(sql);
                let batch = sql_statements.join(";");
                tracing::info!("executing sql: {:?}", batch);
                self.pg_client.batch_execute(&batch).await?;
            }
        }
        Ok(())
    }
}
