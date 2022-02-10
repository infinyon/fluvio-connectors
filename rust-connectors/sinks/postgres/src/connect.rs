use std::cmp::Ordering;

use fluvio::{Fluvio, Offset, PartitionConsumer};
use fluvio_model_postgres::{
    Column, DeleteBody, InsertBody, LogicalReplicationMessage, ReplicationEvent, TruncateBody,
    UpdateBody,
};
use postgres_types::Type;
use tokio_stream::StreamExt;

use crate::PgConnectorOpt;
use fluvio_model_postgres::RelationBody;
use std::collections::BTreeMap;
use std::time::Duration;
use tokio_postgres::{Client, NoTls};

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
                        let sql = Self::to_table_insert(table, &insert);
                        sql_statements.push(sql);
                    } else {
                        tracing::error!("Failed to find table for: {:?}", insert);
                    }
                }
                LogicalReplicationMessage::Relation(new_rel) => {
                    if let Some(old_table) = self.relations.get_mut(&new_rel.rel_id) {
                        let alters = Self::to_table_alter(&new_rel, old_table);
                        sql_statements.extend(alters);

                        *old_table = new_rel;
                    } else {
                        let sql = Self::to_table_create(&new_rel);
                        sql_statements.push(sql);
                        self.relations.insert(new_rel.rel_id, new_rel);
                    }
                }
                LogicalReplicationMessage::Delete(delete) => {
                    if let Some(table) = self.relations.get(&delete.rel_id) {
                        let sql = Self::to_delete(table, &delete);
                        sql_statements.push(sql);
                    } else {
                        tracing::error!("Failed to find table for delete: {:?}", delete);
                    }
                }
                LogicalReplicationMessage::Update(update) => {
                    if let Some(table) = self.relations.get_mut(&update.rel_id) {
                        let sql = Self::to_update(table, &update);
                        sql_statements.push(sql);
                    } else {
                        tracing::error!("Failed to find table for update: {:?}", update);
                    }
                }
                LogicalReplicationMessage::Truncate(trunk) => {
                    let sql = Self::to_table_trucate(&self.relations, trunk);
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

    pub fn to_table_trucate(
        relations: &BTreeMap<u32, RelationBody>,
        trunk: TruncateBody,
    ) -> String {
        let table_names: Vec<String> = trunk
            .rel_ids
            .iter()
            .filter_map(|rel_id| relations.get(rel_id).map(|table| table.name.clone()))
            .collect();
        let mut sql = format!("TRUNCATE {}", table_names.join(","));
        if trunk.options == 1 {
            sql.push_str(" CASCADE");
        } else if trunk.options == 2 {
            sql.push_str(" RESTART IDENTITY");
        }
        sql
    }
    pub fn to_table_alter(new_table: &RelationBody, old_table: &RelationBody) -> Vec<String> {
        let mut alters: Vec<String> = Vec::new();
        if new_table.name != old_table.name {
            alters.push(format!(
                "ALTER TABLE {} RENAME TO {}",
                old_table.name, new_table.name
            ));
        }
        match new_table.columns.len().cmp(&old_table.columns.len()) {
            Ordering::Equal => {
                for (new_col, old_col) in new_table.columns.iter().zip(old_table.columns.iter()) {
                    if new_col.name != old_col.name {
                        alters.push(format!(
                            "ALTER TABLE {}.{} RENAME COLUMN {} TO {}",
                            new_table.namespace, new_table.name, old_col.name, new_col.name
                        ));
                    }
                    if new_col.type_id != old_col.type_id {
                        let type_id = new_col.type_id as u32;
                        let column_type = if let Some(column_type) = Type::from_oid(type_id) {
                            column_type
                        } else {
                            tracing::error!("Failed to find type id: {:?}", new_col.type_id);
                            continue;
                        };
                        let column_type = column_type.name();
                        alters.push(format!(
                            "ALTER TABLE {}.{} ALTER COLUMN {} TYPE {}",
                            new_table.namespace, new_table.name, new_col.name, column_type
                        ));
                    }
                }
            }
            Ordering::Greater => {
                // This is a ADD column
                let old_cols: Vec<String> =
                    old_table.columns.iter().map(|c| c.name.clone()).collect();
                let new_columns: Vec<Column> = new_table
                    .columns
                    .clone()
                    .into_iter()
                    .filter(|col| !old_cols.contains(&col.name))
                    .collect();
                for column in new_columns {
                    let type_id = column.type_id as u32;
                    let column_type = if let Some(column_type) = Type::from_oid(type_id) {
                        column_type
                    } else {
                        tracing::error!("Failed to find type id: {:?}", column.type_id);
                        continue;
                    };
                    let column_type = column_type.name();
                    alters.push(format!(
                        "ALTER TABLE {}.{} ADD COLUMN {} {}",
                        new_table.namespace, new_table.name, column.name, column_type
                    ));
                }
            }
            Ordering::Less => {
                // This is a DROP column
                let new_cols: Vec<String> =
                    new_table.columns.iter().map(|c| c.name.clone()).collect();
                let deleted_columns: Vec<Column> = old_table
                    .columns
                    .clone()
                    .into_iter()
                    .filter(|col| !new_cols.contains(&col.name))
                    .collect();
                for i in deleted_columns {
                    alters.push(format!(
                        "ALTER TABLE {}.{} DROP COLUMN {}",
                        new_table.namespace, new_table.name, i.name
                    ));
                }
            }
        }
        alters
    }
    pub fn to_update(table: &RelationBody, update: &UpdateBody) -> String {
        let filter_tuple = match (&update.key_tuple, &update.old_tuple) {
            (Some(tuple), None) => tuple,
            (None, Some(tuple)) => tuple,
            (Some(tuple), Some(_old_tuple)) => {
                tracing::info!("Delete had both key_tuple and old_tuple {:?}", update);
                tuple
            }
            other => {
                unreachable!("This delete case was not handled {:?}", other);
            }
        };
        let mut update_vals: Vec<String> = Vec::new();
        for (column, new_data) in table.columns.iter().zip(update.new_tuple.0.iter()) {
            let val: String = if let Ok(val) = new_data.try_into() {
                val
            } else {
                tracing::error!("Uncaugh tuple type {:?}", new_data);
                continue;
            };
            update_vals.push(format!("{}={}", column.name, val));
        }

        let mut where_vals: Vec<String> = Vec::new();
        for (column, filter) in table.columns.iter().zip(filter_tuple.0.iter()) {
            let val: String = if let Ok(val) = filter.try_into() {
                val
            } else {
                tracing::error!("Uncaugh tuple type {:?}", filter);
                continue;
            };
            where_vals.push(format!("{}={}", column.name, val));
        }
        format!(
            "UPDATE {}.{} SET {} WHERE {}",
            table.namespace,
            table.name,
            update_vals.join(","),
            where_vals.join(" AND ")
        )
    }
    pub fn to_delete(table: &RelationBody, delete: &DeleteBody) -> String {
        let mut where_clauses: Vec<String> = Vec::new();
        let tuple = match (&delete.key_tuple, &delete.old_tuple) {
            (Some(tuple), None) => tuple,
            (None, Some(tuple)) => tuple,
            (Some(tuple), Some(_old_tuple)) => {
                tracing::info!("Delete had both key_tuple and old_tuple {:?}", delete);
                tuple
            }
            other => {
                unreachable!("This delete case was not handled {:?}", other);
            }
        };
        for (column, tuple) in table.columns.iter().zip(tuple.0.iter()) {
            let val: String = if let Ok(val) = tuple.try_into() {
                val
            } else {
                tracing::error!("Uncaugh tuple type {:?}", tuple);
                continue;
            };
            where_clauses.push(format!("{}={}", column.name, val));
        }
        format!(
            "DELETE FROM {}.{} WHERE {}",
            table.namespace,
            table.name,
            where_clauses.join(" AND ")
        )
    }
    pub fn to_table_insert(table: &RelationBody, insert: &InsertBody) -> String {
        let mut values: Vec<String> = Vec::new();
        let mut col_names: Vec<String> = Vec::new();
        for (column, tuple) in table.columns.iter().zip(insert.tuple.0.iter()) {
            let val: String = if let Ok(val) = tuple.try_into() {
                val
            } else {
                tracing::error!("Uncaugh tuple type {:?}", tuple);
                continue;
            };
            values.push(val);
            col_names.push(column.name.clone());
        }
        let values = values.join(",");
        let columns = col_names.join(",");
        format!(
            "INSERT INTO {}.{} ({}) VALUES ({})",
            table.namespace, table.name, columns, values
        )
    }

    pub fn to_table_create(table: &RelationBody) -> String {
        let mut primary_keys: Vec<String> = Vec::new();
        let mut columns: Vec<String> = Vec::new();
        for column in table.columns.iter() {
            let type_id = column.type_id as u32;

            let column_type = if let Some(column_type) = Type::from_oid(type_id) {
                column_type
            } else {
                tracing::error!("Failed to find type id: {:?}", column.type_id);
                continue;
            };
            let column_type = column_type.name();
            let column_name = &column.name;
            if column.flags == 1 {
                primary_keys.push(column_name.clone());
            }
            columns.push(format!("{} {}", column_name, column_type));
        }
        if !primary_keys.is_empty() {
            columns.push(format!("PRIMARY KEY ({})", primary_keys.join(",")));
        }
        let columns = columns.join(",");
        format!(
            "CREATE TABLE {}.{} ({})",
            table.namespace, table.name, columns
        )
    }
}
