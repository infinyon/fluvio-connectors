use fluvio_connectors_common::opt::CommonSourceOpt;
use schemars::JsonSchema;
use std::collections::HashMap;
use structopt::StructOpt;
use url::Url;

use fluvio::metadata::topic::TopicSpec;
use fluvio::{Fluvio, Offset, PartitionConsumer};
use fluvio_model_postgres::{
    Column, DeleteBody, InsertBody, LogicalReplicationMessage, ReplicationEvent, TupleData,
};
use tokio_stream::StreamExt;

use std::collections::BTreeMap;
use tokio_postgres::{Client, NoTls};

/// The Postgres CDC Connector for Fluvio.
#[derive(StructOpt, Debug, JsonSchema, Clone)]
#[structopt(settings = &[structopt::clap::AppSettings::DeriveDisplayOrder])]
pub struct PgConnectorOpt {
    /// The URL of the postgres leader database to stream from
    ///
    /// Ex: postgres://user:password@localhost:5432/database_name
    #[structopt(long, env = "FLUVIO_PG_DATABASE_URL", hide_env_values = true)]
    pub url: Url,
    /*
    /// The name of the PUBLICATION in the leader database to monitor
    ///
    /// Ex: fluvio_cdc
    ///
    /// Before using this connector, you will need to create a publication
    /// in your leader database, using a SQL command such as
    /// "CREATE PUBLICATION fluvio_cdc FOR ALL TABLES;"
    #[structopt(long, env = "FLUVIO_PG_PUBLICATION")]
    pub publication: String,
    /// The name of the logical replication slot to stream changes from
    #[structopt(long, env = "FLUVIO_PG_SLOT")]
    pub slot: String,
    */
    /// The time (in millis) to wait while fetching latest Fluvio record to resume
    #[structopt(long, env = "FLUVIO_PG_RESUME_TIMEOUT", default_value = "1000")]
    pub resume_timeout: u64,

    #[structopt(long)]
    pub skip_setup: bool,

    #[structopt(flatten)]
    #[schemars(flatten)]
    pub common: CommonSourceOpt,
}

impl PgConnectorOpt {}

/// A Fluvio connector for Postgres CDC.
pub struct PgConnector {
    /// The connector configuration.
    config: PgConnectorOpt,
    /// The Postgres client for streaming replication changes.
    pg_client: Client,
    /// The Fluvio producer for recording change events.
    consumer: PartitionConsumer,
    /// The current Log Sequence Number (offset) to read in the replication stream.
    //lsn: Option<PgLsn>,
    /// Caches the schema for each new table we see, grouped by relation_id
    relations: BTreeMap<u32, Table>,
}

#[derive(Debug)]
pub struct Table {
    name: String,
    columns: Vec<Column>,
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
            config,
            pg_client,
            relations: BTreeMap::new(),
        })
    }
    pub async fn start(&mut self) -> eyre::Result<()> {
        let mut stream = self.consumer.stream(Offset::from_beginning(0)).await?;
        while let Some(Ok(next)) = stream.next().await {
            let next = next.value();
            let event: ReplicationEvent = serde_json::de::from_slice(next)?;
            match event.message {
                LogicalReplicationMessage::Insert(insert) => {
                    if let Some(table) = self.relations.get(&insert.rel_id) {
                        let sql = Self::to_table_insert(table, &insert);
                        if let Err(e) = self.pg_client.execute(sql.as_str(), &[]).await {
                            tracing::error!("Error with insert: {:?}", e);
                        }
                        tracing::info!("TABLE insert: {:?}", sql);
                    } else {
                        tracing::error!("Failed to find table for: {:?}", insert);
                    }
                }
                LogicalReplicationMessage::Relation(rel) => {
                    let new_table = Table {
                        name: rel.name,
                        columns: rel.columns,
                    };
                    if let Some(old_table) = self.relations.get_mut(&rel.rel_id) {
                        tracing::error!("TABLE ALTER for : {:?} to {:?}", new_table, *old_table);
                        let alters = Self::to_table_alter(&new_table, old_table);
                        for sql in alters {
                            tracing::info!("TABLE ALTER: {:?}", sql);
                            if let Err(e) = self.pg_client.execute(sql.as_str(), &[]).await {
                                tracing::error!("Error with table create: {:?}", e);
                            }
                        }

                        *old_table = new_table;
                    } else {
                        let sql = Self::to_table_create(&new_table);
                        tracing::info!("TABLE CREATE: {:?}", sql);
                        if let Err(e) = self.pg_client.execute(sql.as_str(), &[]).await {
                            tracing::error!("Error with table create: {:?}", e);
                        }
                        self.relations.insert(rel.rel_id, new_table);
                    }
                }
                LogicalReplicationMessage::Commit(_) | LogicalReplicationMessage::Begin(_) => {}
                LogicalReplicationMessage::Delete(delete) => {
                    if let Some(table) = self.relations.get(&delete.rel_id) {
                        let sql = Self::to_delete(table, &delete);
                        tracing::info!("TABLE delete: {:?}", sql);
                        if let Err(e) = self.pg_client.execute(sql.as_str(), &[]).await {
                            tracing::error!("Error with delete: {:?}", e);
                        }
                    } else {
                        tracing::error!("Failed to find table for delete: {:?}", delete);
                    }
                }
                other => {
                    tracing::error!("Uncaught replication message: {:?}", other);
                }
            }
        }
        Ok(())
    }
    pub fn to_table_alter(new_table: &Table, old_table: &Table) -> Vec<String> {
        let mut alters: Vec<String> = Vec::new();
        if new_table.columns.len() == old_table.columns.len() {
            if new_table.name != old_table.name {
                alters.push(format!(
                    "ALTER TABLE {} RENAME TO {}",
                    old_table.name, new_table.name
                ));
            } else {
                for (new_col, old_col) in new_table.columns.iter().zip(old_table.columns.iter()) {
                    if new_col.name != old_col.name {
                        alters.push(format!(
                            "ALTER TABLE {} RENAME COLUMN {} TO {}",
                            new_table.name, old_col.name, new_col.name
                        ));
                    }
                    if new_col.type_id != old_col.type_id {
                        let column_type = if let Some(column_type) = TYPE_MAP.get(&new_col.type_id)
                        {
                            column_type
                        } else {
                            tracing::error!(
                                "Failed to find type id: {:?} for column alter",
                                new_col.type_id
                            );
                            continue;
                        };
                        alters.push(format!(
                            "ALTER TABLE {} ALTER COLUMN {} TYPE {}",
                            new_table.name, new_col.name, column_type
                        ));
                    }
                }
            }
        } else if new_table.columns.len() > old_table.columns.len() {
            // This is a ADD column
            let old_cols: Vec<String> = old_table.columns.iter().map(|c| c.name.clone()).collect();
            let new_columns: Vec<Column> = new_table
                .columns
                .clone()
                .into_iter()
                .filter(|col| !old_cols.contains(&col.name))
                .collect();
            for column in new_columns {
                let column_type = if let Some(column_type) = TYPE_MAP.get(&column.type_id) {
                    column_type
                } else {
                    tracing::error!("Failed to find type id: {:?}", column.type_id);
                    continue;
                };
                alters.push(format!(
                    "ALTER TABLE {} ADD COLUMN {} {}",
                    new_table.name, column.name, column_type
                ));
            }
        } else if new_table.columns.len() < old_table.columns.len() {
            // This is a DROP column
            let new_cols: Vec<String> = new_table.columns.iter().map(|c| c.name.clone()).collect();
            let deleted_columns: Vec<Column> = old_table
                .columns
                .clone()
                .into_iter()
                .filter(|col| !new_cols.contains(&col.name))
                .collect();
            for i in deleted_columns {
                alters.push(format!(
                    "ALTER TABLE {} DROP COLUMN {}",
                    new_table.name, i.name
                ));
            }
        } else {
            unreachable!("Column lengths not checked correctly!");
        }
        alters
    }
    pub fn to_delete(table: &Table, delete: &DeleteBody) -> String {
        let mut where_clauses: Vec<String> = Vec::new();
        if let Some(key_tuple) = &delete.key_tuple {
            for (column, tuple) in table.columns.iter().zip(key_tuple.0.iter()) {
                let val = match tuple {
                    TupleData::Bool(v) => format!("{v}"),
                    TupleData::Char(c) => format!("{c}"),
                    TupleData::Int2(i) => format!("{i}"),
                    TupleData::Int4(i) => format!("{i}"),
                    TupleData::Int8(i) => format!("{i}"),
                    TupleData::Oid(i) => format!("{i}"),
                    TupleData::Float4(i) => format!("{i}"),
                    TupleData::Float8(i) => format!("{i}"),
                    TupleData::String(i) => format!("'{i}'"),
                    other => {
                        tracing::error!("Uncaugh tuple type {:?}", other);
                        continue;
                    }
                };
                where_clauses.push(format!("{}={}", column.name, val));
            }
        } else {
            unreachable!("This delete case was not handled for {:?}", delete);
        }
        format!(
            "DELETE FROM {} WHERE {}",
            table.name,
            where_clauses.join(" AND ")
        )
    }
    pub fn to_table_insert(table: &Table, insert: &InsertBody) -> String {
        let mut values: Vec<String> = Vec::new();
        let mut col_names: Vec<String> = Vec::new();
        for (column, tuple) in table.columns.iter().zip(insert.tuple.0.iter()) {
            match tuple {
                TupleData::Bool(v) => values.push(format!("{v}")),
                TupleData::Char(c) => values.push(format!("{c}")),
                TupleData::Int2(i) => values.push(format!("{i}")),
                TupleData::Int4(i) => values.push(format!("{i}")),
                TupleData::Int8(i) => values.push(format!("{i}")),
                TupleData::Oid(i) => values.push(format!("{i}")),
                TupleData::Float4(i) => values.push(format!("{i}")),
                TupleData::Float8(i) => values.push(format!("{i}")),
                TupleData::String(i) => values.push(format!("'{i}'")),
                other => {
                    tracing::error!("Uncaugh tuple type {:?}", other);
                    continue;
                }
            }
            col_names.push(column.name.clone());
        }
        let values = values.join(",");
        let columns = col_names.join(",");
        format!(
            "INSERT INTO {} ({}) VALUES ({})",
            table.name, columns, values
        )
    }

    pub fn to_table_create(table: &Table) -> String {
        let mut primary_keys: Vec<String> = Vec::new();
        let mut columns: Vec<String> = Vec::new();
        for column in table.columns.iter() {
            let column_type = if let Some(column_type) = TYPE_MAP.get(&column.type_id) {
                column_type
            } else {
                tracing::error!("Failed to find type id: {:?}", column.type_id);
                continue;
            };
            let column_name = &column.name;
            if column.flags == 1 {
                primary_keys.push(column_name.clone());
            }
            columns.push(format!("{} {}", column_name, column_type));
        }
        /*
        if !primary_keys.is_empty() {
            columns.push(format!("PRIMARY KEY ({})", primary_keys.join(",")));
        }
        */
        let columns = columns.join(",");
        format!("CREATE TABLE {}({})", table.name, columns)
    }
}

lazy_static! {
    static ref TYPE_MAP: HashMap<i32, &'static str> = HashMap::from([
        (16, "boolean"),
        (17, "bytea"),
        (20, "bigint"),
        (20, "bigserial"),
        (21, "smallint"),
        (21, "smallserial"),
        (23, "integer"),
        (25, "text"),
        (114, "json"),
        (142, "xml"),
        (600, "point"),
        (601, "lseg"),
        (602, "path"),
        (603, "box"),
        (604, "polygon"),
        (628, "line"),
        (650, "cidr"),
        (700, "real"),
        (701, "float8"),
        (718, "circle"),
        (790, "money"),
        (829, "macaddr"),
        (869, "inet"),
        (1042, "character"),
        (1043, "varchar"),
        (1082, "date"),
        (1083, "time"),
        (1114, "timestamp"),
        (1186, "interval"),
        (1560, "bit"),
        (1700, "numeric"),
        (2950, "uuid"),
        (2970, "txid_snapshot"),
        (3220, "pg_lsn"),
        (3614, "tsvector"),
        (3615, "tsquery"),
        (3802, "jsonb"),
    ]);
}
