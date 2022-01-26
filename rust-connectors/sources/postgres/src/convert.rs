use crate::{Error, Result};
use fluvio_model_postgres::*;
use postgres_protocol::message::backend::Column as PgColumn;
use postgres_protocol::message::backend::ReplicaIdentity as PgId;
use postgres_protocol::message::backend::Tuple as PgTuple;
use postgres_protocol::message::backend::TupleData as PgTupleData;
use postgres_protocol::message::backend::{
    LogicalReplicationMessage as PgReplication, XLogDataBody,
};
use std::collections::BTreeMap;
use tokio_postgres::types::Type;

pub fn convert_replication_event(
    schemas: &BTreeMap<u32, Vec<Column>>,
    event: &XLogDataBody<PgReplication>,
) -> Result<ReplicationEvent> {
    let message = convert_replication_message(schemas, event.data())?;
    let data = ReplicationEvent {
        wal_start: event.wal_start(),
        wal_end: event.wal_end(),
        timestamp: event.timestamp(),
        message,
    };
    Ok(data)
}

pub fn convert_replication_message(
    schemas: &BTreeMap<u32, Vec<Column>>,
    pg_msg: &PgReplication,
) -> Result<LogicalReplicationMessage> {
    let logical_replication_message = match pg_msg {
        PgReplication::Begin(begin) => {
            let body = BeginBody {
                xid: begin.xid(),
                final_lsn: begin.final_lsn(),
                timestamp: begin.timestamp(),
            };
            LogicalReplicationMessage::Begin(body)
        }
        PgReplication::Commit(commit) => {
            let body = CommitBody {
                end_lsn: commit.end_lsn(),
                commit_lsn: commit.commit_lsn(),
                flags: commit.flags(),
                timestamp: commit.timestamp(),
            };
            LogicalReplicationMessage::Commit(body)
        }
        PgReplication::Origin(origin) => {
            let body = OriginBody {
                commit_lsn: origin.commit_lsn(),
                name: origin.name().map_err(Error::InvalidString)?.to_string(),
            };
            LogicalReplicationMessage::Origin(body)
        }
        PgReplication::Relation(rel) => {
            let columns = rel
                .columns()
                .iter()
                .map(convert_column)
                .collect::<core::result::Result<Vec<_>, _>>()?;
            let body = RelationBody {
                rel_id: rel.rel_id(),
                namespace: rel.namespace().map_err(Error::InvalidString)?.to_string(),
                name: rel.name().map_err(Error::InvalidString)?.to_string(),
                replica_identity: convert_replica_identity(rel.replica_identity()),
                columns,
            };
            LogicalReplicationMessage::Relation(body)
        }
        PgReplication::Type(ty) => {
            let body = TypeBody {
                id: ty.id(),
                namespace: ty.name().map_err(Error::InvalidString)?.to_string(),
                name: ty.name().map_err(Error::InvalidString)?.to_string(),
            };
            LogicalReplicationMessage::Type(body)
        }
        PgReplication::Insert(insert) => {
            let schema = schemas
                .get(&insert.rel_id())
                .ok_or_else(|| Error::MissingSchema(insert.rel_id()))?;
            let typed_tuple = convert_tuple(schema, insert.tuple())?;
            let body = InsertBody {
                rel_id: insert.rel_id(),
                tuple: typed_tuple,
            };
            LogicalReplicationMessage::Insert(body)
        }
        PgReplication::Update(update) => {
            let schema = schemas
                .get(&update.rel_id())
                .ok_or_else(|| Error::MissingSchema(update.rel_id()))?;
            let old_tuple = update
                .old_tuple()
                .map(|t| convert_tuple(schema, t))
                .transpose()?;
            let key_tuple = update
                .key_tuple()
                .map(|t| convert_tuple(schema, t))
                .transpose()?;
            let new_tuple = convert_tuple(schema, update.new_tuple())?;

            let body = UpdateBody {
                rel_id: update.rel_id(),
                old_tuple,
                key_tuple,
                new_tuple,
            };
            LogicalReplicationMessage::Update(body)
        }
        PgReplication::Delete(delete) => {
            let schema = schemas
                .get(&delete.rel_id())
                .ok_or_else(|| Error::MissingSchema(delete.rel_id()))?;
            let old_tuple = delete
                .old_tuple()
                .map(|t| convert_tuple(schema, t))
                .transpose()?;
            let key_tuple = delete
                .key_tuple()
                .map(|t| convert_tuple(schema, t))
                .transpose()?;

            let body = DeleteBody {
                rel_id: delete.rel_id(),
                old_tuple,
                key_tuple,
            };
            LogicalReplicationMessage::Delete(body)
        }
        PgReplication::Truncate(truncate) => {
            let body = TruncateBody {
                options: truncate.options(),
                rel_ids: truncate.rel_ids().to_vec(),
            };
            LogicalReplicationMessage::Truncate(body)
        }
        e => {
            tracing::error!("Unexpected message! {:?}", e);
            return Err(Error::UnexpectedMessage);
        }
    };
    Ok(logical_replication_message)
}

fn convert_tuple(schema: &[Column], tuple: &PgTuple) -> Result<Tuple> {
    let mut typed_tuple_data = Vec::with_capacity(schema.len());

    let iter = schema.iter().zip(tuple.tuple_data().iter());
    for (column, data) in iter {
        let tid = column.type_id as u32;
        let ty = Type::from_oid(tid).ok_or(Error::UnrecognizedType(tid))?;
        let typed_data = convert_tuple_data(&ty, data)?;
        typed_tuple_data.push(typed_data);
    }

    Ok(Tuple(typed_tuple_data))
}

pub fn convert_tuple_data(ty: &Type, data: &PgTupleData) -> Result<TupleData> {
    let data = match data {
        PgTupleData::Null => TupleData::Null,
        PgTupleData::UnchangedToast => TupleData::UnchangedToast,
        PgTupleData::Text(text) => {
            let string =
                std::str::from_utf8(text.as_ref()).map_err(|e| Error::ParseError(e.to_string()))?;
            match *ty {
                Type::BOOL => TupleData::Bool(
                    string
                        .parse::<bool>()
                        .map_err(|e| Error::ParseError(e.to_string()))?,
                ),
                Type::CHAR => TupleData::Char(
                    string
                        .parse::<i8>()
                        .map_err(|e| Error::ParseError(e.to_string()))?,
                ),
                Type::INT2 => TupleData::Int2(
                    string
                        .parse::<i16>()
                        .map_err(|e| Error::ParseError(e.to_string()))?,
                ),
                Type::INT4 => TupleData::Int4(
                    string
                        .parse::<i32>()
                        .map_err(|e| Error::ParseError(e.to_string()))?,
                ),
                Type::OID => TupleData::Oid(
                    string
                        .parse::<u32>()
                        .map_err(|e| Error::ParseError(e.to_string()))?,
                ),
                Type::INT8 => TupleData::Int8(
                    string
                        .parse::<i64>()
                        .map_err(|e| Error::ParseError(e.to_string()))?,
                ),
                Type::FLOAT4 => TupleData::Float4(
                    string
                        .parse::<f32>()
                        .map_err(|e| Error::ParseError(e.to_string()))?,
                ),
                Type::FLOAT8 => TupleData::Float8(
                    string
                        .parse::<f64>()
                        .map_err(|e| Error::ParseError(e.to_string()))?,
                ),
                Type::VARCHAR | Type::TEXT | Type::BPCHAR | Type::NAME | Type::UNKNOWN => {
                    TupleData::String(string.to_string())
                }
                ref ty if ty.name() == "citext" => TupleData::String(string.to_string()),
                _ => TupleData::RawText(text.to_vec()),
            }
        }
    };
    Ok(data)
}

pub fn convert_column(value: &PgColumn) -> Result<Column, Error> {
    let column = Column {
        flags: value.flags(),
        name: value.name().map_err(Error::InvalidString)?.to_string(),
        type_id: value.type_id(),
        type_modifier: value.type_modifier(),
    };
    Ok(column)
}

pub fn convert_replica_identity(id: &PgId) -> ReplicaIdentity {
    match id {
        PgId::Default => ReplicaIdentity::Default,
        PgId::Nothing => ReplicaIdentity::Nothing,
        PgId::Full => ReplicaIdentity::Full,
        PgId::Index => ReplicaIdentity::Index,
    }
}
