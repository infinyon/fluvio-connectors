use serde::{Deserialize, Serialize};

/// One top-level event from the Postgres logical replication stream.
#[derive(Debug, Serialize, Deserialize)]
pub struct ReplicationEvent {
    /// THe Postgres WAL offset before this event.
    pub wal_start: u64,
    /// THe Postgres WAL offset after this event.
    pub wal_end: u64,
    /// The timestamp when this event occurred.
    pub timestamp: i64,
    /// The payload of the event, describing what took place.
    pub message: LogicalReplicationMessage,
}

/// The payload types of a logical replication event.
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum LogicalReplicationMessage {
    /// The beginning of a transaction.
    Begin(BeginBody),
    /// The ending of a transaction that has been successfully committed.
    Commit(CommitBody),
    /// Indicates that this transaction began on a different replication node.
    Origin(OriginBody),
    /// Describes the schema of a relation (e.g. table).
    Relation(RelationBody),
    /// A Type replication message
    Type(TypeBody),
    /// A new row was inserted to the specified relation with the contained data.
    Insert(InsertBody),
    /// A row was updated in the specified relation with the contained data.
    Update(UpdateBody),
    /// A row was deleted in the specified relation with the contained data and key.
    Delete(DeleteBody),
    /// The given relations were truncated
    Truncate(TruncateBody),
}

/// A row as it appears in the replication stream
#[derive(Debug, Serialize, Deserialize)]
pub struct Tuple(pub Vec<TupleData>);

/// A column as it appears in the replication stream
#[derive(Debug, Serialize, Deserialize)]
pub struct Column {
    /// Currently may be 0 or 1, representing whether this column is part of the key.
    pub flags: i8,
    /// The name of this column.
    pub name: String,
    /// The OID of this type.
    pub type_id: i32,
    pub type_modifier: i32,
}

/// The data of an individual column as it appears in the replication stream
#[derive(Debug, Serialize, Deserialize)]
pub enum TupleData {
    /// Represents a NULL value
    Null,
    /// Represents an unchanged TOASTed value (the actual value is not sent).
    UnchangedToast,
    /// The column contains BOOL
    Bool(bool),
    /// The column contains CHAR
    Char(i8),
    /// The column contains INT2
    Int2(i16),
    /// The column contains INT4
    Int4(i32),
    /// The column contains OID
    Oid(u32),
    /// The column contains INT8
    Int8(i64),
    /// The column contains FLOAT4
    Float4(f32),
    /// The column contains FLOAT8
    Float8(f64),
    /// Represents a UTF-8 string type (VARCHAR, TEXT, BPCHAR, NAME, UNKNOWN, or CITEXT)
    String(String),
    /// The raw column data as a binary string
    RawText(Vec<u8>),
}

/// A BEGIN statement
#[derive(Debug, Serialize, Deserialize)]
pub struct BeginBody {
    /// The final logical replication offset (LSN) of the transaction.
    pub final_lsn: u64,
    /// The commit timestamp of this transaction, in microseconds since 2000-01-01.
    pub timestamp: i64,
    /// The XID of the transaction.
    pub xid: u32,
}

/// The contents of a COMMIT statement
#[derive(Debug, Serialize, Deserialize)]
pub struct CommitBody {
    /// Currently unused, should always be 0.
    pub flags: i8,
    /// The logical replication offset (LSN) of this commit.
    pub commit_lsn: u64,
    /// The end offset (LSN) of this transaction.
    pub end_lsn: u64,
    /// The commit timestamp of this transaction, in microseconds since 2000-01-01.
    pub timestamp: i64,
}

/// The contents of an ORIGIN statement
///
/// ORIGIN may be used in a cascading replication configuration, where follower
/// databases may also publish their logical replication stream to _other_
/// downstream followers. In this case, the ORIGIN statement is used to provide
/// the name of the upstream leader database where the following events originated.
#[derive(Debug, Serialize, Deserialize)]
pub struct OriginBody {
    pub commit_lsn: u64,
    pub name: String,
}

/// Describes the REPLICA IDENTITY setting of a table
#[derive(Debug, Serialize, Deserialize)]
pub enum ReplicaIdentity {
    /// default selection for replica identity (primary key or nothing)
    Default,
    /// no replica identity is logged for this relation
    Nothing,
    /// all columns are logged as replica identity
    Full,
    /// An explicitly chosen candidate key's columns are used as replica identity.
    /// Note this will still be set if the index has been dropped; in that case it
    /// has the same meaning as 'd'.
    Index,
}

/// A RELATION replication message
#[derive(Debug, Serialize, Deserialize)]
pub struct RelationBody {
    /// The OID of the relation (e.g. table) being referred to.
    pub rel_id: u32,
    /// The namespace (i.e. schema) of this relation.
    pub namespace: String,
    /// The name (i.e. table name) of this relation.
    pub name: String,
    /// The identity mode (e.g. primary key or all columns) of this relation.
    pub replica_identity: ReplicaIdentity,
    /// The types of the columns in this relation.
    pub columns: Vec<Column>,
}

/// A Type replication message
#[derive(Debug, Serialize, Deserialize)]
pub struct TypeBody {
    pub id: u32,
    pub namespace: String,
    pub name: String,
}

/// An INSERT statement
#[derive(Debug, Serialize, Deserialize)]
pub struct InsertBody {
    /// The ID of the relation the data is inserted to (i.e. table OID).
    pub rel_id: u32,
    /// The tuple of data being inserted.
    pub tuple: Tuple,
}

/// An UPDATE statement
#[derive(Debug, Serialize, Deserialize)]
pub struct UpdateBody {
    /// The ID of the relation the data is being updated in (i.e. table OID).
    pub rel_id: u32,
    /// The tuple of data that used to belong in this row.
    pub old_tuple: Option<Tuple>,
    /// A tuple of the key columns of this relation.
    pub key_tuple: Option<Tuple>,
    /// The tuple of new data being applied to this row.
    pub new_tuple: Tuple,
}

/// A DELETE statement
#[derive(Debug, Serialize, Deserialize)]
pub struct DeleteBody {
    /// The ID of the relation being deleted (i.e. table OID).
    pub rel_id: u32,
    /// The tuple of data that used to belong in this row.
    pub old_tuple: Option<Tuple>,
    /// A tuple of the key columns of htis relation.
    pub key_tuple: Option<Tuple>,
}

/// A TRUNCATE statement
#[derive(Debug, Serialize, Deserialize)]
pub struct TruncateBody {
    pub options: i8,
    /// The IDs of the relations being truncated (i.e. table OIDs).
    pub rel_ids: Vec<u32>,
}

#[cfg(feature = "connect")]
mod convert {
    use super::*;
    use crate::{Error, Result};
    use postgres_protocol::message::backend::Column as PgColumn;
    use postgres_protocol::message::backend::ReplicaIdentity as PgId;
    use postgres_protocol::message::backend::Tuple as PgTuple;
    use postgres_protocol::message::backend::TupleData as PgTupleData;
    use postgres_protocol::message::backend::{
        LogicalReplicationMessage as PgReplication, XLogDataBody,
    };
    use std::collections::BTreeMap;
    use std::convert::TryFrom;
    use tokio_postgres::types::Type;

    impl ReplicationEvent {
        pub fn from_pg_event(
            schemas: &BTreeMap<u32, Vec<Column>>,
            event: &XLogDataBody<PgReplication>,
        ) -> Result<Self> {
            let message = LogicalReplicationMessage::from_pg_replication(schemas, event.data())?;
            let data = ReplicationEvent {
                wal_start: event.wal_start(),
                wal_end: event.wal_end(),
                timestamp: event.timestamp(),
                message,
            };
            Ok(data)
        }
    }

    impl LogicalReplicationMessage {
        pub fn from_pg_replication(
            schemas: &BTreeMap<u32, Vec<Column>>,
            pg_msg: &PgReplication,
        ) -> Result<Self> {
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
                        .into_iter()
                        .map(Column::try_from)
                        .collect::<core::result::Result<Vec<_>, _>>()?;
                    let body = RelationBody {
                        rel_id: rel.rel_id(),
                        namespace: rel.namespace().map_err(Error::InvalidString)?.to_string(),
                        name: rel.name().map_err(Error::InvalidString)?.to_string(),
                        replica_identity: ReplicaIdentity::from(rel.replica_identity()),
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
                    let typed_tuple = Tuple::from_pg_tuple(schema, insert.tuple())?;
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
                        .map(|t| Tuple::from_pg_tuple(schema, t))
                        .transpose()?;
                    let key_tuple = update
                        .key_tuple()
                        .map(|t| Tuple::from_pg_tuple(schema, t))
                        .transpose()?;
                    let new_tuple = Tuple::from_pg_tuple(schema, update.new_tuple())?;

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
                        .map(|t| Tuple::from_pg_tuple(schema, t))
                        .transpose()?;
                    let key_tuple = delete
                        .key_tuple()
                        .map(|t| Tuple::from_pg_tuple(schema, t))
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
                _ => return Err(Error::UnexpectedMessage),
            };
            Ok(logical_replication_message)
        }
    }

    impl Tuple {
        fn from_pg_tuple(schema: &[Column], tuple: &PgTuple) -> Result<Self> {
            let mut typed_tuple_data = Vec::with_capacity(schema.len());

            let iter = schema.iter().zip(tuple.tuple_data().iter());
            for (column, data) in iter {
                let tid = column.type_id as u32;
                let ty = Type::from_oid(tid).ok_or_else(|| Error::UnrecognizedType(tid))?;
                let typed_data = TupleData::from_tuple_data(&ty, data)?;
                typed_tuple_data.push(typed_data);
            }

            Ok(Self(typed_tuple_data))
        }
    }

    impl TupleData {
        pub fn from_tuple_data(ty: &Type, data: &PgTupleData) -> Result<Self> {
            let data = match data {
                PgTupleData::Null => Self::Null,
                PgTupleData::UnchangedToast => Self::UnchangedToast,
                PgTupleData::Text(text) => {
                    let string = std::str::from_utf8(text.as_ref())
                        .map_err(|e| Error::ParseError(e.to_string()))?;
                    match *ty {
                        Type::BOOL => Self::Bool(
                            string
                                .parse::<bool>()
                                .map_err(|e| Error::ParseError(e.to_string()))?,
                        ),
                        Type::CHAR => Self::Char(
                            string
                                .parse::<i8>()
                                .map_err(|e| Error::ParseError(e.to_string()))?,
                        ),
                        Type::INT2 => Self::Int2(
                            string
                                .parse::<i16>()
                                .map_err(|e| Error::ParseError(e.to_string()))?,
                        ),
                        Type::INT4 => Self::Int4(
                            string
                                .parse::<i32>()
                                .map_err(|e| Error::ParseError(e.to_string()))?,
                        ),
                        Type::OID => Self::Oid(
                            string
                                .parse::<u32>()
                                .map_err(|e| Error::ParseError(e.to_string()))?,
                        ),
                        Type::INT8 => Self::Int8(
                            string
                                .parse::<i64>()
                                .map_err(|e| Error::ParseError(e.to_string()))?,
                        ),
                        Type::FLOAT4 => Self::Float4(
                            string
                                .parse::<f32>()
                                .map_err(|e| Error::ParseError(e.to_string()))?,
                        ),
                        Type::FLOAT8 => Self::Float8(
                            string
                                .parse::<f64>()
                                .map_err(|e| Error::ParseError(e.to_string()))?,
                        ),
                        Type::VARCHAR | Type::TEXT | Type::BPCHAR | Type::NAME | Type::UNKNOWN => {
                            Self::String(string.to_string())
                        }
                        ref ty if ty.name() == "citext" => Self::String(string.to_string()),
                        _ => Self::RawText(text.to_vec()),
                    }
                }
            };
            Ok(data)
        }
    }

    impl TryFrom<&PgColumn> for Column {
        type Error = Error;

        fn try_from(value: &PgColumn) -> Result<Self, Self::Error> {
            let column = Column {
                flags: value.flags(),
                name: value.name().map_err(Error::InvalidString)?.to_string(),
                type_id: value.type_id(),
                type_modifier: value.type_modifier(),
            };
            Ok(column)
        }
    }

    impl From<&PgId> for ReplicaIdentity {
        fn from(id: &PgId) -> Self {
            match id {
                PgId::Default => ReplicaIdentity::Default,
                PgId::Nothing => ReplicaIdentity::Nothing,
                PgId::Full => ReplicaIdentity::Full,
                PgId::Index => ReplicaIdentity::Index,
            }
        }
    }
}
