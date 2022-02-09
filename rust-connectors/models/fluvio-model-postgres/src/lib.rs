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
#[derive(Debug, Serialize, Deserialize, Clone)]
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

// This isn't the best use of TryInto<String>.
impl TryInto<String> for &TupleData {
    type Error = String;
    fn try_into(self) -> Result<String, Self::Error> {
        match self {
            TupleData::Bool(v) => Ok(format!("{v}")),
            TupleData::Char(c) => Ok(format!("{c}")),
            TupleData::Int2(i) => Ok(format!("{i}")),
            TupleData::Int4(i) => Ok(format!("{i}")),
            TupleData::Int8(i) => Ok(format!("{i}")),
            TupleData::Oid(i) => Ok(format!("{i}")),
            TupleData::Float4(i) => Ok(format!("{i}")),
            TupleData::Float8(i) => Ok(format!("{i}")),
            TupleData::String(i) => Ok(format!("'{i}'")),
            other => Err(format!("Unsupported tupple type {:?}", other)),
        }
    }
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
    /// A tuple of the key columns of this relation.
    pub key_tuple: Option<Tuple>,
}

/// A TRUNCATE statement
#[derive(Debug, Serialize, Deserialize)]
pub struct TruncateBody {
    /// Option bits for TRUNCATE: 1 for CASCADE, 2 for RESTART IDENTITY
    pub options: i8,
    /// The IDs of the relations being truncated (i.e. table OIDs).
    pub rel_ids: Vec<u32>,
}
