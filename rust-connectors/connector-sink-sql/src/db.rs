use anyhow::anyhow;
use fluvio_future::tracing::{debug, error};
use fluvio_model_sql::{Operation, Type, Value};
use itertools::Itertools;
use rust_decimal::Decimal;
use sqlx::any::{AnyConnectOptions, AnyKind};
use sqlx::database::HasArguments;
use sqlx::postgres::PgArguments;
use sqlx::query::Query;
use sqlx::sqlite::SqliteArguments;
use sqlx::{
    Connection, Database, Executor, IntoArguments, PgConnection, Postgres, Sqlite, SqliteConnection,
};
use std::str::FromStr;

const NAIVE_DATE_TIME_FORMAT: &str = "%Y-%m-%d %H:%M:%S%.f";

pub enum Db {
    Postgres(Box<PgConnection>),
    Sqlite(Box<SqliteConnection>),
}

impl Db {
    pub async fn connect(url: &str) -> anyhow::Result<Self> {
        let options = AnyConnectOptions::from_str(url)?;
        match options.kind() {
            AnyKind::Postgres => {
                let options = options
                    .as_postgres()
                    .ok_or_else(|| anyhow!("invalid postgres connect options"))?;
                let conn = PgConnection::connect_with(options).await?;
                Ok(Db::Postgres(Box::new(conn)))
            }
            AnyKind::Sqlite => {
                let options = options
                    .as_sqlite()
                    .ok_or_else(|| anyhow!("invalid sqlite connect options"))?;
                let conn = SqliteConnection::connect_with(options).await?;
                Ok(Db::Sqlite(Box::new(conn)))
            }
        }
    }

    pub async fn execute(&mut self, operation: Operation) -> anyhow::Result<()> {
        match operation {
            Operation::Insert { table, values } => {
                self.insert(table, values).await?;
            }
        }
        Ok(())
    }

    async fn insert(&mut self, table: String, values: Vec<Value>) -> anyhow::Result<()> {
        match self {
            Db::Postgres(conn) => {
                do_insert::<Postgres, &mut PgConnection, Self>(conn.as_mut(), table, values).await
            }
            Db::Sqlite(conn) => {
                do_insert::<Sqlite, &mut SqliteConnection, Self>(conn.as_mut(), table, values).await
            }
        }
    }

    pub fn kind(&self) -> &'static str {
        match self {
            Db::Postgres(_) => "postgres",
            Db::Sqlite(_) => "sqlite",
        }
    }
}

async fn do_insert<'c, DB, E, I>(conn: E, table: String, values: Vec<Value>) -> anyhow::Result<()>
where
    DB: Database,
    for<'q> <DB as HasArguments<'q>>::Arguments: IntoArguments<'q, DB>,
    E: Executor<'c, Database = DB>,
    I: Insert<DB>,
{
    let sql = I::query(table.as_str(), values.as_slice());
    debug!(sql, "sending");
    let mut query = sqlx::query(&sql);
    for value in values {
        query = match I::bind_value(query, &value) {
            Ok(q) => q,
            Err(err) => {
                error!("Unable to bind {:?}. Reason: {:?}", value, err);
                return Err(err);
            }
        }
    }
    query.execute(conn).await?;
    Ok(())
}

trait Insert<DB: Database> {
    fn query(table: &str, values: &[Value]) -> String;

    fn bind_value<'a, 'b>(
        query: Query<'a, DB, <DB as HasArguments<'a>>::Arguments>,
        value: &'b Value,
    ) -> anyhow::Result<Query<'a, DB, <DB as HasArguments<'a>>::Arguments>>;
}

impl Insert<Postgres> for Db {
    fn query(table: &str, values: &[Value]) -> String {
        let columns = values.iter().map(|v| v.column.as_str()).join(",");
        let values_clause = (1..=values.len()).map(|i| format!("${}", i)).join(",");
        format!(
            "INSERT INTO {} ({}) VALUES ({})",
            table, columns, values_clause
        )
    }

    fn bind_value<'a, 'b>(
        query: Query<'a, Postgres, PgArguments>,
        value: &'b Value,
    ) -> anyhow::Result<Query<'a, Postgres, PgArguments>> {
        let query = match value.type_ {
            Type::Bool => query.bind(bool::from_str(&value.raw_value)?),
            Type::Char => query.bind(i8::from_str(&value.raw_value)?),
            Type::SmallInt => query.bind(i16::from_str(&value.raw_value)?),
            Type::Int => query.bind(i32::from_str(&value.raw_value)?),
            Type::BigInt => query.bind(i64::from_str(&value.raw_value)?),
            Type::Float => query.bind(f32::from_str(&value.raw_value)?),
            Type::DoublePrecision => query.bind(f64::from_str(&value.raw_value)?),
            Type::Text => query.bind(value.raw_value.clone()),
            Type::Bytes => query.bind(value.raw_value.as_bytes().to_vec()),
            Type::Numeric => query.bind(Decimal::from_str(&value.raw_value)?),
            Type::Timestamp => query.bind(chrono::NaiveDateTime::parse_from_str(
                &value.raw_value,
                NAIVE_DATE_TIME_FORMAT,
            )?),
            Type::Date => query.bind(chrono::NaiveDate::from_str(&value.raw_value)?),
            Type::Time => query.bind(chrono::NaiveTime::from_str(&value.raw_value)?),
            Type::Uuid => query.bind(uuid::Uuid::from_str(&value.raw_value)?),
            Type::Json => query.bind(serde_json::Value::from_str(&value.raw_value)?),
        };
        Ok(query)
    }
}

impl Insert<Sqlite> for Db {
    fn query(table: &str, values: &[Value]) -> String {
        let columns = values.iter().map(|v| v.column.as_str()).join(",");
        let values_clause = (1..=values.len()).map(|_| "?").join(",");
        format!(
            "INSERT INTO {} ({}) VALUES ({})",
            table, columns, values_clause
        )
    }

    fn bind_value<'a, 'b>(
        query: Query<'a, Sqlite, SqliteArguments<'a>>,
        value: &'b Value,
    ) -> anyhow::Result<Query<'a, Sqlite, SqliteArguments<'a>>> {
        let query = match value.type_ {
            Type::Bool => query.bind(bool::from_str(&value.raw_value)?),
            Type::Char => query.bind(i8::from_str(&value.raw_value)?),
            Type::SmallInt => query.bind(i16::from_str(&value.raw_value)?),
            Type::Int => query.bind(i32::from_str(&value.raw_value)?),
            Type::BigInt => query.bind(i64::from_str(&value.raw_value)?),
            Type::Float => query.bind(f32::from_str(&value.raw_value)?),
            Type::DoublePrecision => query.bind(f64::from_str(&value.raw_value)?),
            Type::Text => query.bind(value.raw_value.clone()),
            Type::Bytes => query.bind(value.raw_value.as_bytes().to_vec()),
            Type::Numeric => query.bind(f64::from_str(&value.raw_value)?),
            Type::Timestamp => query.bind(chrono::NaiveDateTime::parse_from_str(
                &value.raw_value,
                NAIVE_DATE_TIME_FORMAT,
            )?),
            Type::Date => query.bind(chrono::NaiveDate::from_str(&value.raw_value)?),
            Type::Time => query.bind(chrono::NaiveTime::from_str(&value.raw_value)?),
            Type::Uuid => query.bind(uuid::Uuid::from_str(&value.raw_value)?),
            Type::Json => query.bind(serde_json::Value::from_str(&value.raw_value)?),
        };
        Ok(query)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{NaiveDateTime, Utc};
    use fluvio_model_sql::Type;
    use sqlx::{Executor, Row};
    use uuid::Uuid;

    #[ignore]
    #[async_std::test]
    async fn test_data_types_postgres() -> anyhow::Result<()> {
        //given
        let mut db =
            Db::connect("postgresql://myusername:mypassword@localhost:5432/myusername").await?;

        match db {
            Db::Postgres(ref mut conn) => {
                conn.execute(
                    r#"CREATE TABLE IF NOT EXISTS big_table (
            json_col JSONB,
            bool_col BOOL,
            char_col CHAR,
            smallint_col SMALLINT,
            int_col INT,
            text_col TEXT,
            bytes_col BYTEA,
            float_col FLOAT,
            double_col DOUBLE PRECISION,
            numeric_col NUMERIC,
            timestamp_col TIMESTAMP,
            uuid_col UUID
            );"#,
                )
                .await?;
            }
            Db::Sqlite(ref mut _conn) => unreachable!(),
        };

        let operation = Operation::Insert {
            table: "big_table".to_string(),
            values: vec![
                Value {
                    column: "json_col".to_string(),
                    raw_value: "{\"json_key\":\"json_value\"}".to_string(),
                    type_: Type::Json,
                },
                Value {
                    column: "bool_col".to_string(),
                    raw_value: "true".to_string(),
                    type_: Type::Bool,
                },
                Value {
                    column: "char_col".to_string(),
                    raw_value: "126".to_string(),
                    type_: Type::Char,
                },
                Value {
                    column: "smallint_col".to_string(),
                    raw_value: "12".to_string(),
                    type_: Type::SmallInt,
                },
                Value {
                    column: "int_col".to_string(),
                    raw_value: "40".to_string(),
                    type_: Type::Int,
                },
                Value {
                    column: "text_col".to_string(),
                    raw_value: "some text".to_string(),
                    type_: Type::Text,
                },
                Value {
                    column: "bytes_col".to_string(),
                    raw_value: "some bytes".to_string(),
                    type_: Type::Bytes,
                },
                Value {
                    column: "float_col".to_string(),
                    raw_value: "3.123".to_string(),
                    type_: Type::Float,
                },
                Value {
                    column: "double_col".to_string(),
                    raw_value: "3.333333333".to_string(),
                    type_: Type::DoublePrecision,
                },
                Value {
                    column: "numeric_col".to_string(),
                    raw_value: Decimal::TEN.to_string(),
                    type_: Type::Numeric,
                },
                Value {
                    column: "timestamp_col".to_string(),
                    raw_value: Utc::now().naive_local().to_string(),
                    type_: Type::Timestamp,
                },
                Value {
                    column: "uuid_col".to_string(),
                    raw_value: "D9C64A1B-9527-4E8C-BE74-D0208A15FF01".to_string(),
                    type_: Type::Uuid,
                },
            ],
        };

        //when
        db.execute(operation).await?;

        //then
        Ok(())
    }

    #[async_std::test]
    async fn test_data_types_sqlite() -> anyhow::Result<()> {
        fluvio_future::subscriber::init_logger();
        //given
        let mut db = Db::connect("sqlite::memory:").await?;

        match db {
            Db::Postgres(_) => unreachable!(),
            Db::Sqlite(ref mut conn) => {
                conn.execute(
                    r#"CREATE TABLE IF NOT EXISTS big_table (
            json_col TEXT,
            bool_col BOOLEAN,
            char_col INTEGER,
            smallint_col INTEGER,
            int_col INTEGER,
            big_int_col BIGINT,
            text_col TEXT,
            bytes_col BLOB,
            float_col REAL,
            double_col REAL,
            numeric_col REAL,
            timestamp_col TIMESTAMP,
            uuid_col TEXT
            );"#,
                )
                .await?;
            }
        };

        let operation = Operation::Insert {
            table: "big_table".to_string(),
            values: vec![
                Value {
                    column: "json_col".to_string(),
                    raw_value: "{\"json_key\":\"json_value\"}".to_string(),
                    type_: Type::Json,
                },
                Value {
                    column: "bool_col".to_string(),
                    raw_value: "true".to_string(),
                    type_: Type::Bool,
                },
                Value {
                    column: "char_col".to_string(),
                    raw_value: "126".to_string(),
                    type_: Type::Char,
                },
                Value {
                    column: "smallint_col".to_string(),
                    raw_value: "12".to_string(),
                    type_: Type::SmallInt,
                },
                Value {
                    column: "int_col".to_string(),
                    raw_value: "40".to_string(),
                    type_: Type::Int,
                },
                Value {
                    column: "big_int_col".to_string(),
                    raw_value: "312".to_string(),
                    type_: Type::BigInt,
                },
                Value {
                    column: "text_col".to_string(),
                    raw_value: "some text".to_string(),
                    type_: Type::Text,
                },
                Value {
                    column: "bytes_col".to_string(),
                    raw_value: "some bytes".to_string(),
                    type_: Type::Bytes,
                },
                Value {
                    column: "float_col".to_string(),
                    raw_value: "3.123".to_string(),
                    type_: Type::Float,
                },
                Value {
                    column: "double_col".to_string(),
                    raw_value: "3.333333333".to_string(),
                    type_: Type::DoublePrecision,
                },
                Value {
                    column: "numeric_col".to_string(),
                    raw_value: Decimal::TEN.to_string(),
                    type_: Type::Numeric,
                },
                Value {
                    column: "timestamp_col".to_string(),
                    raw_value: Utc::now().naive_local().to_string(),
                    type_: Type::Timestamp,
                },
                Value {
                    column: "uuid_col".to_string(),
                    raw_value: "d9c64a1b-9527-4e8c-be74-d0208a15ff01".to_string(),
                    type_: Type::Uuid,
                },
            ],
        };

        //when
        db.execute(operation).await?;

        //then
        match db {
            Db::Postgres(_) => unreachable!(),
            Db::Sqlite(ref mut conn) => {
                let row = conn
                    .fetch_one(
                        r#"SELECT 
                json_col,
                bool_col,
                char_col,
                smallint_col,
                int_col,
                big_int_col,
                text_col,
                bytes_col,
                float_col,
                double_col,
                numeric_col,
                timestamp_col,
                uuid_col
                 FROM big_table"#,
                    )
                    .await?;
                let json_col: String = row.get(0);
                assert_eq!(json_col, "{\"json_key\":\"json_value\"}".to_string());
                let bool_col: bool = row.get(1);
                assert!(bool_col);
                let char_col: i8 = row.get(2);
                assert_eq!(char_col, 126);
                let small_int_col: i32 = row.get(3);
                assert_eq!(small_int_col, 12);
                let int_col: i32 = row.get(4);
                assert_eq!(int_col, 40);
                let big_int_col: i64 = row.get(5);
                assert_eq!(big_int_col, 312);
                let text_col: String = row.get(6);
                assert_eq!(text_col, "some text");
                let bytes_col: Vec<u8> = row.get(7);
                assert_eq!(bytes_col, b"some bytes");
                let float: f32 = row.get(8);
                assert_eq!(float, 3.123);
                let double: f64 = row.get(9);
                assert_eq!(double, 3.333333333);
                let numeric: f64 = row.get(10);
                assert_eq!(numeric, 10f64);
                let _timestamp: NaiveDateTime = row.get(11);
                let uuid: Uuid = row.get(12);
                assert_eq!(
                    uuid.to_string(),
                    "d9c64a1b-9527-4e8c-be74-d0208a15ff01".to_string()
                );
            }
        };
        Ok(())
    }

    #[test]
    fn test_date_time_format() {
        //given
        let timestamp = Utc::now().naive_local();
        let raw = timestamp.to_string();

        //when
        let parsed = NaiveDateTime::parse_from_str(&raw, NAIVE_DATE_TIME_FORMAT).unwrap();

        //then
        assert_eq!(timestamp, parsed);
    }

    #[test]
    fn test_insert_query_postgres() {
        //given
        let table = "test_table";
        let values = [
            Value {
                column: "col1".to_string(),
                raw_value: "1".to_string(),
                type_: Type::Int,
            },
            Value {
                column: "col2".to_string(),
                raw_value: "text".to_string(),
                type_: Type::Text,
            },
        ];

        //when
        let query = <Db as Insert<Postgres>>::query(table, &values);

        //then
        assert_eq!(query, "INSERT INTO test_table (col1,col2) VALUES ($1,$2)");
    }

    #[test]
    fn test_insert_query_sqlite() {
        //given
        let table = "test_table";
        let values = [
            Value {
                column: "col1".to_string(),
                raw_value: "1".to_string(),
                type_: Type::Int,
            },
            Value {
                column: "col2".to_string(),
                raw_value: "text".to_string(),
                type_: Type::Text,
            },
        ];

        //when
        let query = <Db as Insert<Sqlite>>::query(table, &values);

        //then
        assert_eq!(query, "INSERT INTO test_table (col1,col2) VALUES (?,?)");
    }
}
