use fluvio::metadata::topic::TopicSpec;
use fluvio_connectors_common::opt::CommonSourceOpt;
use tokio_postgres::NoTls;
use tokio_stream::StreamExt;
use url::Url;
use tokio::time::{sleep, Duration};

#[tokio::test]
async fn postgres_inserts() -> eyre::Result<()> {
    fluvio_future::subscriber::init_logger();
    let fluvio_topic = "postgres".to_string();
    let admin = fluvio::FluvioAdmin::connect().await?;
    let _ = admin
        .create(
            fluvio_topic.clone(),
            false,
            TopicSpec::new_computed(1, 1, Some(false)),
        )
        .await;
    let postgres_sink_url = std::env::var("FLUVIO_PG_SINK_DATABASE_URL")
        .expect("No FLUVIO_PG_DATABASE_URL environment variable found");
    let _ = start_pg_sink(fluvio_topic.clone(), postgres_sink_url.clone()).await?;

    let postgres_source_url = std::env::var("FLUVIO_PG_SOURCE_DATABASE_URL")
        .expect("No FLUVIO_PG_SOURCE_DATABASE_URL environment variable found");
    let _ = start_pg_source(fluvio_topic.clone(), postgres_source_url.clone()).await?;

    let (pg_source_client, conn) = postgres_source_url
        .parse::<tokio_postgres::Config>()?
        .connect(NoTls)
        .await?;
    tokio::spawn(conn);

    let (pg_sink_client, conn) = postgres_sink_url
        .parse::<tokio_postgres::Config>()?
        .connect(NoTls)
        .await?;
    tokio::spawn(conn);
    // Create table
    let table_create =
        r#"CREATE TABLE names(ID SERIAL PRIMARY KEY, NAME TEXT NOT NULL)"#;
    let _ = pg_source_client.execute(table_create, &[]).await?;

    // Insert into those rows
    for i in 1..100 {
        let query = "INSERT INTO names (NAME) VALUES($1)";
        let name = format!("Fluvio_{}", i);
        let _ = pg_source_client.query(query, &[&name]).await?;
    }
    for i in 1..100 {
        let query = "SELECT * FROM names WHERE name=$1";
        let name = format!("Fluvio_{}", i);
        let sink_name = pg_sink_client.query(query, &[&name]).await?;
        assert_eq!(sink_name.len(), 1, "Found more than one result for select query");
        let row = sink_name.first().unwrap();
        let columns = row.columns();
        assert_eq!(columns.len(), 2, "Number of columns in name result is unexpected");
        let id: i32 = row.get("id");
        let out_name : String = row.get("name");
        assert_eq!(id, i, "id field doesn't match");
        assert_eq!(out_name, name, "name field doesn't match");
    }
    // Alter table
    let table_alter = "ALTER TABLE names ADD COLUMN Email varchar(255);";
    let _ = pg_source_client.execute(table_alter, &[]).await?;
    for i in 100..200 {
        let query = "INSERT INTO names (NAME, Email) VALUES($1, $2)";
        let name = format!("Fluvio_{}", i);
        let email = format!("{}@gmail.com", name);
        let _ = pg_source_client.query(query, &[&name, &email]).await?;
    }
    for i in 100..200 {
        let query = "SELECT * FROM names WHERE name=$1";
        let name = format!("Fluvio_{}", i);
        let email = format!("{}@gmail.com", name);
        let sink_name = pg_sink_client.query(query, &[&name]).await?;
        assert_eq!(sink_name.len(), 1, "Found more than one result for select query");
        let row = sink_name.first().unwrap();
        let columns = row.columns();
        assert_eq!(columns.len(), 3, "Number of columns in name result is unexpected");
        let id: i32 = row.get("id");
        let out_name : String = row.get("name");
        let out_email : String = row.get("email");
        assert_eq!(id, i, "id field doesn't match");
        assert_eq!(out_name, name, "name field doesn't match");
        assert_eq!(out_email, email, "email field doesn't match");
    }
    let table_alter = "ALTER TABLE names RENAME COLUMN name TO fluvio_id";
    let _ = pg_source_client.execute(table_alter, &[]).await?;
    for i in 200..300 {
        let query = "INSERT INTO names (fluvio_id, Email) VALUES($1, $2)";
        let name = format!("Fluvio_{}", i);
        let email = format!("{}@gmail.com", name);
        let _ = pg_source_client.query(query, &[&name, &email]).await?;
    }

    for i in 200..300 {
        let query = "SELECT * FROM names WHERE fluvio_id=$1";
        let name = format!("Fluvio_{}", i);
        let email = format!("{}@gmail.com", name);
        let sink_name = pg_sink_client.query(query, &[&name]).await?;
        assert_eq!(sink_name.len(), 1, "Found more than one result for select query");
        let row = sink_name.first().unwrap();
        let columns = row.columns();
        assert_eq!(columns.len(), 3, "Number of columns in name result is unexpected");
        let id: i32 = row.get("id");
        let out_name : String = row.get("fluvio_id");
        let out_email : String = row.get("email");
        assert_eq!(id, i, "id field doesn't match");
        assert_eq!(out_name, name, "name field doesn't match");
        assert_eq!(out_email, email, "email field doesn't match");
    }
    let table_alter = "ALTER TABLE names RENAME COLUMN fluvio_id TO name";
    let _ = pg_source_client.execute(table_alter, &[]).await?;

    let table_alter = "DELETE FROM names";
    let _ = pg_source_client.execute(table_alter, &[]).await?;
    for i in 300..400 {
        let query = "INSERT INTO names (name, Email) VALUES($1, $2)";
        let name = format!("Fluvio_{}", i);
        let email = format!("{}@gmail.com", name);
        let _ = pg_source_client.query(query, &[&name, &email]).await?;
    }
    sleep(Duration::from_millis(1000)).await;

    for i in 300..400 {
        let query = "SELECT * FROM names WHERE name=$1";
        let name = format!("Fluvio_{}", i);
        let email = format!("{}@gmail.com", name);
        let sink_name = pg_sink_client.query(query, &[&name]).await?;
        assert_eq!(sink_name.len(), 1, "Found more than one result for select query {} - {}", query, name);
        let row = sink_name.first().unwrap();
        let columns = row.columns();
        assert_eq!(columns.len(), 3, "Number of columns in name result is unexpected");
        let id: i32 = row.get("id");
        let out_name : String = row.get("name");
        let out_email : String = row.get("email");
        assert_eq!(id, i, "id field doesn't match");
        assert_eq!(out_name, name, "name field doesn't match");
        assert_eq!(out_email, email, "email field doesn't match");
    }

    let table_alter = "ALTER TABLE names RENAME TO old_names";
    let _ = pg_source_client.execute(table_alter, &[]).await?;
    for i in 400..500 {
        let query = "INSERT INTO old_names (name, Email) VALUES($1, $2)";
        let name = format!("Fluvio_{}", i);
        let email = format!("{}@gmail.com", name);
        let _ = pg_source_client.query(query, &[&name, &email]).await?;
    }
    for i in 400..500 {
        let query = "SELECT * FROM old_names WHERE name=$1";
        let name = format!("Fluvio_{}", i);
        let email = format!("{}@gmail.com", name);
        let sink_name = pg_sink_client.query(query, &[&name]).await?;
        assert_eq!(sink_name.len(), 1, "Found more than one result for select query {} - {}", query, name);
        let row = sink_name.first().unwrap();
        let columns = row.columns();
        assert_eq!(columns.len(), 3, "Number of columns in name result is unexpected");
        let id: i32 = row.get("id");
        let out_name : String = row.get("name");
        let out_email : String = row.get("email");
        assert_eq!(id, i, "id field doesn't match");
        assert_eq!(out_name, name, "name field doesn't match");
        assert_eq!(out_email, email, "email field doesn't match");
    }

    let table_alter = "ALTER TABLE old_names DROP COLUMN Email";
    let _ = pg_source_client.execute(table_alter, &[]).await?;
    for i in 500..600 {
        let query = "INSERT INTO old_names (name) VALUES($1)";
        let name = format!("Fluvio_{}", i);
        let _ = pg_source_client.query(query, &[&name]).await?;
    }
    for i in 500..600 {
        let query = "SELECT * FROM old_names WHERE name=$1";
        let name = format!("Fluvio_{}", i);
        let sink_name = pg_sink_client.query(query, &[&name]).await?;
        assert_eq!(sink_name.len(), 1, "Found more than one result for select query {} - {}", query, name);
        let row = sink_name.first().unwrap();
        let columns = row.columns();
        assert_eq!(columns.len(), 2, "Number of columns in name result is unexpected");
        let id: i32 = row.get("id");
        let out_name : String = row.get("name");
        assert_eq!(id, i, "id field doesn't match");
        assert_eq!(out_name, name, "name field doesn't match");
    }


    Ok(())
}

async fn start_pg_sink(fluvio_topic: String, postgres_url: String) -> eyre::Result<()> {
use postgres_sink::{PgConnector, PgConnectorOpt};

    let mut connector = PgConnector::new(PgConnectorOpt {
        url: Url::parse(&postgres_url).expect("Failed to parse connector url"),
        resume_timeout: 1000,
        skip_setup: false,
        common: CommonSourceOpt {
            fluvio_topic: fluvio_topic.clone(),
            rust_log: None,
            filter: None,
            map: None,
            arraymap: None,
        },
    })
    .await?;
    tokio::spawn(async move { connector.start().await });
    Ok(())

}
async fn start_pg_source(fluvio_topic: String, postgres_url: String) -> eyre::Result<()> {
    use postgres_source::{
        PgConnector as PgSourceConnector,
        PgConnectorOpt as PgSourceConnectorOpt,
    };

    let slot = uuid::Uuid::new_v4().to_string().replace("-", "");
    let publication = uuid::Uuid::new_v4().to_string().replace("-", "");

    let config = PgSourceConnectorOpt {
        url: Url::parse(&postgres_url).expect("Failed to parse connector url"),
        publication,
        slot,
        resume_timeout: 1000,
        skip_setup: false,
        common: CommonSourceOpt {
            fluvio_topic: fluvio_topic.clone(),
            rust_log: None,
            filter: None,
            map: None,
            arraymap: None,
        },
    };
    let mut connector = PgSourceConnector::new(config.clone())
        .await
        .expect("PgConnector failed to initialize");
    let _stream = fluvio_future::task::spawn(async move {
        connector
            .process_stream()
            .await
            .expect("process stream failed");
    });
    Ok(())
}
