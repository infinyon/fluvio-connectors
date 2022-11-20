use fluvio::metadata::topic::TopicSpec;
use fluvio_connectors_common::opt::CommonConnectorOpt;
use tokio::task::JoinHandle;
use tokio::time::{sleep, Duration};
use tokio_postgres::{Client, NoTls};
use url::Url;

#[tokio::test]
async fn postgres_sink_and_source() -> eyre::Result<()> {
    fluvio_future::subscriber::init_logger();
    //let fluvio_topic = "postgres".to_string(); // To help debug, use "postgres"
    let fluvio_topic = uuid::Uuid::new_v4().to_string().replace('-', "");
    let admin = fluvio::FluvioAdmin::connect().await?;
    let _ = admin
        .create(
            fluvio_topic.clone(),
            false,
            TopicSpec::new_computed(1, 1, Some(false)),
        )
        .await;
    let (sink_handle, pg_sink_client) = start_pg_sink(fluvio_topic.clone()).await?;
    let (source_handle, pg_source_client) = start_pg_source(fluvio_topic.clone()).await?;

    // Create table
    let table_create = r#"CREATE TABLE names(ID SERIAL, NAME TEXT NOT NULL)"#;
    let _ = pg_source_client.execute(table_create, &[]).await?;

    // Insert into those rows
    for i in 1..100 {
        let query = "INSERT INTO names (NAME) VALUES($1)";
        let name = format!("Fluvio_{}", i);
        let _ = pg_source_client.query(query, &[&name]).await?;
    }
    sleep(Duration::from_millis(100)).await;
    for i in 1..100 {
        let query = "SELECT * FROM names WHERE name=$1";
        let name = format!("Fluvio_{}", i);
        let sink_name = pg_sink_client.query(query, &[&name]).await?;
        assert_eq!(
            sink_name.len(),
            1,
            "Found more than one result for select query"
        );
        let row = sink_name.first().unwrap();
        let columns = row.columns();
        assert_eq!(
            columns.len(),
            2,
            "Number of columns in name result is unexpected"
        );
        let id: i32 = row.get("id");
        let out_name: String = row.get("name");
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
    sleep(Duration::from_millis(100)).await;
    for i in 100..200 {
        let query = "SELECT * FROM names WHERE name=$1";
        let name = format!("Fluvio_{}", i);
        let email = format!("{}@gmail.com", name);
        let sink_name = pg_sink_client.query(query, &[&name]).await?;
        assert_eq!(
            sink_name.len(),
            1,
            "Found more than one result for select query"
        );
        let row = sink_name.first().unwrap();
        let columns = row.columns();
        assert_eq!(
            columns.len(),
            3,
            "Number of columns in name result is unexpected"
        );
        let id: i32 = row.get("id");
        let out_name: String = row.get("name");
        let out_email: String = row.get("email");
        assert_eq!(id, i, "id field doesn't match");
        assert_eq!(out_name, name, "name field doesn't match");
        assert_eq!(out_email, email, "email field doesn't match");
    }
    sleep(Duration::from_millis(100)).await;
    let table_alter = "ALTER TABLE names RENAME COLUMN name TO fluvio_id";
    let _ = pg_source_client.execute(table_alter, &[]).await?;
    for i in 200..300 {
        let query = "INSERT INTO names (fluvio_id, Email) VALUES($1, $2)";
        let name = format!("Fluvio_{}", i);
        let email = format!("{}@gmail.com", name);
        let _ = pg_source_client.query(query, &[&name, &email]).await?;
    }
    sleep(Duration::from_millis(500)).await;

    for i in 200..300 {
        let query = "SELECT * FROM names WHERE fluvio_id=$1";
        let name = format!("Fluvio_{}", i);
        let email = format!("{}@gmail.com", name);
        let sink_name = pg_sink_client.query(query, &[&name]).await?;
        assert_eq!(
            sink_name.len(),
            1,
            "Found more than one result for select query"
        );
        let row = sink_name.first().unwrap();
        let columns = row.columns();
        assert_eq!(
            columns.len(),
            3,
            "Number of columns in name result is unexpected"
        );
        let id: i32 = row.get("id");
        let out_name: String = row.get("fluvio_id");
        let out_email: String = row.get("email");
        assert_eq!(id, i, "id field doesn't match");
        assert_eq!(out_name, name, "name field doesn't match");
        assert_eq!(out_email, email, "email field doesn't match");
    }
    let table_alter = "ALTER TABLE names RENAME COLUMN fluvio_id TO name";
    let _ = pg_source_client.execute(table_alter, &[]).await?;
    let table_alter = "ALTER TABLE names REPLICA IDENTITY FULL;";
    let _ = pg_source_client.execute(table_alter, &[]).await?;

    let table_alter = "DELETE FROM names";
    let _ = pg_source_client.execute(table_alter, &[]).await?;
    for i in 300..400 {
        let query = "INSERT INTO names (name, Email) VALUES($1, $2)";
        let name = format!("Fluvio_{}", i);
        let email = format!("{}@gmail.com", name);
        let _ = pg_source_client.query(query, &[&name, &email]).await?;
    }
    sleep(Duration::from_millis(5000)).await;
    let query = "SELECT COUNT(*) FROM names";
    let count_query = pg_sink_client.query(query, &[]).await?;
    let count: i64 = count_query.first().unwrap().get("count");
    assert_eq!(count, 100, "Count does not match");

    for i in 300..400 {
        let query = "SELECT * FROM names WHERE name=$1";
        let name = format!("Fluvio_{}", i);
        let email = format!("{}@gmail.com", name);
        let sink_name = pg_sink_client.query(query, &[&name]).await?;
        assert_eq!(
            sink_name.len(),
            1,
            "Found more than one result for select query {} - {}",
            query,
            name
        );
        let row = sink_name.first().unwrap();
        let columns = row.columns();
        assert_eq!(
            columns.len(),
            3,
            "Number of columns in name result is unexpected"
        );
        let id: i32 = row.get("id");
        let out_name: String = row.get("name");
        let out_email: String = row.get("email");
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
    sleep(Duration::from_millis(500)).await;
    for i in 400..500 {
        let query = "SELECT * FROM old_names WHERE name=$1";
        let name = format!("Fluvio_{}", i);
        let email = format!("{}@gmail.com", name);
        let sink_name = pg_sink_client.query(query, &[&name]).await?;
        assert_eq!(
            sink_name.len(),
            1,
            "Found more than one result for select query {} - {}",
            query,
            name
        );
        let row = sink_name.first().unwrap();
        let columns = row.columns();
        assert_eq!(
            columns.len(),
            3,
            "Number of columns in name result is unexpected"
        );
        let id: i32 = row.get("id");
        let out_name: String = row.get("name");
        let out_email: String = row.get("email");
        assert_eq!(id, i, "id field doesn't match");
        assert_eq!(out_name, name, "name field doesn't match");
        assert_eq!(out_email, email, "email field doesn't match");
    }
    let table_alter = "ALTER TABLE old_names RENAME TO names";
    let _ = pg_source_client.execute(table_alter, &[]).await?;

    let table_alter = "ALTER TABLE names DROP COLUMN Email";
    let _ = pg_source_client.execute(table_alter, &[]).await?;

    for i in 500..600 {
        let query = "INSERT INTO names (name) VALUES($1)";
        let name = format!("Fluvio_{}", i);
        let _ = pg_source_client.query(query, &[&name]).await?;
    }
    sleep(Duration::from_millis(1000)).await;
    for i in 500..600 {
        let query = "SELECT * FROM names WHERE name=$1";
        let name = format!("Fluvio_{}", i);
        let sink_name = pg_sink_client.query(query, &[&name]).await?;
        assert_eq!(
            sink_name.len(),
            1,
            "Found more than one result for select query {} - {}",
            query,
            name
        );
        let row = sink_name.first().unwrap();
        let columns = row.columns();
        assert_eq!(
            columns.len(),
            2,
            "Number of columns in name result is unexpected"
        );
        let id: i32 = row.get("id");
        let out_name: String = row.get("name");
        assert_eq!(id, i, "id field doesn't match");
        assert_eq!(out_name, name, "name field doesn't match");
    }

    for i in 300..600 {
        let new_name = format!("Fluvio_fluvio_{}", i);
        //let query = "UPDATE names SET name=$1 WHERE name=$2";
        let query = format!("UPDATE names SET name='{}' WHERE id={}", new_name, i);
        let query = query.as_str();
        let _update = pg_source_client.query(query, &[]).await?;
    }
    sleep(Duration::from_millis(5000)).await;
    for i in 300..600 {
        let query = "SELECT * FROM names WHERE name=$1";
        let name = format!("Fluvio_fluvio_{}", i);
        let sink_name = pg_sink_client.query(query, &[&name]).await?;
        assert_eq!(
            sink_name.len(),
            1,
            "Found more than one result for select query {} - {}",
            query,
            name
        );
        let row = sink_name.first().unwrap();
        let columns = row.columns();
        assert_eq!(
            columns.len(),
            2,
            "Number of columns in name result is unexpected"
        );
        let id: i32 = row.get("id");
        let out_name: String = row.get("name");
        assert_eq!(id, i, "id field doesn't match");
        assert_eq!(out_name, name, "name field doesn't match");
    }
    let table_truncate = "TRUNCATE names";
    let _ = pg_source_client.execute(table_truncate, &[]).await?;
    sleep(Duration::from_millis(500)).await;
    let query = "SELECT COUNT(*) FROM names";
    let count_query = pg_sink_client.query(query, &[]).await?;
    let count: i64 = count_query.first().unwrap().get("count");
    assert_eq!(count, 0, "Count does not match");
    cleanup(
        fluvio_topic,
        sink_handle,
        pg_sink_client,
        source_handle,
        pg_source_client,
    )
    .await
    .expect("Failed to clean up tests");

    Ok(())
}

async fn cleanup(
    fluvio_topic: String,
    sink_handle: JoinHandle<()>,
    pg_sink_client: Client,
    source_handle: JoinHandle<()>,
    pg_source_client: Client,
) -> eyre::Result<()> {
    sink_handle.abort();
    source_handle.abort();

    let admin = fluvio::FluvioAdmin::connect().await?;
    let _ = admin.delete::<TopicSpec, String>(fluvio_topic).await;

    let table_truncate = "DROP TABLE public.names cascade";
    let _ = pg_source_client.execute(table_truncate, &[]).await?;
    let _ = pg_sink_client.execute(table_truncate, &[]).await?;
    let _ = pg_sink_client
        .execute("DROP TABLE fluvio.offset", &[])
        .await?;

    Ok(())
}

async fn start_pg_sink(fluvio_topic: String) -> eyre::Result<(JoinHandle<()>, Client)> {
    use postgres_sink::{PgConnector, PgConnectorOpt};
    let postgres_sink_url = std::env::var("FLUVIO_PG_SINK_DATABASE_URL")
        .expect("No FLUVIO_PG_DATABASE_URL environment variable found");

    let mut connector = PgConnector::new(PgConnectorOpt {
        url: Url::parse(&postgres_sink_url).expect("Failed to parse connector url"),
        common: CommonConnectorOpt {
            fluvio_topic: fluvio_topic.clone(),
            ..Default::default()
        },
    })
    .await?;
    let (pg_sink_client, conn) = postgres_sink_url
        .parse::<tokio_postgres::Config>()?
        .connect(NoTls)
        .await?;
    tokio::spawn(conn);
    let _ = pg_sink_client
        .execute("DROP TABLE IF EXISTS names", &[])
        .await?;
    let handle = tokio::spawn(async move {
        connector
            .process_stream()
            .await
            .expect("process stream failed");
    });
    Ok((handle, pg_sink_client))
}

async fn start_pg_source(fluvio_topic: String) -> eyre::Result<(JoinHandle<()>, Client)> {
    use postgres_source::{PgConnector, PgConnectorOpt};
    let postgres_source_url = std::env::var("FLUVIO_PG_SOURCE_DATABASE_URL")
        .expect("No FLUVIO_PG_SOURCE_DATABASE_URL environment variable found");
    let (pg_source_client, conn) = postgres_source_url
        .parse::<tokio_postgres::Config>()?
        .connect(NoTls)
        .await?;

    let slot = uuid::Uuid::new_v4().to_string().replace('-', "");
    let publication = uuid::Uuid::new_v4().to_string().replace('-', "");

    let config = PgConnectorOpt {
        url: Url::parse(&postgres_source_url).expect("Failed to parse connector url"),
        publication,
        slot,
        resume_timeout: 1000,
        skip_setup: false,
        common: CommonConnectorOpt {
            fluvio_topic: fluvio_topic.clone(),
            ..Default::default()
        },
    };
    let mut connector = PgConnector::new(config.clone())
        .await
        .expect("PgConnector failed to initialize");
    tokio::spawn(conn);
    let _ = pg_source_client
        .execute("DROP TABLE IF EXISTS names", &[])
        .await?;
    let handle = tokio::spawn(async move {
        connector
            .process_stream()
            .await
            .expect("process stream failed");
    });
    Ok((handle, pg_source_client))
}
