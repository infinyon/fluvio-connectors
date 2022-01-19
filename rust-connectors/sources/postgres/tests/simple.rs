use fluvio::metadata::topic::{TopicReplicaParam, TopicSpec};
use fluvio_connectors_common::opt::CommonSourceOpt;
use fluvio_model_postgres::ReplicationEvent;
use postgres_source::{PgConnector, PgConnectorOpt};
use tokio_postgres::NoTls;
use tokio_stream::StreamExt;
use url::Url;

#[tokio::test]
async fn postgres_simple() -> eyre::Result<()> {
    fluvio_future::subscriber::init_logger();
    let postgres_url = std::env::var("FLUVIO_PG_DATABASE_URL")
        .expect("No FLUVIO_PG_DATABASE_URL environment variable found");

    let fluvio_topic = "postgres".to_string();
    let slot = uuid::Uuid::new_v4().to_string().replace("-", "");
    let publication = uuid::Uuid::new_v4().to_string().replace("-", "");

    let admin = fluvio::FluvioAdmin::connect().await?;
    let _ = admin
        .create(
            fluvio_topic.clone(),
            false,
            TopicSpec::Computed(TopicReplicaParam::new(1, 1, false)),
        )
        .await;
    let config = PgConnectorOpt {
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
    let mut connector = PgConnector::new(config)
        .await
        .expect("PgConnector failed to initialize");
    let _stream = fluvio_future::task::spawn(async move {
        connector
            .process_stream()
            .await
            .expect("process stream failed");
    });
    let (pg_client, conn) = postgres_url
        .parse::<tokio_postgres::Config>()?
        .connect(NoTls)
        .await?;
    tokio::spawn(conn);
    let table_create =
        r#"CREATE TABLE IF NOT EXISTS foo(ID SERIAL PRIMARY KEY, NAME TEXT NOT NULL)"#;
    let _ = pg_client.execute(table_create, &[]).await?;
    let consumer = fluvio::consumer(fluvio_topic.clone(), 0).await?;
    let mut stream = consumer.stream(fluvio::Offset::beginning()).await?;

    for i in 1..10 {
        let query = "INSERT INTO foo (NAME) VALUES($1)";
        let name = format!("Fluvio - {}", i);
        let _ = pg_client.query(query, &[&name]).await?;
        let next = stream
            .next()
            .await
            .expect("Failed to get next fluvio steram")?;
        let next = next.value();
        let event: ReplicationEvent = serde_json::de::from_slice(next)?;
        fluvio_future::tracing::info!("FLUVIO EVENT: {:?}", event)
    }
    let table_drop = "DROP TABLE foo";
    let _ = pg_client.execute(table_drop, &[]).await?;
    let admin = fluvio::FluvioAdmin::connect().await?;
    let _ = admin.delete::<TopicSpec, String>(fluvio_topic).await;

    Ok(())
}
