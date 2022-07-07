use fluvio::metadata::topic::TopicSpec;
use fluvio_connectors_common::opt::CommonConnectorOpt;
use fluvio_model_postgres::ReplicationEvent;
use fluvio_model_postgres::{LogicalReplicationMessage, TupleData};
use postgres_source::{PgConnector, PgConnectorOpt};
use tokio_postgres::NoTls;
use tokio_stream::StreamExt;
use url::Url;

#[tokio::test]
async fn postgres_inserts() -> eyre::Result<()> {
    fluvio_future::subscriber::init_logger();
    let postgres_url = std::env::var("FLUVIO_PG_DATABASE_URL")
        .expect("No FLUVIO_PG_DATABASE_URL environment variable found");

    let fluvio_topic = "postgres".to_string();
    let slot = uuid::Uuid::new_v4().to_string().replace('-', "");
    let publication = uuid::Uuid::new_v4().to_string().replace('-', "");

    let admin = fluvio::FluvioAdmin::connect().await?;
    let _ = admin
        .create(
            fluvio_topic.clone(),
            false,
            TopicSpec::new_computed(1, 1, Some(false)),
        )
        .await;
    let config = PgConnectorOpt {
        url: Url::parse(&postgres_url).expect("Failed to parse connector url"),
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
    pg_client.execute(table_create, &[]).await?;
    let consumer = fluvio::consumer(fluvio_topic.clone(), 0).await?;
    let mut stream = consumer.stream(fluvio::Offset::beginning()).await?;
    let query = "INSERT INTO foo (NAME) VALUES($1)";
    let name = format!("Fluvio - {}", 1);
    pg_client.query(query, &[&name]).await?;
    let mut received_relation = false;
    let mut received_first_insert = false;
    for i in 0..8 {
        let next = stream
            .next()
            .await
            .expect("Failed to get next fluvio steram")?;
        let next = next.value();
        let event: ReplicationEvent = serde_json::de::from_slice(next)?;
        println!("EVENT: {:?} COUNT : {:?}", event, i);
        match event.message {
            LogicalReplicationMessage::Relation(_relation) => {
                received_relation = true;
            }
            LogicalReplicationMessage::Insert(insert) => {
                received_first_insert = true;
                let id = insert.tuple.0.get(0).expect("No first ID in tuple");
                let out_name = insert.tuple.0.get(1).expect("No first ID in tuple");
                match (id, out_name) {
                    (TupleData::Int4(id), TupleData::String(out_name)) => {
                        assert_eq!(
                            &name, out_name,
                            "insert name {} doesn't match fluvio stream name {}",
                            name, out_name
                        );
                        assert_eq!(id, &1, "insert id {} doesn't match expected {}", name, 1);
                    }
                    other => {
                        panic!("Unexpected tuple type {:?}", other);
                    }
                }
            }
            _other => {}
        }
    }
    assert!(
        received_relation,
        "Verifying that the fluvio stream has the table create"
    );
    assert!(
        received_first_insert,
        "Verifying that the fluvio stream has first insert"
    );

    for i in 2..10 {
        let query = "INSERT INTO foo (NAME) VALUES($1)";
        let name = format!("Fluvio - {}", i);
        let _ = pg_client.query(query, &[&name]).await?;
    }
    for i in 2..10 {
        let _next = stream
            .next()
            .await
            .expect("Failed to get next fluvio steram")?;
        let next = stream
            .next()
            .await
            .expect("Failed to get next fluvio steram")?;

        let next = next.value();
        let event: ReplicationEvent = serde_json::de::from_slice(next)?;
        println!("FLUVIO EVENT: {:?}", event);
        let name = format!("Fluvio - {}", i);
        match event.message {
            LogicalReplicationMessage::Insert(insert) => {
                let id = insert.tuple.0.get(0).expect("No first ID in tuple");
                let out_name = insert.tuple.0.get(1).expect("No first ID in tuple");
                match (id, out_name) {
                    (TupleData::Int4(id), TupleData::String(out_name)) => {
                        assert_eq!(
                            &name, out_name,
                            "insert name {} doesn't match fluvio stream name {}",
                            name, out_name
                        );
                        assert_eq!(id, &i, "insert id {} doesn't match expected {}", name, i);
                    }
                    other => {
                        panic!("Unexpected tuple type {:?}", other);
                    }
                }
            }
            other => {
                panic!("Received unexpected event {:?}", other);
            }
        }
        let _next = stream
            .next()
            .await
            .expect("Failed to get next fluvio steram")?;
    }
    let table_drop = "DROP TABLE foo";
    pg_client.execute(table_drop, &[]).await?;
    let admin = fluvio::FluvioAdmin::connect().await?;
    let _ = admin.delete::<TopicSpec, String>(fluvio_topic).await;

    Ok(())
}
